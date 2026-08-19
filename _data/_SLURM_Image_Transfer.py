#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 T T Luik
# Copyright (C) 2006-2014 University of Dundee. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
BIOMERO SLURM Image Transfer Script

This script provides comprehensive data export from OMERO to SLURM clusters
with support for multiple data formats and automatic cleanup of temporary
artifacts.

Key Features:
- Multi-format export: TIFF, OME-TIFF, ZARR
- Support for Images, Datasets, and Plates
- Automatic data transfer to SLURM cluster
- Intelligent compression and packaging
- Temporary file annotation cleanup after successful transfer
- Configurable rendering options (channels, Z-projection, time points)
- Robust error handling and logging

Data Export Process:
1. Render and save image data in selected format
2. Package data into zip archive (except single OME-TIFF files)
3. Transfer data to SLURM cluster via SSH
4. Unpack data on SLURM for processing
5. Create temporary file annotation in OMERO
6. Clean up annotation after successful transfer

Supported Formats:
- TIFF: Rendered image planes with configurable options
- OME-TIFF: Original pixel data preservation
- ZARR: Native OME-ZARR format using omero-cli-zarr

Authors: Torec Luik, William Moore, OME Team
Institutions: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

import shutil
import subprocess
import shlex
import json
import omero.scripts as scripts
from omero.gateway import BlitzGateway
import omero.util.script_utils as script_utils
import omero
from omero.rtypes import rstring, rlong, robject
from omero.constants.namespaces import NSCREATED, NSOMETIFF
import os
from pathlib import Path
import glob
import zipfile
from datetime import datetime
try:
    from PIL import Image  # see ticket:2597
except ImportError:
    import Image
from biomero import SlurmClient, constants
from biomero.zarr_contracts import (
    CANONICAL_PLATE_IMAGE_NAMESPACE,
    CANONICAL_PLATE_LABEL_NAMESPACE,
    CANONICAL_PLATE_SOURCE_NAMESPACE,
    CANONICAL_SOURCE_NAMESPACE,
    SHALLOW_COLLECTION_NAMESPACE,
    CanonicalInput,
    CanonicalPlateImage,
    CanonicalPlateImageRecord,
    CanonicalPlateIndex,
    CanonicalPlateLabelRecord,
    CanonicalPlateSource,
    CanonicalZarrSource,
    ManagedZarrNode,
    ShallowZarrReference,
    ZarrLabelComponent,
)
import logging
import sys

logger = logging.getLogger(__name__)

IMPORTER_ENABLED = os.getenv("IMPORTER_ENABLED", "false").lower() == "true"
SHALLOW_ZARR_ENABLED = (
    os.getenv("BIOMERO_SHALLOW_ZARR", "false").lower() == "true"
)
SHALLOW_ZARR_SUPPORT_AVAILABLE = True
if IMPORTER_ENABLED and SHALLOW_ZARR_ENABLED:
    try:
        from biomero_importer.utils.canonical_promotion import (
            CanonicalPromotionService,
        )
        from biomero_importer.utils.canonical_store import CanonicalStore
        from biomero_importer.utils.pixel_identity import (
            IsccBioIdentityProvider,
            pixel_identities_match,
            read_zarr_v2_semantic_guard,
        )
        from biomero_importer.utils.result_zarr import (
            discover_ngff_nodes,
            materialize_shallow_zarr,
        )
    except ImportError as exc:
        SHALLOW_ZARR_SUPPORT_AVAILABLE = False
        logger.warning(
            "BIOMERO_SHALLOW_ZARR is enabled but biomero-importer Zarr "
            "support is unavailable; using normal Zarr export: %s",
            exc,
        )

# Version constant for easy version management
VERSION = "2.8.1"
BIOMERO_CONFIG_FILE = os.getenv(
    "OMERO_BIOMERO_CONFIG_FILE",
    os.getenv("BIOMERO_CONFIG_FILE", "/opt/omero/server/biomero-config.json"),
)
GROUP_MAPPINGS_FILE = os.getenv(
    "OMERO_BIOMERO_GROUP_MAPPINGS_FILE",
    "/opt/omero/server/group-mappings.json",
)
IMPORT_MOUNT_PATH = os.getenv("IMPORT_MOUNT_PATH", "/data")
IMPORT_MOUNT_STORAGE_ROOT = "import-mount-data"
CANONICAL_INPUTS_OUTPUT = "Canonical_Inputs"

# keep track of log strings.
log_strings = []


def is_shallow_zarr_storage_enabled(export_format):
    """Keep importer-owned storage behavior off the legacy result route."""
    return (
        IMPORTER_ENABLED
        and SHALLOW_ZARR_ENABLED
        and SHALLOW_ZARR_SUPPORT_AVAILABLE
        and export_format == constants.transfer.FORMAT_OMEZARR
    )


def _annotation_namespace(annotation):
    """Return a MapAnnotation namespace without relying on wrapper ordering."""
    namespace = annotation.getNs() if hasattr(annotation, "getNs") else None
    if hasattr(namespace, "getValue"):
        namespace = namespace.getValue()
    return getattr(namespace, "val", namespace)


def _annotation_values(annotation):
    """Normalize gateway or model MapAnnotation entries to string values."""
    values = {}
    if hasattr(annotation, "getMapValue"):
        entries = annotation.getMapValue()
    elif hasattr(annotation, "getValue"):
        entries = annotation.getValue()
    else:
        entries = ()
    for entry in entries or []:
        if hasattr(entry, "name"):
            name, value = entry.name, entry.value
        else:
            name, value = entry
        if hasattr(name, "getValue"):
            name = name.getValue()
        if hasattr(value, "getValue"):
            value = value.getValue()
        values[str(getattr(name, "val", name))] = str(
            getattr(value, "val", value)
        )
    return values


def get_shallow_reference(obj):
    """Resolve one deterministic managed shallow reference from an OMERO Image."""
    candidates = []
    for annotation in obj.listAnnotations():
        if _annotation_namespace(annotation) != SHALLOW_COLLECTION_NAMESPACE:
            continue
        try:
            candidates.append(ShallowZarrReference.from_annotation_values(
                _annotation_values(annotation)
            ))
        except Exception as exc:
            logger.warning(
                "Ignoring invalid shallow Zarr annotation on Image %s: %s",
                obj.getId(),
                exc,
            )
    distinct = {
        json.dumps(candidate.to_dict(), sort_keys=True)
        for candidate in candidates
    }
    if len(distinct) > 1:
        raise ValueError(
            f"Shallow Zarr metadata is ambiguous for Image {obj.getId()}"
        )
    if not candidates:
        return None
    reference = candidates[0]
    logger.info(
        "Located shallow Zarr collection for Image %s: storage=%s:%s, "
        "image=%s, labels=%s",
        obj.getId(),
        reference.storage_root,
        reference.relative_path,
        reference.image_node_path,
        list(reference.label_node_paths),
    )
    return reference


def get_canonical_source(obj, object_type):
    """Resolve one deterministic canonical-source record from OMERO metadata."""
    object_id = int(obj.getId())
    candidates = []
    for annotation in obj.listAnnotations():
        if _annotation_namespace(annotation) != CANONICAL_SOURCE_NAMESPACE:
            continue
        try:
            candidate = CanonicalZarrSource.from_annotation_values(
                _annotation_values(annotation)
            )
        except Exception as exc:
            logger.warning(
                "Ignoring invalid canonical Zarr annotation on %s %s: %s",
                object_type, object_id, exc,
            )
            continue
        if (
            candidate.source_object_type != object_type
            or candidate.source_object_id != object_id
        ):
            logger.warning(
                "Ignoring canonical Zarr annotation owned by %s %s on %s %s",
                candidate.source_object_type,
                candidate.source_object_id,
                object_type,
                object_id,
            )
            continue
        candidates.append(candidate)

    if not candidates:
        logger.info(
            "No BIOMERO canonical Zarr record found for %s %s",
            object_type,
            object_id,
        )
        return None
    current_generation = max(
        candidate.source_generation for candidate in candidates
    )
    current = [
        candidate for candidate in candidates
        if candidate.source_generation == current_generation
    ]
    distinct = {
        json.dumps(candidate.to_dict(), sort_keys=True)
        for candidate in current
    }
    if len(distinct) != 1:
        raise ValueError(
            f"Canonical source metadata is ambiguous for {object_type} "
            f"{object_id} generation {current_generation}"
        )
    source = current[0]
    logger.info(
        "Located canonical Zarr for %s %s: generation=%s, "
        "storage=%s:%s, node=%s, pixel ISCC=%s",
        object_type,
        object_id,
        source.source_generation,
        source.storage_root,
        source.relative_path,
        source.node_path,
        source.pixel_identity.iscc_code,
    )
    return source


def get_canonical_plate_source(plate):
    """Resolve old monolithic or new bounded canonical Plate annotations."""
    plate_id = int(plate.getId())
    candidates = []
    indexes = []
    image_records = []
    label_records = []
    for annotation in plate.listAnnotations():
        namespace = _annotation_namespace(annotation)
        if namespace not in {
            CANONICAL_PLATE_SOURCE_NAMESPACE,
            CANONICAL_PLATE_IMAGE_NAMESPACE,
            CANONICAL_PLATE_LABEL_NAMESPACE,
        }:
            continue
        values = _annotation_values(annotation)
        try:
            if namespace == CANONICAL_PLATE_SOURCE_NAMESPACE:
                if "images" in values:
                    candidate = CanonicalPlateSource.from_annotation_values(values)
                    if candidate.source_object_id == plate_id:
                        candidates.append(candidate)
                else:
                    index = CanonicalPlateIndex.from_annotation_values(values)
                    if index.source_object_id == plate_id:
                        indexes.append(index)
            elif namespace == CANONICAL_PLATE_IMAGE_NAMESPACE:
                record = CanonicalPlateImageRecord.from_annotation_values(values)
                if record.source_object_id == plate_id:
                    image_records.append(record)
            elif namespace == CANONICAL_PLATE_LABEL_NAMESPACE:
                record = CanonicalPlateLabelRecord.from_annotation_values(values)
                if record.source_object_id == plate_id:
                    label_records.append(record)
        except Exception as exc:
            logger.warning(
                "Ignoring invalid canonical Plate Zarr annotation on Plate "
                "%s in namespace %s: %s",
                plate_id,
                namespace,
                exc,
            )

    for index in indexes:
        generation_images = [
            record for record in image_records
            if record.source_generation == index.source_generation
        ]
        generation_labels = [
            record for record in label_records
            if record.source_generation == index.source_generation
        ]
        images_by_path = {}
        ambiguous = False
        for record in generation_images:
            path = record.image.image_node_path
            previous = images_by_path.get(path)
            if previous is not None and previous != record.image:
                ambiguous = True
                break
            images_by_path[path] = record.image
        labels_by_image = {}
        label_keys = set()
        for record in generation_labels:
            key = (record.image_node_path, record.label.logical_node_path)
            if key in label_keys:
                previous = next(
                    item.label for item in generation_labels
                    if (
                        item.image_node_path,
                        item.label.logical_node_path,
                    ) == key
                )
                if previous != record.label:
                    ambiguous = True
                    break
                continue
            label_keys.add(key)
            labels_by_image.setdefault(record.image_node_path, []).append(
                record.label
            )
        if ambiguous:
            logger.warning(
                "Ignoring ambiguous split canonical Plate generation %s on "
                "Plate %s",
                index.source_generation,
                plate_id,
            )
            continue
        if (
            len(images_by_path) != index.image_count
            or len(label_keys) != index.label_count
            or any(path not in images_by_path for path in labels_by_image)
        ):
            logger.warning(
                "Ignoring incomplete split canonical Plate generation %s on "
                "Plate %s: expected %s image/%s label records, found %s/%s",
                index.source_generation,
                plate_id,
                index.image_count,
                index.label_count,
                len(images_by_path),
                len(label_keys),
            )
            continue
        images = tuple(
            images_by_path[path].model_copy(update={
                "labels": tuple(sorted(
                    labels_by_image.get(path, ()),
                    key=lambda item: item.logical_node_path,
                )),
            })
            for path in sorted(images_by_path)
        )
        try:
            candidates.append(CanonicalPlateSource(
                storage_root=index.storage_root,
                relative_path=index.relative_path,
                source_object_id=index.source_object_id,
                source_generation=index.source_generation,
                interchange_profile=index.interchange_profile,
                images=images,
                store_identity=index.store_identity,
            ))
        except Exception as exc:
            logger.warning(
                "Ignoring inconsistent split canonical Plate generation %s "
                "on Plate %s: %s",
                index.source_generation,
                plate_id,
                exc,
            )
    if not candidates:
        logger.info("No BIOMERO canonical Zarr record found for Plate %s", plate_id)
        return None
    generation = max(item.source_generation for item in candidates)
    current = [
        item for item in candidates if item.source_generation == generation
    ]
    distinct = {
        json.dumps(item.to_dict(), sort_keys=True) for item in current
    }
    if len(distinct) != 1:
        raise ValueError(
            f"Canonical Plate metadata is ambiguous for Plate {plate_id} "
            f"generation {generation}"
        )
    source = current[0]
    logger.info(
        "Located canonical Zarr for Plate %s: generation=%s, storage=%s:%s, "
        "images=%s (cached ISCC-BIO identities; not recalculated)",
        plate_id,
        source.source_generation,
        source.storage_root,
        source.relative_path,
        len(source.images),
    )
    return source


def discover_canonical_inputs(objects, object_type):
    """Return reusable sources and an all-or-nothing ordered input snapshot."""
    sources = {}
    canonical_inputs = []
    complete = True
    for ordinal, obj in enumerate(objects):
        object_id = int(obj.getId())
        source = (
            get_canonical_plate_source(obj)
            if object_type == "Plate"
            else get_canonical_source(obj, object_type)
        )
        if source is None:
            complete = False
            continue
        sources[object_id] = source
        values = {
            "ordinal": ordinal,
            "selected_object_type": object_type,
            "selected_object_id": object_id,
        }
        values["plate_source" if object_type == "Plate" else "source"] = source
        canonical_inputs.append(CanonicalInput(**values))
    if complete:
        logger.info(
            "Canonical discovery covered all %s selected %s object(s)",
            len(canonical_inputs),
            object_type,
        )
    else:
        logger.info(
            "Canonical discovery covered %s/%s selected %s object(s); "
            "missing sources may be established during export",
            len(canonical_inputs),
            len(objects),
            object_type,
        )
    return sources, tuple(canonical_inputs) if complete else ()


def discover_canonical_label_components(
    source,
    storage_roots,
    identity_provider=None,
):
    """Inventory and hash labels already present in one managed image Zarr."""
    if source.source_object_type != "Image":
        return ()
    root = resolve_managed_source_path(source, storage_roots)
    if root is None:
        raise ValueError("Managed canonical Zarr is unavailable")
    provider = identity_provider or IsccBioIdentityProvider()
    components = []
    for node in discover_ngff_nodes(root):
        if (
            node.role != "label"
            or node.parent_image_node_path != source.node_path
        ):
            continue
        guard = read_zarr_v2_semantic_guard(root, node.node_path)
        identity = provider.generate(
            root,
            node_path=node.node_path,
            role="label",
            shape=guard.shape,
            dtype=guard.dtype,
            axes=guard.axes,
            coordinate_transformations=guard.coordinate_transformations,
        )
        components.append(ZarrLabelComponent(
            logical_node_path=node.node_path,
            pixel_identity=identity,
            source=ManagedZarrNode(
                storage_root=source.storage_root,
                relative_path=source.relative_path,
                node_path=node.node_path,
            ),
        ))
        logger.info(
            "Indexed canonical label for %s %s: node=%s, pixel ISCC=%s",
            source.source_object_type,
            source.source_object_id,
            node.node_path,
            identity.iscc_code,
        )
    return tuple(components)


def build_canonical_plate_source(
    plate,
    zarr_path,
    storage_root_id,
    relative_path,
    *,
    source_generation=1,
    identity_provider=None,
):
    """Hash every declared Plate image and label node into one cache record."""
    root = Path(zarr_path)
    nodes = discover_ngff_nodes(root)
    image_nodes = [node for node in nodes if node.role == "image"]
    if not image_nodes:
        raise ValueError(f"Canonical Plate Zarr has no image nodes: {root}")
    provider = identity_provider or IsccBioIdentityProvider()
    identities = {}
    for node in nodes:
        guard = read_zarr_v2_semantic_guard(root, node.node_path)
        logger.info(
            "Calculating ISCC-BIO pixel identity for Plate %s %s node %s",
            plate.getId(),
            node.role,
            node.node_path,
        )
        identity = provider.generate(
            root,
            node_path=node.node_path,
            role=node.role,
            shape=guard.shape,
            dtype=guard.dtype,
            axes=guard.axes,
            coordinate_transformations=guard.coordinate_transformations,
        )
        identities[node.node_path] = identity
        logger.info(
            "Calculated Plate %s %s identity: node=%s, ISCC=%s, "
            "Data-Code=%s, Instance-Code=%s",
            plate.getId(),
            node.role,
            node.node_path,
            identity.iscc_code,
            identity.data_code,
            identity.instance_code,
        )

    relative_path = Path(relative_path).as_posix()
    images = []
    for image_node in image_nodes:
        source = CanonicalZarrSource(
            storage_root=storage_root_id,
            relative_path=relative_path,
            node_path=image_node.node_path,
            source_object_type="Plate",
            source_object_id=int(plate.getId()),
            source_generation=source_generation,
            interchange_profile="ngff-0.4-zarr-v2",
            pixel_identity=identities[image_node.node_path],
            pixel_identity_origin="canonical-bootstrap",
            canonical_pixel_verified=False,
        )
        labels = tuple(
            ZarrLabelComponent(
                logical_node_path=node.node_path,
                pixel_identity=identities[node.node_path],
                source=ManagedZarrNode(
                    storage_root=storage_root_id,
                    relative_path=relative_path,
                    node_path=node.node_path,
                ),
            )
            for node in nodes
            if (
                node.role == "label"
                and node.parent_image_node_path == image_node.node_path
            )
        )
        images.append(CanonicalPlateImage(
            image_node_path=image_node.node_path,
            source=source,
            labels=labels,
        ))
    return CanonicalPlateSource(
        storage_root=storage_root_id,
        relative_path=relative_path,
        source_object_id=int(plate.getId()),
        source_generation=source_generation,
        interchange_profile="ngff-0.4-zarr-v2",
        images=tuple(images),
    )


def canonical_inputs_from_sources(
    objects,
    object_type,
    sources,
    transfer_artifacts=None,
    storage_roots=None,
    label_components_by_object=None,
):
    """Build an ordered all-or-nothing snapshot after export-side promotion."""
    transfer_artifacts = transfer_artifacts or {}
    label_components_by_object = label_components_by_object or {}
    inputs = []
    missing_ids = []
    for ordinal, obj in enumerate(objects):
        object_id = int(obj.getId())
        source = sources.get(object_id)
        transfer_artifact = transfer_artifacts.get(object_id)
        if source is None or transfer_artifact is None:
            missing_ids.append(object_id)
            continue
        labels = label_components_by_object.get(object_id)
        if (
            object_type == "Image"
            and labels is None
            and storage_roots is not None
        ):
            try:
                labels = discover_canonical_label_components(
                    source,
                    storage_roots,
                )
            except Exception as exc:
                logger.warning(
                    "Canonical label inventory failed for %s %s; disabling "
                    "shallow result matching for this selection: %s",
                    object_type,
                    object_id,
                    exc,
                    exc_info=True,
                )
                missing_ids.append(object_id)
                continue
        if labels is None:
            labels = ()
        values = {
            "ordinal": ordinal,
            "selected_object_type": object_type,
            "selected_object_id": object_id,
            "transfer_artifact": transfer_artifact,
        }
        if object_type == "Plate":
            values["plate_source"] = source
        else:
            values["source"] = source
            values["labels"] = labels
        inputs.append(CanonicalInput(**values))
    if missing_ids:
        logger.info(
            "No complete canonical input snapshot for %s selection: "
            "canonical records or transfer artifact names are missing for "
            "IDs %s",
            object_type,
            missing_ids,
        )
        return ()
    logger.info(
        "Prepared canonical input snapshot for all %s selected %s object(s)",
        len(inputs),
        object_type,
    )
    return tuple(inputs)


def load_group_storage_roots(
    config_file=None,
    group_mappings_file=None,
    import_mount_path=None,
):
    """Derive group storage roots from runtime mappings and the import mount."""
    config_path = Path(config_file or BIOMERO_CONFIG_FILE)
    mappings_path = Path(group_mappings_file or GROUP_MAPPINGS_FILE)
    import_root = Path(import_mount_path or IMPORT_MOUNT_PATH)
    if not import_root.is_absolute():
        raise ValueError("IMPORT_MOUNT_PATH must be an absolute path")
    import_root = import_root.resolve()

    def load_json_object(path):
        if not path.is_file():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"Could not load BIOMERO group mappings from {path}: {exc}"
            ) from exc
        if not isinstance(value, dict):
            raise ValueError(f"BIOMERO group mappings in {path} must be an object")
        return value

    legacy_config = load_json_object(config_path)
    legacy_mappings = legacy_config.get("group_mappings", {})
    if not isinstance(legacy_mappings, dict):
        raise ValueError("biomero-config.json group_mappings must be an object")
    dedicated_mappings = load_json_object(mappings_path)
    group_mappings = dict(legacy_mappings)
    group_mappings.update(dedicated_mappings)

    # The mount-wide root lets BIOMERO index an existing managed Zarr in place
    # even when runtime group mappings have changed since it was imported.
    storage_roots = {IMPORT_MOUNT_STORAGE_ROOT: import_root}
    for group_id, mapping in group_mappings.items():
        try:
            normalized_group_id = int(group_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid OMERO group ID in group mappings: {group_id!r}"
            ) from exc
        if normalized_group_id < 0 or not isinstance(mapping, dict):
            raise ValueError(
                f"Invalid BIOMERO group mapping for OMERO group {group_id}"
            )
        folder = mapping.get("folder")
        if not folder or folder in {".", "root"}:
            storage_root = import_root
        else:
            folder_path = Path(str(folder))
            if folder_path.is_absolute() or ".." in folder_path.parts:
                raise ValueError(
                    f"Group {group_id} folder must stay within IMPORT_MOUNT_PATH"
                )
            storage_root = (import_root / folder_path).resolve()
            try:
                storage_root.relative_to(import_root)
            except ValueError as exc:
                raise ValueError(
                    f"Group {group_id} folder must stay within IMPORT_MOUNT_PATH"
                ) from exc
        storage_roots[f"group-{normalized_group_id}-data"] = storage_root

    logger.info(
        "Derived %s managed storage root(s) from BIOMERO group mappings "
        "beneath IMPORT_MOUNT_PATH=%s",
        len(storage_roots),
        import_root,
    )
    return storage_roots


def locate_managed_zarr(zarr_path, storage_roots):
    """Return the most specific managed root and relative path for a Zarr."""
    resolved_path = Path(zarr_path).resolve()
    candidates = []
    for storage_id, root in storage_roots.items():
        resolved_root = Path(root).resolve()
        try:
            relative_path = resolved_path.relative_to(resolved_root)
        except ValueError:
            continue
        candidates.append((
            len(resolved_root.parts),
            storage_id.startswith("group-"),
            storage_id,
            resolved_root,
            relative_path,
        ))
    if not candidates:
        raise ValueError(
            f"Existing Zarr is outside BIOMERO managed storage: {resolved_path}"
        )
    _, _, storage_id, storage_root, relative_path = max(candidates)
    return storage_id, storage_root, relative_path


def select_object_storage_root(obj, storage_roots):
    """Select the configured managed root belonging to the object's OMERO group."""
    try:
        group_id = int(obj.getDetails().getGroup().getId())
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot determine the OMERO group for object {obj.getId()}"
        ) from exc
    storage_id = f"group-{group_id}-data"
    storage_root = storage_roots.get(storage_id)
    if storage_root is None:
        raise ValueError(
            f"No canonical storage root is configured for OMERO group {group_id}"
        )
    return storage_id, Path(storage_root).resolve()


def derive_canonical_source_directory(obj, storage_root):
    """Derive a managed source directory from ordered legacy provenance."""
    storage_root = Path(storage_root).resolve()
    candidates = {"Imported_from": set(), "Filepath": set()}
    for annotation in obj.listAnnotations():
        if not hasattr(annotation, "getMapValue"):
            continue
        for key, value in _annotation_values(annotation).items():
            if key not in candidates:
                continue
            path = Path(value)
            if not path.is_absolute():
                continue
            resolved = path.resolve()
            directory = resolved.parent if resolved.suffix else resolved
            try:
                relative = directory.relative_to(storage_root)
            except ValueError:
                continue
            candidates[key].add(relative.as_posix())

    for key in ("Imported_from", "Filepath"):
        if len(candidates[key]) > 1:
            raise ValueError(
                f"Managed {key} provenance is ambiguous for object {obj.getId()}"
            )
        if candidates[key]:
            return Path(next(iter(candidates[key])))
    return Path(".")


def attach_canonical_source(
    conn,
    obj,
    object_type,
    source,
    annotation_writer=None,
):
    """Attach one canonical record without creating same-generation ambiguity."""
    existing = get_canonical_source(obj, object_type)
    if existing == source:
        return None
    if (
        existing is not None
        and existing.source_generation >= source.source_generation
    ):
        raise ValueError(
            f"Cannot replace canonical {object_type} {obj.getId()} generation "
            f"{existing.source_generation} with generation "
            f"{source.source_generation}"
        )
    if annotation_writer is None:
        from ezomero import post_map_annotation

        annotation_writer = post_map_annotation
    return annotation_writer(
        conn=conn,
        object_type=object_type,
        object_id=int(obj.getId()),
        kv_dict=source.to_annotation_values(),
        ns=CANONICAL_SOURCE_NAMESPACE,
        across_groups=False,
    )


def attach_canonical_plate_source(
    conn,
    plate,
    source,
    annotation_writer=None,
):
    """Attach bounded node records, then the compact Plate index as commit."""
    existing = get_canonical_plate_source(plate)
    if existing == source:
        return None
    if (
        existing is not None
        and existing.source_generation >= source.source_generation
    ):
        raise ValueError(
            f"Cannot replace canonical Plate {plate.getId()} generation "
            f"{existing.source_generation} with generation "
            f"{source.source_generation}"
        )
    if annotation_writer is None:
        from ezomero import post_map_annotation

        annotation_writer = post_map_annotation
    writes = []
    for image in source.images:
        image_record = CanonicalPlateImageRecord(
            source_object_id=source.source_object_id,
            source_generation=source.source_generation,
            image=image.model_copy(update={"labels": ()}),
        )
        writes.append(annotation_writer(
            conn=conn,
            object_type="Plate",
            object_id=int(plate.getId()),
            kv_dict=image_record.to_annotation_values(),
            ns=CANONICAL_PLATE_IMAGE_NAMESPACE,
            across_groups=False,
        ))
        for label in image.labels:
            label_record = CanonicalPlateLabelRecord(
                source_object_id=source.source_object_id,
                source_generation=source.source_generation,
                image_node_path=image.image_node_path,
                label=label,
            )
            writes.append(annotation_writer(
                conn=conn,
                object_type="Plate",
                object_id=int(plate.getId()),
                kv_dict=label_record.to_annotation_values(),
                ns=CANONICAL_PLATE_LABEL_NAMESPACE,
                across_groups=False,
            ))
    index = CanonicalPlateIndex.from_source(source)
    writes.append(annotation_writer(
        conn=conn,
        object_type="Plate",
        object_id=int(plate.getId()),
        kv_dict=index.to_annotation_values(),
        ns=CANONICAL_PLATE_SOURCE_NAMESPACE,
        across_groups=False,
    ))
    return tuple(writes)


def validate_omero_image_semantics(image, guard):
    """Require exported NGFF dimensions and dtype to match the OMERO Image."""
    sizes = {
        "t": int(image.getSizeT()),
        "c": int(image.getSizeC()),
        "z": int(image.getSizeZ()),
        "y": int(image.getSizeY()),
        "x": int(image.getSizeX()),
    }
    try:
        expected_shape = tuple(sizes[axis.lower()] for axis in guard.axes)
    except KeyError as exc:
        raise ValueError(
            f"Unsupported NGFF axis for OMERO Image {image.getId()}: {exc.args[0]}"
        ) from exc
    if expected_shape != tuple(guard.shape):
        raise ValueError(
            f"Exported NGFF shape {guard.shape} does not match OMERO Image "
            f"{image.getId()} shape {expected_shape}"
        )

    pixels_type = image.getPrimaryPixels().getPixelsType().getValue()
    pixels_type = str(getattr(pixels_type, "val", pixels_type)).lower()
    dtype_names = {
        "bit": "bool",
        "uint8": "uint8",
        "int8": "int8",
        "uint16": "uint16",
        "int16": "int16",
        "uint32": "uint32",
        "int32": "int32",
        "float": "float32",
        "double": "float64",
        "complex": "complex64",
        "double-complex": "complex128",
    }
    expected_dtype = dtype_names.get(pixels_type)
    if expected_dtype is None or expected_dtype != guard.dtype:
        raise ValueError(
            f"Exported NGFF pixel type {guard.dtype} does not match OMERO "
            f"Image {image.getId()} pixel type {pixels_type}"
        )


def promote_exported_image_zarr(
    conn,
    image,
    export_path,
    storage_roots,
    *,
    source_generation=1,
    identity_provider=None,
    semantic_guard_reader=None,
    promotion_service_factory=None,
    annotation_writer=None,
):
    """Verify a fresh Image export, commit it, annotate it, and restore a task copy."""
    storage_id, storage_root = select_object_storage_root(
        image, storage_roots)
    source_directory = derive_canonical_source_directory(image, storage_root)
    semantic_guard_reader = (
        semantic_guard_reader or read_zarr_v2_semantic_guard
    )
    guard = semantic_guard_reader(export_path, ".")
    validate_omero_image_semantics(image, guard)
    logger.info(
        "Validated exported NGFF semantics for Image %s: axes=%s, "
        "shape=%s, dtype=%s",
        image.getId(),
        tuple(guard.axes),
        tuple(guard.shape),
        guard.dtype,
    )
    guard_values = {
        "node_path": ".",
        "role": "image",
        "shape": guard.shape,
        "dtype": guard.dtype,
        "axes": guard.axes,
        "coordinate_transformations": guard.coordinate_transformations,
    }
    identity_provider = identity_provider or IsccBioIdentityProvider()
    logger.info(
        "Calculating ISCC-BIO pixel identity from OMERO Pixels for Image %s",
        image.getId(),
    )
    original_identity = identity_provider.generate_omero(
        conn,
        image_id=int(image.getId()),
        **guard_values,
    )
    logger.info(
        "Calculated OMERO pixel identity for Image %s: ISCC=%s, "
        "Data-Code=%s, Instance-Code=%s",
        image.getId(),
        original_identity.iscc_code,
        original_identity.data_code,
        original_identity.instance_code,
    )
    logger.info(
        "Calculating ISCC-BIO pixel identity from exported Zarr for Image %s: %s",
        image.getId(),
        export_path,
    )
    exported_identity = identity_provider.generate(
        Path(export_path),
        **guard_values,
    )
    logger.info(
        "Calculated exported Zarr pixel identity for Image %s: ISCC=%s, "
        "Data-Code=%s, Instance-Code=%s",
        image.getId(),
        exported_identity.iscc_code,
        exported_identity.data_code,
        exported_identity.instance_code,
    )

    promotion_service_factory = (
        promotion_service_factory or CanonicalPromotionService
    )
    promotion = promotion_service_factory(
        storage_root_id=storage_id,
        storage_root=storage_root,
    )
    result = promotion.promote(
        export_path,
        source_directory=source_directory,
        source_object_type="Image",
        source_object_id=int(image.getId()),
        source_generation=source_generation,
        node_path=".",
        original_identity=original_identity,
        exported_identity=exported_identity,
        pixel_identity_origin="omero-pixels",
    )
    logger.info(
        "ISCC-BIO verification matched OMERO Pixels and exported Zarr for "
        "Image %s; committed canonical generation %s at %s",
        image.getId(),
        source_generation,
        result.path,
    )
    attach_canonical_source(
        conn,
        image,
        "Image",
        result.source,
        annotation_writer=annotation_writer,
    )

    export_path = Path(export_path)
    if not export_path.exists():
        shutil.copytree(
            result.path,
            export_path,
            ignore=shutil.ignore_patterns(".biomero-canonical.json"),
        )
    log(
        " Verified canonical Image %s generation %s at %s"
        % (image.getId(), source_generation, result.path)
    )
    return result.source


def index_existing_image_zarr(
    conn,
    image,
    existing_path,
    storage_roots,
    *,
    source_generation=1,
    identity_provider=None,
    semantic_guard_reader=None,
    promotion_service_factory=None,
    annotation_writer=None,
):
    """Verify and index an existing managed Zarr without copying its pixels."""
    storage_id, storage_root, relative_path = locate_managed_zarr(
        existing_path,
        storage_roots,
    )
    semantic_guard_reader = (
        semantic_guard_reader or read_zarr_v2_semantic_guard
    )
    guard = semantic_guard_reader(existing_path, ".")
    validate_omero_image_semantics(image, guard)
    guard_values = {
        "node_path": ".",
        "role": "image",
        "shape": guard.shape,
        "dtype": guard.dtype,
        "axes": guard.axes,
        "coordinate_transformations": guard.coordinate_transformations,
    }
    identity_provider = identity_provider or IsccBioIdentityProvider()
    logger.info(
        "Calculating ISCC-BIO pixel identity from OMERO Pixels for existing "
        "Zarr Image %s",
        image.getId(),
    )
    original_identity = identity_provider.generate_omero(
        conn,
        image_id=int(image.getId()),
        **guard_values,
    )
    logger.info(
        "Calculated OMERO pixel identity for Image %s: ISCC=%s, "
        "Data-Code=%s, Instance-Code=%s",
        image.getId(),
        original_identity.iscc_code,
        original_identity.data_code,
        original_identity.instance_code,
    )
    logger.info(
        "Calculating ISCC-BIO pixel identity from existing managed Zarr for "
        "Image %s: %s",
        image.getId(),
        existing_path,
    )
    existing_identity = identity_provider.generate(
        Path(existing_path),
        **guard_values,
    )
    logger.info(
        "Calculated existing Zarr pixel identity for Image %s: ISCC=%s, "
        "Data-Code=%s, Instance-Code=%s",
        image.getId(),
        existing_identity.iscc_code,
        existing_identity.data_code,
        existing_identity.instance_code,
    )
    if not pixel_identities_match(original_identity, existing_identity):
        raise ValueError(
            f"Existing managed Zarr pixels do not match OMERO Image "
            f"{image.getId()}"
        )

    promotion_service_factory = (
        promotion_service_factory or CanonicalPromotionService
    )
    indexing = promotion_service_factory(
        storage_root_id=storage_id,
        storage_root=storage_root,
    )
    result = indexing.index_existing(
        existing_path,
        relative_path=relative_path,
        source_object_type="Image",
        source_object_id=int(image.getId()),
        source_generation=source_generation,
        node_path=".",
        original_identity=original_identity,
        existing_identity=existing_identity,
        pixel_identity_origin="omero-pixels",
    )
    attach_canonical_source(
        conn,
        image,
        "Image",
        result.source,
        annotation_writer=annotation_writer,
    )
    logger.info(
        "ISCC-BIO verification matched OMERO Pixels and existing Zarr for "
        "Image %s; indexed generation %s in place at %s:%s",
        image.getId(),
        source_generation,
        storage_id,
        relative_path.as_posix(),
    )
    return result.source


def index_existing_plate_zarr(
    conn,
    plate,
    existing_path,
    storage_roots,
    *,
    source_generation=1,
    identity_provider=None,
    annotation_writer=None,
):
    """Index a managed Plate Zarr in place using per-image identities."""
    storage_id, _storage_root, relative_path = locate_managed_zarr(
        existing_path,
        storage_roots,
    )
    source = build_canonical_plate_source(
        plate,
        existing_path,
        storage_id,
        relative_path,
        source_generation=source_generation,
        identity_provider=identity_provider,
    )
    attach_canonical_plate_source(
        conn,
        plate,
        source,
        annotation_writer=annotation_writer,
    )
    logger.info(
        "Indexed canonical Plate %s generation %s in place at %s:%s with "
        "%s image node(s)",
        plate.getId(),
        source_generation,
        storage_id,
        relative_path.as_posix(),
        len(source.images),
    )
    log(
        " Indexed canonical Plate %s with %s image-level ISCC identities"
        % (plate.getId(), len(source.images))
    )
    return source


def promote_exported_plate_zarr(
    conn,
    plate,
    export_path,
    storage_roots,
    *,
    source_generation=1,
    identity_provider=None,
    canonical_store_factory=None,
    annotation_writer=None,
):
    """Atomically retain a fresh Plate export as its reusable canonical Zarr."""
    storage_id, storage_root = select_object_storage_root(plate, storage_roots)
    source_directory = derive_canonical_source_directory(plate, storage_root)
    store_factory = canonical_store_factory or CanonicalStore
    store = store_factory(storage_root)
    relative_path = store.relative_path_for(
        source_directory,
        "Plate",
        int(plate.getId()),
        source_generation,
    )
    source = build_canonical_plate_source(
        plate,
        export_path,
        storage_id,
        relative_path,
        source_generation=source_generation,
        identity_provider=identity_provider,
    )
    committed = store.commit(export_path, source)
    attach_canonical_plate_source(
        conn,
        plate,
        source,
        annotation_writer=annotation_writer,
    )
    export_path = Path(export_path)
    if not export_path.exists():
        shutil.copytree(
            committed,
            export_path,
            ignore=shutil.ignore_patterns(".biomero-canonical.json"),
        )
    logger.info(
        "Promoted canonical Plate %s generation %s to %s with %s image "
        "node(s)",
        plate.getId(),
        source_generation,
        committed,
        len(source.images),
    )
    log(
        " Cached canonical Plate %s generation %s at %s"
        % (plate.getId(), source_generation, committed)
    )
    return source


def resolve_managed_source_path(source, storage_roots):
    """Resolve an existing canonical root without escaping its configured root."""
    root = storage_roots.get(source.storage_root)
    if root is None:
        return None
    root = Path(root).resolve()
    candidate = (root / Path(source.relative_path)).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"Canonical source escapes storage root {source.storage_root}"
        ) from exc
    if not candidate.is_dir():
        logger.warning("Canonical Zarr path is unavailable: %s", candidate)
        return None
    return candidate


def get_legacy_zarr_path(obj):
    """Resolve legacy Zarr annotations with explicit, stable precedence."""
    candidates = {"Imported_from": set(), "Filepath": set()}
    for annotation in obj.listAnnotations():
        if not hasattr(annotation, "getMapValue"):
            continue
        for key, value in _annotation_values(annotation).items():
            if key not in candidates:
                continue
            path = Path(value)
            if value.lower().endswith(".zarr") and path.is_dir():
                candidates[key].add(str(path.resolve()))
    for key in ("Imported_from", "Filepath"):
        if len(candidates[key]) > 1:
            raise ValueError(f"Legacy {key} Zarr metadata is ambiguous")
        if candidates[key]:
            return Path(next(iter(candidates[key])))
    return None


def select_zarr_source_path(obj, canonical_source, storage_roots):
    """Select a reusable root, never silently bypassing canonical metadata."""
    if canonical_source is not None:
        if (
            isinstance(canonical_source, CanonicalZarrSource)
            and canonical_source.node_path != "."
        ):
            logger.info(
                "Canonical source for %s %s uses nested node %s; exporting "
                "a standalone Zarr until node materialization is available",
                canonical_source.source_object_type,
                canonical_source.source_object_id,
                canonical_source.node_path,
            )
            return None
        source_path = resolve_managed_source_path(
            canonical_source, storage_roots)
        if source_path is None:
            source_type = (
                "Plate"
                if isinstance(canonical_source, CanonicalPlateSource)
                else canonical_source.source_object_type
            )
            logger.warning(
                "Canonical Zarr record for %s %s could not be resolved at "
                "%s:%s; a fresh export is required",
                source_type,
                canonical_source.source_object_id,
                canonical_source.storage_root,
                canonical_source.relative_path,
            )
            return None
        if isinstance(canonical_source, CanonicalPlateSource):
            logger.info(
                "Reusing canonical Zarr for Plate %s: generation=%s, path=%s, "
                "%s cached image ISCC identities (not recalculated)",
                canonical_source.source_object_id,
                canonical_source.source_generation,
                source_path,
                len(canonical_source.images),
            )
        else:
            logger.info(
                "Reusing canonical Zarr for %s %s: generation=%s, path=%s, "
                "pixel ISCC=%s (cached; ISCC-BIO was not recalculated)",
                canonical_source.source_object_type,
                canonical_source.source_object_id,
                canonical_source.source_generation,
                source_path,
                canonical_source.pixel_identity.iscc_code,
            )
        return source_path
    legacy_path = get_legacy_zarr_path(obj)
    if legacy_path is not None:
        logger.info(
            "Reusing legacy Zarr path for OMERO object %s: %s "
            "(not yet a BIOMERO canonical; no cached ISCC)",
            obj.getId(),
            legacy_path,
        )
    return legacy_path


def log(text):
    """Add text to log strings list for later compilation to file.
    
    Args:
        text: Text to add to log, handles unicode encoding.
    """
    # Handle unicode
    try:
        text = text.encode('utf8')
    except UnicodeEncodeError:
        pass
    log_strings.append(str(text))
    logger.debug(str(text))


def compress(target, base):
    """Create a ZIP archive recursively from a given base directory.
    
    Args:
        target (str): Name of the zip file to write (e.g., "folder.zip").
        base (str): Name of folder to zip up (e.g., "folder").
    """
    base_name, ext = target.rsplit(".", 1)
    shutil.make_archive(base_name, ext, base)


def save_plane(image, format, c_name, z_range, project_z, t=0,
               channel=None,
               greyscale=False, zoom_percent=None, folder_name=None):
    """Render and save an image plane to disk.
    
    Args:
        image: OMERO image wrapper to render.
        format (str): Image format to save as (PNG, TIFF, or JPG).
        c_name (str): Channel name for filename.
        z_range (tuple): Either (zIndex,) or (zStart, zStop) for projection.
        project_z (bool): Whether to use Z projection.
        t (int): Time point index. Defaults to 0.
        channel (int, optional): Active channel index. If None, uses current
            rendering settings.
        greyscale (bool): If True, render all visible channels as greyscale.
            Defaults to False.
        zoom_percent (int, optional): Resize image by this percentage.
        folder_name (str, optional): Directory to save the plane in.
    """

    original_name = image.getName()
    log("")
    log("save_plane..")
    log("channel: %s" % c_name)
    log("z: %s" % z_range)
    log("t: %s" % t)

    # if channel == None: use current rendering settings
    if channel is not None:
        image.setActiveChannels([channel+1])    # use 1-based Channel indices
        if greyscale:
            image.setGreyscaleRenderingModel()
        else:
            image.setColorRenderingModel()
    if project_z:
        # imageWrapper only supports projection of full Z range (can't
        # specify)
        image.setProjection('intmax')

    # All Z and T indices in this script are 1-based, but this method uses
    # 0-based.
    plane = image.renderImage(z_range[0]-1, t-1)
    if zoom_percent:
        w, h = plane.size
        fraction = (float(zoom_percent) / 100)
        plane = plane.resize((int(w * fraction), int(h * fraction)),
                             Image.ANTIALIAS)

    if format == "PNG":
        img_name = make_image_name(
            original_name, c_name, z_range, t, "png", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name, "PNG")
    elif format == constants.transfer.FORMAT_TIFF:
        img_name = make_image_name(
            original_name, c_name, z_range, t, "tiff", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name, constants.transfer.FORMAT_TIFF)
    else:
        img_name = make_image_name(
            original_name, c_name, z_range, t, "jpg", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name)


def make_image_name(original_name, c_name, z_range, t, extension, folder_name):
    """Generate filename for saved image with standardized naming convention.
    
    Creates descriptive filenames incorporating image metadata.
    Example: "imported/myImage.dv" → "myImage_DAPI_z13_t01.png"
    
    Args:
        original_name (str): Original image name from OMERO.
        c_name (str): Channel name.
        z_range (tuple): Z-slice range (single index or start-stop range).
        t (int): Time point index.
        extension (str): File extension for output format.
        folder_name (str, optional): Target folder path.
    
    Returns:
        str: Generated filename with full path if folder specified.
    """
    name = os.path.basename(original_name)
    # name = name.rsplit(".",1)[0]  # remove extension
    if len(z_range) == 2:
        z = "%02d-%02d" % (z_range[0], z_range[1])
    else:
        z = "%02d" % z_range[0]
    img_name = "%s_%s_z%s_t%02d.%s" % (name, c_name, z, t, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    name = img_name[:-(len(extension)+1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (name, i, extension)
        i += 1
    return img_name


def save_as_ome_tiff(conn, image, folder_name=None):
    """Save image as OME-TIFF preserving original pixel data.
    
    Args:
        conn: OMERO BlitzGateway connection.
        image: OMERO image wrapper to export.
        folder_name (str, optional): Target folder for the file.
    """

    extension = "ome.tif"
    name = os.path.basename(image.getName())
    img_name = "%s.%s" % (name, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    path_name = img_name[:-(len(extension)+1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (path_name, i, extension)
        i += 1

    log("  Saving file as: %s" % img_name)
    file_size, block_gen = image.exportOmeTiff(bufsize=65536)
    with open(str(img_name), "wb") as f:
        for piece in block_gen:
            f.write(piece)


def save_plate_as_zarr(
    conn,
    suuid,
    plate,
    folder_name=None,
    client=None,
    ome_zarr_version=None,
    canonical_source=None,
    storage_roots=None,
    shallow_zarr_storage=False,
):
    """Export plate as ZARR format using omero-cli-zarr.
    
    Args:
        conn: OMERO BlitzGateway connection.
        suuid: Session UUID for authentication.
        plate: OMERO plate wrapper to export.
        folder_name (str, optional): Target folder for export.
        client: OMERO client (unused, for compatibility).
        ome_zarr_version (str, optional): OMERO version for export.
    """
    # TODO use raw converter directly
    # (1) find out the plate's file
    # (2) (a) if not zarr: subprocess raw on that file
    # (2) (b) if zarr: copy/scp directly
    return save_as_zarr(
        conn,
        suuid,
        plate,
        folder_name,
        constants.transfer.DATA_TYPE_PLATE,
        ome_zarr_version,
        canonical_source,
        storage_roots,
        shallow_zarr_storage,
    )


def save_image_as_zarr(
    conn,
    suuid,
    image,
    folder_name=None,
    ome_zarr_version=None,
    canonical_source=None,
    storage_roots=None,
    shallow_zarr_storage=False,
):
    """Export image as ZARR format using omero-cli-zarr.
    
    Args:
        conn: OMERO BlitzGateway connection.
        suuid: Session UUID for authentication.
        image: OMERO image wrapper to export.
        folder_name (str, optional): Target folder for export.
        ome_zarr_version (str, optional): OMERO version for export.
    """
    return save_as_zarr(
        conn,
        suuid,
        image,
        folder_name,
        constants.transfer.DATA_TYPE_IMAGE,
        ome_zarr_version,
        canonical_source,
        storage_roots,
        shallow_zarr_storage,
    )
    

def build_zarr_export_error(object, data_type, stderr):
    """Build a more actionable error message for failed OME-Zarr exports."""
    stderr_text = stderr.decode('utf-8', errors='replace') if stderr else ''
    context = [
        f"OME-Zarr export failed for {data_type} {object.getId()} ('{object.getName()}')."
    ]

    if ("Error instantiating pixel buffer" in stderr_text and
            data_type == constants.transfer.DATA_TYPE_IMAGE):
        context.append(
            "OMERO could not open the underlying pixel source for this image."
        )
        context.append(
            "A common cause is an in-place imported image whose original file is no longer reachable at its imported path."
        )
        context.append(
            "Check whether the image was imported with --transfer=ln_s or another in-place mode and whether the source file still exists and is readable by OMERO."
        )

    if stderr_text:
        context.append(f"stderr: {stderr_text}")
    else:
        context.append("stderr: Unknown error")

    return " ".join(context)


def save_as_zarr(
    conn,
    suuid,
    object,
    folder_name=None,
    data_type=None,
    ome_zarr_version=None,
    canonical_source=None,
    storage_roots=None,
    shallow_zarr_storage=False,
):
    """Export OMERO object as ZARR using subprocess call to omero-cli-zarr.
    
    Args:
        conn: OMERO BlitzGateway connection.
        suuid: Session UUID for OMERO authentication.
        object: OMERO object wrapper (Image or Plate) to export.
        folder_name (str, optional): Target folder for export.
        data_type (str, optional): Type of OMERO object for appropriate export.
        ome_zarr_version (str, optional): Ome-zarr version to use for export.
    
    Raises:
        ValueError: If unsupported data_type is provided.
    """
    extension = "zarr"
    name = os.path.basename(object.getName())
    img_name = "%s.%s" % (name, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    path_name = img_name[:-(len(extension) + 1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (path_name, i, extension)
        i += 1

    shallow_reference = None
    if shallow_zarr_storage and data_type == constants.transfer.DATA_TYPE_IMAGE:
        shallow_reference = get_shallow_reference(object)
    if shallow_reference is not None:
        materialized = materialize_shallow_zarr(
            shallow_reference,
            img_name,
            storage_roots or {},
        )
        logger.info(
            "Reconstructed shallow Image %s as full workflow input %s with "
            "%s label layer(s)",
            object.getId(),
            img_name,
            len(materialized.labels),
        )
        log(
            " Reconstructed original pixels and %s managed label layer(s)"
            % len(materialized.labels)
        )
        return (
            shallow_reference.source,
            os.path.basename(img_name),
            materialized.labels,
        )

    source_path = select_zarr_source_path(
        object, canonical_source, storage_roots or {})
    if source_path is not None:
        if shallow_zarr_storage and canonical_source is None:
            try:
                if data_type == constants.transfer.DATA_TYPE_PLATE:
                    canonical_source = index_existing_plate_zarr(
                        conn,
                        object,
                        source_path,
                        storage_roots or {},
                    )
                elif data_type == constants.transfer.DATA_TYPE_IMAGE:
                    canonical_source = index_existing_image_zarr(
                        conn,
                        object,
                        source_path,
                        storage_roots or {},
                    )
            except Exception as exc:
                logger.warning(
                    "Existing Zarr indexing failed for %s %s; retaining "
                    "normal legacy reuse and disabling shallow result "
                    "matching for this selection: %s",
                    data_type,
                    object.getId(),
                    exc,
                    exc_info=True,
                )
        log(" Copying file as: %s" % img_name)
        shutil.copytree(
            source_path,
            img_name,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns(".biomero-canonical.json"),
        )
    else:
        if canonical_source is None:
            if data_type == constants.transfer.DATA_TYPE_IMAGE:
                if shallow_zarr_storage:
                    logger.info(
                        "No reusable canonical or legacy Zarr found for %s "
                        "%s; exporting NGFF for importer-enabled result "
                        "processing",
                        data_type,
                        object.getId(),
                    )
                else:
                    logger.info(
                        "No reusable Zarr found for %s %s; exporting a fresh "
                        "NGFF store on the standard Get Results route",
                        data_type,
                        object.getId(),
                    )
            else:
                logger.info(
                    "No reusable canonical or legacy Zarr found for %s %s; "
                    "exporting a fresh NGFF store",
                    data_type,
                    object.getId(),
                )
        log("  Saving file as: %s" % img_name)
        curr_dir = os.getcwd()
        exp_dir = os.path.join(curr_dir, folder_name)

        # TODO: Check if import omero-cli-zarr and #export(...) works better than subprocess?
        # https://github.com/ome/omero-cli-zarr/blob/a35ade1d8177585b3e21ef860fd645a8d6eb5aea/src/omero_zarr/cli.py#L327C9-L327C15

        # command = f'omero zarr -s "$CONFIG_omero_master_host" -k "{suuid}" export --bf Image:{image.getId()}'
        cmd1 = 'export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")'
        command = f'omero zarr -s "{conn.host}" -k "{suuid}" export'
        if ome_zarr_version:
            command += f' --format {ome_zarr_version}'   # argument only available in ome-cli-zarr v0.8.0+
        command += f' --output "{exp_dir}"'
        if data_type == constants.transfer.DATA_TYPE_PLATE:
            command += f' Plate:{object.getId()}'
        elif data_type == constants.transfer.DATA_TYPE_IMAGE:
            command += f' Image:{object.getId()}'
        else:
            log(f"OMERO ZARR command for {data_type}: {command}")
            print(f"OMERO ZARR command for {data_type}: {command}")
            raise ValueError(f"No OMERO ZARR command known for data_type: {data_type}")
        log(f"OMERO ZARR command for {data_type}: {command}")
        cmd = cmd1 + " && " + command
        logger.debug(cmd)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True
        )
        stdout, stderr = process.communicate()
        if stderr:
            logger.warning(stderr.decode("utf-8"))
        if process.returncode == 0:
            log(f"OME ZARR CLI: {stdout}")
            logger.debug(img_name)

            # Check for both .ome.zarr and .zarr extensions for compatibility
            source_ome_zarr = f"{exp_dir}/{object.getId()}.ome.zarr"
            source_zarr = f"{exp_dir}/{object.getId()}.zarr"

            if os.path.exists(source_ome_zarr):
                os.rename(source_ome_zarr, img_name)
                log(f"Renamed .ome.zarr file: {source_ome_zarr} -> {img_name}")
            elif os.path.exists(source_zarr):
                os.rename(source_zarr, img_name)
                log(f"Renamed .zarr file: {source_zarr} -> {img_name}")
            else:
                error_msg = (f"Neither {source_ome_zarr} nor {source_zarr} "
                             f"found after ZARR export")
                raise FileNotFoundError(error_msg)
        else:
            error_msg = (
                f"ZARR export failed with return code {process.returncode}: "
                f"{build_zarr_export_error(object, data_type, stderr)}"
            )
            logger.error(f"Critical error: {error_msg}")
            raise Exception(error_msg)
        if shallow_zarr_storage and canonical_source is None:
            try:
                if data_type == constants.transfer.DATA_TYPE_PLATE:
                    canonical_source = promote_exported_plate_zarr(
                        conn,
                        object,
                        img_name,
                        storage_roots or {},
                    )
                elif data_type == constants.transfer.DATA_TYPE_IMAGE:
                    canonical_source = promote_exported_image_zarr(
                        conn,
                        object,
                        img_name,
                        storage_roots or {},
                    )
            except Exception as exc:
                logger.warning(
                    "Canonical Zarr promotion failed for %s %s; retaining "
                    "the normal exported input and disabling shallow result "
                    "matching for this selection: %s",
                    data_type,
                    object.getId(),
                    exc,
                    exc_info=True,
                )
                log(
                    " Canonical caching unavailable for %s %s; using normal "
                    "export" % (data_type, object.getId())
                )
    return canonical_source, os.path.basename(img_name), ()


def save_planes_for_image(suuid, image, size_c, split_cs, merged_cs,
                          channel_names=None, z_range=None, t_range=None,
                          greyscale=False, zoom_percent=None, project_z=False,
                          format="PNG", folder_name=None):
    """
    Saves all the required planes for a single image, either as individual
    planes or projection.

    @param renderingEngine:     Rendering Engine, NOT initialised.
    @param queryService:        OMERO query service
    @param imageId:             Image ID
    @param zRange:              Tuple: (zStart, zStop). If None, use default
                                Zindex
    @param tRange:              Tuple: (tStart, tStop). If None, use default
                                Tindex
    @param greyscale:           If true, all visible channels will be
                                greyscale
    @param zoomPercent:         Resize image by this percent if specified.
    @param projectZ:            If true, project over Z range.
    """

    channels = []
    if merged_cs:
        # render merged first with current rendering settings
        channels.append(None)
    if split_cs:
        for i in range(size_c):
            channels.append(i)

    # set up rendering engine with the pixels
    """
    renderingEngine.lookupPixels(pixelsId)
    if not renderingEngine.lookupRenderingDef(pixelsId):
        renderingEngine.resetDefaults()
    if not renderingEngine.lookupRenderingDef(pixelsId):
        raise "Failed to lookup Rendering Def"
    renderingEngine.load()
    """

    if t_range is None:
        # use 1-based indices throughout script
        t_indexes = [image.getDefaultT()+1]
    else:
        if len(t_range) > 1:
            t_indexes = range(t_range[0], t_range[1])
        else:
            t_indexes = [t_range[0]]

    c_name = 'merged'
    for c in channels:
        if c is not None:
            g_scale = greyscale
            if c < len(channel_names):
                c_name = channel_names[c].replace(" ", "_")
            else:
                c_name = "c%02d" % c
        else:
            # if we're rendering 'merged' image - don't want grey!
            g_scale = False
        for t in t_indexes:
            if z_range is None:
                default_z = image.getDefaultZ()+1
                save_plane(image, format, c_name, (default_z,), project_z, t,
                           c, g_scale, zoom_percent, folder_name)
            elif project_z:
                save_plane(image, format, c_name, z_range, project_z, t, c,
                           g_scale, zoom_percent, folder_name)
            else:
                if len(z_range) > 1:
                    for z in range(z_range[0], z_range[1]):
                        save_plane(image, format, c_name, (z,), project_z, t,
                                   c, g_scale, zoom_percent, folder_name)
                else:
                    save_plane(image, format, c_name, z_range, project_z, t,
                               c, g_scale, zoom_percent, folder_name)


def batch_image_export(conn, script_params, slurmClient: SlurmClient,
                       suuid: str, client):
    """
    Export selected OMERO data to SLURM cluster with automatic cleanup.
    
    This function handles the complete export pipeline:
    1. Processes selected images, datasets, or plates from OMERO
    2. Renders and saves data in specified format (TIFF/OME-TIFF/OME-ZARR)
    3. Packages data for transfer (zip compression when needed)
    4. Transfers data to SLURM cluster via SSH
    5. Unpacks data on SLURM for processing
    6. Creates temporary file annotation in OMERO
    7. Automatically cleans up annotation after successful transfer
    
    Args:
        conn: OMERO BlitzGateway connection
        script_params: Dictionary of script parameters from user input
        slurmClient: Active SLURM client for data transfer
        suuid: OMERO session UUID for authentication
        client: OMERO script client for creating annotations
        
    Returns:
        tuple: (file_annotation, message) where file_annotation is None
               if successfully cleaned up, or the annotation object if
               cleanup failed or transfer was unsuccessful
               
    Raises:
        Exception: Various exceptions during export process, all logged
    """
    # for params with default values, we can get the value directly
    split_cs = script_params[constants.transfer.CHANNELS]
    merged_cs = script_params[constants.transfer.MERGED]
    greyscale = script_params[constants.transfer.CHANNELS_GREY]
    data_type = script_params[constants.transfer.DATA_TYPE]
    folder_name = script_params[constants.transfer.FOLDER]
    folder_name = os.path.basename(folder_name)
    format = script_params[constants.transfer.FORMAT]
    ome_zarr_version = script_params[constants.transfer.OME_VERSION]
    project_z = constants.transfer.Z in script_params and \
        script_params[constants.transfer.Z] == constants.transfer.Z_MAXPROJ
    message = ""
    canonical_inputs = ()
    canonical_sources = {}
    transfer_artifacts = {}
    label_components_by_object = {}
    storage_roots = {}
    shallow_zarr_storage = is_shallow_zarr_storage_enabled(format)

    if (not split_cs) and (not merged_cs):
        log("Not chosen to save Individual Channels OR Merged Image")
        return None, "No channel export mode selected", canonical_inputs

    # check if we have these params
    channel_names = []
    if constants.transfer.CHANNELS_NAMES in script_params:
        channel_names = script_params[constants.transfer.CHANNELS_NAMES]
    zoom_percent = None
    if constants.transfer.ZOOM in script_params and script_params[constants.transfer.ZOOM] != constants.transfer.ZOOM_100:
        zoom_percent = int(script_params[constants.transfer.ZOOM][:-1])

    # functions used below for each imaage.
    def get_z_range(size_z, script_params):
        z_range = None
        if constants.transfer.Z in script_params:
            z_choice = script_params[constants.transfer.Z]
            # NB: all Z indices in this script are 1-based
            if z_choice == constants.transfer.Z_ALL:
                z_range = (1, size_z+1)
            elif constants.transfer.Z_IDX in script_params:
                z_index = script_params[constants.transfer.Z_IDX]
                z_index = min(z_index, size_z)
                z_range = (z_index,)
            elif constants.transfer.Z_IDX_START in script_params and \
                    constants.transfer.Z_IDX_END in script_params:
                start = script_params[constants.transfer.Z_IDX_START]
                start = min(start, size_z)
                end = script_params[constants.transfer.Z_IDX_END]
                end = min(end, size_z)
                # in case user got z_start and z_end mixed up
                z_start = min(start, end)
                z_end = max(start, end)
                if z_start == z_end:
                    z_range = (z_start,)
                else:
                    z_range = (z_start, z_end+1)
        return z_range

    def get_t_range(size_t, script_params):
        t_range = None
        if constants.transfer.T in script_params:
            t_choice = script_params[constants.transfer.T]
            # NB: all T indices in this script are 1-based
            if t_choice == constants.transfer.T_ALL:
                t_range = (1, size_t+1)
            elif constants.transfer.T_IDX in script_params:
                t_index = script_params[constants.transfer.T_IDX]
                t_index = min(t_index, size_t)
                t_range = (t_index,)
            elif constants.transfer.T_IDX_START in script_params and \
                    constants.transfer.T_IDX_END in script_params:
                start = script_params[constants.transfer.T_IDX_START]
                start = min(start, size_t)
                end = script_params[constants.transfer.T_IDX_END]
                end = min(end, size_t)
                # in case user got t_start and t_end mixed up
                t_start = min(start, end)
                t_end = max(start, end)
                if t_start == t_end:
                    t_range = (t_start,)
                else:
                    t_range = (t_start, t_end+1)
        return t_range

    # Get the images or datasets
    objects, log_message = script_utils.get_objects(conn, script_params)
    message += log_message
    if not objects:
        return None, message, canonical_inputs

    # Attach figure to the first image
    parent = objects[0]

    if data_type == constants.transfer.DATA_TYPE_DATASET:
        images = []
        for ds in objects:
            images.extend(list(ds.listChildren()))
        if not images:
            message += "No image found in dataset(s)"
            return None, message, canonical_inputs
    elif data_type == constants.transfer.DATA_TYPE_PLATE:
        if format == constants.transfer.FORMAT_OMEZARR:
            log("Processing %s Plates to ZARR, not individual images." % len(objects))         
            images = []  # skip the rest of the processing below
            wells = []
        else:
            images = []
            wells = []
            for plate in objects:
                wells.extend(list(plate.listChildren()))
            for well in wells:
                nr_samples = well.countWellSample()
                for index in range(0, nr_samples):
                    image = well.getImage(index)
                    images.append(image)
            if not images:
                message += "No image found in plate(s)"
                return None, message, canonical_inputs
    else:
        images = objects

    if shallow_zarr_storage:
        try:
            storage_roots = load_group_storage_roots()
            if data_type == constants.transfer.DATA_TYPE_PLATE:
                export_objects = objects
                canonical_object_type = "Plate"
            else:
                export_objects = images
                canonical_object_type = "Image"
            canonical_sources, canonical_inputs = discover_canonical_inputs(
                export_objects, canonical_object_type)
        except Exception as exc:
            logger.warning(
                "Canonical Zarr setup failed; using normal Zarr export and "
                "disabling shallow result matching for this selection: %s",
                exc,
                exc_info=True,
            )
            log(" Canonical caching unavailable; using normal Zarr export")
            shallow_zarr_storage = False
            canonical_inputs = ()
            canonical_sources = {}
            storage_roots = {}

    log("Processing %s images" % len(images))

    # somewhere to put images
    curr_dir = os.getcwd()
    exp_dir = os.path.join(curr_dir, folder_name)
    try:
        os.mkdir(exp_dir)
    except OSError:
        pass
    # max size (default 12kx12k)
    size = conn.getDownloadAsMaxSizeSetting()
    size = int(size)

    ids = []
    # do the saving to disk
    
    if format == constants.transfer.FORMAT_OMEZARR and data_type == constants.transfer.DATA_TYPE_PLATE:
        for plate in objects:
            log("Processing plate: ID %s: %s" % (plate.id, plate.getName()))
            promoted_source, transfer_artifact, label_components = save_plate_as_zarr(
                conn,
                suuid,
                plate,
                folder_name,
                client,
                ome_zarr_version=ome_zarr_version,
                canonical_source=canonical_sources.get(int(plate.getId())),
                storage_roots=storage_roots,
                shallow_zarr_storage=shallow_zarr_storage,
            )
            object_id = int(plate.getId())
            transfer_artifacts[object_id] = transfer_artifact
            label_components_by_object[object_id] = label_components
            if promoted_source is not None:
                canonical_sources[object_id] = promoted_source
            write_logfile(exp_dir)
            
    for img in images:
        log("Processing image: ID %s: %s" % (img.id, img.getName()))
        pixels = img.getPrimaryPixels()
        if (pixels.getId() in ids):
            continue
        ids.append(pixels.getId())

        if format == constants.transfer.FORMAT_OMETIFF:
            if img._prepareRE().requiresPixelsPyramid():
                log("  ** Can't export a 'Big' image to OME-TIFF. **")
                if len(images) == 1:
                    return (
                        None,
                        "Can't export a 'Big' image to %s." % format,
                        canonical_inputs,
                    )
                continue
            else:
                save_as_ome_tiff(conn, img, folder_name)
        elif format == constants.transfer.FORMAT_OMEZARR:
            promoted_source, transfer_artifact, label_components = save_image_as_zarr(
                conn,
                suuid,
                img,
                folder_name,
                ome_zarr_version=ome_zarr_version,
                canonical_source=canonical_sources.get(int(img.getId())),
                storage_roots=storage_roots,
                shallow_zarr_storage=shallow_zarr_storage,
            )
            object_id = int(img.getId())
            transfer_artifacts[object_id] = transfer_artifact
            label_components_by_object[object_id] = label_components
            if promoted_source is not None:
                canonical_sources[object_id] = promoted_source
        else:
            size_x = pixels.getSizeX()
            size_y = pixels.getSizeY()
            if size_x*size_y > size:
                msg = "Can't export image over %s pixels. " \
                      "See 'omero.client.download_as.max_size'" % size
                log("  ** %s. **" % msg)
                if len(images) == 1:
                    return None, msg, canonical_inputs
                continue
            else:
                log("Exporting image as %s: %s" % (format, img.getName()))

            log("\n----------- Saving planes from image: '%s' ------------"
                % img.getName())
            size_c = img.getSizeC()
            size_z = img.getSizeZ()
            size_t = img.getSizeT()
            z_range = get_z_range(size_z, script_params)
            t_range = get_t_range(size_t, script_params)
            log("Using:")
            if z_range is None:
                log("  Z-index: Last-viewed")
            elif len(z_range) == 1:
                log("  Z-index: %d" % z_range[0])
            else:
                log("  Z-range: %s-%s" % (z_range[0], z_range[1]-1))
            if project_z:
                log("  Z-projection: ON")
            if t_range is None:
                log("  T-index: Last-viewed")
            elif len(t_range) == 1:
                log("  T-index: %d" % t_range[0])
            else:
                log("  T-range: %s-%s" % (t_range[0], t_range[1]-1))
            log("  Format: %s" % format)
            if zoom_percent is None:
                log("  Image Zoom: 100%")
            else:
                log("  Image Zoom: %s" % zoom_percent)
            log("  Greyscale: %s" % greyscale)
            log("Channel Rendering Settings:")
            for ch in img.getChannels():
                log("  %s: %d-%d"
                    % (ch.getLabel(), ch.getWindowStart(), ch.getWindowEnd()))

            try:
                save_planes_for_image(suuid, img, size_c, split_cs, merged_cs,
                                      channel_names, z_range, t_range,
                                      greyscale, zoom_percent,
                                      project_z=project_z, format=format,
                                      folder_name=folder_name)
            finally:
                # Make sure we close Rendering Engine
                img._re.close()

        # write log for exported images (not needed for ome-tiff)
        write_logfile(exp_dir)

    if shallow_zarr_storage:
        canonical_inputs = canonical_inputs_from_sources(
            export_objects,
            canonical_object_type,
            canonical_sources,
            transfer_artifacts,
            storage_roots,
            label_components_by_object,
        )

    if len(os.listdir(exp_dir)) == 0:
        error_msg = "No files exported. Check export settings and data availability."
        logger.error(f"Critical error: {error_msg}")
        raise Exception(error_msg)
    # zip everything up (unless we've only got a single ome-tiff)
    if format == constants.transfer.FORMAT_OMETIFF and len(os.listdir(exp_dir)) == 1:
        ometiff_ids = [t.id for t in parent.listAnnotations(ns=NSOMETIFF)]
        conn.deleteObjects("Annotation", ometiff_ids)
        export_file = os.path.join(folder_name, os.listdir(exp_dir)[0])
        namespace = NSOMETIFF
        output_display_name = "OME-TIFF"
        mimetype = 'image/tiff'
    else:
        export_file = "%s.zip" % folder_name
        compress(export_file, folder_name)
        mimetype = 'application/zip'
        output_display_name = f"Batch export zip '{folder_name}'"
        namespace = NSCREATED + "/omero/export_scripts/Batch_Image_Export"

    # Copy to SLURM
    transfer_successful = False
    try:
        r = slurmClient.transfer_data(Path(export_file))
        logger.debug(r)
        if hasattr(r, 'ok') and not r.ok:
            error_msg = (
                f"Copying to SLURM reported failure: "
                f"{getattr(r, 'stderr', r)}"
            )
            logger.error(error_msg)
            raise Exception(error_msg)
        message += f"'{folder_name}' succesfully copied to SLURM!\n"
        transfer_successful = True
    except Exception as e:
        logger.error(f"Critical error: Copying to SLURM failed: {e}")
        raise Exception(f"Data transfer to SLURM failed: {e}") from e
        
    # Unpack on SLURM
    unpack_successful = False
    if transfer_successful:
        try:
            unpack_result = slurmClient.unpack_data(folder_name)
            logger.debug(unpack_result.stdout)
            if not unpack_result.ok:
                error_msg = f"Error unpacking data on SLURM: {unpack_result.stderr}"
                logger.error(error_msg)
                raise Exception(error_msg)
            else:
                unpack_successful = True
        except Exception as e:
            logger.error(f"Critical error: Unzipping on SLURM failed: {e}")
            raise Exception(f"Data unpacking on SLURM failed: {e}") from e
    
    file_annotation, ann_message = script_utils.create_link_file_annotation(
        conn, export_file, parent, output=output_display_name,
        namespace=namespace, mimetype=mimetype)
    message += ann_message
    
    # Clean up file annotation if transfer and unpack were successful AND cleanup is enabled
    cleanup_enabled = script_params.get("Cleanup?", True)
    if transfer_successful and unpack_successful and file_annotation and cleanup_enabled:
        try:
            conn.deleteObjects("FileAnnotation", [file_annotation.id],
                               deleteAnns=True, deleteChildren=True, wait=True)
            message += ("Temporary file annotation cleaned up after "
                        "successful transfer.\n")
            logger.info(f"Cleaned up file annotation {file_annotation.id}")
            # Return None to indicate cleanup was done
            file_annotation = None
        except Exception as cleanup_error:
            # Cleanup failure is non-critical - log warning but don't fail script
            logger.warning(f"Failed to cleanup file annotation: "
                           f"{cleanup_error}")
            message += (f"Warning: Could not cleanup temporary file "
                        f"annotation: {cleanup_error}\n")
    elif transfer_successful and unpack_successful and file_annotation and not cleanup_enabled:
        message += ("File annotation preserved in OMERO as requested. "
                    "You can download the zip/ZARR from the attachments.\n")
        logger.info(f"File annotation {file_annotation.id} preserved for download")

    return file_annotation, message, canonical_inputs


def write_logfile(exp_dir):
    """Write accumulated log strings to a batch export log file.
    
    Args:
        exp_dir (str): Export directory path where log file will be created.
    """
    name = 'Batch_Image_Export.txt'
    with open(os.path.join(exp_dir, name), 'w') as log_file:
        for s in log_strings:
            log_file.write(s)
            log_file.write("\n")


def run_script():
    """Main entry point for SLURM image transfer script.
    
    Called by OMERO scripting service to handle data export from OMERO
    to SLURM clusters. Configures script parameters, processes user inputs,
    and delegates to batch_image_export for the actual transfer work.
    """

    with SlurmClient.from_config() as slurmClient:

        data_types = [rstring(constants.transfer.DATA_TYPE_DATASET),
                      rstring(constants.transfer.DATA_TYPE_IMAGE),
                      rstring(constants.transfer.DATA_TYPE_PLATE)]
        formats = [rstring(constants.transfer.FORMAT_TIFF),
                   rstring(constants.transfer.FORMAT_OMETIFF),
                   rstring(constants.transfer.FORMAT_OMEZARR)]
        ome_zarr_versions = [rstring(constants.transfer.OME_ZARR_VERSION_0_4),
                             rstring(constants.transfer.OME_ZARR_VERSION_0_5)]
        default_z_option = constants.transfer.Z_DEFAULT
        z_choices = [rstring(default_z_option),
                     rstring(constants.transfer.Z_ALL),
                     # currently ImageWrapper only allows full Z-stack
                     # projection
                     rstring(constants.transfer.Z_MAXPROJ),
                     rstring(constants.transfer.Z_OTHER)]
        default_t_option = constants.transfer.T_DEFAULT
        t_choices = [rstring(default_t_option),
                     rstring(constants.transfer.T_ALL),
                     rstring(constants.transfer.T_OTHER)]
        zoom_percents = omero.rtypes.wrap([constants.transfer.ZOOM_25,
                                           constants.transfer.ZOOM_50,
                                           constants.transfer.ZOOM_100,
                                           constants.transfer.ZOOM_200,
                                           constants.transfer.ZOOM_300,
                                           constants.transfer.ZOOM_400])

        client = scripts.client(
            '_SLURM_Image_Transfer',
            f"""Save multiple images as TIFF or ZARR
            in a zip file and export them to SLURM.
            
            Note that TIFF will be a rendered version of your image
            as shown in OMERO.web currently: not the original pixel
            values. This matters for e.g. Mask images, where each
            ROI / mask should be a very specific pixel value.
            
            ZARR will use the specific pixel values of the original 
            file, but you will need to convert it to a format that
            the workflows can read (which is TIFF).
            
            Please use SLURM_Run_Workflow directly instead if you 
            don't know how to convert ZARR to TIFF on Slurm!
            Otherwise, use the conversion job on Slurm to convert.

            This runs a script remotely on your SLURM cluster.
            Connection ready? {slurmClient.validate()}""",

            scripts.String(
                constants.transfer.DATA_TYPE, optional=False, grouping="1",
                description="The data you want to work with.",
                values=data_types,
                default=constants.transfer.DATA_TYPE_IMAGE),

            scripts.List(
                constants.transfer.IDS, optional=False, grouping="2",
                description="List of Dataset IDs or Image IDs").ofType(
                    rlong(0)),

            scripts.Bool(
                constants.transfer.SETTINGS, grouping="5",
                description="Select how to export your images",
                optional=False,
                default=True
            ),

            scripts.Bool(
                constants.transfer.CHANNELS, grouping="5.6",
                description="Save individual channels as separate images",
                default=False),

            scripts.Bool(
                constants.transfer.CHANNELS_GREY, grouping="5.6.1",
                description="If true, all individual channel images will be"
                " grayscale", default=False),

            scripts.List(
                constants.transfer.CHANNELS_NAMES, grouping="5.6.2",
                description="Names for saving individual channel images"),

            scripts.Bool(
                constants.transfer.MERGED, grouping="5.5",
                description="Save merged image, using current \
                    rendering settings",
                default=True),

            scripts.String(
                constants.transfer.Z, grouping="5.7",
                description="Default Z is last viewed Z for each image\
                    , OR choose"
                " Z below.", values=z_choices, default=default_z_option),

            scripts.Int(
                constants.transfer.Z_IDX, grouping="5.7.1",
                description="Choose a specific Z-index to export", min=1),

            scripts.Int(
                constants.transfer.Z_IDX_START, grouping="5.7.2",
                description="Choose a specific Z-index to export", min=1),

            scripts.Int(
                constants.transfer.Z_IDX_END, grouping="5.7.3",
                description="Choose a specific Z-index to export", min=1),

            scripts.String(
                constants.transfer.T, grouping="5.8",
                description="Default T is last viewed T for each image"
                ", OR choose T below.", values=t_choices,
                default=default_t_option),

            scripts.Int(
                constants.transfer.T_IDX, grouping="5.8.1",
                description="Choose a specific T-index to export", min=1),

            scripts.Int(
                constants.transfer.T_IDX_START, grouping="5.8.2",
                description="Choose a specific T-index to export", min=1),

            scripts.Int(
                constants.transfer.T_IDX_END, grouping="5.8.3",
                description="Choose a specific T-index to export", min=1),

            scripts.String(
                constants.transfer.ZOOM, grouping="5.9", values=zoom_percents,
                description="Zoom (jpeg, png or tiff) before saving with"
                " ANTIALIAS interpolation",
                default=constants.transfer.ZOOM_100),

            scripts.String(
                constants.transfer.FORMAT, grouping="5.1",
                description="Format to save image", values=formats,
                default=constants.transfer.FORMAT_OMEZARR),

            scripts.String(
                constants.transfer.OME_VERSION, grouping="5.2",
                description="Ome-zarr version", values=ome_zarr_versions,
                default=constants.transfer.OME_ZARR_VERSION_0_4),

            scripts.String(
                constants.transfer.FOLDER, grouping="3",
                description="Name of folder (and zip file) to store images. Don't use spaces!",
                default=constants.transfer.FOLDER_DEFAULT+str(int(datetime.now().timestamp()))),

            scripts.Bool(
                "Cleanup?", grouping="4",
                description="Remove zip/annotation from OMERO after successful transfer to SLURM. Uncheck to keep downloadable copy in OMERO.",
                default=True),

            version=VERSION,
            authors=["Torec Luik", "William Moore", "OME Team"],
            institutions=["Amsterdam UMC", "University of Dundee"],
            contact='cellularimaging@amsterdamumc.nl',
            authorsInstitutions=[[1], [2]],
            namespaces=[omero.constants.namespaces.NSDYNAMIC],
        )

        try:
            start_time = datetime.now()
            script_params = {}

            conn = BlitzGateway(client_obj=client)

            suuid = client.getSessionId()

            script_params = client.getInputs(unwrap=True)
            for key, value in script_params.items():
                log("%s:%s" % (key, value))

            # call the main script - returns a file annotation wrapper
            file_annotation, message, canonical_inputs = batch_image_export(
                conn, script_params, slurmClient, suuid, client)

            stop_time = datetime.now()
            log("Duration: %s" % str(stop_time-start_time))

            # return this fileAnnotation to the client.
            client.setOutput("Message", rstring(message))
            client.setOutput(
                CANONICAL_INPUTS_OUTPUT,
                rstring(json.dumps([
                    item.to_dict() for item in canonical_inputs
                ], separators=(",", ":"), sort_keys=True)),
            )
            if file_annotation is not None:
                client.setOutput("File_Annotation",
                                 robject(file_annotation._obj))

        finally:
            client.closeSession()


if __name__ == "__main__":
    # Some defaults from OMERO; don't feel like reading ice files.
    # Retrieve the value of the OMERODIR environment variable
    OMERODIR = os.environ.get('OMERODIR', '/opt/omero/server/OMERO.server')
    LOGDIR = os.path.join(OMERODIR, 'var', 'log')
    LOGFORMAT = "%(asctime)s %(levelname)-5.5s [%(name)40s] " \
                "[%(process)d] (%(threadName)-10s) %(message)s"
    # Added the process id
    LOGSIZE = 500000000
    LOGNUM = 9
    log_filename = 'biomero.log'
    # Create a stream handler with INFO level (for OMERO.web output)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    # Create DEBUG logging to rotating logfile at var/log
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGFORMAT,
                        handlers=[
                            stream_handler,
                            logging.handlers.RotatingFileHandler(
                                os.path.join(LOGDIR, log_filename),
                                maxBytes=LOGSIZE,
                                backupCount=LOGNUM)
                        ])

    # Silence some of the DEBUG - Extended for cleaner BIOMERO logs
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('omero.gateway').setLevel(logging.WARNING)  # Silences proxy creation spam
    logging.getLogger('omero.client').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)
    logging.getLogger('paramiko.sftp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('requests_cache').setLevel(logging.WARNING)  # Cache logs
    logging.getLogger('requests-cache').setLevel(logging.WARNING)  # Alt naming
    logging.getLogger('requests_cache.core').setLevel(logging.WARNING)  # Core module
    logging.getLogger('requests_cache.backends').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.base').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.sqlite').setLevel(
        logging.WARNING)
    logging.getLogger('requests_cache.policy').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.policy.actions').setLevel(
        logging.WARNING)
    logging.getLogger('invoke').setLevel(logging.WARNING)
    logging.getLogger('fabric').setLevel(logging.WARNING)  # SSH operations
    logging.getLogger('Ice').setLevel(logging.ERROR)
    logging.getLogger('ZeroC').setLevel(logging.ERROR)

    run_script()
