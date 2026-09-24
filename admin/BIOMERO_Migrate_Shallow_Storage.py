#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Administratively migrate BIOMERO shallow-storage contracts."""

import json
import logging
from pathlib import Path
import re
import shutil
from uuid import uuid4

import omero
from omero import scripts
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, unwrap
from omero.sys import ParametersI

from biomero_schema.zarr import (
    CANONICAL_PLATE_IMAGE_NAMESPACE,
    CANONICAL_PLATE_LABEL_NAMESPACE,
    CANONICAL_PLATE_SOURCE_NAMESPACE,
    CANONICAL_SOURCE_NAMESPACE,
    SHALLOW_COLLECTION_MANIFEST,
    SHALLOW_COLLECTION_NAMESPACE,
    CanonicalPlateIndex,
    CanonicalPlateSource,
    CanonicalZarrSource,
    ShallowManifest,
    ShallowPlateReference,
    ShallowZarrReference,
)
from biomero_importer.utils.canonical_store import (
    CANONICAL_MARKER_NAME,
    external_canonical_marker_path,
    load_canonical_marker,
    write_indexed_canonical_marker,
)
from biomero_shallower.migration import (
    migrate_shallow_store_v1,
    rebind_shallow_store_sources,
    restore_shallow_store_v1,
    upgrade_annotation_reference_v1,
    upgrade_manifest_v1,
)
from biomero_shallower.result_zarr import load_managed_storage_roots


logger = logging.getLogger(__name__)
VERSION = "2.9.0"

OBJECT_TYPE = "Object Type"
OBJECT_IDS = "Object IDs (optional)"
DRY_RUN = "Dry Run"
BACKUP_DIRECTORY = "Backup Directory (optional)"
REQUIRED_MIGRATION_CAPABILITIES = (
    "canonical-single-store",
    "schema-1-to-2",
    "schema-1-path-only-labels",
)


def _require_migration_capabilities():
    """Reject an incompatible local Shallower before reading stored data."""
    try:
        from biomero_shallower.capabilities import require_migrations
    except ImportError as error:
        raise RuntimeError(
            "The installed biomero-shallower package cannot perform this "
            "migration. Update biomeroworker before retrying."
        ) from error
    require_migrations(*REQUIRED_MIGRATION_CAPABILITIES)


def _pairs_to_values(pairs):
    values = {}
    for key, value in pairs:
        key = str(key)
        if key in values:
            raise ValueError(f"Duplicate shallow-reference key: {key}")
        values[key] = str(value)
    return values


def _resolve_store_path(values, storage_roots):
    root_name = values["storageRoot"]
    try:
        root = Path(storage_roots[root_name]).resolve()
    except KeyError as exc:
        raise ValueError(f"Unknown managed storage root: {root_name}") from exc
    relative = Path(values["relativePath"])
    if relative.is_absolute():
        raise ValueError("Shallow relativePath must be relative")
    store = (root / relative).resolve()
    if not store.is_relative_to(root):
        raise ValueError("Shallow relativePath escapes its managed storage root")
    return store


def _stable_canonical_relative_path(relative_path, object_type, object_id):
    """Remove the prerelease generation suffix from generated stores."""
    relative = Path(relative_path)
    legacy = re.fullmatch(
        rf"{re.escape(object_type)}-{int(object_id)}\.g\d+\.ome\.zarr",
        relative.name,
    )
    if legacy:
        return relative.with_name(
            f"{object_type}-{int(object_id)}.ome.zarr"
        )
    return relative


def _canonical_content_signature(source):
    """Identify canonical content independently of path/trust revisions."""
    ignored = {
        "relativePath",
        "sourceGeneration",
        "pixelIdentityOrigin",
        "canonicalPixelVerified",
    }

    def normalize(value):
        if isinstance(value, dict):
            return {
                key: normalize(item)
                for key, item in value.items()
                if key not in ignored
            }
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        return value

    return json.dumps(normalize(source.to_dict()), sort_keys=True)


def _normalize_canonical_source(source, relative_path):
    """Point one canonical contract at its stable physical store."""
    relative_path = Path(relative_path).as_posix()
    if not isinstance(source, CanonicalPlateSource):
        return source.model_copy(update={
            "relative_path": relative_path,
            "source_generation": 1,
        })

    images = []
    for image in source.images:
        image_source = image.source.model_copy(update={
            "relative_path": relative_path,
            "source_generation": 1,
        })
        labels = tuple(
            label.model_copy(update={
                "source": label.source.model_copy(update={
                    "relative_path": relative_path,
                }) if label.source is not None else None,
            })
            for label in image.labels
        )
        images.append(image.model_copy(update={
            "source": image_source,
            "labels": labels,
        }))
    return source.model_copy(update={
        "relative_path": relative_path,
        "source_generation": 1,
        "images": tuple(images),
    })


def _plan_canonical_consolidation(records, storage_roots):
    """Select one equivalent canonical store and its stable destination."""
    if not records:
        raise ValueError("Canonical consolidation requires registrations")
    signatures = {
        _canonical_content_signature(record["source"])
        for record in records
    }
    if len(signatures) != 1:
        raise ValueError("Canonical registrations contain different canonical content")

    first = records[0]["source"]
    object_type = (
        "Plate" if isinstance(first, CanonicalPlateSource)
        else first.source_object_type
    )
    object_id = first.source_object_id

    native_locations = {
        (record["source"].storage_root, record["source"].relative_path)
        for record in records
        if _stable_canonical_relative_path(
            record["source"].relative_path, object_type, object_id,
        ) == Path(record["source"].relative_path)
        and Path(record["source"].relative_path).name
            != f"{object_type}-{object_id}.ome.zarr"
    }
    if len(native_locations) > 1:
        raise ValueError(
            "Canonical registrations point at multiple managed input Zarrs"
        )

    def preference(record):
        source = record["source"]
        relative = Path(source.relative_path)
        stable = _stable_canonical_relative_path(
            relative, object_type, object_id,
        )
        stable_name = f"{object_type}-{object_id}.ome.zarr"
        if stable == relative and relative.name != stable_name:
            kind = 0  # Existing managed input Zarr: never copy it.
        elif stable == relative:
            kind = 1  # Already uses the stable generated name.
        else:
            kind = 2  # Legacy gN store; prefer the latest only as input.
        return kind, -source.source_generation

    selected = min(records, key=preference)
    source = selected["source"]
    relative = _stable_canonical_relative_path(
        source.relative_path, object_type, object_id,
    )
    try:
        root = Path(storage_roots[source.storage_root]).resolve()
    except KeyError as error:
        raise ValueError(
            f"Unknown managed storage root: {source.storage_root}"
        ) from error
    destination = (root / relative).resolve()
    if not destination.is_relative_to(root):
        raise ValueError("Canonical destination escapes its managed storage root")
    return {
        "selected": selected,
        "source": _normalize_canonical_source(source, relative),
        "relative_path": relative,
        "destination": destination,
    }


def _annotation_links(conn, object_type, namespace):
    offset = 0
    while True:
        page = list(conn.getAnnotationLinks(
            object_type,
            ns=namespace,
            params=ParametersI().page(offset, 500),
        ))
        yield from page
        if len(page) < 500:
            return
        offset += len(page)


def _canonical_marker_status(marker, source, annotation_id):
    """Classify repairable marker drift without accepting new pixels."""
    if marker is None:
        return "missing"
    if _canonical_content_signature(marker) != _canonical_content_signature(
        source
    ):
        raise ValueError(
            "Canonical marker pixel identity disagrees with annotation "
            f"{annotation_id}"
        )
    return "matching" if marker == source else "locator-drift"


def _bind_canonical_plate_source(source, index):
    """Bind a shared physical Plate inventory to one OMERO Plate."""
    images = []
    for image in source.images:
        image_source = image.source.model_copy(update={
            "storage_root": index.storage_root,
            "relative_path": index.relative_path,
            "source_object_id": index.source_object_id,
            "source_generation": index.source_generation,
        })
        labels = tuple(
            label.model_copy(update={
                "source": label.source.model_copy(update={
                    "storage_root": index.storage_root,
                    "relative_path": index.relative_path,
                }) if label.source is not None else None,
            })
            for label in image.labels
        )
        images.append(image.model_copy(update={
            "source": image_source,
            "labels": labels,
        }))
    return source.model_copy(update={
        "storage_root": index.storage_root,
        "relative_path": index.relative_path,
        "source_object_id": index.source_object_id,
        "source_generation": index.source_generation,
        "images": tuple(images),
    })


def discover_canonical_registrations(
    conn, storage_roots, *, object_type="All", object_ids=None,
):
    """Load every canonical registration and its authoritative marker."""
    if not conn.isAdmin():
        raise ValueError("Canonical consolidation requires an administrator")
    kinds = ("Image", "Plate") if object_type == "All" else (object_type,)
    selected = set(object_ids or ())
    records = []
    original_group = conn.SERVICE_OPTS.getOmeroGroup()
    conn.SERVICE_OPTS.setOmeroGroup("-1")
    try:
        for kind in kinds:
            namespace = (
                CANONICAL_SOURCE_NAMESPACE if kind == "Image"
                else CANONICAL_PLATE_SOURCE_NAMESPACE
            )
            for link in _annotation_links(conn, kind, namespace):
                parent = link.getParent()
                ident = parent.getId()
                if selected and ident not in selected:
                    continue
                annotation = link.getAnnotation()
                pairs = [list(pair) for pair in annotation.getValue()]
                values = _pairs_to_values(pairs)
                shared_plate_marker = False
                if kind == "Image":
                    source = CanonicalZarrSource.from_annotation_values(values)
                elif "images" in values:
                    source = CanonicalPlateSource.from_annotation_values(values)
                else:
                    index = CanonicalPlateIndex.from_annotation_values(values)
                    path = _resolve_store_path(index.to_annotation_values(),
                                               storage_roots)
                    marker_source = load_canonical_marker(path)
                    if not isinstance(marker_source, CanonicalPlateSource):
                        raise ValueError(
                            f"Plate {ident} canonical marker is missing or invalid"
                        )
                    source = _bind_canonical_plate_source(
                        marker_source, index,
                    )
                    shared_plate_marker = True
                if source.source_object_id != ident:
                    raise ValueError(
                        f"Canonical annotation {annotation.getId()} is linked "
                        f"to the wrong {kind}"
                    )
                path = _resolve_store_path(source.to_annotation_values(),
                                           storage_roots)
                if not path.is_dir():
                    raise ValueError(
                        f"Canonical store does not exist for annotation "
                        f"{annotation.getId()}: {path}"
                    )
                marker = load_canonical_marker(path)
                # Early prerelease registrations did not consistently write
                # a sidecar for reused managed Zarrs. Apply mode repairs that
                # omission, but a different pixel identity remains fatal.
                if shared_plate_marker and marker is not None:
                    marker_status = (
                        "matching"
                        if marker.storage_root == source.storage_root
                        and marker.relative_path == source.relative_path
                        and len(marker.images) == len(source.images)
                        and sum(len(image.labels) for image in marker.images)
                        == sum(len(image.labels) for image in source.images)
                        else "locator-drift"
                    )
                else:
                    marker_status = _canonical_marker_status(
                        marker, source, annotation.getId(),
                    )
                records.append({
                    "object_type": kind,
                    "object_id": ident,
                    "group_id": parent.getDetails().group.id.val,
                    "annotation_id": annotation.getId(),
                    "pairs": pairs,
                    "values": values,
                    "source": source,
                    "path": path,
                    "marker_status": marker_status,
                })
    finally:
        conn.SERVICE_OPTS.setOmeroGroup(original_group)
    return records


def discover_shallow_references(
    conn, *, object_type="All", object_ids=None,
):
    """Discover schema-2 result references that may use canonical stores."""
    kinds = ("Image", "Plate") if object_type == "All" else (object_type,)
    selected = set(object_ids or ())
    records = []
    original_group = conn.SERVICE_OPTS.getOmeroGroup()
    conn.SERVICE_OPTS.setOmeroGroup("-1")
    try:
        for kind in kinds:
            for link in _annotation_links(
                conn, kind, SHALLOW_COLLECTION_NAMESPACE,
            ):
                parent = link.getParent()
                ident = parent.getId()
                if selected and ident not in selected:
                    continue
                annotation = link.getAnnotation()
                pairs = [list(pair) for pair in annotation.getValue()]
                values = _pairs_to_values(pairs)
                if values.get("schema") != "2":
                    continue
                reference = (
                    ShallowZarrReference.from_annotation_values(values)
                    if kind == "Image"
                    else ShallowPlateReference.from_annotation_values(values)
                )
                records.append({
                    "object_type": kind,
                    "object_id": ident,
                    "group_id": parent.getDetails().group.id.val,
                    "annotation_id": annotation.getId(),
                    "pairs": pairs,
                    "values": values,
                    "reference": reference,
                })
    finally:
        conn.SERVICE_OPTS.setOmeroGroup(original_group)
    return records


def _managed_locators(value):
    """Collect managed storage locators from a contract tree."""
    locators = set()
    if isinstance(value, dict):
        if "storageRoot" in value and "relativePath" in value:
            locators.add((str(value["storageRoot"]), str(value["relativePath"])))
        for item in value.values():
            locators.update(_managed_locators(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            locators.update(_managed_locators(item))
    return locators


def _requires_locator_update(value, replacements):
    if isinstance(value, dict):
        locator = (value.get("storageRoot"), value.get("relativePath"))
        if locator in replacements and (
            value.get("relativePath") != replacements[locator]
            or value.get("sourceGeneration", 1) != 1
        ):
            return True
        return any(
            _requires_locator_update(item, replacements)
            for item in value.values()
        )
    if isinstance(value, (list, tuple)):
        return any(_requires_locator_update(item, replacements)
                   for item in value)
    return False


def _shallow_reference_values(record, manifest):
    values = record["values"]
    common = {
        "storage_root": values["storageRoot"],
        "relative_path": values["relativePath"],
    }
    if record["object_type"] == "Image":
        reference = ShallowZarrReference.from_manifest(
            manifest,
            image_node_path=values["imageNodePath"],
            label_node_paths=tuple(json.loads(values["labelNodePaths"])),
            **common,
        )
    else:
        reference = ShallowPlateReference.from_manifest(manifest, **common)
    return reference.to_annotation_values()


def _tree_metrics(path):
    files = 0
    size = 0
    for item in path.rglob("*"):
        if item.is_file() and not item.is_symlink():
            files += 1
            size += item.stat().st_size
    return {"files": files, "bytes": size}


def _canonical_marker_path(path):
    internal = path / CANONICAL_MARKER_NAME
    external = external_canonical_marker_path(path)
    markers = [candidate for candidate in (internal, external)
               if candidate.is_file()]
    if len(markers) != 1:
        raise ValueError(
            f"Canonical store must have exactly one marker: {path}"
        )
    return markers[0]


def _write_canonical_marker(path, source):
    try:
        marker_path = _canonical_marker_path(path)
    except ValueError:
        internal = path / CANONICAL_MARKER_NAME
        external = external_canonical_marker_path(path)
        if internal.exists() or external.exists():
            raise
        return write_indexed_canonical_marker(path, source)
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    if marker.get("state") != "committed" or "markerSchema" not in marker:
        raise ValueError(f"Canonical marker is not committed: {marker_path}")
    marker["source"] = source.to_dict()
    _write_json(marker_path, marker)
    if load_canonical_marker(path) != source:
        raise ValueError(f"Canonical marker update did not validate: {path}")
    return marker_path


def _move_canonical_store(source, destination):
    """Rename one generated store and any external marker without copying."""
    if source == destination:
        return
    if destination.exists():
        raise FileExistsError(f"Stable canonical destination exists: {destination}")
    old_external = external_canonical_marker_path(source)
    new_external = external_canonical_marker_path(destination)
    source.rename(destination)
    if old_external.is_file():
        new_external.parent.mkdir(parents=True, exist_ok=True)
        old_external.rename(new_external)


def _canonical_registration_values(source):
    if isinstance(source, CanonicalPlateSource):
        return CanonicalPlateIndex.from_source(source).to_annotation_values()
    return source.to_annotation_values()


def _delete_annotation(conn, record):
    conn.SERVICE_OPTS.setOmeroGroup(str(record["group_id"]))
    conn.deleteObjects("Annotation", [record["annotation_id"]], wait=True)


def _legacy_plate_records(conn, plate_id):
    records = []
    for namespace in (
        CANONICAL_PLATE_IMAGE_NAMESPACE,
        CANONICAL_PLATE_LABEL_NAMESPACE,
    ):
        for link in _annotation_links(conn, "Plate", namespace):
            parent = link.getParent()
            if parent.getId() != plate_id:
                continue
            annotation = link.getAnnotation()
            records.append({
                "group_id": parent.getDetails().group.id.val,
                "annotation_id": annotation.getId(),
            })
    return records


def plan_canonical_consolidation(
    conn, storage_roots, *, object_type="All", object_ids=None,
):
    records = discover_canonical_registrations(
        conn, storage_roots, object_type=object_type, object_ids=object_ids,
    )
    grouped = {}
    for record in records:
        grouped.setdefault(
            (record["object_type"], record["object_id"]), []
        ).append(record)

    plans = []
    replacements = {}
    for (kind, ident), group_records in grouped.items():
        plan = _plan_canonical_consolidation(group_records, storage_roots)
        legacy_metadata = (
            _legacy_plate_records(conn, ident) if kind == "Plate" else []
        )
        selected_path = plan["selected"]["path"]
        legacy_paths = []
        for record in group_records:
            source = record["source"]
            old_path = record["path"]
            if old_path != plan["destination"]:
                expected = _stable_canonical_relative_path(
                    source.relative_path, kind, ident,
                )
                if expected == Path(source.relative_path):
                    if old_path != selected_path:
                        raise ValueError(
                            f"Refusing to remove non-generated canonical "
                            f"store: {old_path}"
                        )
                else:
                    legacy_paths.append(old_path)
        changed = (
            len(group_records) != 1
            or selected_path != plan["destination"]
            or plan["selected"]["source"] != plan["source"]
            or any(record["marker_status"] != "matching"
                   for record in group_records)
            or bool(legacy_metadata)
        )
        if not changed:
            continue
        for record in group_records:
            source = record["source"]
            replacements[(source.storage_root, source.relative_path)] = (
                plan["source"].relative_path
            )
        reclaim = {"files": 0, "bytes": 0}
        for path in set(legacy_paths):
            if path == selected_path and selected_path != plan["destination"]:
                continue
            metrics = _tree_metrics(path)
            reclaim["files"] += metrics["files"]
            reclaim["bytes"] += metrics["bytes"]
        plan.update({
            "object_type": kind,
            "object_id": ident,
            "records": group_records,
            "legacy_paths": tuple(sorted(set(legacy_paths))),
            "legacy_metadata": tuple(legacy_metadata),
            "reclaim": reclaim,
        })
        plans.append(plan)
    return plans, replacements


def _apply_canonical_consolidation(
    conn, plans, replacements, storage_roots, directory,
):
    """Apply prevalidated consolidation, deleting duplicates only at the end."""
    if not plans:
        return []
    relevant = set(replacements)
    shallow_records = discover_shallow_references(conn)
    shallow_groups = {}
    for record in shallow_records:
        store = _resolve_store_path(record["values"], storage_roots)
        manifest = ShallowManifest.from_dict(json.loads(
            (store / SHALLOW_COLLECTION_MANIFEST).read_text(encoding="utf-8")
        ))
        manifest_value = manifest.to_dict()
        used = _managed_locators(manifest_value).intersection(relevant)
        if used and _requires_locator_update(manifest_value, replacements):
            group = shallow_groups.setdefault(store, {
                "manifest": manifest,
                "records": [],
                "replacements": {},
            })
            group["records"].append(record)
            group["replacements"].update({
                locator: replacements[locator] for locator in used
            })

    results = []
    for index, plan in enumerate(plans, start=1):
        selected_path = plan["selected"]["path"]
        destination = plan["destination"]
        snapshot = {
            "objectType": plan["object_type"],
            "objectId": plan["object_id"],
            "selected": str(selected_path),
            "destination": str(destination),
            "annotations": [record["annotation_id"]
                            for record in plan["records"]],
            "markerStatus": {
                str(record["annotation_id"]): record["marker_status"]
                for record in plan["records"]
            },
            "reclaim": plan["reclaim"],
        }
        _write_json(directory / f"canonical-{index:06d}.json", snapshot)
        _move_canonical_store(selected_path, destination)
        marker_backup = directory / f"canonical-{index:06d}-marker.json"
        try:
            marker_path = _canonical_marker_path(destination)
        except ValueError:
            marker_path = None
        if marker_path is not None:
            shutil.copy2(marker_path, marker_backup)
        _write_canonical_marker(destination, plan["source"])
        results.append(snapshot)

    for index, (store, group) in enumerate(shallow_groups.items(), start=1):
        migration = rebind_shallow_store_sources(
            store,
            group["replacements"],
            backup_path=directory / f"shallow-{index:06d}-files",
        )
        for record in group["records"]:
            _write_annotation(
                conn, record,
                _shallow_reference_values(record, migration.manifest),
            )

    for plan in plans:
        selected = plan["selected"]
        _write_annotation(
            conn, selected, _canonical_registration_values(plan["source"]),
        )
        for record in plan["records"]:
            if record["annotation_id"] != selected["annotation_id"]:
                _delete_annotation(conn, record)
        if plan["object_type"] == "Plate":
            for record in plan["legacy_metadata"]:
                _delete_annotation(conn, record)

    # This is the only destructive phase. Every live reference and marker now
    # resolves to the stable store, and only generated gN directories qualify.
    for plan in plans:
        destination = plan["destination"]
        selected_old = plan["selected"]["path"]
        for path in plan["legacy_paths"]:
            if path == selected_old and selected_old != destination:
                continue
            if path.exists():
                shutil.rmtree(path)
            external_canonical_marker_path(path).unlink(missing_ok=True)
    return results


def _build_group_plan(records, storage_roots, manifest):
    """Build the coupled filesystem/OMERO plan for one shallow store."""
    stores = {
        _resolve_store_path(record["values"], storage_roots)
        for record in records
    }
    if len(stores) != 1:
        raise ValueError("A migration group must reference one shallow store")
    annotations = []
    for record in records:
        after = upgrade_annotation_reference_v1(record["values"], manifest)
        annotations.append({
            "record": record,
            "before": record["values"],
            "after": after,
        })
    return {"store_path": stores.pop(), "annotations": annotations}


def _write_annotation(conn, record, values):
    conn.SERVICE_OPTS.setOmeroGroup(str(record["group_id"]))
    annotation = conn.getObject("MapAnnotation", record["annotation_id"])
    if annotation is None:
        raise ValueError(
            f"MapAnnotation {record['annotation_id']} no longer exists"
        )
    annotation.setValue([[key, value] for key, value in values.items()])
    annotation.save()


def _apply_annotation_updates(conn, plan, *, writer=None):
    """Write projections and restore every attempted write on failure."""
    writer = writer or _write_annotation
    attempted = []
    try:
        for item in plan["annotations"]:
            attempted.append(item)
            writer(conn, item["record"], item["after"])
    except Exception as error:
        rollback_errors = []
        for item in reversed(attempted):
            try:
                writer(conn, item["record"], item["before"])
            except Exception as rollback_error:
                rollback_errors.append(str(rollback_error))
        if rollback_errors:
            raise RuntimeError(
                "OMERO shallow-reference migration failed and annotation "
                f"rollback was incomplete: {'; '.join(rollback_errors)}"
            ) from error
        raise


def _preflight_annotation(conn, record):
    linked = []
    for kind in (
        "Project", "Dataset", "Image", "Screen", "Plate", "Well",
        "PlateAcquisition", "Annotation",
    ):
        linked.extend(
            (kind, link)
            for link in conn.getAnnotationLinks(
                kind, ann_ids=[record["annotation_id"]]
            )
        )
    if (
        len(linked) != 1
        or linked[0][0] != record["object_type"]
        or linked[0][1].getParent().getId() != record["object_id"]
    ):
        raise ValueError(
            "Shared or unexpected shallow-reference annotation links: "
            f"{record['annotation_id']}"
        )
    current = conn.getObject("MapAnnotation", record["annotation_id"])
    if current is None:
        raise ValueError(
            f"MapAnnotation {record['annotation_id']} no longer exists"
        )
    if current.getNs() != SHALLOW_COLLECTION_NAMESPACE:
        raise ValueError(
            f"MapAnnotation {record['annotation_id']} namespace changed"
        )
    if [list(pair) for pair in current.getValue()] != record["pairs"]:
        raise ValueError(
            f"MapAnnotation {record['annotation_id']} changed after discovery"
        )


def _parse_object_ids(value):
    if not value:
        return set()
    ids = set()
    for item in str(value).replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        ident = int(item)
        if ident <= 0:
            raise ValueError("Object IDs must be positive integers")
        ids.add(ident)
    return ids


def discover_schema_1_references(conn, *, object_type="All", object_ids=None):
    """Discover prerelease shallow references across visible OMERO groups."""
    if not conn.isAdmin():
        raise ValueError("Shallow-storage migration requires an administrator")
    kinds = ("Image", "Plate") if object_type == "All" else (object_type,)
    selected = set(object_ids or ())
    original_group = conn.SERVICE_OPTS.getOmeroGroup()
    records = []
    conn.SERVICE_OPTS.setOmeroGroup("-1")
    try:
        for kind in kinds:
            offset = 0
            while True:
                page = list(conn.getAnnotationLinks(
                    kind,
                    ns=SHALLOW_COLLECTION_NAMESPACE,
                    params=ParametersI().page(offset, 500),
                ))
                for link in page:
                    ident = link.getParent().getId()
                    if selected and ident not in selected:
                        continue
                    annotation = link.getAnnotation()
                    pairs = [list(pair) for pair in annotation.getValue()]
                    values = _pairs_to_values(pairs)
                    if values.get("schema") != "1":
                        continue
                    if values.get("model") != "rfc8-shallow-copy":
                        raise ValueError(
                            "Unknown schema-1 shallow model on MapAnnotation "
                            f"{annotation.getId()}"
                        )
                    records.append({
                        "object_type": kind,
                        "object_id": ident,
                        "group_id": link.getParent().getDetails().group.id.val,
                        "annotation_id": annotation.getId(),
                        "pairs": pairs,
                        "values": values,
                    })
                if len(page) < 500:
                    break
                offset += len(page)
    finally:
        conn.SERVICE_OPTS.setOmeroGroup(original_group)
    return records


def _load_planning_manifest(store_path):
    path = store_path / SHALLOW_COLLECTION_MANIFEST
    raw = json.loads(path.read_text(encoding="utf-8"))
    schema = raw.get("schema")
    if schema == 1:
        return schema, upgrade_manifest_v1(raw, store_path=store_path)
    if schema == 2:
        return schema, ShallowManifest.from_dict(raw)
    raise ValueError(f"Unsupported shallow manifest schema at {path}: {schema}")


def _json_record(item):
    record = item["record"]
    return {
        "objectType": record["object_type"],
        "objectId": record["object_id"],
        "groupId": record["group_id"],
        "annotationId": record["annotation_id"],
        "before": item["before"],
        "after": item["after"],
    }


def _write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def _migration_directory_path(value):
    if value:
        directory = Path(value)
        if not directory.is_absolute():
            raise ValueError("Backup Directory must be an absolute worker path")
    else:
        root = Path("/data/biomero-shallow-migrations")
        directory = root / f"shallow-storage-{uuid4()}"
    return directory


def _preflight_migration_directory(directory):
    """Verify that the exact worker path can be created, then clean it up."""
    if directory.exists():
        raise FileExistsError(
            f"Backup Directory already exists inside biomeroworker: {directory}"
        )

    missing = []
    current = directory
    while not current.exists():
        missing.append(current)
        parent = current.parent
        if parent == current:
            raise FileNotFoundError(
                f"Cannot resolve Backup Directory inside biomeroworker: "
                f"{directory}"
            )
        current = parent
    if not current.is_dir():
        raise NotADirectoryError(
            f"Backup Directory parent is not a directory inside "
            f"biomeroworker: {current}"
        )

    created = []
    try:
        for path in reversed(missing):
            path.mkdir(mode=0o700)
            created.append(path)
    except OSError as error:
        raise PermissionError(
            error.errno,
            "Cannot create Backup Directory inside biomeroworker: "
            f"{directory}: {error}",
            str(directory),
        ) from error
    finally:
        for path in reversed(created):
            try:
                path.rmdir()
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning(
                    "Could not remove migration preflight directory %s", path,
                )
                break
    return directory


def _create_migration_directory(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    return directory


def migrate_schema_1_references(
    conn,
    *,
    dry_run=True,
    object_type="All",
    object_ids=None,
    backup_directory=None,
):
    """Upgrade storage metadata and OMERO projections as one admin action."""
    if not conn.isAdmin():
        raise ValueError("Shallow-storage migration requires an administrator")
    _require_migration_capabilities()
    planned_directory = _migration_directory_path(backup_directory)
    _preflight_migration_directory(planned_directory)
    roots = load_managed_storage_roots()
    canonical_plans, canonical_replacements = plan_canonical_consolidation(
        conn, roots, object_type=object_type, object_ids=object_ids,
    )
    records = discover_schema_1_references(
        conn, object_type=object_type, object_ids=object_ids,
    )
    grouped = {}
    for record in records:
        key = (
            record["values"]["storageRoot"],
            record["values"]["relativePath"],
        )
        grouped.setdefault(key, []).append(record)

    directory = (
        None if dry_run else _create_migration_directory(planned_directory)
    )
    report = {
        "migration": "biomero-shallow-storage",
        "operations": ["schema-1-to-2", "canonical-single-store"],
        "dryRun": dry_run,
        "backupDirectory": str(planned_directory),
        "stores": [],
        "counts": {
            "stores": len(grouped),
            "annotations": len(records),
            "migrated": 0,
            "recovered": 0,
            "failed": 0,
            "canonicalObjects": len(canonical_plans),
            "canonicalStoresRemoved": sum(
                len(plan["legacy_paths"]) - (
                    1 if plan["selected"]["path"] != plan["destination"]
                    and plan["selected"]["path"] in plan["legacy_paths"]
                    else 0
                )
                for plan in canonical_plans
            ),
            "canonicalBytesReclaimed": sum(
                plan["reclaim"]["bytes"] for plan in canonical_plans
            ),
        },
        "canonical": [{
            "objectType": plan["object_type"],
            "objectId": plan["object_id"],
            "selected": str(plan["selected"]["path"]),
            "destination": str(plan["destination"]),
            "registrations": len(plan["records"]),
            "reclaim": plan["reclaim"],
        } for plan in canonical_plans],
    }
    original_group = conn.SERVICE_OPTS.getOmeroGroup()
    try:
        for index, group_records in enumerate(grouped.values(), start=1):
            store_path = _resolve_store_path(group_records[0]["values"], roots)
            schema, manifest = _load_planning_manifest(store_path)
            plan = _build_group_plan(group_records, roots, manifest)
            item = {
                "store": str(store_path),
                "sourceSchema": schema,
                "status": "planned" if dry_run else "pending",
                "annotations": [_json_record(change)
                                for change in plan["annotations"]],
            }
            report["stores"].append(item)
            if dry_run:
                continue

            for record in group_records:
                conn.SERVICE_OPTS.setOmeroGroup("-1")
                _preflight_annotation(conn, record)

            snapshot = directory / f"store-{index:06d}-omero.json"
            _write_json(snapshot, item)
            migration = None
            try:
                if schema == 1:
                    migration = migrate_shallow_store_v1(
                        store_path,
                        backup_path=directory / f"store-{index:06d}-files",
                    )
                    plan = _build_group_plan(group_records, roots,
                                             migration.manifest)
                _apply_annotation_updates(conn, plan)
            except Exception:
                if migration is not None:
                    try:
                        restore_shallow_store_v1(
                            store_path, migration.backup_path,
                        )
                    except Exception as restore_error:
                        item["status"] = "failed-partial"
                        item["error"] = (
                            "OMERO update failed and filesystem rollback also "
                            f"failed: {restore_error}"
                        )
                        report["counts"]["failed"] += 1
                        _write_json(directory / "report.json", report)
                        raise RuntimeError(item["error"]) from restore_error
                item["status"] = "failed-rolled-back"
                report["counts"]["failed"] += 1
                _write_json(directory / "report.json", report)
                raise
            else:
                item["status"] = (
                    "migrated" if schema == 1 else "reference-recovered"
                )
                counter = "migrated" if schema == 1 else "recovered"
                report["counts"][counter] += 1
                item["filesystemBackup"] = (
                    str(migration.backup_path) if migration else None
                )
                _write_json(directory / "report.json", report)
        if not dry_run and canonical_plans:
            report["canonical"] = _apply_canonical_consolidation(
                conn,
                canonical_plans,
                canonical_replacements,
                roots,
                directory,
            )
            _write_json(directory / "report.json", report)
    finally:
        conn.SERVICE_OPTS.setOmeroGroup(original_group)
    return report


def _summary(report):
    counts = report["counts"]
    if report["dryRun"]:
        return (
            "Shallow-storage migration dry run: "
            f"{counts['stores']} stores and {counts['annotations']} OMERO "
            "references would be upgraded from schema 1 to schema 2; "
            f"{counts['canonicalObjects']} canonical objects would be "
            "consolidated, reclaiming approximately "
            f"{counts['canonicalBytesReclaimed']} bytes. "
            "No data was changed. Backup location validated inside "
            f"biomeroworker: {report['backupDirectory']}"
        )
    return (
        "Shallow-storage migration complete: "
        f"{counts['migrated']} stores migrated, "
        f"{counts['recovered']} reference-only recoveries, "
        f"{counts['canonicalObjects']} canonical objects consolidated, "
        f"{counts['failed']} failures. Backups and report: "
        f"{report['backupDirectory']}"
    )


def runScript():
    client = scripts.client(
        "BIOMERO Migrate Shallow Storage (Admin Only)",
        """Migrate older BIOMERO shallow-storage metadata and consolidate
        canonical source registrations. Run a dry run first. Applying always
        creates recovery files; image pixels are not copied.""",
        scripts.String(
            OBJECT_TYPE,
            default="All",
            values=[rstring("All"), rstring("Image"), rstring("Plate")],
            description="Limit discovery to Images, Plates, or both.",
        ),
        scripts.String(
            OBJECT_IDS,
            optional=True,
            description=(
                "Optional comma-separated OMERO IDs. IDs apply to the selected "
                "object type; leave empty to inspect all shallow results."
            ),
        ),
        scripts.Bool(
            DRY_RUN,
            default=True,
            description=(
                "Preview the complete migration and verify the exact Backup "
                "Directory inside biomeroworker without retaining files."
            ),
        ),
        scripts.String(
            BACKUP_DIRECTORY,
            optional=True,
            description=(
                "Optional new absolute worker directory for recovery files. "
                "Leave empty for /data/biomero-shallow-migrations."
            ),
        ),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact="cellularimaging@amsterdamumc.nl",
        authorsInstitutions=[[1]],
    )
    try:
        conn = BlitzGateway(client_obj=client)
        if not conn.isAdmin():
            raise ValueError("This script requires an OMERO administrator")
        client.enableKeepAlive(60)
        object_type = unwrap(client.getInput(OBJECT_TYPE)) or "All"
        if object_type not in {"All", "Image", "Plate"}:
            raise ValueError(f"Unsupported Object Type: {object_type}")
        object_ids = _parse_object_ids(unwrap(client.getInput(OBJECT_IDS)))
        dry_run = unwrap(client.getInput(DRY_RUN))
        if dry_run is None:
            dry_run = True
        report = migrate_schema_1_references(
            conn,
            dry_run=dry_run,
            object_type=object_type,
            object_ids=object_ids,
            backup_directory=unwrap(client.getInput(BACKUP_DIRECTORY)),
        )
        client.setOutput("Message", rstring(_summary(report)))
    except Exception as error:
        logger.exception("Shallow-storage migration failed")
        client.setOutput("Message", rstring(
            f"Shallow-storage migration failed: {error}"
        ))
    finally:
        client.closeSession()


if __name__ == "__main__":
    runScript()
