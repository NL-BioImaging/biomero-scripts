#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Administratively migrate BIOMERO shallow-storage contracts."""

import json
import logging
from pathlib import Path
from uuid import uuid4

import omero
from omero import scripts
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, unwrap
from omero.sys import ParametersI

from biomero_schema.zarr import (
    SHALLOW_COLLECTION_MANIFEST,
    SHALLOW_COLLECTION_NAMESPACE,
    ShallowManifest,
)
from biomero_shallower.migration import (
    migrate_shallow_store_v1,
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


def _migration_directory(value):
    if value:
        directory = Path(value)
        if not directory.is_absolute():
            raise ValueError("Backup Directory must be an absolute worker path")
    else:
        root = Path("/data/biomero-shallow-migrations")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory = root / f"schema-1-to-2-{uuid4()}"
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
    roots = load_managed_storage_roots()
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

    directory = None if dry_run else _migration_directory(backup_directory)
    report = {
        "migration": "biomero-shallow-schema-1-to-2",
        "dryRun": dry_run,
        "backupDirectory": str(directory) if directory else None,
        "stores": [],
        "counts": {
            "stores": len(grouped),
            "annotations": len(records),
            "migrated": 0,
            "recovered": 0,
            "failed": 0,
        },
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
    finally:
        conn.SERVICE_OPTS.setOmeroGroup(original_group)
    return report


def _summary(report):
    counts = report["counts"]
    if report["dryRun"]:
        return (
            "Shallow-storage migration dry run: "
            f"{counts['stores']} stores and {counts['annotations']} OMERO "
            "references would be upgraded from schema 1 to schema 2. "
            "No data was changed."
        )
    return (
        "Shallow-storage migration complete: "
        f"{counts['migrated']} stores migrated, "
        f"{counts['recovered']} reference-only recoveries, "
        f"{counts['failed']} failures. Backups and report: "
        f"{report['backupDirectory']}"
    )


def runScript():
    client = scripts.client(
        "BIOMERO Migrate Shallow Storage (Admin Only)",
        """Upgrade prerelease BIOMERO shallow-Zarr schema-1 storage metadata
        and its linked OMERO references to schema 2. Run a dry run first.
        Applying always creates recovery files; image pixels are not copied.""",
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
            description="Preview the complete migration without writing.",
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
