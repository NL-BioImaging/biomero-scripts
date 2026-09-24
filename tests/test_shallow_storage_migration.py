import ast
import json
import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "admin" / "BIOMERO_Migrate_Shallow_Storage.py"


def _load_helpers():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted = {
        "_pairs_to_values",
        "_resolve_store_path",
        "_build_group_plan",
        "_apply_annotation_updates",
        "_load_planning_manifest",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "Path": Path,
        "json": json,
        "SHALLOW_COLLECTION_MANIFEST": ".biomero-shallow.json",
        "upgrade_manifest_v1": lambda value, **kwargs: (value, kwargs),
        "ShallowManifest": SimpleNamespace(
            from_dict=lambda value: value,
        ),
        "upgrade_annotation_reference_v1": lambda values, _manifest: {
            **{key: value for key, value in values.items() if key != "model"},
            "schema": "2",
            "format": "biomero-shallow-zarr",
        },
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT_PATH),
                 "exec"), namespace)
    assert wanted.issubset(namespace)
    return namespace


def test_schema_1_planning_supplies_store_for_label_reconstruction(tmp_path):
    helpers = _load_helpers()
    store = tmp_path / "result.zarr"
    store.mkdir()
    (store / ".biomero-shallow.json").write_text(
        json.dumps({"schema": 1, "images": []}),
        encoding="utf-8",
    )

    schema, upgraded = helpers["_load_planning_manifest"](store)

    assert schema == 1
    assert upgraded == (
        {"schema": 1, "images": []},
        {"store_path": store},
    )


def _record(annotation_id, relative_path="results/result.zarr"):
    values = {
        "schema": "1",
        "model": "rfc8-shallow-copy",
        "storageRoot": "group-0-data",
        "relativePath": relative_path,
        "workflowId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    }
    return {
        "annotation_id": annotation_id,
        "object_type": "Plate",
        "object_id": 100 + annotation_id,
        "group_id": 3,
        "pairs": [[key, value] for key, value in values.items()],
        "values": values,
    }


def test_group_plan_upgrades_every_omero_projection_once(tmp_path):
    helpers = _load_helpers()
    records = [_record(1), _record(2)]

    plan = helpers["_build_group_plan"](
        records,
        {"group-0-data": tmp_path},
        SimpleNamespace(),
    )

    assert plan["store_path"] == (tmp_path / "results/result.zarr").resolve()
    assert [item["after"]["schema"] for item in plan["annotations"]] == ["2", "2"]
    assert all("model" not in item["after"] for item in plan["annotations"])


def test_group_plan_rejects_references_to_different_stores(tmp_path):
    helpers = _load_helpers()

    with pytest.raises(ValueError, match="one shallow store"):
        helpers["_build_group_plan"](
            [_record(1), _record(2, "results/other.zarr")],
            {"group-0-data": tmp_path},
            SimpleNamespace(),
        )


def test_annotation_write_failure_rolls_back_prior_updates():
    helpers = _load_helpers()
    records = [_record(1), _record(2)]
    plan = {
        "annotations": [
            {
                "record": record,
                "before": record["values"],
                "after": {"schema": "2"},
            }
            for record in records
        ]
    }
    writes = []

    def writer(_conn, record, values):
        writes.append((record["annotation_id"], values["schema"]))
        if record["annotation_id"] == 2 and values["schema"] == "2":
            raise RuntimeError("injected OMERO failure")

    with pytest.raises(RuntimeError, match="injected OMERO failure"):
        helpers["_apply_annotation_updates"](
            object(), plan, writer=writer,
        )

    assert writes == [(1, "2"), (2, "2"), (2, "1"), (1, "1")]


def test_pairs_reject_duplicate_projection_keys():
    helpers = _load_helpers()

    with pytest.raises(ValueError, match="Duplicate"):
        helpers["_pairs_to_values"]([["schema", "1"], ["schema", "2"]])


def test_migration_requires_local_shallower_capabilities():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_require_migration_capabilities"
    )
    namespace = {
        "REQUIRED_MIGRATION_CAPABILITIES": (
            "schema-1-to-2",
            "schema-1-path-only-labels",
        ),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    capabilities = ModuleType("biomero_shallower.capabilities")
    capabilities.require_migrations = Mock()

    with patch.dict(sys.modules, {
        "biomero_shallower.capabilities": capabilities,
    }):
        namespace["_require_migration_capabilities"]()

    capabilities.require_migrations.assert_called_once_with(
        "schema-1-to-2", "schema-1-path-only-labels"
    )


def test_migration_rejects_package_without_capability_api():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_require_migration_capabilities"
    )
    namespace = {
        "REQUIRED_MIGRATION_CAPABILITIES": (
            "schema-1-to-2",
            "schema-1-path-only-labels",
        ),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)

    with patch.dict(sys.modules, {
        "biomero_shallower.capabilities": None,
    }):
        with pytest.raises(RuntimeError, match="Update biomeroworker"):
            namespace["_require_migration_capabilities"]()


def test_incompatible_migration_package_stops_before_discovery():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "migrate_schema_1_references"
    )
    capability_check = Mock(
        side_effect=RuntimeError("missing migration capability")
    )
    load_roots = Mock()
    discover = Mock()
    namespace = {
        "_require_migration_capabilities": capability_check,
        "load_managed_storage_roots": load_roots,
        "discover_schema_1_references": discover,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    connection = SimpleNamespace(isAdmin=lambda: True)

    with pytest.raises(RuntimeError, match="missing migration capability"):
        namespace["migrate_schema_1_references"](connection)

    capability_check.assert_called_once_with()
    load_roots.assert_not_called()
    discover.assert_not_called()
