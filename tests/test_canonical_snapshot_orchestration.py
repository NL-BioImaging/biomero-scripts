import ast
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from biomero_schema.zarr import (
    CanonicalInput,
    CanonicalInputManifest,
    CanonicalZarrSource,
    PixelIdentity,
)


SCRIPT_PATH = (
    Path(__file__).parents[1] / "__workflows" / "SLURM_Run_Workflow.py"
)
CANONICAL_INPUTS_OUTPUT = "Canonical_Inputs"


def _load_snapshot_functions():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted = {
        "ensure_tracking_uuid",
        "parse_canonical_inputs_output",
        "persist_canonical_input_snapshot",
    }
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "CANONICAL_INPUTS_OUTPUT": CANONICAL_INPUTS_OUTPUT,
        "CanonicalInput": CanonicalInput,
        "CanonicalInputManifest": CanonicalInputManifest,
        "UUID": UUID,
        "json": json,
        "logger": SimpleNamespace(info=lambda *args, **kwargs: None),
        "unwrap": lambda value: getattr(value, "val", value),
        "uuid4": uuid4,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(SCRIPT_PATH),
                 "exec"), namespace)
    missing = wanted.difference(namespace)
    assert not missing, f"Missing snapshot helpers: {sorted(missing)}"
    return namespace


def canonical_input():
    identity = PixelIdentity(
        node_path=".",
        role="image",
        iscc_code="ISCC:KPIXEL",
        data_code="ISCC:GDATA",
        instance_code="ISCC:IINSTANCE",
        tool_version="0.1.0",
        imagewalk_revision="draft-2026-06",
        shape=(1, 1, 1, 16, 16),
        dtype="uint16",
        axes=("t", "c", "z", "y", "x"),
    )
    source = CanonicalZarrSource(
        storage_root="group-3-data",
        relative_path=".processed/Image-7.g1.ome.zarr",
        node_path=".",
        source_object_type="Image",
        source_object_id=7,
        source_generation=1,
        interchange_profile="ngff-0.4-zarr-v2",
        pixel_identity=identity,
        pixel_identity_origin="raw",
        canonical_pixel_verified=True,
    )
    return CanonicalInput(
        ordinal=0,
        selected_object_type="Image",
        selected_object_id=7,
        source=source,
    )


class Wrapped:
    def __init__(self, value):
        self.val = value


class Tracker:
    def __init__(self):
        self.calls = []

    def record_canonical_inputs(self, workflow_id, task_id, inputs):
        self.calls.append((workflow_id, task_id, inputs))


class Slurm:
    def __init__(self):
        self.workflowTracker = Tracker()
        self.manifests = []

    def write_canonical_input_manifest(self, folder, manifest):
        self.manifests.append((folder, manifest))
        return f"/remote/{folder}/.biomero/canonical-inputs.json"


def test_parse_canonical_inputs_output_validates_wire_payload():
    ns = _load_snapshot_functions()
    item = canonical_input()
    results = {
        CANONICAL_INPUTS_OUTPUT: Wrapped(json.dumps([item.to_dict()]))
    }

    parsed = ns["parse_canonical_inputs_output"](results)

    assert parsed == (item,)


def test_parse_canonical_inputs_output_rejects_non_list_payload():
    ns = _load_snapshot_functions()

    with pytest.raises(ValueError, match="JSON list"):
        ns["parse_canonical_inputs_output"]({
            CANONICAL_INPUTS_OUTPUT: Wrapped("{}")
        })


def test_missing_canonical_output_is_an_empty_snapshot():
    ns = _load_snapshot_functions()

    assert ns["parse_canonical_inputs_output"]({}) == ()


def test_persist_snapshot_records_event_and_recovery_manifest():
    ns = _load_snapshot_functions()
    slurm = Slurm()
    workflow_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    task_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    item = canonical_input()

    manifest = ns["persist_canonical_input_snapshot"](
        slurm, workflow_id, task_id, "batch-1", (item,))

    assert isinstance(manifest, CanonicalInputManifest)
    assert slurm.workflowTracker.calls == [
        (workflow_id, task_id, (item,))
    ]
    assert slurm.manifests == [("batch-1", manifest)]


def test_empty_snapshot_does_not_write_partial_provenance():
    ns = _load_snapshot_functions()
    slurm = Slurm()

    result = ns["persist_canonical_input_snapshot"](
        slurm, uuid4(), uuid4(), "batch-1", ())

    assert result is None
    assert slurm.workflowTracker.calls == []
    assert slurm.manifests == []


def test_tracking_uuid_preserves_value_or_generates_fallback():
    ns = _load_snapshot_functions()
    existing = uuid4()

    assert ns["ensure_tracking_uuid"](existing) == existing
    assert isinstance(ns["ensure_tracking_uuid"](None), UUID)
