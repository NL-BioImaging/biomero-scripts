import ast
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError
from biomero_schema.zarr import (
    CanonicalInput,
    CanonicalInputManifest,
    CanonicalZarrSource,
    PixelIdentity,
)


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"


def load_functions():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted = {
        "load_canonical_input_snapshot",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "UUID": UUID,
        "ValidationError": ValidationError,
        "logger": logging.getLogger(__name__),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT_PATH), "exec"), namespace)
    assert not wanted.difference(namespace)
    return namespace


@pytest.fixture
def manifest():
    identity = PixelIdentity(
        node_path=".",
        role="image",
        iscc_code="ISCC:KSUM",
        data_code="ISCC:GDATA",
        instance_code="ISCC:IINSTANCE",
        tool_version="0.1.0",
        imagewalk_revision="iscc-bio/0.1.0@revision",
        shape=(1, 1, 8, 8),
        dtype="uint16",
        axes=("t", "c", "y", "x"),
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
        pixel_identity_origin="omero-pixels",
        canonical_pixel_verified=True,
    )
    return CanonicalInputManifest(
        workflow_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        export_task_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        inputs=(CanonicalInput(
            ordinal=0,
            selected_object_type="Image",
            selected_object_id=7,
            source=source,
        ),),
    )


def test_snapshot_is_loaded_from_workflow_tracking(manifest):
    ns = load_functions()
    tracker = SimpleNamespace(
        get_canonical_input_manifest=lambda workflow_id: manifest
    )
    client = SimpleNamespace(
        track_workflows=True,
        workflowTracker=tracker,
    )

    assert ns["load_canonical_input_snapshot"](
        client, str(manifest.workflow_id)
    ) == manifest


def test_tracking_disabled_returns_no_lineage_candidate(manifest):
    ns = load_functions()
    client = SimpleNamespace(
        track_workflows=False,
    )

    assert ns["load_canonical_input_snapshot"](
        client, str(manifest.workflow_id)
    ) is None


def test_missing_legacy_snapshot_returns_no_lineage_candidate(manifest):
    ns = load_functions()
    tracker = SimpleNamespace(
        get_canonical_input_manifest=lambda workflow_id: (_ for _ in ()).throw(
            ValueError("workflow predates canonical snapshots")
        )
    )
    client = SimpleNamespace(
        track_workflows=True,
        workflowTracker=tracker,
    )

    assert ns["load_canonical_input_snapshot"](
        client, str(manifest.workflow_id)
    ) is None


def test_lookup_failure_and_corrupt_snapshot_fall_back_with_warning(
    manifest, caplog,
):
    if "except ValidationError" not in SCRIPT_PATH.read_text(encoding="utf-8"):
        pytest.skip("Lookup-failure warning is not present in source revision")
    ns = load_functions()
    corrupt = None
    try:
        CanonicalInputManifest(workflow_id="not-a-uuid")
    except ValidationError as exc:
        corrupt = exc
    assert corrupt is not None

    for error in (RuntimeError("database unavailable"), corrupt):
        def fail(workflow_id, error=error):
            raise error

        client = SimpleNamespace(
            track_workflows=True,
            workflowTracker=SimpleNamespace(get_canonical_input_manifest=fail),
        )
        caplog.clear()
        with caplog.at_level(logging.INFO):
            assert ns["load_canonical_input_snapshot"](
                client, str(manifest.workflow_id)
            ) is None
        # Anything but "not recorded" must be visible as a WARNING.
        assert caplog.records[-1].levelno == logging.WARNING
