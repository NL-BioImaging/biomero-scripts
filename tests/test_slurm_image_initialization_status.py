import ast
import os
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch
import pytest
from pathlib import Path


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
CHECK_SCRIPT = SOURCE_ROOT / "admin" / "SLURM_check_setup.py"
INIT_SCRIPT = SOURCE_ROOT / "admin" / "SLURM_Init_environment.py"
RUN_SCRIPT = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"


def load_function(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function = next((
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    ), None)
    if function is None:
        raise AssertionError(f"{name} is not available on this source revision")
    namespace = {"SlurmClient": object}
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(path), "exec"), namespace)
    return namespace[name]


def sample_status():
    ready = [{
        "kind": "workflow",
        "name": f"workflow-{index}",
        "version": "v1",
        "state": "READY",
        "exit_code": 0,
        "reason": "built and validated",
        "destination": f"/images/workflow-{index}.sif",
    } for index in range(15)]
    failed = [{
        "kind": "workflow",
        "name": "imagej",
        "version": "v2",
        "state": "FAILED",
        "exit_code": 22,
        "reason": "manifest unknown",
        "destination": "/images/imagej.sif",
    }]
    return {
        "counts": {"READY": 15, "RUNNING": 0, "FAILED": 1},
        "images": ready + failed,
    }


@pytest.mark.skipif('def format_metadata_summary' not in INIT_SCRIPT.read_text(encoding='utf-8'),
                    reason='compact Init summary not present')
def test_metadata_summary_is_compact_and_counts_actual_changes():
    formatter = load_function(INIT_SCRIPT, 'format_metadata_summary')
    report = {'dry_run': True, 'view_version': 'v0', 'discovered': 4,
              'counts': {'skipped': 1, 'failed': 0}, 'results': [
                  {'status': 'planned', 'plan': {'annotations': [{'action': 'update'}]}},
                  {'status': 'planned', 'plan': {'annotations': [{'action': 'unchanged'}]}},
                  {'status': 'planned', 'plan': {'annotations': [{'action': 'unlink'}]}},
                  {'status': 'skipped', 'reason': 'private detailed reason'}]}
    result = formatter(report)
    assert 'Would update: 2' in result
    assert 'Unchanged: 1' in result
    assert 'Skipped: 1' in result
    assert 'No OMERO metadata was changed' in result
    assert 'private detailed reason' not in result
    assert len(result) < 500


def test_check_setup_formats_exact_ready_running_failed_counts():
    formatter = load_function(CHECK_SCRIPT, "format_image_pull_status")

    message = formatter(sample_status())

    assert "READY: 15" in message
    assert "RUNNING: 0" in message
    assert "FAILED: 1" in message
    assert "imagej:v2 - manifest unknown (exit 22)" in message
    assert "available version: ''" not in message


def test_initializer_reports_array_id_and_initial_counts():
    formatter = load_function(INIT_SCRIPT, "format_image_submission")

    message = formatter(98765, {
        "counts": {"READY": 15, "RUNNING": 1, "FAILED": 0},
        "images": [],
    })

    assert "Image pull array ID: 98765" in message
    assert "READY: 15" in message
    assert "RUNNING: 1" in message
    assert "FAILED: 0" in message


def test_check_setup_uses_structured_image_status_not_legacy_log():
    source = CHECK_SCRIPT.read_text(encoding="utf-8")

    assert "slurmClient.get_image_pull_status()" in source
    assert "format_image_pull_status(image_status)" in source
    assert "slurmClient.slurm_script_path}/image-pulls" in source
    assert "get_logfile_from_slurm" not in source
    assert "sing.log" not in source


def test_initializer_submits_one_combined_array_and_reads_its_status():
    source = INIT_SCRIPT.read_text(encoding="utf-8")

    assert "converter_specs = slurmClient.prepare_converters()" in source
    assert "extra_image_specs=converter_specs" in source
    assert "image_array_id = slurmClient.setup_container_images(" in source
    assert "image_status = slurmClient.get_image_pull_status()" in source
    assert "get_logfile_from_slurm" not in source
    assert "sing.log" not in source


def test_initializer_adds_configured_remote_shallower_to_image_array():
    source = INIT_SCRIPT.read_text(encoding="utf-8")

    assert "slurmClient.remote_shallow_zarr" in source
    assert "slurmClient.remote_shallower_image" in source
    assert "converter_specs.append(image_spec(slurmClient))" in source


def test_workflow_preflight_accepts_ready_remote_shallower():
    validator = load_function(RUN_SCRIPT, "validate_remote_shallower_ready")
    destination = "/images/shallower.sif"
    client = SimpleNamespace(
        remote_shallow_zarr=True,
        _partition_existing_images=Mock(return_value=([{"destination": destination}], [])),
    )
    remote_shallower = ModuleType("biomero.remote_shallower")
    remote_shallower.image_spec = Mock(return_value={"destination": destination})

    validator.__globals__.update(
        IMPORTER_ENABLED=True,
        SHALLOW_ZARR_ENABLED=True,
    )
    with patch.dict(sys.modules, {"biomero.remote_shallower": remote_shallower}):
        validator(client, use_zarr_format=True)

    client._partition_existing_images.assert_called_once_with(
        [{"destination": destination}]
    )


def test_workflow_preflight_rejects_missing_remote_shallower_before_launch():
    validator = load_function(RUN_SCRIPT, "validate_remote_shallower_ready")
    destination = "/images/shallower.sif"
    client = SimpleNamespace(
        remote_shallow_zarr=True,
        _partition_existing_images=Mock(return_value=([], [{"destination": destination}])),
    )
    remote_shallower = ModuleType("biomero.remote_shallower")
    remote_shallower.image_spec = Mock(return_value={"destination": destination})

    validator.__globals__.update(
        IMPORTER_ENABLED=True,
        SHALLOW_ZARR_ENABLED=True,
    )
    with patch.dict(sys.modules, {"biomero.remote_shallower": remote_shallower}):
        with pytest.raises(RuntimeError, match="SLURM Init Environment"):
            validator(client, use_zarr_format=True)
