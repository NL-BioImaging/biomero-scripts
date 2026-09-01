import ast
import os
from pathlib import Path
import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
CHECK_SCRIPT = SOURCE_ROOT / "admin" / "SLURM_check_setup.py"
INIT_SCRIPT = SOURCE_ROOT / "admin" / "SLURM_Init_environment.py"


def load_function(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function = next((
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    ), None)
    if function is None:
        pytest.skip(f"{name} is not available on this source revision")
    namespace = {}
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
