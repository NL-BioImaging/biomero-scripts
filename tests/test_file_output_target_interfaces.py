import ast
import os
from pathlib import Path


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))


def source(relative_path):
    return (SOURCE_ROOT / relative_path).read_text(encoding="utf-8")


def referenced_constants(relative_path):
    tree = ast.parse(source(relative_path))
    references = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        if node.attr == "OUTPUT_ATTACH_FILE_OUTPUTS_TARGET":
            references.add(node.attr)
    return references


def test_single_and_batched_workflow_scripts_expose_identical_target_interface():
    expected = {"OUTPUT_ATTACH_FILE_OUTPUTS_TARGET"}

    assert referenced_constants(
        "__workflows/SLURM_Run_Workflow.py"
    ) == expected
    assert referenced_constants(
        "__workflows/SLURM_Run_Workflow_Batched.py"
    ) == expected


def test_batched_script_forwards_the_declared_target_with_child_inputs():
    batched = source("__workflows/SLURM_Run_Workflow_Batched.py")

    assert "inputs = client.getInputs()" in batched
    assert "constants.workflow.OUTPUT_ATTACH_FILE_OUTPUTS_TARGET" in batched
    assert "svc.runScript(script_id, inputs, None)" in batched


def test_both_result_routes_accept_the_forwarded_target_mode():
    expected = {"OUTPUT_ATTACH_FILE_OUTPUTS_TARGET"}

    assert referenced_constants("_data/SLURM_Import_Results.py") == expected
    assert referenced_constants("_data/SLURM_Get_Results.py") == expected
