import ast
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"
SOURCE_TEXT = SCRIPT_PATH.read_text(encoding="utf-8")
pytestmark = pytest.mark.skipif(
    "ROI postprocessing for Plate workflows is not yet supported" not in SOURCE_TEXT,
    reason="the Plate ROI guard is not available in this revision",
)


def load_validate_roi_output_request():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "validate_roi_output_request"
    )
    constants = SimpleNamespace(
        workflow=SimpleNamespace(OUTPUT_CREATE_ROIS="create_rois"),
        transfer=SimpleNamespace(
            DATA_TYPE="Data_Type",
            DATA_TYPE_PLATE="Plate",
        ),
    )
    namespace = {
        "constants": constants,
        "get_roi_script_capability": lambda *_args: (_ for _ in ()).throw(
            AssertionError("Plate ROI requests must stop before capability lookup")
        ),
        "unwrap": lambda value: value,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["validate_roi_output_request"], constants


def test_plate_roi_request_is_disabled_before_workflow_execution():
    validate, constants = load_validate_roi_output_request()
    selected_output = {constants.workflow.OUTPUT_CREATE_ROIS: True}
    client = SimpleNamespace(
        getInput=lambda name: "Plate" if name == "Data_Type" else None
    )

    pattern, warning = validate(client, object(), selected_output, [])

    assert pattern == ""
    assert selected_output[constants.workflow.OUTPUT_CREATE_ROIS] is False
    assert "Plate" in warning
    assert "not yet supported" in warning
