from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).parents[1]
    / "__workflows"
    / "SLURM_Run_Workflow.py"
)


def test_run_workflow_declares_plate_label_preview_inputs():
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "constants.results.IMPORT_PLATE_LABEL_PREVIEW" in source
    assert "constants.results.PLATE_LABEL_PREVIEW_NAME" in source


def test_preview_is_only_forwarded_to_importer_backed_shallow_plate_screen():
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "if IMPORTER_ENABLED:" in source
    assert "and SHALLOW_ZARR_ENABLED" in source
    assert "data_type == constants.transfer.DATA_TYPE_PLATE" in source
    assert "selected_output[constants.workflow.OUTPUT_NEW_SCREEN]" in source
    assert (
        "inputs[constants.results.IMPORT_PLATE_LABEL_PREVIEW] = rbool("
        in source
    )
