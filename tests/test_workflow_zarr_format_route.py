"""Native Zarr defaults and the temporary legacy TIFF conversion boundary."""
import ast
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
RUNNER = ROOT / "__workflows" / "SLURM_Run_Workflow.py"
HAS_V2_DEFAULT = "def is_compatible_zarr_source(" in (
    ROOT / "_data" / "_SLURM_Image_Transfer.py").read_text(encoding="utf-8")
pytestmark = pytest.mark.skipif(
    "def is_legacy_tiff_conversion_source(" not in (
        ROOT / "_data" / "_SLURM_Image_Transfer.py").read_text(encoding="utf-8"),
    reason="Requires workflow format-routing source change",
)


@pytest.mark.parametrize("use_zarr,requested,expected", [
    (True, None, "0.5"),
    (True, "0.4", "0.4"),
    (True, "0.5", "0.5"),
    (False, None, "0.4"),
    (False, "0.4", "0.4"),
    (False, "0.5", "0.4"),
])
@pytest.mark.parametrize("recorded", [False, True])
def test_detached_and_batch_child_input_resolution(use_zarr, requested, expected, recorded):
    if HAS_V2_DEFAULT and use_zarr and requested is None:
        expected = "0.4"
    tree = ast.parse(RUNNER.read_text(encoding="utf-8"))
    function = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == "resolve_pipeline_inputs")
    namespace = {
        "unwrap": lambda value: value,
        "constants": SimpleNamespace(
            transfer=SimpleNamespace(OME_VERSION="version", OME_ZARR_VERSION_0_4="0.4",
                                     OME_ZARR_VERSION_0_5="0.5"),
            workflow=SimpleNamespace(USE_ZARR_FORMAT="use_zarr")),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(RUNNER), "exec"), namespace)
    params = {"selected_output": {}, "roi_label_pattern": "", "email": "test@example.org",
              "group": 1, "output_settings": {"output": "Dataset"}}
    inputs = {"use_zarr": use_zarr, "version": requested}
    if recorded:
        params.update(use_zarr_format=use_zarr, ome_zarr_version=requested)
    client = SimpleNamespace(getInput=lambda key: inputs.get(key))
    result = namespace["resolve_pipeline_inputs"](client, None, None, params)
    assert result["use_zarr_format"] == use_zarr
    assert result["ome_zarr_version"] == expected


@pytest.mark.parametrize("script", [
    "_data/_SLURM_Image_Transfer.py",
    "__workflows/SLURM_Run_Workflow.py",
    "__workflows/SLURM_Run_Workflow_Batched.py",
])
def test_script_parameter_default_matches_available_format_policy(script):
    tree = ast.parse((ROOT / script).read_text(encoding="utf-8"))
    parameter = next(node for node in ast.walk(tree)
                     if isinstance(node, ast.Call) and node.args
                     and isinstance(node.args[0], ast.Attribute)
                     and node.args[0].attr == "OME_VERSION")
    default = next(k.value for k in parameter.keywords if k.arg == "default")
    expected = "0_4" if HAS_V2_DEFAULT else "0_5"
    assert ast.unparse(default) == f"constants.transfer.OME_ZARR_VERSION_{expected}"
