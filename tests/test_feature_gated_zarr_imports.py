import ast
import os
from pathlib import Path


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))


def _guarded_imports(path, module):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    guarded = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        condition = ast.unparse(node.test)
        if not (
            "IMPORTER_ENABLED" in condition
            and "SHALLOW_ZARR_ENABLED" in condition
        ):
            continue
        guarded.extend(
            child for child in ast.walk(node)
            if isinstance(child, ast.ImportFrom) and child.module == module
        )
    return guarded


def test_image_transfer_loads_zarr_contracts_for_evergreen_restore():
    path = SOURCE_ROOT / "_data" / "_SLURM_Image_Transfer.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = [
        node
        for node in tree.body
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "biomero.zarr_contracts"
        )
    ]

    assert len(imports) == 1
    assert not _guarded_imports(path, "biomero.zarr_contracts")


def test_workflow_runner_loads_zarr_contracts_only_inside_feature_gate():
    path = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"
    assert _guarded_imports(path, "biomero.zarr_contracts")
