import ast
import os
from pathlib import Path

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))


def _zarr_module(path):
    """The module the script imports the Zarr contracts from.

    Older source revisions used the biomero.zarr_contracts re-export of the
    schema; current ones import biomero_schema.zarr directly.
    """
    source = path.read_text(encoding="utf-8")
    return ("biomero_schema.zarr" if "from biomero_schema.zarr import" in source
            else "biomero.zarr_contracts")


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
            and node.module == _zarr_module(path)
        )
    ]

    assert len(imports) == 1
    assert not _guarded_imports(path, _zarr_module(path))


def test_workflow_runner_loads_zarr_contracts_only_inside_feature_gate():
    path = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"
    assert _guarded_imports(path, _zarr_module(path))


def test_scripts_import_zarr_contracts_from_the_schema_package():
    sources = [
        SOURCE_ROOT / "_data" / "_SLURM_Image_Transfer.py",
        SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py",
    ]
    if all(_zarr_module(path) == "biomero.zarr_contracts" for path in sources):
        pytest.skip("source revision predates the direct schema imports")
    for path in sources:
        assert "biomero.zarr_contracts" not in path.read_text(encoding="utf-8")
