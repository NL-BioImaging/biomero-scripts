import ast
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "_SLURM_Image_Transfer.py"
SOURCE_TEXT = SCRIPT_PATH.read_text(encoding="utf-8")
pytestmark = pytest.mark.skipif(
    "def write_transfer_input_markers" not in SOURCE_TEXT,
    reason="temporary Zarr input markers are not available in this revision",
)


def load_writer():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "write_transfer_input_markers"
    ]
    assert functions, "write_transfer_input_markers is not implemented"
    namespace = {
        "CanonicalInput": object,
        "Path": Path,
        "TRANSFER_INPUT_MARKER": ".biomero-input.json",
        "json": json,
        "logger": logging.getLogger("transfer-input-marker-test"),
        "os": os,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["write_transfer_input_markers"]


def test_writes_one_marker_into_each_temporary_transfer_store(tmp_path):
    first = tmp_path / "first.zarr"
    second = tmp_path / "second.zarr"
    first.mkdir()
    second.mkdir()
    inputs = (
        SimpleNamespace(
            transfer_artifact="first.zarr",
            selected_object_type="Image",
            selected_object_id=11,
            ordinal=0,
            to_dict=lambda: {"ordinal": 0, "schema": 1},
        ),
        SimpleNamespace(
            transfer_artifact="second.zarr",
            selected_object_type="Image",
            selected_object_id=12,
            ordinal=1,
            to_dict=lambda: {"ordinal": 1, "schema": 1},
        ),
    )

    written = load_writer()(tmp_path, inputs)

    assert written == 2
    assert json.loads(
        (first / ".biomero-input.json").read_text(encoding="utf-8")
    )["ordinal"] == 0
    assert json.loads(
        (second / ".biomero-input.json").read_text(encoding="utf-8")
    )["ordinal"] == 1
