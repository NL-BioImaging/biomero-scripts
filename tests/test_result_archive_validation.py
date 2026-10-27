"""Validate archive content rather than assuming small results are invalid."""
import ast
import logging
import os
import random
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple
from unittest.mock import Mock
from uuid import UUID, uuid4
import zipfile

import pytest

ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
SCRIPT = ROOT / "_data" / "SLURM_Import_Results.py"
pytestmark = pytest.mark.skipif(
    "zip_size < 1024" in SCRIPT.read_text(encoding="utf-8")
    and os.getenv("BIOMERO_TEST_SMALL_ARCHIVES") != "1",
    reason="selected source predates content-based result archive validation",
)


@pytest.fixture
def retrieve(tmp_path):
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name == "extract_slurm_results_zip")
    move = Mock(return_value=str(tmp_path / "permanent"))
    namespace = dict(
        SlurmClient=object, UUID=UUID, Tuple=Tuple, os=os, zipfile=zipfile,
        logger=logging.getLogger(__name__), shallow_remote_results=Mock(),
        move_results_to_permanent_storage=move,
    )
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(SCRIPT), "exec"), namespace)
    slurm = SimpleNamespace(
        extract_data_location_from_log=Mock(return_value="/scratch/job"),
        run_commands=Mock(return_value=SimpleNamespace(ok=True, stdout="1\n")),
        zip_data_on_slurm_server=Mock(return_value=SimpleNamespace(ok=True, stdout="Everything is Ok")),
        copy_zip_locally=Mock(),
    )
    archive = tmp_path / "123_out.zip"

    def run():
        return namespace["extract_slurm_results_zip"](
            slurm, "123", str(tmp_path), "group", uuid4(), "")

    return SimpleNamespace(run=run, archive=archive, move=move, slurm=slurm)


@pytest.mark.parametrize("payload", [b"II*\x00" + bytes(6144), random.Random(0).randbytes(2048)],
                         ids=["compressed-small", "larger"])
def test_valid_result_archive_is_accepted_regardless_of_compressed_size(retrieve, payload):
    with zipfile.ZipFile(retrieve.archive, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("result.tif", payload)
    if payload.startswith(b"II*"):
        assert retrieve.archive.stat().st_size < 1024
    result = retrieve.run()
    assert Path(result[2]) == retrieve.archive
    retrieve.move.assert_called_once()
    assert Path(retrieve.move.call_args.args[-1]) == retrieve.archive
    with zipfile.ZipFile(retrieve.archive) as archive:
        assert archive.read("result.tif") == payload


@pytest.mark.parametrize("contents,message", [
    ("empty", "contains no files"),
    ("directories", "only directories"),
    ("corrupt", "not a valid zip"),
    ("missing", "not found"),
])
def test_unusable_archive_never_reaches_permanent_storage(retrieve, contents, message):
    if contents in ("empty", "directories"):
        with zipfile.ZipFile(retrieve.archive, "w") as archive:
            if contents == "directories":
                archive.writestr("empty/", b"")
    elif contents == "corrupt":
        retrieve.archive.write_bytes(b"not a ZIP archive")
    with pytest.raises(RuntimeError, match=message):
        retrieve.run()
    retrieve.move.assert_not_called()
