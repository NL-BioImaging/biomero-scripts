import ast
import json
import logging
import os
from pathlib import Path, PurePosixPath
from typing import Optional, Tuple
from uuid import UUID
import zipfile

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"
SOURCE_TEXT = SCRIPT_PATH.read_text(encoding="utf-8")
pytestmark = pytest.mark.skipif(
    "def find_existing_result_storage" not in SOURCE_TEXT,
    reason="safe staged-result retry is not available in this revision",
)


def load_find_existing_result_storage(base_path):
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    required_functions = {
        "extracted_results_match_archive",
        "find_existing_result_storage",
        "has_result_retrieval_marker",
    }
    body = [
        node for node in tree.body
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id == "RESULTS_RETRIEVED_MARKER"
                for target in node.targets
            )
        ) or (
            isinstance(node, ast.FunctionDef)
            and node.name in required_functions
        )
    ]
    namespace = {
        "Optional": Optional,
        "PurePosixPath": PurePosixPath,
        "Tuple": Tuple,
        "UUID": UUID,
        "json": json,
        "logger": logging.getLogger(__name__),
        "os": os,
        "zipfile": zipfile,
        "get_importer_group_base_path": lambda _group_name: str(base_path),
    }
    exec(compile(ast.Module(body=body, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["find_existing_result_storage"]


def load_extract_or_reuse():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "extract_or_reuse_slurm_results"
    )
    namespace = {
        "Optional": Optional,
        "SlurmClient": object,
        "Tuple": Tuple,
        "UUID": UUID,
        "os": os,
        "extract_slurm_results_zip": lambda *_args: (_ for _ in ()).throw(
            AssertionError("Staged retries must not retrieve from Slurm")
        ),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["extract_or_reuse_slurm_results"]


def load_write_result_retrieval_marker():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "write_result_retrieval_marker"
    )
    namespace = {
        "UUID": UUID,
        "datetime": __import__("datetime").datetime,
        "json": json,
        "os": os,
        "uuid": __import__("uuid"),
        "RESULTS_RETRIEVED_MARKER": ".biomero-results-retrieved.json",
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["write_result_retrieval_marker"]


def test_import_retry_reuses_latest_complete_staged_result(tmp_path):
    workflow_id = "b4e4a5f8-a7a3-4a76-873b-daa164f0f3e7"
    analyzed = tmp_path / ".analyzed" / workflow_id
    complete = analyzed / "20260827_140415"
    incomplete = analyzed / "20260827_142700"
    complete.mkdir(parents=True)
    incomplete.mkdir()
    (complete / "cells.ome.zarr").mkdir()
    (complete / "omero-490.log").write_text("completed", encoding="utf-8")
    (complete / ".biomero-results-retrieved.json").write_text(
        json.dumps({
            "schema": 1,
            "status": "complete",
            "workflowId": workflow_id,
            "slurmJobId": "490",
        }),
        encoding="utf-8",
    )
    (incomplete / "omero-490.log").write_text("log only", encoding="utf-8")

    find_existing = load_find_existing_result_storage(tmp_path)

    storage_path, log_path = find_existing("system", workflow_id, "490")

    assert storage_path == str(complete)
    assert log_path == str(complete / "omero-490.log")


def test_import_retry_does_not_reuse_log_only_directory(tmp_path):
    workflow_id = "b4e4a5f8-a7a3-4a76-873b-daa164f0f3e7"
    staged = tmp_path / ".analyzed" / workflow_id / "20260827_142700"
    staged.mkdir(parents=True)
    (staged / "omero-490.log").write_text("log only", encoding="utf-8")

    find_existing = load_find_existing_result_storage(tmp_path)

    assert find_existing("system", workflow_id, "490") == (None, None)


def test_import_retry_does_not_reuse_partial_result_directory(tmp_path):
    workflow_id = "b4e4a5f8-a7a3-4a76-873b-daa164f0f3e7"
    staged = tmp_path / ".analyzed" / workflow_id / "20260827_142700"
    staged.mkdir(parents=True)
    (staged / "cells.ome.zarr").mkdir()
    (staged / "cells.ome.zarr" / "partial").write_bytes(b"partial")

    find_existing = load_find_existing_result_storage(tmp_path)

    assert find_existing("system", workflow_id, "490") == (None, None)


def test_import_retry_accepts_complete_legacy_archive(tmp_path):
    workflow_id = "b4e4a5f8-a7a3-4a76-873b-daa164f0f3e7"
    staged = tmp_path / ".analyzed" / workflow_id / "20260827_140415"
    result_file = staged / "cells.ome.zarr" / "0" / "chunk"
    result_file.parent.mkdir(parents=True)
    result_file.write_bytes(b"complete result")
    with zipfile.ZipFile(staged / "490_out.zip", "w") as archive:
        archive.write(result_file, "cells.ome.zarr/0/chunk")

    find_existing = load_find_existing_result_storage(tmp_path)

    storage_path, log_path = find_existing("system", workflow_id, "490")

    assert storage_path == str(staged)
    assert log_path is None


def test_successful_retrieval_writes_matching_completion_marker(tmp_path):
    workflow_id = UUID("b4e4a5f8-a7a3-4a76-873b-daa164f0f3e7")
    write_marker = load_write_result_retrieval_marker()

    marker_path = write_marker(str(tmp_path), workflow_id, "490")
    marker = json.loads(Path(marker_path).read_text(encoding="utf-8"))

    assert marker["schema"] == 1
    assert marker["status"] == "complete"
    assert marker["workflowId"] == str(workflow_id)
    assert marker["slurmJobId"] == "490"
    assert list(tmp_path.glob("*.tmp")) == []


def test_import_retry_skips_slurm_retrieval_for_staged_results(tmp_path):
    staged = tmp_path / "20260827_140415"
    staged.mkdir()
    archive = staged / "490_out.zip"
    archive.write_bytes(b"existing")
    extract_or_reuse = load_extract_or_reuse()

    result = extract_or_reuse(
        object(), "490", None, "system",
        UUID("b4e4a5f8-a7a3-4a76-873b-daa164f0f3e7"),
        "start", str(staged),
    )

    assert result[0] is None
    assert result[1] == str(staged)
    assert result[2] == str(archive)
    assert result[3] == "490_out"
    assert "Reusing results already retrieved" in result[4]
