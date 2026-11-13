"""The workflow runner removes the transfer input marker after the import.

The marker (``.biomero-input.json``) is written into the exported input, comes
back inside the result, and is read by the importer. The importer removes it
only after a local evaluation or when the helper kept the full result, so the
run script removes it from every result once the import succeeded.
"""
import ast
import logging
import os
import subprocess
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from script_output_names import OUTPUT_NAMES

SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
RUN_SCRIPT = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"
IMPORT_SCRIPT = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"
from biomero_schema.zarr import TRANSFER_INPUT_MARKER

# The file name the transfer writes; pinned against the schema constant below.
MARKER = ".biomero-input.json"
FUNCTIONS = (
    "is_biomero_result_folder",
    "log_walk_error",
    "remove_input_markers",
    "remove_input_markers_after_import",
    "importResultsToOmero",
)


class Node(str):
    """A constant that is its own dotted name, like ``constants.a.B``."""

    def __getattr__(self, name):
        if name.startswith("get"):  # not an rtype: unwrap() must skip it
            raise AttributeError(name)
        return Node(f"{self}.{name}")


class FakeResults:
    """``constants.results``: core's real shared output names, and unique
    values for every other constant."""

    def __getattr__(self, name):
        if hasattr(OUTPUT_NAMES, name):
            return getattr(OUTPUT_NAMES, name)
        return Node(f"results.{name}")


class FakeConstants:
    """Stand-in for ``biomero.constants`` with unique string values."""

    file_output_targets = SimpleNamespace(
        USER_VALUES=("input",), INPUT_CONTAINER="input")

    # The shared output names are core's real ones, as the scripts use them.
    results = FakeResults()

    def __getattr__(self, name):
        return Node(name)


class RString:
    """Like ``rstring``: a value that is read with ``getValue()``."""

    def __init__(self, value):
        self.value = value

    def getValue(self):
        return self.value


# The scripts read the output name from biomero.constants.results, which is
# injected here like every other constant.
RESULTS_PATH_OUTPUT = OUTPUT_NAMES.RESULTS_PATH_OUTPUT


def load_functions(shallow_enabled=True, importer_enabled=True, **extra):
    tree = ast.parse(RUN_SCRIPT.read_text(encoding="utf-8"))
    nodes = [n for n in tree.body
             if isinstance(n, ast.FunctionDef) and n.name in FUNCTIONS]
    assert {n.name for n in nodes} == set(FUNCTIONS), (
        "input marker cleanup is not implemented in SLURM_Run_Workflow.py")
    module_constants = {
        "IMPORTER_ENABLED": importer_enabled,
        "SHALLOW_ZARR_ENABLED": shallow_enabled,
        "SHALLOW_ZARR_STORAGE_ENABLED": importer_enabled and shallow_enabled,
        "TRANSFER_INPUT_MARKER": MARKER,
        "RESULTS_FOLDER_NAME": ".analyzed",
        "ZARR_ROOT_FILES": (".zgroup", "zarr.json"),
        "IMPORT_SCRIPTS": ["SLURM_Import_Results.py"],
        "VERSION": "test",
    }
    namespace = {
        "os": os, "Path": Path, "logger": logging.getLogger("marker-test"),
        "unwrap": lambda value: (value.getValue()
                                 if hasattr(value, "getValue") else value),
        "rstring": lambda v: v, "rbool": lambda v: v, "rlist": lambda v: v,
        "rlong": lambda v: v, "Parameters": MagicMock, "UUID": str,
        "BlitzGateway": object, "SlurmClient": object,
        "omscripts": SimpleNamespace(client=object),
        "constants": FakeConstants(), "wf_failed": False,
        "wait_for_job_completion": lambda *a, **k: None,
        **module_constants, **extra,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(RUN_SCRIPT),
                 "exec"), namespace)
    return namespace


def make_store(root: Path, name="result.zarr", shallow=False) -> Path:
    store = root / name
    store.mkdir(parents=True)
    (store / "zarr.json").write_text("{}", encoding="utf-8")
    (store / MARKER).write_text("{}", encoding="utf-8")
    if shallow:
        (store / ".biomero-shallow.json").write_text("{}", encoding="utf-8")
    else:
        (store / "c").mkdir()
        (store / "c" / "0").write_bytes(b"chunk")
    return store


@pytest.fixture
def results_root(tmp_path):
    root = tmp_path / "group" / ".analyzed" / "wf-1" / "20260101_000000"
    root.mkdir(parents=True)
    return root


# --- the helper -----------------------------------------------------------

def test_removes_the_marker_of_every_store_in_the_result(results_root):
    first = make_store(results_root, "a.zarr")
    second = make_store(results_root / "nested", "b.zarr", shallow=True)
    log = results_root / "omero-1.log"
    log.write_text("log", encoding="utf-8")

    removed = load_functions()["remove_input_markers"](
        str(results_root), MARKER)

    assert removed == 2
    assert not (first / MARKER).exists()
    assert not (second / MARKER).exists()
    assert (first / "zarr.json").is_file()
    assert (first / "c" / "0").is_file()
    assert (second / ".biomero-shallow.json").is_file()
    assert log.is_file()


def test_removes_only_files_named_like_the_marker(results_root):
    store = make_store(results_root)
    lookalike = store / f"{MARKER}.bak"
    lookalike.write_text("{}", encoding="utf-8")
    inside_array = store / "c" / MARKER  # below a store root: not a marker
    inside_array.write_text("{}", encoding="utf-8")

    load_functions()["remove_input_markers"](str(results_root), MARKER)

    assert lookalike.is_file()
    assert inside_array.is_file()


def test_never_touches_a_folder_outside_biomero_result_storage(tmp_path):
    user_folder = tmp_path / "group" / "user-data"
    store = make_store(user_folder)

    removed = load_functions()["remove_input_markers"](
        str(user_folder), MARKER)

    assert removed == 0
    assert (store / MARKER).is_file()


def test_a_path_that_climbs_out_of_the_result_folder_is_refused(
        results_root):
    group_folder = results_root.parents[2]
    user_store = make_store(group_folder / "user-data")
    escape = results_root / ".." / ".." / ".." / "user-data"
    assert (escape / user_store.name / MARKER).is_file()

    removed = load_functions()["remove_input_markers"](str(escape), MARKER)

    assert removed == 0
    assert (user_store / MARKER).is_file()


def test_a_missing_result_folder_is_not_an_error(tmp_path):
    missing = tmp_path / ".analyzed" / "gone"

    assert load_functions()["remove_input_markers"](
        str(missing), MARKER) == 0


def test_cleanup_is_repeatable(results_root):
    make_store(results_root)
    remove = load_functions()["remove_input_markers"]

    assert remove(str(results_root), MARKER) == 1
    assert remove(str(results_root), MARKER) == 0


def test_a_marker_that_cannot_be_removed_does_not_fail_the_run(
        results_root, caplog):
    store = make_store(results_root)
    functions = load_functions()

    def refuse(path):
        raise PermissionError("read-only")

    functions["os"] = SimpleNamespace(
        walk=os.walk, remove=refuse, path=os.path)
    with caplog.at_level(logging.WARNING, logger="marker-test"):
        removed = functions["remove_input_markers"](
            str(results_root), MARKER)

    assert removed == 0
    assert (store / MARKER).is_file()
    assert any(r.levelno == logging.WARNING and "Could not remove" in
               r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("depth", [".analyzed", ".analyzed/wf-1"])
def test_refuses_the_storage_folder_and_a_workflow_folder(tmp_path, depth):
    folder = tmp_path / "group" / depth
    store = make_store(folder)
    other_import = make_store(
        tmp_path / "group" / ".analyzed" / "wf-2" / "20260102_000000")

    removed = load_functions()["remove_input_markers"](str(folder), MARKER)

    assert removed == 0
    assert (store / MARKER).is_file()
    assert (other_import / MARKER).is_file()


def test_a_result_folder_linked_to_user_data_is_refused(tmp_path):
    user_store = make_store(tmp_path / "user-data")
    link = tmp_path / "group" / ".analyzed" / "wf-1" / "20260101_000000"
    link.parent.mkdir(parents=True)
    try:
        link.symlink_to(user_store.parent, target_is_directory=True)
    except (OSError, NotImplementedError):
        if os.name != "nt" or subprocess.run(  # a junction resolves alike
                ["cmd", "/c", "mklink", "/J", str(link),
                 str(user_store.parent)],
                capture_output=True).returncode != 0:
            pytest.skip("links cannot be created here")

    removed = load_functions()["remove_input_markers"](str(link), MARKER)

    assert removed == 0
    assert (user_store / MARKER).is_file()


def test_links_inside_a_result_folder_are_not_followed(
        tmp_path, results_root):
    user_store = make_store(tmp_path / "user-data")
    try:
        (results_root / "shortcut").symlink_to(
            user_store.parent, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks cannot be created here")

    removed = load_functions()["remove_input_markers"](
        str(results_root), MARKER)

    assert removed == 0
    assert (user_store / MARKER).is_file()


def test_the_search_never_follows_links(results_root):
    # Also covers platforms where the link test above has to skip.
    make_store(results_root)
    functions = load_functions()
    calls = []

    def recording_walk(top, **kwargs):
        calls.append(kwargs)
        return os.walk(top, **kwargs)

    functions["os"] = SimpleNamespace(
        walk=recording_walk, remove=os.remove, path=os.path)
    functions["remove_input_markers"](str(results_root), MARKER)

    assert calls and calls[0].get("followlinks") is False


def test_an_unreadable_folder_only_warns(results_root, caplog):
    store = make_store(results_root)
    functions = load_functions()

    def failing_walk(top, onerror=None, followlinks=False):
        onerror(PermissionError("no access"))
        return iter(())

    functions["os"] = SimpleNamespace(
        walk=failing_walk, remove=os.remove, path=os.path)
    with caplog.at_level(logging.WARNING, logger="marker-test"):
        removed = functions["remove_input_markers"](
            str(results_root), MARKER)

    assert removed == 0
    assert (store / MARKER).is_file()
    assert any(r.levelno == logging.WARNING and "skipped a folder" in
               r.getMessage() for r in caplog.records)


# --- the decision after an import -----------------------------------------

@pytest.mark.parametrize("importer_enabled, shallow_enabled", [
    (True, False), (False, True), (False, False)],
    ids=["shallow-off", "importer-off", "both-off"])
def test_skips_when_shallow_zarr_is_off_and_says_why(
        results_root, caplog, importer_enabled, shallow_enabled):
    store = make_store(results_root)

    with caplog.at_level(logging.DEBUG, logger="marker-test"):
        removed = load_functions(
            shallow_enabled=shallow_enabled,
            importer_enabled=importer_enabled)[
            "remove_input_markers_after_import"](
                {RESULTS_PATH_OUTPUT: RString(str(results_root))})

    assert removed == 0
    assert (store / MARKER).is_file()
    assert any(r.levelno == logging.DEBUG and "shallow Zarr storage is off"
               in r.getMessage() for r in caplog.records)


def test_warns_when_the_result_folder_output_is_empty(caplog):
    with caplog.at_level(logging.WARNING, logger="marker-test"):
        removed = load_functions()["remove_input_markers_after_import"](
            {RESULTS_PATH_OUTPUT: RString("")})

    assert removed == 0
    assert any(r.levelno == logging.WARNING and "empty" in r.getMessage()
               for r in caplog.records)


def test_warns_when_the_import_returned_no_result_folder(caplog):
    with caplog.at_level(logging.WARNING, logger="marker-test"):
        removed = load_functions()["remove_input_markers_after_import"]({})

    assert removed == 0
    assert any(r.levelno == logging.WARNING and RESULTS_PATH_OUTPUT in
               r.getMessage() for r in caplog.records)


# --- one test per result path, through the real import step ---------------

def importer_removes_marker(store: Path):
    """Local evaluation or helper kept the full result: importer removes it."""
    (store / MARKER).unlink()


PATHS = {
    # Result shallowed on the HPC: the importer accepts it, marker stays.
    "remote-shallow": (True, False),
    # Helper kept the full result: the importer removes the marker.
    "remote-kept-full": (False, True),
    # The importer evaluated the store itself: it removes the marker.
    "local-evaluation": (False, True),
    # Importer received a plain (legacy) import: it never looks at the marker.
    "no-importer-evaluation": (False, False),
}


def fake_script_environment(run_omero_script):
    functions = load_functions(run_omero_script=run_omero_script)
    script = SimpleNamespace(id=7, getVersion=lambda: "v",
                             getName=lambda: "SLURM_Import_Results.py")
    conn = MagicMock()
    conn.getScriptService.return_value.getScripts.return_value = [script]
    constants = FakeConstants()
    inputs = {constants.transfer.IDS: [1],
              constants.transfer.DATA_TYPE: constants.transfer.DATA_TYPE_DATASET}
    client = SimpleNamespace(getInput=lambda key: inputs.get(key))

    def import_results():
        return functions["importResultsToOmero"](
            client, conn, MagicMock(), 99, defaultdict(bool), "wf-1")
    return import_results


def job_with_status(status_id):
    return SimpleNamespace(getStatus=lambda: SimpleNamespace(
        getId=lambda: status_id))


@pytest.mark.parametrize("importer_removes_today", [True, False],
                         ids=["importer-removes-as-today",
                              "importer-no-longer-removes"])
@pytest.mark.parametrize("path", sorted(PATHS))
def test_marker_is_gone_after_import_on_every_path(
        path, importer_removes_today, results_root):
    shallow, importer_removes_on_this_path = PATHS[path]
    store = make_store(results_root, shallow=shallow)
    seen = {}

    def fake_import(client, svc, script_id, inputs, **kwargs):
        seen["marker_present_at_import"] = (store / MARKER).is_file()
        if importer_removes_today and importer_removes_on_this_path:
            importer_removes_marker(store)
        return ({"Message": "imported",
                 RESULTS_PATH_OUTPUT: RString(str(results_root))}, job_with_status(3))

    fake_script_environment(fake_import)()

    assert seen["marker_present_at_import"], (
        "the marker must still exist when the importer runs")
    assert not (store / MARKER).exists()
    assert (store / "zarr.json").is_file()


def test_marker_stays_when_the_import_fails(results_root):
    store = make_store(results_root)

    def failing_import(client, svc, script_id, inputs, **kwargs):
        return ({"Message": "FAILED: importer down",
                 RESULTS_PATH_OUTPUT: RString(str(results_root))}, job_with_status(6))

    with pytest.raises(RuntimeError):
        fake_script_environment(failing_import)()

    assert (store / MARKER).is_file(), "a failed import is retried later"


def test_a_failing_cleanup_does_not_fail_the_import(results_root, caplog):
    store = make_store(results_root)

    def fake_import(client, svc, script_id, inputs, **kwargs):
        return ({"Message": "imported",
                 RESULTS_PATH_OUTPUT: RString(str(results_root))}, job_with_status(3))

    def broken_cleanup(import_outputs):
        raise RuntimeError("disk gone")

    functions = load_functions(run_omero_script=fake_import)
    functions["remove_input_markers_after_import"] = broken_cleanup
    script = SimpleNamespace(id=7, getVersion=lambda: "v",
                             getName=lambda: "SLURM_Import_Results.py")
    conn = MagicMock()
    conn.getScriptService.return_value.getScripts.return_value = [script]
    constants = FakeConstants()
    inputs = {constants.transfer.IDS: [1],
              constants.transfer.DATA_TYPE: constants.transfer.DATA_TYPE_DATASET}
    client = SimpleNamespace(getInput=lambda key: inputs.get(key))

    with caplog.at_level(logging.WARNING, logger="marker-test"):
        rv = functions["importResultsToOmero"](
            client, conn, MagicMock(), 99, defaultdict(bool), "wf-1")

    assert rv["Message"] == "imported"
    assert (store / MARKER).is_file()
    record = next(r for r in caplog.records
                  if "cleanup failed" in r.getMessage())
    assert record.levelno == logging.WARNING and record.exc_info


# --- the contract with the import script -----------------------------------

def test_the_guarded_schema_import_gives_the_file_name_the_transfer_writes():
    tree = ast.parse(RUN_SCRIPT.read_text(encoding="utf-8"))
    guarded_names = {
        alias.name
        for node in ast.walk(tree) if isinstance(node, ast.If)
        and "IMPORTER_ENABLED" in ast.unparse(node.test)
        for child in ast.walk(node)
        if isinstance(child, ast.ImportFrom)
        and child.module == "biomero_schema.zarr"
        for alias in child.names
    }

    assert "TRANSFER_INPUT_MARKER" in guarded_names
    assert TRANSFER_INPUT_MARKER == MARKER


def test_both_scripts_use_the_core_constant_for_the_result_folder_output():
    # The contract between the two scripts is textual: the importer sets an
    # output by name and the runner reads it by name, and only a real OMERO
    # run connects them. Both must take the name from biomero.constants, so
    # they cannot drift apart (biomero#110).
    run_source = RUN_SCRIPT.read_text(encoding="utf-8")
    import_source = IMPORT_SCRIPT.read_text(encoding="utf-8")
    core_constant = "constants.results.RESULTS_PATH_OUTPUT"

    assert core_constant in run_source
    assert (
        f"client.setOutput({core_constant}, "
        "rstring(str(permanent_storage_path)))"
    ) in import_source
    for source in (run_source, import_source):
        assert "RESULTS_PATH_OUTPUT =" not in source, "defined locally"
        assert '"Results_Path"' not in source
