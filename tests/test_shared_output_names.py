"""Scripts take shared output names from core instead of string literals.

A producer and its reader (SLURM_Run_Workflow.py reads the outputs of the
transfer and result scripts) must use one string, defined in
biomero.constants.results. The test reads the source only.
"""
import ast
import os
from pathlib import Path

import pytest

from script_output_names import OUTPUT_NAMES

SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPTS = sorted(
    path for folder in ("__workflows", "_data", "admin")
    for path in (SOURCE_ROOT / folder).glob("*.py"))
# Output name -> the biomero.constants.results constant that holds it.
SHARED_OUTPUTS = {
    OUTPUT_NAMES.MESSAGE_OUTPUT: "results.MESSAGE_OUTPUT",
    OUTPUT_NAMES.URL_OUTPUT: "results.URL_OUTPUT",
    OUTPUT_NAMES.FILE_ANNOTATION_OUTPUT: "results.FILE_ANNOTATION_OUTPUT",
    OUTPUT_NAMES.CANONICAL_INPUTS_OUTPUT: "results.CANONICAL_INPUTS_OUTPUT",
    OUTPUT_NAMES.RESULTS_PATH_OUTPUT: "results.RESULTS_PATH_OUTPUT",
    OUTPUT_NAMES.SLURM_PATH_OUTPUT: "file_transfer.SLURM_PATH_OUTPUT",
}
# The batched runner keeps its own UI message buckets under these names.
UI_MESSAGES_NAME = "UI_messages"


def uses_core_constants():
    """Tell whether the scripts take their output names from core.

    Gated on ``URL_OUTPUT``, which only the production PR that converts the
    scripts (biomero-scripts#101) reads from core: master and the PRs before
    it use other core output names (for example ``RESULTS_PATH_OUTPUT``) and
    must keep passing.
    """
    return any("constants.results.URL_OUTPUT" in path.read_text(
        encoding="utf-8") for path in SCRIPTS)


def is_allowed(node, parent):
    """Tell whether a literal equal to an output name is not an output name."""
    if isinstance(parent, ast.Expr):  # a docstring
        return True
    if isinstance(parent, ast.Dict):
        index = next((i for i, key in enumerate(parent.keys)
                      if key is node), None)
        if index is None:  # a dict value, e.g. {"type": "URL"}
            position = next(i for i, value in enumerate(parent.values)
                            if value is node)
            key = parent.keys[position]
            return isinstance(key, ast.Constant) and key.value == "type"
        return True  # a dict key: the UI_messages buckets, see below
    if isinstance(parent, ast.Subscript):
        return (isinstance(parent.value, ast.Name)
                and parent.value.id == UI_MESSAGES_NAME)
    return False


def output_literals(source):
    """Yield (line, literal) for every use of a shared output name.

    Any string constant equal to one of the names counts, wherever it
    appears, except docstrings, the ``"type": "URL"`` value of an OMERO URL
    and the keys of the batched runner's ``UI_messages``.
    """
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree)
               for child in ast.iter_child_nodes(parent)}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and node.value in SHARED_OUTPUTS):
            continue
        parent = parents[node]
        if isinstance(parent, ast.Dict) and node in parent.keys:
            # Allowed only as the keys of the dict assigned to UI_messages.
            assignment = parents.get(parent)
            if (isinstance(assignment, ast.Assign)
                    and any(isinstance(t, ast.Name)
                            and t.id == UI_MESSAGES_NAME
                            for t in assignment.targets)):
                continue
        elif is_allowed(node, parent):
            continue
        yield node.lineno, node.value


def test_the_scripts_are_found():
    assert SCRIPTS, f"no scripts found below {SOURCE_ROOT}"


def test_the_detector_finds_a_literal():
    source = (
        "client.setOutput('Message', 1)\n"
        "x = rv['URL']\n"
        "if 'File_Annotation' in results: pass\n"
        "y = rv.get('Canonical_Inputs')\n"
        "MESSAGE_NAME = 'Message'\n"
        "OTHER: str = 'URL'\n"
        "name = 'File_Annotation'\n"
        "client.setOutput(constants.results.MESSAGE_OUTPUT, 1)\n")
    assert [name for _, name in sorted(output_literals(source))] == [
        "Message", "URL", "File_Annotation", "Canonical_Inputs",
        "Message", "URL", "File_Annotation"]


def test_the_detector_allows_what_is_not_an_output_name():
    source = (
        '"""Sets the Message output."""\n'
        "UI_messages = {'Message': [], 'File_Annotation': []}\n"
        "UI_messages['Message'].extend([])\n"
        "value = {'type': 'URL', 'href': url}\n"
        "logger.info('Message sent')\n")
    assert list(output_literals(source)) == []


# Remove this gate once biomero-scripts#100 is merged and the harness no
# longer has to test source revisions that predate it (follow-up issue:
# biomero-scripts#109).
requires_core_constants = pytest.mark.skipif(
    SCRIPTS and not uses_core_constants(),
    reason="the scripts do not use the core output constants in this revision")

# Per script: how often it names each output other than ``Message`` through
# core. A wrong constant (for example URL_OUTPUT where FILE_ANNOTATION_OUTPUT
# is meant) changes these counts; ``Message`` is covered by the other checks
# (existence in core, no literal, setOutput rooted at core).
EXPECTED_OUTPUT_USES = {
    "__workflows/SLURM_Run_Workflow.py": {
        "CANONICAL_INPUTS_OUTPUT": 5, "FILE_ANNOTATION_OUTPUT": 3,
        "RESULTS_PATH_OUTPUT": 3, "URL_OUTPUT": 4},
    "__workflows/SLURM_Run_Workflow_Batched.py": {
        "FILE_ANNOTATION_OUTPUT": 1},
    "_data/SLURM_Get_Results.py": {
        "FILE_ANNOTATION_OUTPUT": 2, "URL_OUTPUT": 1},
    "_data/SLURM_Get_Update.py": {
        "FILE_ANNOTATION_OUTPUT": 1, "URL_OUTPUT": 1},
    "_data/SLURM_Import_Results.py": {
        "FILE_ANNOTATION_OUTPUT": 2, "RESULTS_PATH_OUTPUT": 1,
        "URL_OUTPUT": 1, "WORKFLOW_UUID_OUTPUT": 1},
    "_data/SLURM_Remote_Conversion.py": {"URL_OUTPUT": 1},
    "_data/_SLURM_Image_Transfer.py": {
        "CANONICAL_INPUTS_OUTPUT": 1, "FILE_ANNOTATION_OUTPUT": 1},
}
# Outputs only one script sets and no other script reads: module constants of
# that script, not core names.
SCRIPT_LOCAL_OUTPUTS = {"JOB_LOG_OUTPUT", "CONVERSION_LOG_OUTPUT"}
# The file transfer's output, which the runner reads: a core name too.
EXPECTED_FILE_TRANSFER_USES = {
    "__workflows/SLURM_Run_Workflow.py": 1,
    "_data/_SLURM_File_Transfer.py": 1,
}


def results_constants_used(source):
    """Return every ``constants.results.<NAME>`` of the source, by name."""
    tree = ast.parse(source)
    return [node.attr for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and ast.unparse(node.value) == "constants.results"]


def set_output_names(source):
    """Return the first argument of every ``setOutput`` call, as source text."""
    tree = ast.parse(source)
    return [ast.unparse(node.args[0]) for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "setOutput" and node.args]


def relative(path):
    return path.relative_to(SOURCE_ROOT).as_posix()


def test_the_detectors_read_a_source():
    source = ("client.setOutput(constants.results.URL_OUTPUT, 1)\n"
              "x = rv[constants.results.MESSAGE_OUTPUT]\n"
              "client.setOutput('Job_Log', 2)\n")
    assert results_constants_used(source) == ["URL_OUTPUT", "MESSAGE_OUTPUT"]
    assert set_output_names(source) == [
        "constants.results.URL_OUTPUT", "'Job_Log'"]


@requires_core_constants
@pytest.mark.parametrize("path", SCRIPTS, ids=relative)
def test_shared_output_names_come_from_core_constants(path):
    offenders = [f"{path.name}:{line} uses {name!r}; use "
                 f"constants.{SHARED_OUTPUTS[name]}"
                 for line, name in output_literals(
                     path.read_text(encoding="utf-8"))]
    assert not offenders, "\n".join(offenders)


@requires_core_constants
@pytest.mark.parametrize("path", SCRIPTS, ids=relative)
def test_every_constant_a_script_reads_exists_in_core(path):
    from biomero.constants import results as core_results
    unknown = sorted({name for name in results_constants_used(
        path.read_text(encoding="utf-8")) if not hasattr(core_results, name)})
    assert not unknown, f"{path.name} uses unknown core names {unknown}"


@requires_core_constants
@pytest.mark.parametrize("path", SCRIPTS, ids=relative)
def test_every_set_output_is_named_by_a_core_constant(path):
    names = set_output_names(path.read_text(encoding="utf-8"))
    not_from_core = [
        name for name in names
        if not name.startswith(("constants.results.",
                                "constants.file_transfer."))
        and name not in SCRIPT_LOCAL_OUTPUTS
        # the batched runner's per-batch File_Annotation_<n> outputs
        and not name.startswith("f'File_Annotation_")]
    assert not not_from_core, (
        f"{path.name} sets outputs {not_from_core} without a core constant")


@requires_core_constants
@pytest.mark.parametrize("path", SCRIPTS, ids=relative)
def test_each_script_names_its_outputs_as_expected(path):
    from collections import Counter
    uses = Counter(name for name in results_constants_used(
        path.read_text(encoding="utf-8"))
        if name.endswith("_OUTPUT") and name != "MESSAGE_OUTPUT")
    assert dict(uses) == EXPECTED_OUTPUT_USES.get(relative(path), {}), (
        f"{path.name}: the core output constants it uses changed; if that is "
        f"intended, update EXPECTED_OUTPUT_USES")


@requires_core_constants
@pytest.mark.parametrize("path", SCRIPTS, ids=relative)
def test_the_file_transfer_output_name_comes_from_core(path):
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    uses = sum(1 for node in ast.walk(tree)
               if isinstance(node, ast.Attribute)
               and ast.unparse(node) == "constants.file_transfer.SLURM_PATH_OUTPUT")
    assert uses == EXPECTED_FILE_TRANSFER_USES.get(relative(path), 0)


@requires_core_constants
def test_the_cancel_script_is_named_by_core_and_uses_its_parameter_names():
    from biomero import constants
    script = SOURCE_ROOT / "admin" / constants.CANCEL_WORKFLOW_SCRIPT
    source = script.read_text(encoding="utf-8")
    assert "constants.cancel_workflow.WORKFLOW_ID" in source
    assert "constants.cancel_workflow.REASON" in source
    assert not [name for name in ("Workflow_ID", "Reason")
                if f'"{name}"' in source]


@requires_core_constants
@pytest.mark.parametrize("script, constant, value", [
    ("__workflows/SLURM_Run_Workflow.py", "JOB_LOG_OUTPUT", "Job_Log"),
    ("_data/SLURM_Remote_Conversion.py", "CONVERSION_LOG_OUTPUT",
     "Conversion_Log"),
])
def test_script_local_outputs_keep_their_wire_values(script, constant, value):
    from script_output_names import module_constant
    assert module_constant(SOURCE_ROOT / script, constant) == value


# Names the scripts share with core, other than the output names above: a
# literal equal to one of them (in any quote style) is a name core owns.
SHARED_NAMES = {
    "Workflow_ID": "metadata_keys.WORKFLOW_ID",
    "Job_ID": "metadata_keys.JOB_ID",
    "Batch_Supervisor_Workflow_ID": "metadata_keys.BATCH_SUPERVISOR_WORKFLOW_ID",
    "Task_ID": "results.TASK_ID",
    "Cleanup?": "CLEANUP",
    "ROI_Name_Prefix": "results.ROI_NAME_PREFIX",
    "ROI_Color": "results.ROI_COLOR",
    "IMPORTING": "workflow_status.IMPORTING",
    "IMPORTED": "workflow_status.IMPORTED",
    "legacy_input_container": "file_output_targets.LEGACY",
    "auto": "file_output_targets.AUTO",
    "result_destination": "file_output_targets.RESULT_DESTINATION",
    "input_parent": "file_output_targets.INPUT_PARENT",
    "BIOMERO_SHALLOW_ZARR": "slurm_env.BIOMERO_SHALLOW_ZARR",
    "SLURM_Import_Results.py": "IMPORT_RESULTS_SCRIPT",
}


def shared_name_literals(source):
    """Yield (line, literal) of every string equal to a shared core name.

    Docstrings are text, not names; any other string constant counts, in
    single or double quotes.
    """
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree)
               for child in ast.iter_child_nodes(parent)}
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and node.value in SHARED_NAMES
                and not isinstance(parents[node], ast.Expr)):
            yield node.lineno, node.value


def test_the_shared_name_detector_sees_both_quote_styles():
    source = ("a = {'Workflow_ID': 1, \"Job_ID\": 2}\n"
              "x.get('Cleanup?')\n"
              "if mode == \"auto\": pass\n"
              "\"\"\"Docs mention Workflow_ID.\"\"\"\n"
              "constants.metadata_keys.WORKFLOW_ID\n")
    assert [name for _, name in shared_name_literals(source)] == [
        "Workflow_ID", "Job_ID", "Cleanup?", "auto"]


@requires_core_constants
@pytest.mark.parametrize("path", SCRIPTS, ids=relative)
def test_shared_names_come_from_core_constants(path):
    offenders = [f"{path.name}:{line} uses {name!r}; use "
                 f"constants.{SHARED_NAMES[name]}"
                 for line, name in shared_name_literals(
                     path.read_text(encoding="utf-8"))]
    assert not offenders, "\n".join(offenders)


@requires_core_constants
def test_the_shared_names_equal_what_core_defines():
    from biomero import constants
    for name, dotted in SHARED_NAMES.items():
        value = constants
        for part in dotted.split("."):
            value = getattr(value, part)
        assert value == name, dotted
