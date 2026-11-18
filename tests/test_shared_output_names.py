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
    OUTPUT_NAMES.MESSAGE_OUTPUT: "MESSAGE_OUTPUT",
    OUTPUT_NAMES.URL_OUTPUT: "URL_OUTPUT",
    OUTPUT_NAMES.FILE_ANNOTATION_OUTPUT: "FILE_ANNOTATION_OUTPUT",
    OUTPUT_NAMES.CANONICAL_INPUTS_OUTPUT: "CANONICAL_INPUTS_OUTPUT",
    OUTPUT_NAMES.RESULTS_PATH_OUTPUT: "RESULTS_PATH_OUTPUT",
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
# Output names the scripts still write as literals until core defines them.
OUTPUTS_NOT_YET_IN_CORE = {"Job_Log", "Conversion_Log", "Slurm_Path"}


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
                 f"constants.results.{SHARED_OUTPUTS[name]}"
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
        if not name.startswith("constants.results.")
        and name.strip("'\"") not in OUTPUTS_NOT_YET_IN_CORE
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
