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
    path for folder in ("__workflows", "_data")
    for path in (SOURCE_ROOT / folder).glob("*.py"))
# Output name -> the biomero.constants.results constant that holds it.
SHARED_OUTPUTS = {
    OUTPUT_NAMES.MESSAGE_OUTPUT: "MESSAGE_OUTPUT",
    OUTPUT_NAMES.URL_OUTPUT: "URL_OUTPUT",
    OUTPUT_NAMES.FILE_ANNOTATION_OUTPUT: "FILE_ANNOTATION_OUTPUT",
    OUTPUT_NAMES.CANONICAL_INPUTS_OUTPUT: "CANONICAL_INPUTS_OUTPUT",
}
# The batched runner keeps its own UI message buckets under these names.
UI_MESSAGES_NAME = "UI_messages"


def uses_core_constants():
    """Tell whether any script already reads a shared name from core."""
    return any(f"constants.results.{constant}" in path.read_text(
        encoding="utf-8") for path in SCRIPTS
        for constant in SHARED_OUTPUTS.values())


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
# longer has to test source revisions that predate it.
@pytest.mark.skipif(
    SCRIPTS and not uses_core_constants(),
    reason="the scripts do not use the core output constants in this revision")
@pytest.mark.parametrize("path", SCRIPTS, ids=lambda path: path.name)
def test_shared_output_names_come_from_core_constants(path):
    offenders = [f"{path.name}:{line} uses {name!r}; use "
                 f"constants.results.{SHARED_OUTPUTS[name]}"
                 for line, name in output_literals(
                     path.read_text(encoding="utf-8"))]
    assert not offenders, "\n".join(offenders)
