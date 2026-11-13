"""Scripts take shared output names from core instead of string literals.

A producer and its reader (SLURM_Run_Workflow.py reads the outputs of the
transfer and result scripts) must use one string, defined in
biomero.constants.results. The test reads the source only, so it runs without
core installed.
"""
import ast
import os
from pathlib import Path

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPTS = sorted(
    path for folder in ("__workflows", "_data")
    for path in (SOURCE_ROOT / folder).glob("*.py"))
# Output name -> the biomero.constants.results constant that holds it.
SHARED_OUTPUTS = {
    "Message": "MESSAGE_OUTPUT",
    "URL": "URL_OUTPUT",
    "File_Annotation": "FILE_ANNOTATION_OUTPUT",
    "Canonical_Inputs": "CANONICAL_INPUTS_OUTPUT",
}


def uses_core_constants():
    return any("constants.results.MESSAGE_OUTPUT" in path.read_text(
        encoding="utf-8") for path in SCRIPTS)


# Remove this gate once biomero-scripts#100 is merged and the harness no
# longer has to test source revisions that predate it.
pytestmark = pytest.mark.skipif(
    not uses_core_constants(),
    reason="the scripts do not use the core output constants in this revision",
)


def output_literals(path):
    """Yield (line, literal) for shared output names used as outputs.

    An output is set with ``setOutput(<name>, ...)``, read with
    ``<results>[<name>]``, ``<name> in <results>`` or ``<results>.get(<name>)``.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    results_names = {"rv", "rv_imp", "rv_conv", "ft_rv", "results"}

    def literal(node):
        if isinstance(node, ast.Constant) and node.value in SHARED_OUTPUTS:
            return node.value
        return None

    for node in ast.walk(tree):
        found = None
        if isinstance(node, ast.Call) and isinstance(
                node.func, ast.Attribute):
            if node.func.attr in ("setOutput", "getOutput") and node.args:
                found = literal(node.args[0])
            elif (node.func.attr == "get" and node.args
                  and isinstance(node.func.value, ast.Name)
                  and node.func.value.id in results_names):
                found = literal(node.args[0])
        elif (isinstance(node, ast.Subscript)
              and isinstance(node.value, ast.Name)
              and node.value.id in results_names):
            found = literal(node.slice)
        elif (isinstance(node, ast.Compare) and len(node.ops) == 1
              and isinstance(node.ops[0], (ast.In, ast.NotIn))
              and isinstance(node.comparators[0], ast.Name)
              and node.comparators[0].id in results_names):
            found = literal(node.left)
        if found:
            yield node.lineno, found


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda path: path.name)
def test_shared_output_names_come_from_core_constants(path):
    offenders = [f"{path.name}:{line} uses {name!r}; use "
                 f"constants.results.{SHARED_OUTPUTS[name]}"
                 for line, name in output_literals(path)]
    assert not offenders, "\n".join(offenders)


def test_canonical_inputs_output_is_not_defined_by_the_scripts():
    for path in SCRIPTS:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        defined = [
            node.lineno for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name)
                    and target.id.endswith("_OUTPUT")
                    and isinstance(node.value, ast.Constant)
                    and node.value.value in SHARED_OUTPUTS
                    for target in node.targets)]
        assert not defined, f"{path.name}:{defined} defines an output name"


def test_the_detector_finds_a_literal(tmp_path):
    script = tmp_path / "script.py"
    script.write_text(
        "client.setOutput('Message', 1)\n"
        "x = rv['URL']\n"
        "if 'File_Annotation' in results: pass\n"
        "y = rv.get('Canonical_Inputs')\n"
        "client.setOutput(constants.results.MESSAGE_OUTPUT, 1)\n",
        encoding="utf-8")
    assert [name for _, name in output_literals(script)] == [
        "Message", "URL", "File_Annotation", "Canonical_Inputs"]
