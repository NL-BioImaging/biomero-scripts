"""The shared output names, as the scripts read them from biomero.constants.

Tests that run script functions in a bare namespace provide this as
``constants.results`` (or merge it into their own). The values come from
core itself, so they cannot drift from it.
"""
from types import SimpleNamespace

from biomero.constants import file_transfer, results

OUTPUT_NAMES = SimpleNamespace(
    MESSAGE_OUTPUT=results.MESSAGE_OUTPUT,
    URL_OUTPUT=results.URL_OUTPUT,
    FILE_ANNOTATION_OUTPUT=results.FILE_ANNOTATION_OUTPUT,
    CANONICAL_INPUTS_OUTPUT=results.CANONICAL_INPUTS_OUTPUT,
    RESULTS_PATH_OUTPUT=results.RESULTS_PATH_OUTPUT,
    SLURM_PATH_OUTPUT=file_transfer.SLURM_PATH_OUTPUT,
)


def module_constant(script, name):
    """Return the value of a module-level string constant of a script, or None.

    Args:
        script (Path): The script to read (parsed, not imported).
        name (str): Name of the module-level constant.

    Returns:
        Optional[str]: The constant's value; None when the script has no such
        constant (a source before the constant existed).
    """
    import ast
    for node in ast.parse(script.read_text(encoding="utf-8")).body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == name
                and isinstance(node.value, ast.Constant)):
            return node.value.value
    return None
