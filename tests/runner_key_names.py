"""Names the workflow runner defines once and its functions refer to.

Tests that run single runner functions in a hand-built namespace need those
names, which the runner defines at module level: its ``LAUNCHER_*`` keys and,
from core, the prefixes of the workflow parameters on the Slurm task.
"""
import ast
from pathlib import Path


def runner_key_names(source_path: Path) -> dict:
    """Return the runner's launcher key constants plus core's task prefixes.

    Args:
        source_path (Path): ``SLURM_Run_Workflow.py`` of the source under test.

    Returns:
        dict: Name to string value, ready to add to a function namespace.
    """
    names = {}
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id.startswith("LAUNCHER_")
                and isinstance(node.value, ast.Constant)):
            names[node.targets[0].id] = node.value.value
    try:
        from biomero.metadata_views import (WF_FILE_PARAMS_PREFIX,
                                            WF_PARAMS_PREFIX)
    except ImportError:
        return names
    names.update(WF_PARAMS_PREFIX=WF_PARAMS_PREFIX,
                 WF_FILE_PARAMS_PREFIX=WF_FILE_PARAMS_PREFIX)
    return names
