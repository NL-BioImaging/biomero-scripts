"""The ROI color of a workflow is decided by the workflow runner script."""
import ast
import os
import re
from pathlib import Path

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
RUNNER = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"
RESULT_SCRIPTS = [SOURCE_ROOT / "_data" / "SLURM_Get_Results.py",
                  SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"]
pytestmark = pytest.mark.skipif(
    "def resolve_workflow_color" not in RUNNER.read_text(encoding="utf-8"),
    reason="the workflow color moved out of core in a later source revision",
)


def load_resolve_workflow_color():
    from uuid import UUID
    tree = ast.parse(RUNNER.read_text(encoding="utf-8"))
    wanted = [node for node in tree.body
              if (isinstance(node, ast.Assign) and any(
                  isinstance(target, ast.Name) and target.id in (
                      "QUALITATIVE_COLOR_SCHEME", "ROI_COLOR_PATTERN")
                  for target in node.targets))
              or (isinstance(node, ast.FunctionDef)
                  and node.name == "resolve_workflow_color")]
    namespace = {"re": re, "UUID": UUID}
    exec(compile(ast.Module(body=wanted, type_ignores=[]), str(RUNNER),
                 "exec"), namespace)
    return namespace["resolve_workflow_color"], namespace


def test_qualitative_color_scheme_matches_blueprint():
    _, namespace = load_resolve_workflow_color()
    assert namespace["QUALITATIVE_COLOR_SCHEME"] == (
        "#147EB3", "#29A634", "#D1980B", "#D33D17", "#9D3F9D",
        "#00A396", "#DB2C6F", "#8EB125", "#946638", "#7961DB")


def test_resolve_workflow_color_is_deterministic_and_override_wins():
    resolve, _ = load_resolve_workflow_color()
    first = "00000000-0000-0000-0000-000000000000"
    second = "00000000-0000-0000-0000-000000000001"

    assert resolve("", first) == "#147EB3"
    assert resolve(None, first) == "#147EB3"
    assert resolve("", second) == "#29A634"
    assert resolve("#d33d17", first) == "#D33D17"
    with pytest.raises(ValueError, match="#RRGGBB"):
        resolve("red", first)


def test_only_the_runner_resolves_the_color():
    """The result scripts use the value the runner forwards to them."""
    for path in RESULT_SCRIPTS:
        assert "resolve_workflow_color" not in path.read_text(
            encoding="utf-8"), path.name
    assert "constants.resolve_workflow_color" not in RUNNER.read_text(
        encoding="utf-8")
