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


def test_palette_wraps_around_for_large_workflow_ids():
    from uuid import UUID
    resolve, namespace = load_resolve_workflow_color()
    palette = namespace["QUALITATIVE_COLOR_SCHEME"]
    size = len(palette)
    assert resolve("", UUID(int=size)) == palette[0]
    assert resolve("", UUID(int=size + 3)) == palette[3]
    assert resolve("", UUID(int=size - 1)) == palette[-1]
    largest = UUID(int=2 ** 128 - 1)
    assert resolve("", largest) == palette[(2 ** 128 - 1) % size]
    assert resolve("", str(largest)) == resolve("", largest)


def _calls(path, function_name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and ast.unparse(node.func).endswith(function_name)]


def test_runner_forwards_the_resolved_color_to_the_result_scripts():
    tree = ast.parse(RUNNER.read_text(encoding="utf-8"))
    assignments = {
        ast.unparse(node.targets[0]): ast.unparse(node.value)
        for node in ast.walk(tree) if isinstance(node, ast.Assign)}
    resolved = assignments["roi_color"]
    assert resolved.startswith("resolve_workflow_color(")
    assert "constants.workflow.ROI_COLOR" in resolved
    assert "wf_id" in resolved
    assert assignments[
        "inputs[constants.results.ROI_COLOR]"] == "rstring(roi_color)"


def test_result_scripts_use_the_forwarded_color_as_given():
    for path in RESULT_SCRIPTS:
        calls = _calls(path, "execute_roi_postprocessing")
        assert len(calls) == 1, path.name
        keywords = {kw.arg: ast.unparse(kw.value) for kw in calls[0].keywords}
        assert "constants.results.ROI_COLOR" in keywords["roi_color"], path.name
        assert "resolve_workflow_color" not in keywords["roi_color"]
        assert keywords["roi_color"].endswith("or ''")
