import ast
import logging
import os
from pathlib import Path


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"


def load_keepalive_helper():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    constant = next(
        node for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name)
            and target.id == "SCRIPT_CLIENT_KEEPALIVE_SECONDS"
            for target in node.targets
        )
    )
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "enable_script_client_keepalive"
    )
    namespace = {"logger": logging.getLogger(__name__)}
    exec(compile(ast.Module(body=[constant, function], type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["enable_script_client_keepalive"]


class FakeScriptClient:
    def __init__(self):
        self.keepalive_periods = []

    def enableKeepAlive(self, seconds):
        self.keepalive_periods.append(seconds)


def test_script_client_keepalive_uses_omero_resource_thread():
    client = FakeScriptClient()

    load_keepalive_helper()(client)

    assert client.keepalive_periods == [60]


def test_run_script_enables_keepalive_immediately_after_client_creation():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    run_script = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "runScript"
    )
    client_creation_line = min(
        node.lineno for node in ast.walk(run_script)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "scripts"
        and node.func.attr == "client"
    )
    keepalive_line = min(
        node.lineno for node in ast.walk(run_script)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "enable_script_client_keepalive"
    )
    first_input_line = min(
        node.lineno for node in ast.walk(run_script)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"getInput", "getInputs"}
    )

    assert client_creation_line < keepalive_line < first_input_line
