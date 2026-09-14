import ast
import logging
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from pathlib import Path

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "_SLURM_Image_Transfer.py"


def _tree():
    return ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))


def _load_functions(*names):
    selected = [
        node for node in _tree().body
        if (
            isinstance(node, ast.FunctionDef) and node.name in names
        ) or (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id == "SCRIPT_CLIENT_KEEPALIVE_SECONDS"
                for target in node.targets
            )
        )
    ]
    namespace = {
        "Future": Future,
        "FutureTimeoutError": TimeoutError,
        "ThreadPoolExecutor": ThreadPoolExecutor,
        "logger": logging.getLogger(__name__),
    }
    exec(compile(ast.Module(body=selected, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace


class FakeScriptClient:
    def __init__(self):
        self.keepalive_periods = []

    def enableKeepAlive(self, seconds):
        self.keepalive_periods.append(seconds)


def test_image_transfer_enables_script_client_keepalive():
    namespace = _load_functions("enable_script_client_keepalive")
    client = FakeScriptClient()

    namespace["enable_script_client_keepalive"](client)

    assert client.keepalive_periods == [60]


def test_image_transfer_enables_keepalive_before_using_client():
    run_script = next(
        node for node in _tree().body
        if isinstance(node, ast.FunctionDef) and node.name == "run_script"
    )
    client_creation_end = max(
        getattr(node, "end_lineno", node.lineno) for node in ast.walk(run_script)
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
    first_client_use = min(
        node.lineno for node in ast.walk(run_script)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "client"
        and node.func.attr in {"getSessionId", "getInputs", "setOutput"}
    )

    assert client_creation_end < keepalive_line < first_client_use


def test_blocking_operation_rejects_failed_gateway_keepalive():
    namespace = _load_functions(
        "_require_keepalive",
        "run_with_keepalive",
    )

    with pytest.raises(ConnectionError, match="OMERO connection"):
        namespace["run_with_keepalive"](
            lambda: time.sleep(0.03),
            lambda: False,
            keepalive_interval=0.001,
        )
