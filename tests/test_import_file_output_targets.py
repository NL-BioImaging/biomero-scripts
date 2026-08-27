import ast
import os
from pathlib import Path


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"


def load_target_resolver():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "resolve_non_image_output_targets"
    ]
    namespace = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["resolve_non_image_output_targets"]


def test_importer_destination_is_used_when_result_container_was_created():
    resolve_targets = load_target_resolver()
    input_plate = object()
    destination_screen = object()

    assert resolve_targets(
        [input_plate], destination_screen, [input_plate]
    ) == [destination_screen]


def test_configured_input_target_is_used_when_no_result_container_was_created():
    resolve_targets = load_target_resolver()
    input_plate = object()

    assert resolve_targets([input_plate], None, []) == [input_plate]


def test_input_container_fallback_supports_older_or_manual_invocations():
    resolve_targets = load_target_resolver()
    input_screen = object()

    assert resolve_targets([], None, [input_screen]) == [input_screen]
