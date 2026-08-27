import ast
import os
from pathlib import Path

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"
TARGET_INTERFACE_PATH = (
    SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"
)
TARGET_INTERFACE_AVAILABLE = (
    TARGET_INTERFACE_PATH.exists()
    and "OUTPUT_ATTACH_FILE_OUTPUTS_TARGET"
    in TARGET_INTERFACE_PATH.read_text(encoding="utf-8")
)
pytestmark = pytest.mark.skipif(
    not TARGET_INTERFACE_AVAILABLE,
    reason=(
        "file-output destination policies are not available in this source "
        "revision"
    ),
)


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


class Container:
    def __init__(self, object_type, object_id, parents=None):
        self.object_type = object_type
        self.object_id = object_id
        self.parents = list(parents or [])

    def listParents(self):
        return iter(self.parents)


def test_result_dataset_wins_over_input_dataset_and_fallback():
    resolve_targets = load_target_resolver()
    input_dataset = Container("Dataset", 10)
    result_dataset = Container("Dataset", 20)
    input_project = Container("Project", 30)

    assert resolve_targets(
        "auto", [input_dataset], result_dataset, [input_project]
    ) == [result_dataset]


def test_result_screen_wins_over_input_plate_and_fallback():
    resolve_targets = load_target_resolver()
    input_plate = Container("Plate", 11)
    result_screen = Container("Screen", 21)
    input_screen = Container("Screen", 31)

    assert resolve_targets(
        "result_destination", [input_plate], result_screen, [input_screen]
    ) == [result_screen]


def test_input_dataset_is_used_when_no_result_container_was_created():
    resolve_targets = load_target_resolver()
    input_dataset = Container("Dataset", 12)

    assert resolve_targets(
        "input_container", [input_dataset], None, []
    ) == [input_dataset]


def test_input_plate_is_used_when_no_result_container_was_created():
    resolve_targets = load_target_resolver()
    input_plate = Container("Plate", 13)

    assert resolve_targets(
        "legacy_input_container", [input_plate], None, []
    ) == [input_plate]


def test_all_configured_input_targets_are_preserved_without_a_result():
    resolve_targets = load_target_resolver()
    input_datasets = [Container("Dataset", 14), Container("Dataset", 15)]

    assert resolve_targets(
        "input_container", input_datasets, None, []
    ) == input_datasets


def test_input_container_fallback_supports_older_or_manual_invocations():
    resolve_targets = load_target_resolver()
    input_screen = Container("Screen", 32)

    assert resolve_targets(
        "legacy_input_container", [], None, [input_screen]
    ) == [input_screen]


def test_input_parent_uses_typed_container_parents():
    resolve_targets = load_target_resolver()
    input_screen = Container("Screen", 51)
    input_plate = Container("Plate", 301, parents=[input_screen])

    assert resolve_targets(
        "input_parent", [input_plate], None, []
    ) == [input_screen]


def test_input_parent_falls_back_to_input_container_when_unlinked():
    resolve_targets = load_target_resolver()
    input_dataset = Container("Dataset", 52)

    assert resolve_targets(
        "input_parent", [input_dataset], None, []
    ) == [input_dataset]


def test_missing_mode_preserves_legacy_input_container_behavior():
    resolve_targets = load_target_resolver()
    input_plate = Container("Plate", 301)
    result_screen = Container("Screen", 51)

    assert resolve_targets(
        None, [input_plate], result_screen, []
    ) == [input_plate]


def test_no_available_container_preserves_legacy_empty_target_behavior():
    resolve_targets = load_target_resolver()

    assert resolve_targets(None, [], None, []) == []
