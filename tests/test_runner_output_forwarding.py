"""The runner re-publishes the result import's outputs under the same names.

``import_completed_job`` reads the ``Message``, ``URL`` and ``File_Annotation``
outputs of the result script and sets the last two on its own client. A wrong
name on either side (for example the URL read under the file annotation's
name) would show the user the wrong output, so each value is distinct and the
names are core's.
"""
import ast
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import Mock, call

import pytest

from script_output_names import OUTPUT_NAMES

ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
SOURCE = ROOT / "__workflows" / "SLURM_Run_Workflow.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
NODES = [node for node in TREE.body
         if isinstance(node, (ast.FunctionDef, ast.ClassDef))
         and node.name in {"import_completed_job", "JobUpdate"}]
if "import_completed_job" not in {node.name for node in NODES}:
    pytest.skip("Source has no import_completed_job stage",
                allow_module_level=True)


class Output:
    """An rtype stand-in with a distinct, readable value."""

    def __init__(self, value):
        self.value = value

    def getValue(self):
        return self.value


def run_import(results):
    """Run the real stage against a result import that returns ``results``."""
    pipeline = SimpleNamespace(
        tracker=Mock(), client=Mock(), conn=Mock(), slurmClient=Mock(),
        wf_id="wf")
    namespace = dict(
        logger=logging.getLogger("forwarding"), dataclass=dataclass,
        Any=Any, Optional=Optional,
        constants=SimpleNamespace(results=OUTPUT_NAMES),
        importResultsToOmero=Mock(return_value=results))
    exec(compile(ast.Module(body=NODES, type_ignores=[]), str(SOURCE), "exec"),
         namespace)
    update = namespace["import_completed_job"](
        pipeline, 7, "task", "workflow", {}, "")
    return pipeline, update


def test_url_and_file_annotation_are_forwarded_under_their_own_names():
    url, annotation = Output("the-url"), Output("the-annotation")

    pipeline, update = run_import({
        OUTPUT_NAMES.MESSAGE_OUTPUT: Output("imported"),
        OUTPUT_NAMES.URL_OUTPUT: url,
        OUTPUT_NAMES.FILE_ANNOTATION_OUTPUT: annotation})

    assert pipeline.client.setOutput.call_args_list == [
        call(OUTPUT_NAMES.URL_OUTPUT, url),
        call(OUTPUT_NAMES.FILE_ANNOTATION_OUTPUT, annotation)]
    assert update.message == "imported" and not update.failed


def test_a_missing_url_or_annotation_is_noted_not_forwarded():
    pipeline, update = run_import({
        OUTPUT_NAMES.MESSAGE_OUTPUT: Output("imported")})

    pipeline.client.setOutput.assert_not_called()
    assert "|No URL|" in update.message
    assert "|No Annotation|" in update.message
