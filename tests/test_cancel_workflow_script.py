"""The admin script that requests a cancel for a workflow."""
import ast
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / 'admin' / 'SLURM_Cancel_Workflow.py'
if not SOURCE.exists():
    pytest.skip('Source has no cancel script', allow_module_level=True)
TREE = ast.parse(SOURCE.read_text(encoding='utf-8'))
FUNCTION = [n for n in TREE.body
            if isinstance(n, ast.FunctionDef) and n.name == 'request_cancel']


def request_cancel(*args):
    namespace = dict(logger=logging.getLogger('cancel-script'))
    exec(compile(ast.Module(body=FUNCTION, type_ignores=[]), str(SOURCE),
                 'exec'), namespace)
    return namespace['request_cancel'](*args)


def tracker_for(owner, already=False):
    tracker = Mock()
    tracker.repository.get.return_value = SimpleNamespace(user=owner)
    tracker.is_workflow_cancel_requested.return_value = already
    return tracker


def test_owner_can_cancel_their_workflow():
    tracker = tracker_for(owner=7)
    message = request_cancel(tracker, 'wf', 7, False, 'because')
    tracker.request_workflow_cancel.assert_called_once_with('wf', 'because')
    assert 'Cancel requested' in message


def test_admin_can_cancel_anyones_workflow():
    tracker = tracker_for(owner=7)
    request_cancel(tracker, 'wf', 1, True, 'because')
    tracker.request_workflow_cancel.assert_called_once_with('wf', 'because')


def test_other_user_cannot_cancel_and_it_is_logged(caplog):
    tracker = tracker_for(owner=7)
    with caplog.at_level(logging.WARNING, logger='cancel-script'):
        with pytest.raises(PermissionError, match='belongs to another user'):
            request_cancel(tracker, 'wf', 8, False, 'because')
    tracker.request_workflow_cancel.assert_not_called()
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_second_request_changes_nothing():
    tracker = tracker_for(owner=7, already=True)
    message = request_cancel(tracker, 'wf', 7, False, 'because')
    tracker.request_workflow_cancel.assert_not_called()
    assert 'already requested' in message
