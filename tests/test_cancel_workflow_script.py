"""The admin script that requests a cancel for a workflow (real tracker)."""
import ast
import logging
import os
from pathlib import Path

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / 'admin' / 'SLURM_Cancel_Workflow.py'
if not SOURCE.exists():
    pytest.skip('Source has no cancel script', allow_module_level=True)
TREE = ast.parse(SOURCE.read_text(encoding='utf-8'))
FUNCTION = [n for n in TREE.body
            if isinstance(n, ast.FunctionDef) and n.name == 'request_cancel']
CONSTANTS = [n for n in TREE.body if isinstance(n, ast.Assign)
             and n.targets[0].id in ('_MAX_REASON_LENGTH',)]

try:
    from biomero.database import EngineManager
    from biomero.eventsourcing import WorkflowTracker
except ImportError:
    if os.environ.get('CI'):
        pytest.fail('biomero must be installed in CI', pytrace=False)
    pytest.skip('biomero core is not installed', allow_module_level=True)
if not hasattr(WorkflowTracker, 'is_workflow_finished'):
    if os.environ.get('CI'):
        pytest.fail('biomero lacks is_workflow_finished', pytrace=False)
    pytest.skip('Installed biomero is too old', allow_module_level=True)


@pytest.fixture
def tracker(tmp_path):
    os.environ['PERSISTENCE_MODULE'] = 'eventsourcing_sqlalchemy'
    os.environ['SQLALCHEMY_URL'] = f"sqlite:///{(tmp_path / 'wf.db').as_posix()}"
    EngineManager.create_scoped_session()
    yield WorkflowTracker()
    del os.environ['PERSISTENCE_MODULE']
    del os.environ['SQLALCHEMY_URL']
    EngineManager.close_engine()


def request_cancel(*args):
    namespace = dict(logger=logging.getLogger('cancel-script'))
    exec(compile(ast.Module(body=CONSTANTS + FUNCTION, type_ignores=[]),
                 str(SOURCE), 'exec'), namespace)
    return namespace['request_cancel'](*args)


def test_owner_can_cancel_their_workflow(tracker):
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)
    message = request_cancel(tracker, workflow_id, 7, False, 'because')
    assert tracker.is_workflow_cancel_requested(workflow_id)
    assert tracker.repository.get(workflow_id).cancel_reason == 'because'
    assert 'Cancel requested' in message


def test_admin_can_cancel_anyones_workflow(tracker):
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)
    request_cancel(tracker, workflow_id, 1, True, 'because')
    assert tracker.is_workflow_cancel_requested(workflow_id)


def test_other_user_cannot_cancel(tracker):
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)
    with pytest.raises(PermissionError, match='belongs to another user'):
        request_cancel(tracker, workflow_id, 8, False, 'because')
    assert not tracker.is_workflow_cancel_requested(workflow_id)


def test_second_request_changes_nothing(tracker):
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)
    request_cancel(tracker, workflow_id, 7, False, 'first')
    message = request_cancel(tracker, workflow_id, 7, False, 'second')
    assert tracker.repository.get(workflow_id).cancel_reason == 'first'
    assert 'already requested' in message


@pytest.mark.parametrize('finish', ['complete_workflow', 'fail_workflow'])
def test_finished_workflow_is_not_given_a_cancel_request(tracker, finish,
                                                         caplog):
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)
    getattr(tracker, finish)(*((workflow_id,) if finish == 'complete_workflow'
                               else (workflow_id, 'broke')))
    with caplog.at_level(logging.INFO, logger='cancel-script'):
        message = request_cancel(tracker, workflow_id, 7, False, 'late')
    assert not tracker.is_workflow_cancel_requested(workflow_id)
    assert 'already finished' in message
    assert any('already finished' in r.getMessage() for r in caplog.records)


def test_reason_is_bounded(tracker):
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)
    request_cancel(tracker, workflow_id, 7, False, 'x' * 5000)
    assert len(tracker.repository.get(workflow_id).cancel_reason) == 200
