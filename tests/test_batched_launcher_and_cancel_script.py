"""Batched launcher registration and the cancel script's runScript.

Both run the real source functions against a real tracker: the launcher params
are the contract with the detached supervisor's batched parent, and the cancel
script decides admin versus owner from the OMERO session.
"""
import ast
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
BATCHED = ROOT / '__workflows' / 'SLURM_Run_Workflow_Batched.py'
CANCEL = ROOT / 'admin' / 'SLURM_Cancel_Workflow.py'


def core_unavailable(reason):
    if os.environ.get('CI'):
        pytest.fail(f'{reason}: add the released biomero to '
                    f'tests/requirements.txt', pytrace=False)
    pytest.skip(reason, allow_module_level=True)


try:
    from biomero import constants
    from biomero.database import EngineManager
    from biomero.eventsourcing import WorkflowTracker, is_launcher_task
except ImportError:
    core_unavailable('biomero core is not installed')
if not hasattr(WorkflowTracker, 'add_launcher_task'):
    core_unavailable('Installed biomero lacks the detached mechanisms')


def source_nodes(path, names):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    found = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            found.append(node)
        elif isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id in names
                for t in node.targets):
            found.append(node)
    return found


def run_source(path, nodes, **namespace):
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'),
         namespace)
    return namespace


@pytest.fixture(autouse=True)
def tracking_database(tmp_path):
    os.environ['PERSISTENCE_MODULE'] = 'eventsourcing_sqlalchemy'
    os.environ['SQLALCHEMY_URL'] = f"sqlite:///{(tmp_path / 'wf.db').as_posix()}"
    EngineManager.create_scoped_session()
    yield
    del os.environ['PERSISTENCE_MODULE']
    del os.environ['SQLALCHEMY_URL']
    EngineManager.close_engine()


# ---------------------- batched launcher registration ---------------------- #

class RType:
    """Stand-in for an OMERO rtype: unwrap() gives .val."""

    def __init__(self, val):
        self.val = val


def batched_registration():
    nodes = source_nodes(BATCHED, {'register_detached_batches', 'VERSION'})
    if not any(isinstance(n, ast.FunctionDef) for n in nodes):
        pytest.skip('Source has no batched launcher registration')
    return run_source(
        BATCHED, nodes, constants=constants,
        logger=logging.getLogger('batched'),
        unwrap=lambda value: getattr(value, 'val', value))


def test_batched_launcher_records_the_contract_the_supervisor_reads():
    tracker = WorkflowTracker()
    workflow_id = tracker.initiate_workflow('wf', '', user=1, group=2)
    names = batched_registration()
    client = SimpleNamespace(getInputs=lambda unwrap: {
        'IDs': [1, 2, 3], 'Data_Type': 'Image', 'Batch_Size': 2})
    inputs = {'IDs': RType([1, 2, 3]), 'Data_Type': RType('Image'),
              'Duplicate': RType(False)}

    task_id = names['register_detached_batches'](
        client, SimpleNamespace(workflowTracker=tracker), workflow_id,
        ['analysis'], [[1, 2], [3]], 2, 'Image', inputs,
        {'New Dataset': True}, 7, [1, 2, 3])

    # read back through a fresh tracker, as the supervisor does
    task = WorkflowTracker().repository.get(task_id)
    assert task.task_name == constants.RUN_WF_BATCHED_SCRIPT
    assert task.input_data == [1, 2, 3]
    assert is_launcher_task(task)
    assert task.params == {
        'IDs': [1, 2, 3], 'Data_Type': 'Image', 'Batch_Size': 2,
        'workflows': ['analysis'], 'batches': [[1, 2], [3]], 'batch_size': 2,
        'effective_type': 'Image',
        'base_inputs': {'IDs': [1, 2, 3], 'Data_Type': 'Image',
                        'Duplicate': False},
        'selected_output': {'New Dataset': True}, 'group': 7,
        constants.LAUNCHER_MARKER: names['VERSION']}
    assert WorkflowTracker().find_launcher_task(workflow_id).id == task_id


def test_batched_launcher_params_include_every_key_the_processor_reads():
    """The processor's batched parent reads exactly these keys."""
    processor_keys = {'batches', 'base_inputs', 'effective_type',
                      'selected_output', 'group', 'workflows'}
    tracker = WorkflowTracker()
    workflow_id = tracker.initiate_workflow('wf', '', user=1, group=2)
    task_id = batched_registration()['register_detached_batches'](
        SimpleNamespace(getInputs=lambda unwrap: {}),
        SimpleNamespace(workflowTracker=tracker), workflow_id, [], [[1]], 1,
        'Image', {}, {}, 1, [1])
    assert processor_keys <= set(tracker.repository.get(task_id).params)


# ----------------------------- cancel script ----------------------------- #

def cancel_runscript(inputs, user_id, is_admin):
    if not CANCEL.exists():
        pytest.skip('Source has no cancel script')
    nodes = source_nodes(CANCEL, {
        'request_cancel', 'runScript', '_WORKFLOW_ID', '_REASON',
        '_DEFAULT_REASON', '_MAX_REASON_LENGTH', 'VERSION'})
    if not any(isinstance(n, ast.FunctionDef) and n.name == 'runScript'
               for n in nodes):
        pytest.skip('Source has no cancel script')
    outputs = {}
    client = SimpleNamespace(
        getInputs=lambda unwrap=False: dict(inputs),
        setOutput=lambda key, value: outputs.__setitem__(key, value),
        closeSession=Mock())
    connection = SimpleNamespace(
        getUserId=lambda: user_id,
        getUser=lambda: SimpleNamespace(isAdmin=lambda: is_admin))
    namespace = run_source(
        CANCEL, nodes,
        logger=logging.getLogger('cancel-script'),
        UUID=UUID, rstring=lambda value: value,
        omscripts=SimpleNamespace(client=lambda *a, **k: client,
                                  String=lambda *a, **k: None),
        omero=SimpleNamespace(constants=SimpleNamespace(
            namespaces=SimpleNamespace(NSDYNAMIC='dynamic'))),
        BlitzGateway=lambda client_obj: connection,
        WorkflowTracker=WorkflowTracker)
    namespace['runScript']()
    client.closeSession.assert_called_once()
    return outputs['Message']


@pytest.mark.parametrize('user_id, is_admin', [(1, True), (7, False)],
                         ids=['administrator', 'owner'])
def test_cancel_script_runscript_lets_admin_and_owner_cancel(user_id,
                                                             is_admin):
    tracker = WorkflowTracker()
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)

    message = cancel_runscript(
        {'Workflow_ID': f' {workflow_id} ', 'Reason': 'because'}, user_id,
        is_admin)

    assert 'Cancel requested' in message
    cold = WorkflowTracker()
    assert cold.is_workflow_cancel_requested(workflow_id)
    assert cold.repository.get(workflow_id).cancel_reason == 'because'


def test_cancel_script_runscript_refuses_another_user():
    tracker = WorkflowTracker()
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)

    message = cancel_runscript(
        {'Workflow_ID': str(workflow_id), 'Reason': 'because'}, 8, False)

    assert message.startswith('Error:') and 'belongs to another user' in message
    assert not WorkflowTracker().is_workflow_cancel_requested(workflow_id)


def test_cancel_script_runscript_reports_a_bad_workflow_id():
    message = cancel_runscript({'Workflow_ID': 'not-a-uuid'}, 1, True)
    assert message.startswith('Error:')


def test_cancel_script_runscript_uses_the_default_reason():
    tracker = WorkflowTracker()
    workflow_id = tracker.initiate_workflow('wf', '', user=7, group=1)
    cancel_runscript({'Workflow_ID': str(workflow_id)}, 7, False)
    assert WorkflowTracker().repository.get(
        workflow_id).cancel_reason == 'Cancelled by user'
