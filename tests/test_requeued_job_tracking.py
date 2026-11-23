"""The runner follows a Slurm job that Slurm requeues after a node failure.

Core owns the rule (`find_requeued_job_state`); the runner's tracking loop asks
it for NODE_FAIL and PREEMPTED, keeps tracking a requeued job and fails a job
that was not requeued. These tests execute the real tracking functions.
"""
import ast
import contextlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple
from unittest.mock import Mock
from uuid import UUID

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / '__workflows' / 'SLURM_Run_Workflow.py'
NODE_NAMES = {'JobUpdate', 'monitor_slurm_jobs', 'process_job_state',
              'end_failed_job'}
NODES = [node for node in ast.parse(SOURCE.read_text(encoding='utf-8')).body
         if isinstance(node, (ast.FunctionDef, ast.ClassDef))
         and node.name in NODE_NAMES]

try:
    from biomero.slurm_client import REQUEUEABLE_JOB_STATES, SlurmJob
    from paramiko.ssh_exception import SSHException
except ImportError:
    if os.environ.get('CI'):
        pytest.fail('biomero 2.9.0b32 or newer must be installed in CI',
                    pytrace=False)
    pytest.skip('Installed biomero has no requeue support',
                allow_module_level=True)
if 'process_job_state' not in {node.name for node in NODES} or \
        'REQUEUEABLE_JOB_STATES' not in SOURCE.read_text(encoding='utf-8'):
    pytest.skip('Source does not follow requeued jobs yet',
                allow_module_level=True)

JOB_ID = 42
TASK_ID = UUID(int=1)


def tracking_functions(job_states, requeue_answer):
    """Load the tracking functions; the Slurm client reports `job_states`."""
    polls = iter(job_states)
    slurm_client = SimpleNamespace(
        check_job_status=Mock(
            side_effect=lambda ids: ({JOB_ID: next(polls)}, None)),
        find_requeued_job_state=Mock(side_effect=requeue_answer),
        get_active_job_progress=Mock(return_value='0%'))
    pipeline = SimpleNamespace(
        slurmClient=slurm_client, conn=SimpleNamespace(keepAlive=Mock()),
        tracker=Mock(), client=Mock(), wf_id='workflow-id',
        raise_if_stopped=Mock(), raise_if_claim_lost=Mock())
    namespace = dict(
        logger=logging.getLogger('runner'), dataclass=dataclass,
        SlurmJob=SlurmJob, REQUEUEABLE_JOB_STATES=REQUEUEABLE_JOB_STATES,
        Dict=Dict, Any=Any, List=List, Tuple=Tuple, UUID=UUID,
        hpc_recovery_scope=lambda *args: contextlib.nullcontext(),
        poll_slurm_jobs=lambda client, ids: client.check_job_status(ids)[0],
        timesleep=SimpleNamespace(sleep=lambda seconds: None),
        upload_job_log_to_omero=lambda *args: ' (log uploaded)')
    exec(compile(ast.Module(body=NODES, type_ignores=[]), str(SOURCE), 'exec'),
         namespace)
    namespace['import_completed_job'] = Mock(
        return_value=namespace['JobUpdate'](finished=True))
    return namespace, pipeline


def monitor(namespace, pipeline):
    return namespace['monitor_slurm_jobs'](
        pipeline, {'analysis': JOB_ID}, {JOB_ID: TASK_ID}, {}, '', 3, '')


@pytest.mark.parametrize('ended_state', sorted(REQUEUEABLE_JOB_STATES))
def test_requeued_job_is_tracked_to_completion(ended_state):
    namespace, pipeline = tracking_functions(
        [ended_state, 'RUNNING', 'COMPLETED'], ['PENDING'])

    wf_failed, _ = monitor(namespace, pipeline)

    assert wf_failed is False
    pipeline.slurmClient.find_requeued_job_state.assert_called_once_with(
        JOB_ID)
    recorded = [call.args[1] for call
                in pipeline.tracker.update_task_status.call_args_list]
    assert recorded == ['PENDING', 'RUNNING', 'COMPLETED']
    pipeline.tracker.fail_task.assert_not_called()
    namespace['import_completed_job'].assert_called_once()


def test_job_that_was_not_requeued_fails_with_a_clear_message():
    namespace, pipeline = tracking_functions(['NODE_FAIL'], [None])

    wf_failed, messages = monitor(namespace, pipeline)

    assert wf_failed is True
    assert f'Slurm job {JOB_ID} ended in NODE_FAIL and was not requeued' \
        in messages
    pipeline.tracker.fail_task.assert_called_once_with(
        TASK_ID, 'Slurm job state NODE_FAIL')
    namespace['import_completed_job'].assert_not_called()


def test_failing_requeue_check_propagates():
    namespace, pipeline = tracking_functions(
        ['PREEMPTED'], SSHException('squeue unreachable'))

    with pytest.raises(SSHException, match='squeue unreachable'):
        monitor(namespace, pipeline)

    pipeline.tracker.fail_task.assert_not_called()


def test_other_ended_states_do_not_ask_for_a_requeue():
    namespace, pipeline = tracking_functions(['FAILED'], [])

    wf_failed, _ = monitor(namespace, pipeline)

    assert wf_failed is True
    pipeline.slurmClient.find_requeued_job_state.assert_not_called()
