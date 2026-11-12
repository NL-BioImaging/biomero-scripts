"""Resume decisions, cancellation and launcher registration of the workflow runner.

Core owns the mechanisms (claim, cancel request, scancel); the runner script
decides how an interrupted run resumes and what a cancelled run does. These
tests execute the real runner functions against a real tracker.
"""
import ast
import contextlib
import logging
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest.mock import Mock
from uuid import UUID

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / '__workflows' / 'SLURM_Run_Workflow.py'
TREE = ast.parse(SOURCE.read_text(encoding='utf-8'))
NODE_NAMES = {
    'WorkflowResume', 'ResumeDecision', 'load_task_history', 'task_completed',
    'slurm_job_failed', 'results_imported', 'decide_workflow_resume',
    'cancel_workflow_run', 'register_detached_launcher',
    'execute_workflow_pipeline', 'run_pipeline_stages'}
NODES = [node for node in TREE.body
         if isinstance(node, (ast.FunctionDef, ast.ClassDef))
         and node.name in NODE_NAMES]
if not any(node.name == 'decide_workflow_resume' for node in NODES):
    pytest.skip('Source does not own resume decisions yet',
                allow_module_level=True)

biomero_constants = pytest.importorskip('biomero.constants')
eventsourcing = pytest.importorskip('biomero.eventsourcing')
if not hasattr(eventsourcing.WorkflowTracker, 'add_launcher_task'):
    pytest.skip('Requires core with the detached mechanisms',
                allow_module_level=True)

from biomero.database import EngineManager  # noqa: E402
from biomero.slurm_client import SlurmClient, SlurmJob  # noqa: E402

WORKFLOW = 'analysis'
EXPORT, CONVERT, IMPORT = 'export', 'convert', 'import'


@pytest.fixture(autouse=True)
def tracking_database(tmp_path):
    os.environ['PERSISTENCE_MODULE'] = 'eventsourcing_sqlalchemy'
    os.environ['SQLALCHEMY_URL'] = f"sqlite:///{(tmp_path / 'wf.db').as_posix()}"
    EngineManager.create_scoped_session()
    yield
    del os.environ['PERSISTENCE_MODULE']
    del os.environ['SQLALCHEMY_URL']
    EngineManager.close_engine()


@pytest.fixture
def tracker():
    return eventsourcing.WorkflowTracker()


@pytest.fixture
def workflow_id(tracker):
    return tracker.initiate_workflow('wf', '', user=1, group=2)


def runner_namespace(**overrides):
    namespace = dict(
        logger=logging.getLogger('runner'), unwrap=lambda value: value,
        SlurmClient=SlurmClient, SlurmJob=SlurmJob, UUID=UUID,
        Optional=Optional, Enum=Enum, dataclass=dataclass,
        WorkflowCancelled=eventsourcing.WorkflowCancelled,
        constants=biomero_constants, VERSION='2.9.0-test',
        EXPORT_SCRIPTS=(EXPORT,), CONVERSION_SCRIPTS=(CONVERT,),
        IMPORT_SCRIPTS=(IMPORT,),
        hpc_recovery_scope=lambda *args: contextlib.nullcontext(),
        timesleep=SimpleNamespace(sleep=lambda seconds: None),
        createFileName=lambda *args: 'input-folder')
    namespace.update(overrides)
    exec(compile(ast.Module(body=NODES, type_ignores=[]), str(SOURCE), 'exec'),
         namespace)
    return namespace


def add_task(tracker, workflow_id, name, job_ids=(), status=None, params=None,
             outcome=None):
    task_id = tracker.add_task_to_workflow(workflow_id, name, 'v1', [],
                                           params or {})
    for job_id in job_ids:
        tracker.add_job_id(task_id, job_id)
    if status:
        tracker.update_task_status(task_id, status)
    if outcome == 'completed':
        tracker.complete_task(task_id, 'done')
    if outcome == 'failed':
        tracker.fail_task(task_id, 'broke')
    return task_id


def history_of(tracker, workflow_id):
    return list(tracker.iter_workflow_tasks(workflow_id))


# ---------------------------- resume decisions ---------------------------- #

def test_unreadable_history_fails_the_resume_instead_of_starting_over():
    names = runner_namespace()
    slurm = SimpleNamespace(get_workflow_tasks=Mock(
        side_effect=RuntimeError('database down')))
    with pytest.raises(RuntimeError, match='starting over could submit'):
        names['load_task_history'](slurm, 'wf')


def test_history_is_the_workflows_tasks_in_order(tracker, workflow_id):
    first = add_task(tracker, workflow_id, EXPORT)
    second = add_task(tracker, workflow_id, WORKFLOW)
    slurm = SimpleNamespace(get_workflow_tasks=lambda wf: history_of(
        tracker, wf))
    history = runner_namespace()['load_task_history'](slurm, workflow_id)
    assert [task.id for task in history] == [first, second]


def test_task_completed_needs_a_completed_task_of_that_name(tracker,
                                                            workflow_id):
    add_task(tracker, workflow_id, EXPORT, outcome='failed')
    add_task(tracker, workflow_id, CONVERT, outcome='completed')
    add_task(tracker, workflow_id, WORKFLOW)
    history = history_of(tracker, workflow_id)
    names = runner_namespace()
    assert names['task_completed'](history, (CONVERT,))
    assert not names['task_completed'](history, (EXPORT,))
    assert not names['task_completed'](history, (WORKFLOW,))


def decision(tracker, workflow_id):
    return runner_namespace()['decide_workflow_resume'](
        history_of(tracker, workflow_id), WORKFLOW)


def test_fresh_run_submits(tracker, workflow_id):
    assert decision(tracker, workflow_id).action.value == 'submit'


def test_running_job_is_monitored_not_resubmitted(tracker, workflow_id):
    task_id = add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING')
    result = decision(tracker, workflow_id)
    assert (result.action.value, result.slurm_job_id, result.task_id) == (
        'monitor', 42, task_id)


def test_finished_job_with_pending_import_is_monitored(tracker, workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [42], 'COMPLETED')
    assert decision(tracker, workflow_id).action.value == 'monitor'


@pytest.mark.parametrize('job_state', [
    'FAILED', 'CANCELLED', 'CANCELLED by 1234', 'TIMEOUT', 'OUT_OF_MEMORY',
    'NODE_FAIL'])
def test_job_that_failed_or_was_cancelled_is_submitted_again(
        tracker, workflow_id, job_state):
    add_task(tracker, workflow_id, WORKFLOW, [42], job_state)
    assert decision(tracker, workflow_id).action.value == 'submit'


def test_task_failed_by_the_earlier_run_is_submitted_again(tracker,
                                                           workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING', outcome='failed')
    assert decision(tracker, workflow_id).action.value == 'submit'


def test_newer_good_attempt_wins_over_an_older_failed_one(tracker,
                                                          workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [41], 'FAILED', outcome='failed')
    newer = add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING')
    result = decision(tracker, workflow_id)
    assert (result.action.value, result.slurm_job_id, result.task_id) == (
        'monitor', 42, newer)


def test_attempt_without_a_real_job_id_is_not_a_job(tracker, workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [-1])
    assert decision(tracker, workflow_id).action.value == 'submit'


@pytest.mark.parametrize('recorded_job_id', [42, '42'])
def test_job_whose_results_were_imported_is_skipped_not_submitted_again(
        tracker, workflow_id, recorded_job_id):
    add_task(tracker, workflow_id, WORKFLOW, [42], 'COMPLETED')
    add_task(tracker, workflow_id, IMPORT, params={
        biomero_constants.results.OUTPUT_SLURM_JOB_ID: recorded_job_id},
        outcome='completed')
    assert decision(tracker, workflow_id).action.value == 'already imported'


def test_import_of_another_job_does_not_count(tracker, workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [42], 'COMPLETED')
    add_task(tracker, workflow_id, IMPORT, params={
        biomero_constants.results.OUTPUT_SLURM_JOB_ID: 7},
        outcome='completed')
    assert decision(tracker, workflow_id).action.value == 'monitor'


def test_import_that_did_not_finish_does_not_count(tracker, workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [42], 'COMPLETED')
    add_task(tracker, workflow_id, IMPORT, params={
        biomero_constants.results.OUTPUT_SLURM_JOB_ID: 42}, outcome='failed')
    assert decision(tracker, workflow_id).action.value == 'monitor'


# ------------------------------ cancellation ------------------------------ #

def test_cancelled_run_cancels_jobs_closes_open_tasks_and_fails_the_workflow(
        tracker, workflow_id):
    running = add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING')
    finished = add_task(tracker, workflow_id, WORKFLOW, [43], 'COMPLETED',
                        outcome='completed')
    slurm = SimpleNamespace(workflowTracker=tracker,
                            cancel_slurm_jobs=Mock())

    message = runner_namespace()['cancel_workflow_run'](
        slurm, workflow_id, {42: running, 43: finished}, 'Cancelled: asked',
        'Exported. ')

    slurm.cancel_slurm_jobs.assert_called_once_with([42, 43])
    assert tracker.repository.get(running).failed
    assert tracker.repository.get(finished).completed
    assert not tracker.repository.get(finished).failed
    topics = [event.topic.rsplit('.', 1)[-1] for event in
              tracker.recorder.select_events(workflow_id)]
    assert topics[-1] == 'WorkflowFailed'
    assert message == 'Exported. Cancelled: asked'


def pipeline_namespace(tracker, **overrides):
    """What execute_workflow_pipeline needs around it, with Slurm stubbed."""
    export = Mock(return_value=({}, 'export-task'))
    convert = Mock(return_value=({}, 'convert-task'))
    import_results = Mock(return_value={})
    job_ids = iter([42, 43])

    def submit(slurm_client, params, file_params, client, conn, ui_messages,
               zipfile, email, wf_name, wf_id, output_settings=None):
        job_id = next(job_ids)
        task_id = add_task(tracker, wf_id, wf_name, [job_id], 'PENDING')
        return ui_messages, job_id, wf_id, task_id

    names = runner_namespace(
        exportImageToSLURM=export, convertDataOnSLURM=convert,
        importResultsToOmero=import_results,
        run_workflow=Mock(side_effect=submit),
        poll_slurm_jobs=Mock(return_value={42: 'RUNNING'}),
        upload_job_log_to_omero=Mock(return_value=''), **overrides)
    slurm = SimpleNamespace(
        workflowTracker=tracker, update_slurm_scripts=Mock(
            return_value=SimpleNamespace()),
        get_active_job_progress=Mock(return_value='0%'),
        cancel_slurm_jobs=Mock(), get_workflow_tasks=lambda wf: history_of(
            tracker, wf))
    return names, slurm, export, convert, import_results


def run_pipeline(names, slurm, workflow_id, resume=False):
    client = SimpleNamespace(getInput=lambda name: True)
    conn = SimpleNamespace(keepAlive=lambda: None)
    return names['execute_workflow_pipeline'](
        client, conn, slurm, workflow_id, [WORKFLOW], {}, {WORKFLOW: {}},
        {WORKFLOW: {}}, '', 3, True, '0.4', zipfile='input-folder',
        resume=resume)


def test_run_cancelled_before_it_starts_does_no_work(tracker, workflow_id):
    names, slurm, export, convert, _ = pipeline_namespace(tracker)
    tracker.request_workflow_cancel(workflow_id, 'user asked')

    message = run_pipeline(names, slurm, workflow_id)

    export.assert_not_called()
    convert.assert_not_called()
    names['run_workflow'].assert_not_called()
    slurm.cancel_slurm_jobs.assert_called_once_with([])
    assert 'user asked' in message
    assert [event.topic.rsplit('.', 1)[-1] for event in
            tracker.recorder.select_events(workflow_id)][-1] == (
                'WorkflowFailed')


def test_run_cancelled_while_its_job_runs_cancels_the_job(tracker, workflow_id):
    """The cancel request arrives during a poll; the next poll stops the run."""
    names, slurm, _, _, import_results = pipeline_namespace(tracker)

    polls = []

    def poll(slurm_client, job_ids):
        assert not polls, 'the run kept polling after the cancel request'
        polls.append(list(job_ids))
        tracker.request_workflow_cancel(workflow_id, 'user asked')
        return {42: 'RUNNING'}
    names['poll_slurm_jobs'] = poll

    message = run_pipeline(names, slurm, workflow_id)

    assert polls == [[42]]
    slurm.cancel_slurm_jobs.assert_called_once_with([42])
    import_results.assert_not_called()
    assert 'user asked' in message
    assert [event.topic.rsplit('.', 1)[-1] for event in
            tracker.recorder.select_events(workflow_id)][-1] == (
                'WorkflowFailed')


def test_resumed_run_monitors_the_running_job_and_does_not_resubmit(
        tracker, workflow_id):
    add_task(tracker, workflow_id, EXPORT, outcome='completed')
    add_task(tracker, workflow_id, CONVERT, outcome='completed')
    add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING')
    names, slurm, export, convert, import_results = pipeline_namespace(tracker)
    names['poll_slurm_jobs'] = Mock(return_value={42: 'COMPLETED'})

    run_pipeline(names, slurm, workflow_id, resume=True)

    export.assert_not_called()
    convert.assert_not_called()
    names['run_workflow'].assert_not_called()
    import_results.assert_called_once()


def test_resumed_run_does_not_reattach_to_a_cancelled_job(tracker, workflow_id):
    add_task(tracker, workflow_id, EXPORT, outcome='completed')
    add_task(tracker, workflow_id, CONVERT, outcome='completed')
    add_task(tracker, workflow_id, WORKFLOW, [41], 'CANCELLED by 1')
    names, slurm, *_ = pipeline_namespace(tracker)  # submits job 42
    polled = []

    def poll(slurm_client, job_ids):
        polled.append(list(job_ids))  # the pipeline empties its list after
        return {42: 'COMPLETED'}
    names['poll_slurm_jobs'] = poll

    run_pipeline(names, slurm, workflow_id, resume=True)

    names['run_workflow'].assert_called_once()
    assert polled == [[42]]


def test_resumed_run_with_imported_results_runs_nothing(tracker, workflow_id):
    add_task(tracker, workflow_id, EXPORT, outcome='completed')
    add_task(tracker, workflow_id, CONVERT, outcome='completed')
    add_task(tracker, workflow_id, WORKFLOW, [42], 'COMPLETED')
    add_task(tracker, workflow_id, IMPORT, params={
        biomero_constants.results.OUTPUT_SLURM_JOB_ID: '42'},
        outcome='completed')
    names, slurm, _, _, import_results = pipeline_namespace(tracker)

    message = run_pipeline(names, slurm, workflow_id, resume=True)

    names['run_workflow'].assert_not_called()
    names['poll_slurm_jobs'].assert_not_called()
    import_results.assert_not_called()
    assert 'already imported' in message
    assert [event.topic.rsplit('.', 1)[-1] for event in
            tracker.recorder.select_events(workflow_id)][-1] == (
                'WorkflowCompleted')


def test_resume_with_unreadable_history_submits_nothing(tracker, workflow_id):
    names, slurm, export, _, _ = pipeline_namespace(tracker)
    slurm.get_workflow_tasks = Mock(side_effect=RuntimeError('database down'))

    with pytest.raises(RuntimeError, match='starting over could submit'):
        run_pipeline(names, slurm, workflow_id, resume=True)

    export.assert_not_called()
    names['run_workflow'].assert_not_called()


# ------------------------- launcher registration ------------------------- #

def test_launcher_records_every_input_and_the_derived_values(tracker,
                                                             workflow_id):
    client = SimpleNamespace(getInputs=lambda unwrap: {
        'IDs': [1, 2], 'Data_Type': 'Image'})
    slurm = SimpleNamespace(workflowTracker=tracker)

    task_id = runner_namespace()['register_detached_launcher'](
        client, slurm, workflow_id, biomero_constants.RUN_WF_SCRIPT,
        ['analysis'], {'zipfile': 'input-folder', 'group': 3})

    task = tracker.repository.get(task_id)
    assert task.params == {
        'IDs': [1, 2], 'Data_Type': 'Image', 'zipfile': 'input-folder',
        'group': 3, 'workflows': ['analysis'],
        biomero_constants.LAUNCHER_MARKER: '2.9.0-test'}
    assert eventsourcing.is_launcher_task(task)
    assert tracker.find_launcher_task(workflow_id).id == task_id
