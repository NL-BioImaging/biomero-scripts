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
from typing import Any, Callable, Optional
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
    'execute_workflow_pipeline', 'run_pipeline_stages', 'export_data_stage',
    'convert_data_stage', 'submit_workflow_jobs', 'monitor_slurm_jobs',
    'exportImageToSLURM', 'convertDataOnSLURM', 'ensure_tracking_uuid',
    'PipelineRun', 'JobUpdate', 'process_job_state', 'end_failed_job',
    'import_completed_job'}
NODES = [node for node in TREE.body
         if isinstance(node, (ast.FunctionDef, ast.ClassDef))
         and node.name in NODE_NAMES]
FOUND = {node.name for node in NODES}
if 'run_pipeline_stages' not in FOUND:
    pytest.skip('Source has no detached pipeline yet', allow_module_level=True)
# From here the source claims the feature: a renamed or missing piece must
# fail these tests, not skip them.
MISSING = {'decide_workflow_resume', 'cancel_workflow_run', 'PipelineRun',
           'load_task_history', 'register_detached_launcher'} - FOUND
if MISSING:
    pytest.fail(f'Source lacks {sorted(MISSING)}', pytrace=False)



def core_unavailable(reason):
    """Skip locally; in CI the missing core must not hide these tests."""
    if os.environ.get('CI'):
        pytest.fail(f"{reason}: add the released biomero to "
                    f"tests/requirements.txt so these tests run in CI",
                    pytrace=False)
    pytest.skip(reason, allow_module_level=True)


try:
    import biomero.constants as biomero_constants
    import biomero.eventsourcing as eventsourcing
except ImportError:
    core_unavailable('biomero core is not installed')
if not hasattr(eventsourcing.WorkflowTracker, 'add_launcher_task'):
    core_unavailable('Installed biomero lacks the detached mechanisms')

from biomero.database import EngineManager  # noqa: E402
from biomero.slurm_client import SlurmClient, SlurmJob  # noqa: E402

WORKFLOW = 'analysis'


def eventsourcing_uuid(value):
    return value if isinstance(value, UUID) else UUID(str(value))


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
        is_launcher_task=eventsourcing.is_launcher_task,
        rbool=lambda value: value, rstring=lambda value: value,
        omscripts=SimpleNamespace(client=object), BlitzGateway=object,
        Any=Any, Callable=Callable, WorkflowInterrupted=eventsourcing.WorkflowInterrupted,
        Parameters=object, rlong=lambda value: value,
        IMPORTER_ENABLED=False, SHALLOW_ZARR_ENABLED=False,
        ensure_tracking_uuid=eventsourcing_uuid,
        constants=biomero_constants, VERSION='2.9.0-test',
        EXPORT_SCRIPTS=(EXPORT,), CONVERSION_SCRIPTS=(CONVERT,),
        IMPORT_SCRIPTS=(IMPORT,),
        hpc_recovery_scope=lambda *args: contextlib.nullcontext(),
        timesleep=SimpleNamespace(sleep=lambda seconds: None),
        createFileName=lambda *args: 'input-folder')
    exec(compile(ast.Module(body=NODES, type_ignores=[]), str(SOURCE), 'exec'),
         namespace)
    namespace.update(overrides)  # after the exec: stand-ins for source functions
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

def test_cancelled_run_cancels_jobs_closes_every_open_task_and_fails_the_workflow(
        tracker, workflow_id):
    launcher = tracker.add_launcher_task(
        workflow_id, biomero_constants.RUN_WF_SCRIPT, 'v1', [], {})
    transfer = add_task(tracker, workflow_id, EXPORT)  # started, no job
    running = add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING')
    finished = add_task(tracker, workflow_id, WORKFLOW, [43], 'COMPLETED',
                        outcome='completed')
    slurm = SimpleNamespace(
        workflowTracker=tracker, cancel_slurm_jobs=Mock(),
        get_workflow_tasks=lambda wf: history_of(tracker, wf))

    message = runner_namespace()['cancel_workflow_run'](
        slurm, workflow_id, 'Cancelled: asked', 'Exported. ')

    slurm.cancel_slurm_jobs.assert_called_once_with([42])
    assert tracker.repository.get(running).failed
    assert tracker.repository.get(transfer).failed
    assert tracker.repository.get(finished).completed
    assert not tracker.repository.get(finished).failed
    assert not tracker.repository.get(launcher).failed
    topics = [event.topic.rsplit('.', 1)[-1] for event in
              tracker.recorder.select_events(workflow_id)]
    assert topics[-1] == 'WorkflowFailed'
    assert message == 'Exported. Cancelled: asked'


def budgeted_poll(budget=5):
    """A poll stub that fails loudly instead of letting a broken run loop."""
    calls = []

    def poll(slurm_client, job_ids):
        calls.append(list(job_ids))
        assert len(calls) <= budget, 'the run kept polling'
        return {job_id: 'RUNNING' for job_id in job_ids}
    return Mock(side_effect=poll)


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
        poll_slurm_jobs=budgeted_poll(),
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


# ------------------- cancel passes through every stage ------------------- #

class ClaimLost(eventsourcing.WorkflowInterrupted):
    """What a detached worker raises when it lost its claim."""


JOB = SimpleNamespace(getStatus=lambda: SimpleNamespace(getId=lambda: 8))


def stage_setup(tracker, workflow_id):
    """Client, connection and Slurm client for the real export/convert stages."""
    scripts = [SimpleNamespace(id=5, getVersion=lambda: 'v', getName=lambda: EXPORT),
               SimpleNamespace(id=6, getVersion=lambda: 'v', getName=lambda: CONVERT)]
    conn = SimpleNamespace(
        getScriptService=lambda: SimpleNamespace(getScripts=lambda: scripts),
        keepAlive=lambda: None)
    inputs = {'IDs': [1], 'Data_Type': 'Dataset'}
    client = SimpleNamespace(getInput=lambda name: inputs.get(name),
                             setOutput=lambda *args: None)
    slurm = SimpleNamespace(
        workflowTracker=tracker, cancel_slurm_jobs=Mock(),
        update_slurm_scripts=lambda: SimpleNamespace(),
        get_active_job_progress=lambda job: '0%',
        get_workflow_tasks=lambda wf: history_of(tracker, wf))
    tracker.add_launcher_task(workflow_id, biomero_constants.RUN_WF_SCRIPT,
                              'v1', [], {})
    return client, conn, slurm


def last_event(tracker, workflow_id):
    return [e.topic.rsplit('.', 1)[-1]
            for e in tracker.recorder.select_events(workflow_id)][-1]


def open_stage_tasks(tracker, workflow_id):
    return [t.task_name for t in history_of(tracker, workflow_id)
            if not (t.completed or t.failed)
            and not eventsourcing.is_launcher_task(t)]


def sub_script_that(stage, action):
    """run_omero_script stand-in: `action` happens in the named stage only."""
    def run_omero_script(client, svc, script_id, inputs, **kwargs):
        if script_id == {'export': 5, 'convert': 6}[stage]:
            action()
        return {'Message': SimpleNamespace(getValue=lambda: 'ok')}, JOB
    return run_omero_script


def run_real_stages(names, tracker, workflow_id, client, conn, slurm):
    return names['execute_workflow_pipeline'](
        client, conn, slurm, workflow_id, ['analysis'], {}, {}, {}, '', 3, True,
        '0.4', zipfile='input-folder')


@pytest.mark.parametrize('stage', ['export', 'convert'])
def test_cancel_inside_a_stage_script_ends_in_a_consistent_failed_state(
        tracker, workflow_id, stage):
    client, conn, slurm = stage_setup(tracker, workflow_id)

    def cancel():
        tracker.request_workflow_cancel(workflow_id, 'user asked')
        tracker.raise_if_cancel_requested(workflow_id)
    names = runner_namespace(run_omero_script=sub_script_that(stage, cancel))
    slurm.get_all_image_versions_and_data_files = lambda: (None, ['input-folder'])

    message = run_real_stages(names, tracker, workflow_id, client, conn, slurm)

    assert 'user asked' in message
    assert open_stage_tasks(tracker, workflow_id) == []
    assert last_event(tracker, workflow_id) == 'WorkflowFailed'


def test_cancel_inside_the_import_ends_in_a_consistent_failed_state(
        tracker, workflow_id):
    client, conn, slurm = stage_setup(tracker, workflow_id)
    client.getInput = lambda name: True

    def submit(slurm_client, params, file_params, client_, conn_, ui, zipfile,
               email, wf_name, wf_id, output_settings=None):
        return ui, 42, wf_id, add_task(tracker, wf_id, wf_name, [42], 'PENDING')

    def import_results(*args, **kwargs):
        add_task(tracker, workflow_id, IMPORT)
        tracker.request_workflow_cancel(workflow_id, 'user asked')
        tracker.raise_if_cancel_requested(workflow_id)
    names = runner_namespace(
        exportImageToSLURM=Mock(return_value=({}, 'x')),
        convertDataOnSLURM=Mock(return_value=({}, 'x')),
        run_workflow=submit, importResultsToOmero=import_results,
        poll_slurm_jobs=lambda s, ids: {42: 'COMPLETED'},
        upload_job_log_to_omero=lambda *args: '')

    message = names['execute_workflow_pipeline'](
        client, conn, slurm, workflow_id, ['analysis'], {}, {'analysis': {}},
        {'analysis': {}}, '', 3, True, '0.4', zipfile='input-folder')

    assert 'user asked' in message
    assert open_stage_tasks(tracker, workflow_id) == []
    assert last_event(tracker, workflow_id) == 'WorkflowFailed'
    slurm.cancel_slurm_jobs.assert_called_once_with([])  # already completed


@pytest.mark.parametrize('stage', ['export', 'convert'])
def test_claim_lost_inside_a_stage_leaves_the_workflow_and_tasks_alone(
        tracker, workflow_id, stage):
    client, conn, slurm = stage_setup(tracker, workflow_id)

    def lose_claim():
        raise ClaimLost('lost')
    names = runner_namespace(
        run_omero_script=sub_script_that(stage, lose_claim))
    slurm.get_all_image_versions_and_data_files = lambda: (None, ['input-folder'])

    with pytest.raises(ClaimLost):
        run_real_stages(names, tracker, workflow_id, client, conn, slurm)

    assert not any(t.failed for t in history_of(tracker, workflow_id))
    assert last_event(tracker, workflow_id) not in ('WorkflowFailed',
                                                    'WorkflowCompleted')
    slurm.cancel_slurm_jobs.assert_not_called()


def test_stop_check_stops_the_run_between_steps_without_failing_it(
        tracker, workflow_id):
    names, slurm, export, convert, _ = pipeline_namespace(tracker)
    checks = []

    def stop_check():
        checks.append(1)
        if len(checks) == 2:  # after the export, before the conversion
            raise ClaimLost('lost')
    client = SimpleNamespace(getInput=lambda name: True)
    conn = SimpleNamespace(keepAlive=lambda: None)

    with pytest.raises(ClaimLost):
        names['execute_workflow_pipeline'](
            client, conn, slurm, workflow_id, [WORKFLOW], {}, {WORKFLOW: {}},
            {WORKFLOW: {}}, '', 3, True, '0.4', zipfile='input-folder',
            stop_check=stop_check)

    export.assert_called_once()
    convert.assert_not_called()
    assert last_event(tracker, workflow_id) not in ('WorkflowFailed',
                                                    'WorkflowCompleted')


def test_stop_check_also_stops_the_poll_loop(tracker, workflow_id):
    names, slurm, *_ = pipeline_namespace(tracker)
    polls = []
    names['poll_slurm_jobs'] = lambda s, ids: polls.append(1) or {42: 'RUNNING'}

    def stop_check():
        if polls:
            raise ClaimLost('lost')
    client = SimpleNamespace(getInput=lambda name: True)
    conn = SimpleNamespace(keepAlive=lambda: None)

    with pytest.raises(ClaimLost):
        names['execute_workflow_pipeline'](
            client, conn, slurm, workflow_id, [WORKFLOW], {}, {WORKFLOW: {}},
            {WORKFLOW: {}}, '', 3, True, '0.4', zipfile='input-folder',
            stop_check=stop_check)

    assert polls == [1]
    slurm.cancel_slurm_jobs.assert_not_called()


def test_failing_scancel_still_fails_the_tasks_and_the_workflow(
        tracker, workflow_id, caplog):
    running = add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING')
    slurm = SimpleNamespace(
        workflowTracker=tracker,
        cancel_slurm_jobs=Mock(side_effect=RuntimeError('ssh down')),
        get_workflow_tasks=lambda wf: history_of(tracker, wf))

    with caplog.at_level(logging.ERROR, logger='runner'):
        runner_namespace()['cancel_workflow_run'](
            slurm, workflow_id, 'Cancelled: asked', '')

    assert tracker.repository.get(running).failed
    assert last_event(tracker, workflow_id) == 'WorkflowFailed'
    assert any(r.levelno == logging.ERROR and 'ssh down' in r.getMessage()
               for r in caplog.records)


@pytest.mark.parametrize('job_state', ['FAILED', 'CANCELLED by 1', 'TIMEOUT',
                                       'MYSTERY'])
def test_a_job_that_did_not_succeed_fails_its_task_keeps_its_log_and_fails_the_run(
        tracker, workflow_id, job_state):
    add_task(tracker, workflow_id, EXPORT, outcome='completed')
    add_task(tracker, workflow_id, CONVERT, outcome='completed')
    names, slurm, _, _, import_results = pipeline_namespace(tracker)
    names['poll_slurm_jobs'] = lambda s, ids: {42: job_state}
    names['upload_job_log_to_omero'] = Mock(return_value=' [log]')

    message = run_pipeline(names, slurm, workflow_id, resume=False)

    names['upload_job_log_to_omero'].assert_called_once()
    import_results.assert_not_called()
    assert ' [log]' in message
    job_task = [t for t in history_of(tracker, workflow_id)
                if t.task_name == WORKFLOW][0]
    assert job_task.failed
    assert last_event(tracker, workflow_id) == 'WorkflowFailed'


def test_an_import_that_reports_failure_fails_the_run(tracker, workflow_id):
    names, slurm, _, _, import_results = pipeline_namespace(tracker)
    names['poll_slurm_jobs'] = lambda s, ids: {42: 'COMPLETED'}
    import_results.return_value = {
        'Message': SimpleNamespace(getValue=lambda: 'FAILED: no write access')}

    message = run_pipeline(names, slurm, workflow_id)

    assert 'FAILED: no write access' in message
    assert last_event(tracker, workflow_id) == 'WorkflowFailed'


def test_every_stage_that_catches_exceptions_lets_a_cancel_pass():
    """Each generic handler around a sub-script run re-raises a cancel first."""
    problems = []
    # validate_importer_write_access runs before a workflow exists: no cancel
    # can be pending there.
    for func in (n for n in TREE.body if isinstance(n, ast.FunctionDef)
                 and n.name != 'validate_importer_write_access'):
        for node in ast.walk(func):
            if not isinstance(node, ast.Try):
                continue
            runs_script = any(
                isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                and c.func.id == 'run_omero_script'
                for stmt in node.body for c in ast.walk(stmt))
            catches_all = any(
                isinstance(h.type, ast.Name) and h.type.id == 'Exception'
                for h in node.handlers)
            if not (runs_script and catches_all):
                continue
            handler_types = [h.type.id for h in node.handlers
                             if isinstance(h.type, ast.Name)]
            if 'WorkflowInterrupted' not in handler_types or (
                    handler_types.index('WorkflowInterrupted')
                    > handler_types.index('Exception')):
                problems.append(func.name)
    assert not problems, f'{problems} swallow a cancel as a generic error'


def test_resume_with_two_good_attempts_monitors_the_newest(tracker,
                                                           workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [41], 'RUNNING')
    newest = add_task(tracker, workflow_id, WORKFLOW, [42], 'PENDING')
    result = decision(tracker, workflow_id)
    assert (result.slurm_job_id, result.task_id) == (42, newest)


def test_cancel_check_before_each_submission_stops_the_run(tracker,
                                                           workflow_id):
    """A cancel that arrives while the data is converted stops before submit."""
    names, slurm, export, convert, _ = pipeline_namespace(tracker)

    def convert_then_cancel(*args):
        tracker.request_workflow_cancel(workflow_id, 'user asked')
        return {}, 'convert-task'
    convert.side_effect = convert_then_cancel

    run_pipeline(names, slurm, workflow_id)

    names['run_workflow'].assert_not_called()
    slurm.cancel_slurm_jobs.assert_called_once_with([])


def test_cancel_check_between_export_and_conversion(tracker, workflow_id):
    names, slurm, export, convert, _ = pipeline_namespace(tracker)

    def export_then_cancel(*args, **kwargs):
        tracker.request_workflow_cancel(workflow_id, 'user asked')
        return {}, 'export-task'
    export.side_effect = export_then_cancel

    run_pipeline(names, slurm, workflow_id)

    convert.assert_not_called()


# ------------- the claim is checked before the owner-only writes ------------- #

class Claim:
    """A stop check that can be flipped from inside a stub."""

    def __init__(self):
        self.lost = False

    def __call__(self):
        if self.lost:
            raise ClaimLost('lost')


def run_with_claim(names, slurm, workflow_id, claim):
    client = SimpleNamespace(getInput=lambda name: True)
    conn = SimpleNamespace(keepAlive=lambda: None)
    return names['execute_workflow_pipeline'](
        client, conn, slurm, workflow_id, [WORKFLOW], {}, {WORKFLOW: {}},
        {WORKFLOW: {}}, '', 3, True, '0.4', zipfile='input-folder',
        stop_check=claim)


def test_claim_lost_during_a_poll_writes_nothing_after_the_poll(
        tracker, workflow_id):
    claim = Claim()
    names, slurm, _, _, import_results = pipeline_namespace(tracker)

    def poll(slurm_client, job_ids):
        claim.lost = True
        return {42: 'COMPLETED'}
    names['poll_slurm_jobs'] = poll

    with pytest.raises(ClaimLost):
        run_with_claim(names, slurm, workflow_id, claim)

    job_task = [t for t in history_of(tracker, workflow_id)
                if t.task_name == WORKFLOW][0]
    assert job_task.status == 'PENDING'  # the COMPLETED state was not written
    assert not job_task.completed
    import_results.assert_not_called()
    assert last_event(tracker, workflow_id) not in ('WorkflowFailed',
                                                    'WorkflowCompleted')


def test_claim_lost_during_the_import_leaves_the_outcome_to_the_new_owner(
        tracker, workflow_id):
    claim = Claim()
    names, slurm, _, _, import_results = pipeline_namespace(tracker)
    names['poll_slurm_jobs'] = lambda s, ids: {42: 'COMPLETED'}

    def import_then_lose_claim(*args, **kwargs):
        claim.lost = True
        return {}
    import_results.side_effect = import_then_lose_claim

    with pytest.raises(ClaimLost):
        run_with_claim(names, slurm, workflow_id, claim)

    assert last_event(tracker, workflow_id) not in ('WorkflowFailed',
                                                    'WorkflowCompleted')


def test_cancel_arriving_during_the_import_does_not_stop_the_final_write(
        tracker, workflow_id):
    """Only the claim is checked before the outcome: a late cancel completes."""
    names, slurm, _, _, import_results = pipeline_namespace(tracker)
    names['poll_slurm_jobs'] = lambda s, ids: {42: 'COMPLETED'}
    import_results.side_effect = lambda *a, **k: tracker.request_workflow_cancel(
        workflow_id, 'too late') or {}

    run_with_claim(names, slurm, workflow_id, Claim())

    assert last_event(tracker, workflow_id) == 'WorkflowCompleted'


def test_claim_lost_during_the_conversion_stops_before_any_submission(
        tracker, workflow_id):
    claim = Claim()
    names, slurm, _, convert, _ = pipeline_namespace(tracker)
    convert.side_effect = lambda *a: setattr(claim, 'lost', True) or ({}, 'x')

    with pytest.raises(ClaimLost):
        run_with_claim(names, slurm, workflow_id, claim)

    names['run_workflow'].assert_not_called()


def test_failing_scancel_names_the_jobs_to_cancel_by_hand(tracker, workflow_id):
    add_task(tracker, workflow_id, WORKFLOW, [42], 'RUNNING')
    slurm = SimpleNamespace(
        workflowTracker=tracker,
        cancel_slurm_jobs=Mock(side_effect=RuntimeError('ssh down')),
        get_workflow_tasks=lambda wf: history_of(tracker, wf))

    message = runner_namespace()['cancel_workflow_run'](
        slurm, workflow_id, 'Cancelled: asked', '')

    assert 'Slurm jobs [42] could not be cancelled; cancel them manually' in message
    failed = tracker.repository.get(workflow_id)
    assert failed.finished


def test_unreadable_tasks_still_fail_the_workflow(tracker, workflow_id):
    slurm = SimpleNamespace(
        workflowTracker=tracker, cancel_slurm_jobs=Mock(),
        get_workflow_tasks=Mock(side_effect=RuntimeError('database down')))

    runner_namespace()['cancel_workflow_run'](
        slurm, workflow_id, 'Cancelled: asked', '')

    assert last_event(tracker, workflow_id) == 'WorkflowFailed'
