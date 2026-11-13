"""The orchestration layer must respect the core transport recovery deadline."""
import ast
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from script_output_names import OUTPUT_NAMES

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / '__workflows/SLURM_Run_Workflow.py'


def load(name):
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name == name]
    if not functions and not os.environ.get('BIOMERO_TEST_SSH_RECOVERY'):
        pytest.skip('SSH recovery orchestration is not present in source')
    assert functions, 'Missing SSH recovery orchestration'
    namespace = {'logger': logging.getLogger(__name__)}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), 'exec'),
         namespace)
    return namespace[name]


@pytest.mark.parametrize('window', [0, None])
def test_legacy_poll_failure_retains_existing_retry_behavior(window):
    client = SimpleNamespace(check_job_status=Mock(side_effect=EOFError()))
    if window is not None:
        client.ssh_recovery_timeout = window
    assert load('poll_slurm_jobs')(client, [42]) == {}


@pytest.mark.parametrize('error', [
    ConnectionError('SSH recovery deadline exceeded'),
    PermissionError('authentication refused'),
    ValueError('bad scheduler configuration'),
])
def test_recovery_failures_are_not_swallowed_by_an_infinite_poll_loop(error):
    client = SimpleNamespace(ssh_recovery_timeout=900,
                             check_job_status=Mock(side_effect=error))
    with pytest.raises(type(error)) as caught:
        load('poll_slurm_jobs')(client, [42])
    assert caught.value is error
    client.check_job_status.assert_called_once_with([42])


def test_recovered_job_state_uses_the_existing_job_id():
    client = SimpleNamespace(ssh_recovery_timeout=900,
                             check_job_status=Mock(return_value=({42: 'RUNNING'}, None)))
    assert load('poll_slurm_jobs')(client, [42]) == {42: 'RUNNING'}
    client.check_job_status.assert_called_once_with([42])


def test_preflight_uses_declared_read_only_route():
    class Client:
        run = Mock(side_effect=AssertionError('unsafe route'))
        run_read_only = Mock(return_value='script exists')
    client = Client()
    assert load('read_hpc_command')(client, 'test -f job.sh', warn=True) == 'script exists'
    client.run_read_only.assert_called_once_with('test -f job.sh', warn=True)


def test_preflight_supports_older_core_without_recovery():
    class Client:
        run = Mock(return_value='script exists')
    client = Client()
    assert load('read_hpc_command')(client, 'test -f job.sh', warn=True) == 'script exists'
    client.run.assert_called_once_with('test -f job.sh', warn=True)


@pytest.mark.parametrize('detached', [False, True])
@pytest.mark.parametrize('progress_timeout', [False, True])
def test_pipeline_keeps_session_alive_during_status_and_progress_outages(monkeypatch, detached, progress_timeout):
    """Both callers execute the same pipeline; detached does not excuse inline gaps."""
    ssh_recovery = pytest.importorskip('biomero.ssh_recovery')
    if not hasattr(ssh_recovery, 'scope'):
        pytest.skip('Requires recovery-capable core')
    if (not hasattr(ssh_recovery, 'DEFAULT_SSH_RECOVERY_TIMEOUT')
            and not os.environ.get('BIOMERO_TEST_SSH_RECOVERY')):
        pytest.skip('Requires core with recovery enabled by default')
    from biomero.slurm_client import SlurmClient
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    names = {'hpc_recovery_scope', 'poll_slurm_jobs', 'execute_workflow_pipeline'}
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    if not any(n.name == 'hpc_recovery_scope' for n in functions) and not os.environ.get('BIOMERO_TEST_SSH_RECOVERY'):
        pytest.skip('Pipeline heartbeat integration is not present in source')
    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(ssh_recovery.time, 'monotonic', lambda: clock.now)
    monkeypatch.setattr(ssh_recovery.time, 'sleep', lambda delay: setattr(clock, 'now', clock.now + delay))
    # Core adds jitter to retry waits; remove it so the fake clock is exact
    # (older core has no jitter seam).
    if hasattr(ssh_recovery, 'random'):
        monkeypatch.setattr(ssh_recovery.random, 'uniform', lambda low, high: 0)
    pulses = []
    conn = SimpleNamespace(keepAlive=lambda: pulses.append(clock.now))
    tracker = Mock()
    client = SimpleNamespace(getInput=lambda name: True)
    slurm = SlurmClient(config_only=True)
    assert slurm.ssh_recovery_timeout == 900
    slurm.close = Mock()
    slurm.workflowTracker = tracker
    slurm.update_slurm_scripts = Mock(return_value=SimpleNamespace())
    status_calls = []

    def status(ids):
        status_calls.append(list(ids))
        def attempt():
            if clock.now < 600:
                raise EOFError('status disconnected')
            return ({42: 'COMPLETED'}, None)
        return ssh_recovery.retry(slurm, attempt, 'status')

    def progress(job):
        if progress_timeout:
            raise ssh_recovery.SSHRecoveryTimeout('progress outage exhausted')
        start = clock.now
        def attempt():
            if clock.now - start < 60:
                raise EOFError('progress disconnected')
            return '100%'
        return ssh_recovery.retry(slurm, attempt, 'progress')

    slurm.check_job_status = status
    slurm.get_active_job_progress = progress
    history = ['recorded'] if detached else []
    run = Mock(return_value=('', 42, 'workflow-id', 'task-id'))
    import_results = Mock(return_value={'Message': SimpleNamespace(getValue=lambda: 'Imported')})
    namespace = dict(logger=logging.getLogger(__name__), unwrap=lambda value: value,
                     constants=SimpleNamespace(results=OUTPUT_NAMES),
        detached=SimpleNamespace(load_task_history=lambda *a: history,
                                 task_completed=lambda *a: True,
                                 submitted_job=lambda *a: (42, 'task-id')),
        EXPORT_SCRIPTS=(), CONVERSION_SCRIPTS=(), IMPORT_SCRIPTS=(),
        exportImageToSLURM=Mock(return_value=({}, 'export-task')),
        convertDataOnSLURM=Mock(return_value=({}, 'convert-task')),
        run_workflow=run, importResultsToOmero=import_results,
        timesleep=SimpleNamespace(sleep=lambda delay: None))
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    args = (client, conn, slurm, 'workflow-id', ['analysis'],
            {}, {'analysis': {}}, {'analysis': {}}, '', 3, True, '0.4')
    if progress_timeout:
        with pytest.raises(ssh_recovery.SSHRecoveryTimeout):
            namespace['execute_workflow_pipeline'](
                *args, zipfile='existing-input', resume=detached)
        import_results.assert_not_called()
        tracker.complete_workflow.assert_not_called()
        assert run.call_count == (0 if detached else 1)
        return
    namespace['execute_workflow_pipeline'](client, conn, slurm, 'workflow-id', ['analysis'],
        {}, {'analysis': {}}, {'analysis': {}}, '', 3, True, '0.4',
        zipfile='existing-input', resume=detached)
    assert pulses and max(b - a for a, b in zip([0] + pulses, pulses + [clock.now])) <= 10 + 1e-6
    assert status_calls == [[42]]
    assert run.call_count == (0 if detached else 1)
    import_results.assert_called_once()
    tracker.complete_workflow.assert_called_once_with('workflow-id')
    tracker.fail_workflow.assert_not_called()


def test_recovery_scope_preserves_disabled_legacy_behavior():
    """Cores before the one-path change: a window of 0 bypasses the scope."""
    ssh_recovery = pytest.importorskip('biomero.ssh_recovery')
    if not hasattr(ssh_recovery, 'enabled'):
        pytest.skip('Core runs one code path; see the window-zero test below')
    heartbeat = Mock()
    client = SimpleNamespace(ssh_recovery_timeout=0, close=Mock())
    with load('hpc_recovery_scope')(client, heartbeat):
        assert ssh_recovery.retry(client, lambda: 'legacy', 'read') == 'legacy'
    heartbeat.assert_not_called()
    client.close.assert_not_called()


def test_recovery_scope_with_window_zero_is_the_same_path_without_retries():
    """Core runs one code path: 0 only turns retrying off."""
    ssh_recovery = pytest.importorskip('biomero.ssh_recovery')
    if hasattr(ssh_recovery, 'enabled'):
        pytest.skip('This core still bypasses the scope when the window is 0')
    heartbeat = Mock()
    client = SimpleNamespace(ssh_recovery_timeout=0, close=Mock())
    with load('hpc_recovery_scope')(client, heartbeat):
        assert ssh_recovery.retry(client, lambda: 'once', 'read') == 'once'
        operation = Mock(side_effect=EOFError('lost'))
        with pytest.raises(EOFError):
            ssh_recovery.retry(client, operation, 'read')
    # The same scope and heartbeat path run (one pulse per attempt), but a
    # failed attempt is not retried and the connection is left alone.
    assert heartbeat.call_count == 2
    operation.assert_called_once()
    client.close.assert_not_called()


def test_recovery_scope_supports_core_without_recovery(monkeypatch):
    import builtins
    original_import = builtins.__import__
    scope = load('hpc_recovery_scope')
    def old_core_import(name, *args, **kwargs):
        if name == 'biomero':
            raise ImportError('old core has no ssh_recovery module')
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', old_core_import)
    heartbeat = Mock()
    with scope(SimpleNamespace(), heartbeat):
        pass
    heartbeat.assert_not_called()
