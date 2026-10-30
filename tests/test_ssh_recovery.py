"""The orchestration layer must respect the core transport recovery deadline."""
import ast
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

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
