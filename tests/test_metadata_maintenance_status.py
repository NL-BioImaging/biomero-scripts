import ast
import os
from pathlib import Path
import logging
from unittest.mock import Mock
from types import SimpleNamespace

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
source = ROOT / 'admin' / 'SLURM_check_setup.py'
tree = ast.parse(source.read_text(encoding='utf-8'))
nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)
         and n.name == 'format_metadata_maintenance_status']
if not nodes and not os.environ.get('BIOMERO_TEST_METADATA_DETACHED'):
    pytest.skip('Maintenance status unavailable in source', allow_module_level=True)


def test_check_setup_summarizes_maintenance_without_full_reports():
    namespace = dict(logger=logging.getLogger(__name__), detached_mode_enabled=lambda: True)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), 'exec'), namespace)
    formatter = namespace['format_metadata_maintenance_status']
    requests = [{'request_id': 'chosen', 'status': 'RUNNING', 'report': {
        'discovered': 1124, 'processed': 560, 'counts': {'updated': 500, 'skipped': 60}},
        'created_on': 'start', 'modified_on': 'now', 'error': '', 'options': {}}]
    message = formatter(requests)
    assert 'chosen' in message and 'RUNNING' in message and '560/1124' in message
    assert 'updated=500' in message
    assert '{' not in message
    assert 'No metadata maintenance requests' in formatter([])


def test_failed_status_shows_reason_and_disabled_supervisor_warns():
    namespace = dict(logger=logging.getLogger(__name__), detached_mode_enabled=lambda: False)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), 'exec'), namespace)
    requests = [{'request_id': 'failed', 'status': 'FAILED', 'report': {'counts': {'failed': 1}},
                 'created_on': 'start', 'modified_on': 'now', 'error': 'lost connection', 'options': {}},
                {'request_id': 'queued', 'status': 'QUEUED', 'report': {},
                 'created_on': 'start', 'modified_on': 'now', 'error': '', 'options': {}}]
    message = namespace['format_metadata_maintenance_status'](requests)
    assert 'lost connection' in message
    assert 'disabled' in message and 'queued' in message


def test_maintenance_only_check_never_opens_slurm_and_requires_admin():
    script_nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name in ('runScript', 'format_metadata_maintenance_status')]
    client, conn, slurm, tracker = Mock(), Mock(), Mock(), Mock()
    conn.getUser.return_value.isAdmin.return_value = True
    client.getInput.return_value = False
    scripts = Mock()
    scripts.client.return_value = client
    tracker_context = Mock(__enter__=Mock(return_value=tracker), __exit__=Mock(return_value=False))
    namespace = dict(logger=logging.getLogger(__name__), scripts=scripts,
                     detached_mode_enabled=lambda: True, VERSION='2.9.0',
                     omero=SimpleNamespace(constants=SimpleNamespace(
                         namespaces=SimpleNamespace(NSDYNAMIC='dynamic'))),
                     rstring=lambda value: value, unwrap=lambda value: value,
                     BlitzGateway=Mock(return_value=conn), SlurmClient=slurm,
                     WorkflowTracker=Mock(return_value=tracker_context),
                     metadata_refresh_statuses=Mock(return_value=[]))
    exec(compile(ast.Module(body=script_nodes, type_ignores=[]), str(source), 'exec'), namespace)
    namespace['runScript']()
    slurm.from_config.assert_not_called()
    client.setOutput.assert_called_once_with('Message', 'No metadata maintenance requests.')
    client.closeSession.assert_called_once()
    namespace['WorkflowTracker'].reset_mock()
    conn.getUser.return_value.isAdmin.return_value = False
    namespace['runScript']()
    namespace['WorkflowTracker'].assert_not_called()
