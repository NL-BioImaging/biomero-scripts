import ast
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest


ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / 'admin/SLURM_Init_environment.py'
TREE = ast.parse(SOURCE.read_text(encoding='utf-8'))
NODES = [node for node in TREE.body if isinstance(node, ast.FunctionDef)
         and node.name == 'get_metadata_workflow_choices']
if not NODES and not os.environ.get('BIOMERO_TEST_UUID_CHOICES'):
    pytest.skip('UUID choices not available in this revision', allow_module_level=True)


def load(discover):
    assert NODES, 'Missing existing-workflow UUID choices'
    client, conn = Mock(), Mock()
    namespace = dict(omero=SimpleNamespace(client=Mock(return_value=client)),
                     BlitzGateway=Mock(return_value=conn), rstring=lambda x: x,
                     UUID=UUID, logger=logging.getLogger(__name__),
                     discover_metadata_targets=discover)
    exec(compile(ast.Module(body=NODES, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace['get_metadata_workflow_choices'], client, conn


def test_choices_are_unique_valid_existing_uuids():
    first = '11111111-1111-4111-8111-111111111111'
    second = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
    discover = Mock(return_value=[('Plate', 1, second.upper()),
                                 ('Image', 2, first), ('Image', 3, second),
                                 ('Plate', 4, 'invalid')])
    choices, client, conn = load(discover)
    assert choices() == [first, second]
    discover.assert_called_once_with(conn)
    client.closeSession.assert_called_once()


def test_discovery_failure_keeps_init_available_and_closes_session(caplog):
    choices, client, _ = load(Mock(side_effect=RuntimeError('unavailable')))
    assert choices() == []
    client.closeSession.assert_called_once()
    assert 'UUID' in caplog.text


def test_uuid_field_uses_choices_and_explicit_empty_default():
    field = next(node for node in ast.walk(TREE) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Attribute) and node.func.attr == 'List'
                 and node.args and isinstance(node.args[0], ast.Constant)
                 and node.args[0].value == 'Metadata Workflow UUIDs')
    kwargs = {item.arg: item.value for item in field.keywords}
    assert field.func.attr == 'List'
    assert ast.literal_eval(kwargs['default']) == []
    assert ast.literal_eval(kwargs['optional']) is True
    assert isinstance(kwargs['values'], ast.Call)
    assert kwargs['values'].func.id == 'get_metadata_workflow_choices'
