import ast
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / '_data/SLURM_Import_Results.py'


def load():
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
             and node.name == 'normalize_remote_results']
    if not nodes and not os.environ.get('BIOMERO_TEST_REMOTE_SHALLOWER'):
        pytest.skip('Remote shallower feature is not present in source revision')
    assert nodes, 'Missing remote normalization stage'
    namespace = dict(IMPORTER_ENABLED=True, SHALLOW_ZARR_ENABLED=True,
                     SHALLOW_ZARR_OPERATION_AVAILABLE=True,
                     IMPORTER_ORDER_API_AVAILABLE=True,
                     load_canonical_input_snapshot=lambda *args: 'canonical')
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace['normalize_remote_results']


def test_enabled_runs_remote_normalization():
    client = SimpleNamespace(remote_shallow_zarr=True, normalize_results_on_slurm=Mock())
    load()(client, '/data', 'workflow')
    client.normalize_results_on_slurm.assert_called_once_with('/data', 'workflow', 'canonical')


def test_absent_flag_is_a_noop():
    client = SimpleNamespace(normalize_results_on_slurm=Mock())
    load()(client, '/data', 'workflow')
    client.normalize_results_on_slurm.assert_not_called()


def test_normalizer_runs_before_zip_creation():
    source = SOURCE.read_text(encoding='utf-8')
    if 'def normalize_remote_results' not in source and not os.environ.get('BIOMERO_TEST_REMOTE_SHALLOWER'):
        pytest.skip('Remote shallower feature is not present in source revision')
    tree = ast.parse(source)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == 'extract_slurm_results_zip')
    stage = [node.lineno for node in ast.walk(function) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == 'normalize_remote_results']
    archive = [node.lineno for node in ast.walk(function) if isinstance(node, ast.Call)
               and isinstance(node.func, ast.Attribute) and node.func.attr == 'zip_data_on_slurm_server']
    assert stage and stage[0] < archive[0]
