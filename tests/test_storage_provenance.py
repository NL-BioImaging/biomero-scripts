import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import biomero.constants as biomero_constants
import biomero_schema.imports as schema_imports
import biomero_schema.zarr as schema_zarr

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / '_data' / 'SLURM_Import_Results.py'
WF = '4f596081-5d48-41af-a539-a3c5e120ba66'
OTHER_WF = '9a1c1d1e-0000-4000-8000-000000000001'
pytestmark = pytest.mark.skipif('def collect_import_storage_provenance' not in SOURCE.read_text(encoding='utf-8'),
                                reason='storage provenance is not present')


def load():
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    names = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)
             and 'storage_provenance' in n.name}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    namespace = dict(os=os, json=json, Path=Path, uuid=__import__('uuid'),
                     logger=Mock(), IMPORTER_CONFIG={},
                     constants=biomero_constants,
                     SHALLOW_COLLECTION_MANIFEST=schema_zarr.SHALLOW_COLLECTION_MANIFEST,
                     SHALLOW_COLLECTION_NAMESPACE=schema_zarr.SHALLOW_COLLECTION_NAMESPACE,
                     ShallowManifest=schema_zarr.ShallowManifest,
                     IMPORT_STORAGE_FILENAME=schema_imports.IMPORT_STORAGE_FILENAME,
                     IMPORT_STORAGE_FULL=schema_imports.IMPORT_STORAGE_FULL,
                     IMPORT_STORAGE_SCHEMA=schema_imports.IMPORT_STORAGE_SCHEMA,
                     IMPORT_STORAGE_SHALLOW=schema_imports.IMPORT_STORAGE_SHALLOW,
                     parse_import_storage_record=schema_imports.parse_import_storage_record)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace


def connection(annotations):
    conn = Mock()
    anns = []
    for namespace, values in annotations:
        ann = Mock()
        ann.getNs.return_value = namespace
        ann.getValue.return_value = list(values.items())
        anns.append(ann)
    conn.getObject.return_value.listAnnotations.return_value = anns
    return conn


def test_full_result_does_not_become_shallow_from_flags(tmp_path, monkeypatch):
    monkeypatch.setenv('IMPORT_MOUNT_PATH', str(tmp_path))
    monkeypatch.setenv('BIOMERO_REMOTE_SHALLOW_ZARR', 'true')
    root = tmp_path / 'result.zarr'
    root.mkdir()
    (root / '.zgroup').write_text('{}')
    conn = connection([('biomero.import', {'Imported_from': str(root)})])
    data = load()['collect_import_storage_provenance'](conn, 'Plate', 1, WF)
    assert data['storage'] == 'full-zarr'
    assert 'container' not in data


def test_local_outcome_version_is_read_not_guessed(tmp_path, monkeypatch):
    monkeypatch.setenv('IMPORT_MOUNT_PATH', str(tmp_path))
    root = tmp_path / 'result.zarr'
    root.mkdir()
    (root / '.zgroup').write_text('{}')
    (root / '.biomero-import-storage.json').write_text(json.dumps({
        'schema': 1, 'workflow_id': WF, 'storage': 'full-zarr',
        'location': 'importer', 'tool_version': 'recorded-version', 'reason': 'changed pixels'}))
    conn = connection([('biomero.import', {'Imported_from': str(root)})])
    data = load()['collect_import_storage_provenance'](conn, 'Image', 1, WF)
    assert data['tool_version'] == 'recorded-version'
    assert data['reason'] == 'changed pixels'


def test_storage_facts_are_recorded_on_import_task_not_normalizer():
    from uuid import uuid4
    ns = load()
    evidence = {'storage': 'full-zarr'}
    ns['collect_import_storage_provenance'] = Mock(return_value=evidence)
    wid, tid, other = uuid4(), uuid4(), uuid4()
    objects = {wid: SimpleNamespace(tasks=[tid, other]),
               tid: SimpleNamespace(id=tid, task_name='SLURM_Import_Results.py'),
               other: SimpleNamespace(id=other, task_name='_SLURM_Result_Normalizer')}
    tracker = Mock()
    tracker.repository.get.side_effect = objects.__getitem__
    ns['record_import_storage_provenance'](object(), SimpleNamespace(workflowTracker=tracker),
                                         'Plate', 12, wid)
    tracker.record_storage_provenance.assert_called_once_with(tid, 'Plate:12', evidence)


def test_receipt_from_other_workflow_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv('IMPORT_MOUNT_PATH', str(tmp_path))
    root = tmp_path / 'result.zarr'
    root.mkdir()
    (root / '.zgroup').write_text('{}')
    (root / '.biomero-import-storage.json').write_text(json.dumps({
        'schema': 1, 'workflow_id': OTHER_WF, 'storage': 'full-zarr',
        'location': 'importer', 'tool_version': 'v', 'reason': 'r'}))
    conn = connection([('biomero.import', {'Imported_from': str(root)})])
    with pytest.raises(ValueError, match='does not match'):
        load()['collect_import_storage_provenance'](conn, 'Plate', 1, WF)
