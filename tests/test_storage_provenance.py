import ast
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(os.environ.get('BIOMERO_SCRIPTS_ROOT', Path(__file__).parents[1]))
SOURCE = ROOT / '_data' / 'SLURM_Import_Results.py'
pytestmark = pytest.mark.skipif('def collect_import_storage_provenance' not in SOURCE.read_text(encoding='utf-8'),
                                reason='storage provenance is not present')


def load():
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    names = {'collect_import_storage_provenance', 'record_import_storage_provenance'}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    namespace = dict(os=os, json=json, Path=Path, uuid=__import__('uuid'),
                     logger=Mock(), IMPORTER_CONFIG={})
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
    data = load()['collect_import_storage_provenance'](conn, 'Plate', 1, 'wf')
    assert data['storage'] == 'full-zarr'
    assert 'container' not in data


def test_local_outcome_version_is_read_not_guessed(tmp_path, monkeypatch):
    monkeypatch.setenv('IMPORT_MOUNT_PATH', str(tmp_path))
    root = tmp_path / 'result.zarr'
    root.mkdir()
    (root / '.zgroup').write_text('{}')
    (root / '.biomero-import-storage.json').write_text(json.dumps({
        'schema': 1, 'workflow_id': 'wf', 'storage': 'full-zarr',
        'location': 'importer', 'tool_version': 'recorded-version', 'reason': 'changed pixels'}))
    conn = connection([('biomero.import', {'Imported_from': str(root)})])
    data = load()['collect_import_storage_provenance'](conn, 'Image', 1, 'wf')
    assert data['tool_version'] == 'recorded-version'
    assert data['reason'] == 'changed pixels'


def test_shallow_manifest_supplies_biocodes_and_remote_receipt(tmp_path, monkeypatch):
    monkeypatch.setenv('IMPORT_MOUNT_PATH', str(tmp_path))
    root = tmp_path / 'result.zarr'
    root.mkdir()
    manifest = {'workflowId': 'wf', 'images': [
        {'imageNodePath': '.', 'source': {'pixelIdentity': {'iscc': 'ISCC:SOURCE'}}}]}
    (root / '.biomero-shallow.json').write_text(json.dumps(manifest))
    (root / '.biomero-import-storage.json').write_text(json.dumps({
        'schema': 1, 'workflow_id': 'wf', 'storage': 'shallow-zarr',
        'location': 'remote', 'container': 'helper:1', 'tool_version': '1'}))
    conn = connection([('biomero.zarr.shallow', {'storageRoot': 'import-mount-data',
        'relativePath': 'result.zarr', 'workflowId': 'wf'})])
    data = load()['collect_import_storage_provenance'](conn, 'Plate', 1, 'wf')
    assert data['storage'] == 'shallow-zarr'
    assert data['container'] == 'helper:1'
    assert data['source_biocodes'] == ['ISCC:SOURCE']
    assert data['manifest_sha256'] == hashlib.sha256((root / '.biomero-shallow.json').read_bytes()).hexdigest()


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
        'schema': 1, 'workflow_id': 'other', 'storage': 'full-zarr'}))
    conn = connection([('biomero.import', {'Imported_from': str(root)})])
    with pytest.raises(ValueError, match='does not match'):
        load()['collect_import_storage_provenance'](conn, 'Plate', 1, 'wf')


def test_image_only_gets_its_own_canonical_biocode(tmp_path, monkeypatch):
    monkeypatch.setenv('IMPORT_MOUNT_PATH', str(tmp_path))
    root = tmp_path / 'result.zarr'
    root.mkdir()
    (root / '.biomero-shallow.json').write_text(json.dumps({
        'workflowId': 'wf', 'images': [
            {'imageNodePath': path, 'source': {'pixelIdentity': {'iscc': code}}}
            for path, code in [('A/1/0', 'ISCC:FIRST'), ('A/2/0', 'ISCC:SECOND')]]}))
    conn = connection([('biomero.zarr.shallow', {
        'storageRoot': 'import-mount-data', 'relativePath': 'result.zarr',
        'workflowId': 'wf', 'imageNodePath': 'A/2/0'})])
    data = load()['collect_import_storage_provenance'](conn, 'Image', 2, 'wf')
    assert data['source_biocodes'] == ['ISCC:SECOND']
    assert data['location'] == 'unknown'
    assert 'tool_version' not in data
