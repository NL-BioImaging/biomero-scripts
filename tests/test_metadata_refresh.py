from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import ast
import json
import os
from pathlib import Path
import sys
from types import ModuleType

ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
source = ROOT / 'admin' / 'SLURM_Refresh_Metadata.py'
if not source.exists():
    source = ROOT / "_data" / "SLURM_Import_Results.py"
tree = ast.parse(source.read_text(encoding="utf-8"))
if not any(isinstance(n, ast.FunctionDef) and n.name == 'refresh_workflow_metadata'
           for n in tree.body):
    pytest.skip('refresh adapter is not available in this source revision',
                allow_module_level=True)
names = {"metadata_pairs", "_read_values", "refresh_workflow_metadata", "runScript"}
names.update({'discover_metadata_targets', 'refresh_all_metadata'})
nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
assert len(nodes) >= 3, "Refresh adapter must live in the scripts layer"

def MetadataAnnotation(namespace, values):
    return SimpleNamespace(namespace=namespace, values=values)

def MetadataChange(before, after):
    return SimpleNamespace(before=before, after=after)

adapter = ModuleType("metadata_refresh_test_adapter")
adapter.__dict__.update(json=json, Path=Path, MetadataAnnotation=MetadataAnnotation,
                        NAMESPACE="biomero/workflow", plan_metadata_refresh=Mock())
sys.modules[adapter.__name__] = adapter
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"),
     adapter.__dict__)
metadata_pairs = adapter.metadata_pairs
refresh_workflow_metadata = adapter.refresh_workflow_metadata


@pytest.mark.skipif(not hasattr(adapter, 'refresh_all_metadata'), reason='bulk refresh not present')
def test_bulk_refresh_skips_missing_history_and_continues():
    assert hasattr(adapter, 'refresh_all_metadata')
    class MissingHistory(Exception):
        pass
    conn = Mock()
    conn.isAdmin.return_value = True
    targets = [('Image', 1, 'wf1'), ('Plate', 2, 'wf2')]
    refresh = Mock(side_effect=[MissingHistory(), {'annotations': []}])
    with patch.dict(adapter.__dict__, discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=refresh, AggregateNotFoundError=MissingHistory):
        result = adapter.refresh_all_metadata(conn, object(), view_version='v1')
    assert result['counts'] == {'planned': 1, 'updated': 0, 'unchanged': 0,
                                'skipped': 1, 'failed': 0}
    assert result['results'][0]['reason'] == 'missing event-store history'
    assert refresh.call_count == 2
    assert all(call.kwargs['dry_run'] for call in refresh.call_args_list)


@pytest.mark.skipif(not hasattr(adapter, 'discover_metadata_targets'), reason='bulk refresh not present')
def test_discovery_deduplicates_targets_and_restores_group():
    assert hasattr(adapter, 'discover_metadata_targets')
    conn = Mock()
    conn.isAdmin.return_value = True
    conn.SERVICE_OPTS.getOmeroGroup.return_value = '4'
    link = Mock()
    link.getParent.return_value.getId.return_value = 12
    link.getAnnotation.return_value.getValue.return_value = [('Workflow_ID', 'wf')]
    conn.getAnnotationLinks.side_effect = [[link, link], []]
    with patch.dict(adapter.__dict__, ParametersI=Mock()):
        assert adapter.discover_metadata_targets(conn) == [('Image', 12, 'wf')]
    conn.SERVICE_OPTS.setOmeroGroup.assert_called_with('4')


@pytest.mark.skipif(not hasattr(adapter, 'refresh_all_metadata'), reason='bulk refresh not present')
def test_bulk_apply_reports_partial_failure_and_preserves_per_target_backups(tmp_path):
    class MissingHistory(Exception):
        pass
    conn = Mock()
    conn.isAdmin.return_value = True
    targets = [('Image', 1, 'wf1'), ('Plate', 2, 'wf2')]
    plan = {'annotations': [{'action': 'update'}]}
    refresh = Mock(side_effect=[plan, RuntimeError('write failed'), plan, plan])
    backup = tmp_path / 'new-backups'
    with patch.dict(adapter.__dict__, discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=refresh, AggregateNotFoundError=MissingHistory):
        result = adapter.refresh_all_metadata(conn, object(), dry_run=False,
                                              backup_directory=backup)
    assert result['counts']['failed'] == 1
    assert result['counts']['updated'] == 1
    assert result['results'][0]['possibly_partial'] is True
    assert refresh.call_args_list[1].kwargs['backup_path'] != refresh.call_args_list[3].kwargs['backup_path']
    assert json.loads((backup / 'report.json').read_text()) == result


@pytest.mark.skipif(source.parent.name != 'admin', reason='admin script not present')
def test_standalone_admin_entrypoint_denies_nonadmin_before_tracker():
    assert hasattr(adapter, 'runScript'), 'Refresh needs its own admin entrypoint'
    client = Mock()
    conn = Mock()
    conn.isAdmin.return_value = False
    tracker = Mock()
    with patch.dict(adapter.__dict__, scripts=Mock(client=Mock(return_value=client)),
                    BlitzGateway=Mock(return_value=conn), WorkflowTracker=tracker,
                    rstring=lambda value: value, VERSION='2.9.0',
                    NSDYNAMIC='dynamic'):
        adapter.runScript()
    tracker.assert_not_called()
    client.closeSession.assert_called_once()
    assert 'denied' in client.setOutput.call_args.args[1].lower()


@pytest.mark.skipif(source.parent.name != 'admin', reason='admin script not present')
def test_admin_entrypoint_defaults_to_dry_run_and_only_refreshes_metadata():
    from uuid import UUID
    client = Mock()
    client.getInputs.return_value = {
        'Data_Type': 'Plate', 'ID': 10,
        'Workflow_ID': '00000000-0000-0000-0000-000000000001'}
    conn = Mock()
    conn.isAdmin.return_value = True
    tracker = Mock()
    context = Mock()
    context.__enter__ = Mock(return_value=tracker)
    context.__exit__ = Mock(return_value=False)
    refresh = Mock(return_value={'dry_run': True})
    with patch.dict(adapter.__dict__, scripts=Mock(client=Mock(return_value=client)),
                    BlitzGateway=Mock(return_value=conn),
                    WorkflowTracker=Mock(return_value=context), UUID=UUID,
                    rstring=lambda value: value, VERSION='2.9.0',
                    NSDYNAMIC='dynamic', refresh_workflow_metadata=refresh):
        adapter.runScript()
    refresh.assert_called_once_with(
        conn, tracker, 'Plate', 10, client.getInputs.return_value['Workflow_ID'],
        view_version='v0', dry_run=True, backup_path=None)
    client.closeSession.assert_called_once()
    importer = ast.parse((ROOT / '_data' / 'SLURM_Import_Results.py').read_text(encoding='utf-8'))
    assert not any(isinstance(n, ast.FunctionDef) and n.name in
                   {'metadata_pairs', '_read_values', 'refresh_workflow_metadata'}
                   for n in importer.body)


@pytest.fixture
def connection():
    conn = Mock()
    conn.isAdmin.return_value = True
    conn.SERVICE_OPTS.getOmeroGroup.return_value = '4'
    target = Mock()
    target.getDetails.return_value.group.id.val = 4
    ann = Mock()
    ann.getId.return_value = 12
    ann.getNs.return_value = 'biomero/workflow'
    ann.getValue.return_value = [['Workflow_ID', 'wf'], ['Name', 'run']]
    target.listAnnotations.return_value = [ann]
    conn.getObject.side_effect = lambda kind, ident: target if kind == 'Plate' else ann
    link = Mock()
    link.getParent.return_value.getId.return_value = 10
    link.getId.return_value = 123
    conn.getAnnotationLinks.side_effect = lambda kind, **kw: [link] if kind == 'Plate' else []
    return conn, ann


@pytest.fixture
def planner():
    before = MetadataAnnotation('biomero/workflow', {'Workflow_ID': 'wf', 'Name': 'run'})
    after = MetadataAnnotation(before.namespace, {**before.values, 'Metadata_View_Version': 'v0'})
    with patch('metadata_refresh_test_adapter.plan_metadata_refresh',
               return_value=[MetadataChange(before, after)]) as mock:
        yield mock


def test_default_is_read_only(connection, planner):
    conn, ann = connection
    result = refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf')
    assert result['annotations'][0]['action'] == 'update'
    ann.save.assert_not_called()
    ann.setValue.assert_not_called()
    conn.SERVICE_OPTS.setOmeroGroup.assert_called_with('4')


def test_apply_preserves_annotation_id_and_backs_up(connection, planner, tmp_path):
    conn, ann = connection
    backup = tmp_path / 'backup.json'
    refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf',
                              dry_run=False, backup_path=backup)
    assert '"annotation_id": 12' in backup.read_text()
    ann.save.assert_called_once()
    ann.setValue.assert_called_once_with([
        ['Workflow_ID', 'wf'], ['Name', 'run'], ['Metadata_View_Version', 'v0']])


def test_shared_maps_refused_before_backup_or_write(connection, planner, tmp_path):
    conn, ann = connection
    conn.getAnnotationLinks.side_effect = lambda *a, **kw: [Mock()]
    backup = tmp_path / 'backup.json'
    with pytest.raises(ValueError, match='Shared'):
        refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf',
                                  dry_run=False, backup_path=backup)
    assert not backup.exists()
    ann.save.assert_not_called()


def test_list_representation_matches_legacy_writer():
    assert metadata_pairs({'Input_Data': [2, 3], 'Status': 'DONE'}) == [
        ['Input_Data', '2'], ['Input_Data', '3'], ['Status', 'DONE']]


def test_no_overwrite_of_backup(connection, planner, tmp_path):
    conn, ann = connection
    backup = tmp_path / 'backup.json'
    backup.write_text('preserve')
    with pytest.raises(FileExistsError):
        refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf',
                                  dry_run=False, backup_path=backup)
    assert backup.read_text() == 'preserve'
    ann.save.assert_not_called()


def test_unlink_explicitly_excludes_map_annotations(connection, planner, tmp_path):
    conn, ann = connection
    planner.return_value[0].after = None
    delete = Mock()
    option = Mock()
    modules = {'omero.cmd': SimpleNamespace(Delete2=delete),
               'omero.cmd.graphs': SimpleNamespace(ChildOption=option)}
    with patch.dict('sys.modules', modules):
        refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf',
                                  dry_run=False, backup_path=tmp_path / 'backup.json')
    option.assert_called_once_with(excludeType=['MapAnnotation'])
    delete.assert_called_once_with(targetObjects={'PlateAnnotationLink': [123]},
                                   childOptions=[option.return_value])
    ann.save.assert_not_called()
