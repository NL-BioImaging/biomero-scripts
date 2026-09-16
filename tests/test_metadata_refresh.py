from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import ast
import json
import logging
import os
from pathlib import Path
import sys
from uuid import UUID, uuid4
from types import ModuleType

ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
source = ROOT / 'admin' / 'SLURM_Refresh_Metadata.py'
if not source.exists():
    source = ROOT / 'admin' / 'SLURM_Init_environment.py'
tree = ast.parse(source.read_text(encoding="utf-8"))
if not any(isinstance(n, ast.FunctionDef) and n.name == 'refresh_workflow_metadata'
           for n in tree.body):
    pytest.skip('refresh adapter is not available in this source revision',
                allow_module_level=True)
names = {"metadata_pairs", "_read_values", "refresh_workflow_metadata", "runScript"}
names.update({'discover_metadata_targets', 'refresh_all_metadata'})
names.add('refresh_metadata_from_init')
nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
assert len(nodes) >= 3, "Refresh adapter must live in the scripts layer"

def MetadataAnnotation(namespace, values):
    return SimpleNamespace(namespace=namespace, values=values)

def MetadataChange(before, after):
    return SimpleNamespace(before=before, after=after)

adapter = ModuleType("metadata_refresh_test_adapter")
adapter.__dict__.update(json=json, Path=Path, MetadataAnnotation=MetadataAnnotation,
                        UUID=UUID, uuid4=uuid4, logger=logging.getLogger(__name__),
                        NAMESPACE="biomero/workflow", plan_metadata_refresh=Mock())
sys.modules[adapter.__name__] = adapter
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"),
     adapter.__dict__)
metadata_pairs = adapter.metadata_pairs
refresh_workflow_metadata = adapter.refresh_workflow_metadata


@pytest.mark.skipif(not hasattr(adapter, 'refresh_metadata_from_init'), reason='Init option unavailable')
@pytest.mark.parametrize('enabled,dry_run', [(False, None), (True, None), (True, False)])
def test_init_refresh_is_optional_and_defaults_to_preview(enabled, dry_run):
    inputs = {'Refresh OMERO Metadata': enabled, 'Metadata Dry Run': dry_run,
              'Metadata View Version': 'v0', 'Metadata Backup Directory': '/private/new'}
    client = Mock()
    client.getInput.side_effect = inputs.get
    conn = Mock()
    conn.isAdmin.return_value = True
    tracker = Mock()
    context = Mock(__enter__=Mock(return_value=tracker), __exit__=Mock(return_value=False))
    factory = Mock(return_value=context)
    refresh = Mock(return_value={'counts': {}})
    with patch.dict(adapter.__dict__, unwrap=lambda value: value,
                    WorkflowTracker=factory, refresh_all_metadata=refresh):
        result = adapter.refresh_metadata_from_init(client, conn)
    if enabled:
        refresh.assert_called_once_with(conn, tracker, view_version='v0',
            dry_run=True if dry_run is None else dry_run, backup_directory='/private/new')
        client.enableKeepAlive.assert_called_once_with(60)
    else:
        assert result is None
        factory.assert_not_called()
        refresh.assert_not_called()


@pytest.mark.skipif(not hasattr(adapter, 'refresh_metadata_from_init'), reason='Init option unavailable')
def test_init_refresh_denies_nonadmin_before_opening_tracker():
    client, conn, factory = Mock(), Mock(), Mock()
    client.getInput.return_value = True
    conn.isAdmin.return_value = False
    with patch.dict(adapter.__dict__, unwrap=lambda value: value, WorkflowTracker=factory):
        with pytest.raises(ValueError, match='administrator'):
            adapter.refresh_metadata_from_init(client, conn)
    factory.assert_not_called()


@pytest.mark.skipif('Metadata Workflow UUID' not in source.read_text(encoding='utf-8'), reason='UUID filter unavailable')
def test_bulk_refresh_limits_processing_to_selected_workflow():
    conn = Mock()
    conn.isAdmin.return_value = True
    chosen = 'c329bc34-af12-4a65-a9f5-8efb62417f53'
    targets = [('Image', 751, chosen), ('Plate', 10, 'other')]
    refresh = Mock(return_value={'annotations': []})
    with patch.dict(adapter.__dict__, discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=refresh):
        selection = ({'workflow_ids': [chosen]}
                     if 'workflow_ids' in adapter.refresh_all_metadata.__code__.co_varnames
                     else {'workflow_id': chosen})
        report = adapter.refresh_all_metadata(conn, object(), **selection)
    assert report['discovered'] == 1
    assert refresh.call_count == 1
    assert refresh.call_args.args[2:] == ('Image', 751, chosen)


@pytest.mark.skipif('Metadata Workflow UUID' not in source.read_text(encoding='utf-8'), reason='UUID filter unavailable')
def test_bulk_refresh_rejects_bad_uuid_before_discovery():
    discovery = Mock()
    with patch.dict(adapter.__dict__, discover_metadata_targets=discovery):
        with pytest.raises(ValueError):
            selection = ({'workflow_ids': ['not-a-uuid']}
                         if 'workflow_ids' in adapter.refresh_all_metadata.__code__.co_varnames
                         else {'workflow_id': 'not-a-uuid'})
            adapter.refresh_all_metadata(Mock(), object(), **selection)
    discovery.assert_not_called()


@pytest.mark.skipif('workflow_ids' not in adapter.refresh_all_metadata.__code__.co_varnames,
                    reason='Multiple UUID selection unavailable')
def test_bulk_refresh_selects_multiple_workflows_without_duplicates():
    first = '11111111-1111-4111-8111-111111111111'
    second = '22222222-2222-4222-8222-222222222222'
    targets = [('Image', 1, first), ('Plate', 2, second), ('Plate', 3, 'other')]
    refresh = Mock(return_value={'annotations': []})
    with patch.dict(adapter.__dict__, discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=refresh):
        report = adapter.refresh_all_metadata(
            Mock(), object(), workflow_ids=[second, first, first])
    assert report['discovered'] == 2
    assert refresh.call_count == 2
    assert {call.args[4] for call in refresh.call_args_list} == {first, second}


@pytest.mark.skipif('workflow_ids' not in adapter.refresh_all_metadata.__code__.co_varnames,
                    reason='Multiple UUID selection unavailable')
def test_init_forwards_all_selected_workflow_uuids():
    chosen = ['11111111-1111-4111-8111-111111111111',
              '22222222-2222-4222-8222-222222222222']
    inputs = {'Refresh OMERO Metadata': True, 'Metadata Workflow UUIDs': chosen,
              'Filter Metadata by Workflow UUIDs': True}
    client = Mock()
    client.getInput.side_effect = inputs.get
    tracker = Mock()
    context = Mock(__enter__=Mock(return_value=tracker), __exit__=Mock(return_value=False))
    refresh = Mock()
    with patch.dict(adapter.__dict__, unwrap=lambda value: value,
                    WorkflowTracker=Mock(return_value=context), refresh_all_metadata=refresh):
        adapter.refresh_metadata_from_init(client, Mock())
    assert refresh.call_args.kwargs['workflow_ids'] == chosen
    assert refresh.call_args.kwargs['dry_run'] is True


@pytest.mark.skipif('Filter Metadata by Workflow UUIDs' not in source.read_text(encoding='utf-8')
                   and not os.environ.get('BIOMERO_TEST_METADATA_UUID_GATE'),
                   reason='Explicit UUID filter gate unavailable')
@pytest.mark.parametrize('enabled', [None, False])
def test_unchecked_uuid_filter_ignores_prefilled_selection(enabled):
    inputs = {'Refresh OMERO Metadata': True, 'Metadata Workflow UUIDs': ['invalid-prefill'],
              'Filter Metadata by Workflow UUIDs': enabled}
    client = Mock()
    client.getInput.side_effect = inputs.get
    context = Mock(__enter__=Mock(return_value=object()), __exit__=Mock(return_value=False))
    refresh = Mock()
    with patch.dict(adapter.__dict__, unwrap=lambda value: value,
                    WorkflowTracker=Mock(return_value=context), refresh_all_metadata=refresh):
        adapter.refresh_metadata_from_init(client, Mock())
    assert 'workflow_ids' not in refresh.call_args.kwargs


@pytest.mark.skipif('Filter Metadata by Workflow UUIDs' not in source.read_text(encoding='utf-8')
                   and not os.environ.get('BIOMERO_TEST_METADATA_UUID_GATE'),
                   reason='Explicit UUID filter gate unavailable')
def test_checked_uuid_filter_refuses_empty_selection_before_tracker():
    inputs = {'Refresh OMERO Metadata': True, 'Filter Metadata by Workflow UUIDs': True}
    client = Mock()
    client.getInput.side_effect = inputs.get
    factory = Mock()
    with patch.dict(adapter.__dict__, unwrap=lambda value: value, WorkflowTracker=factory):
        with pytest.raises(ValueError, match='Select at least one workflow UUID'):
            adapter.refresh_metadata_from_init(client, Mock())
    factory.assert_not_called()


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
        result = adapter.refresh_all_metadata(conn, object(), view_version='v0')
    assert result['counts'] == {'planned': 1, 'updated': 0, 'unchanged': 0,
                                'skipped': 1, 'failed': 0}
    assert result['results'][0]['reason'] == 'missing event-store history'
    assert refresh.call_count == 2
    assert all(call.kwargs['dry_run'] for call in refresh.call_args_list)


@pytest.mark.skipif(not hasattr(adapter, 'discover_metadata_targets'), reason='bulk refresh not present')
@pytest.mark.parametrize('view', ['v1', 'latest'])
def test_bulk_refresh_rejects_unsupported_views_before_discovery(view):
    conn = Mock()
    conn.isAdmin.return_value = True
    discover = Mock()
    with patch.dict(adapter.__dict__, discover_metadata_targets=discover):
        with pytest.raises(ValueError, match='View_Version must be v0'):
            adapter.refresh_all_metadata(conn, object(), view_version=view)
    discover.assert_not_called()


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


@pytest.mark.skipif(source.name != 'SLURM_Refresh_Metadata.py', reason='standalone entrypoint replaced by Init')
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


@pytest.mark.skipif(source.name != 'SLURM_Refresh_Metadata.py', reason='standalone entrypoint replaced by Init')
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


BACKUP_OPTIONS_AVAILABLE = ('backup_enabled' in refresh_workflow_metadata.__code__.co_varnames
                            or os.environ.get('BIOMERO_TEST_METADATA_BACKUPS'))


@pytest.mark.skipif(not BACKUP_OPTIONS_AVAILABLE, reason='Automatic metadata backups unavailable')
def test_bulk_apply_automatically_creates_unique_durable_backup_directories(tmp_path, caplog):
    root = tmp_path / 'durable-backups'
    path = lambda value: root if value == '/data/biomero-metadata-backups' else Path(value)
    refresh = Mock(return_value={'annotations': [{'action': 'update'}]})
    with patch.dict(adapter.__dict__, Path=path,
                    discover_metadata_targets=Mock(return_value=[('Plate', 10, 'wf')]),
                    refresh_workflow_metadata=refresh), caplog.at_level(logging.INFO):
        first = adapter.refresh_all_metadata(Mock(), object(), dry_run=False)
        second = adapter.refresh_all_metadata(Mock(), object(), dry_run=False)
    directories = [Path(report['backup_directory']) for report in (first, second)]
    assert directories[0] != directories[1]
    for directory, report in zip(directories, (first, second)):
        assert directory.parent == root
        assert json.loads((directory / 'report.json').read_text()) == report
        assert str(directory) in caplog.text
    assert all(call.kwargs['backup_path'].parent in directories
               for call in refresh.call_args_list if not call.kwargs['dry_run'])


@pytest.mark.skipif(not BACKUP_OPTIONS_AVAILABLE, reason='Automatic metadata backups unavailable')
def test_apply_without_backup_remains_preflighted(connection, planner, caplog):
    conn, ann = connection
    with caplog.at_level(logging.WARNING):
        refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf',
                                  dry_run=False, backup_enabled=False)
    ann.save.assert_called_once()
    assert conn.getAnnotationLinks.called
    assert 'disabled' in caplog.text.lower()


@pytest.mark.skipif(not BACKUP_OPTIONS_AVAILABLE, reason='Automatic metadata backups unavailable')
def test_shared_maps_still_refused_without_backup(connection, planner):
    conn, ann = connection
    conn.getAnnotationLinks.side_effect = lambda *a, **kw: [Mock()]
    with pytest.raises(ValueError, match='Shared'):
        refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf',
                                  dry_run=False, backup_enabled=False)
    ann.save.assert_not_called()


@pytest.mark.skipif(not BACKUP_OPTIONS_AVAILABLE, reason='Automatic metadata backups unavailable')
def test_bulk_backup_opt_out_creates_no_files_and_is_reported(tmp_path, caplog):
    directory = tmp_path / 'unused'
    refresh = Mock(return_value={'annotations': [{'action': 'update'}]})
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Plate', 10, 'wf')]),
                    refresh_workflow_metadata=refresh), caplog.at_level(logging.WARNING):
        report = adapter.refresh_all_metadata(Mock(), object(), dry_run=False,
                                             backup_enabled=False, backup_directory=directory)
    assert not directory.exists()
    assert report['backup_directory'] is None
    assert report['counts']['updated'] == 1
    assert refresh.call_args.kwargs['backup_enabled'] is False
    assert refresh.call_args.kwargs['backup_path'] is None
    assert 'disabled' in caplog.text.lower()


@pytest.mark.skipif(not BACKUP_OPTIONS_AVAILABLE, reason='Automatic metadata backups unavailable')
def test_dry_run_shows_exact_namespace_and_final_pairs_without_backups(connection, planner):
    conn, ann = connection
    report = refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf')
    planned = report['annotations'][0]
    assert planned['namespace'] == 'biomero/workflow'
    assert planned['before_pairs'] == [['Workflow_ID', 'wf'], ['Name', 'run']]
    assert planned['after_pairs'] == [['Workflow_ID', 'wf'], ['Name', 'run'],
                                     ['Metadata_View_Version', 'v0']]
    planner.return_value[0].after = None
    unlinked = refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf')['annotations'][0]
    assert unlinked['action'] == 'unlink'
    assert unlinked['after_pairs'] == []
    ann.save.assert_not_called()


@pytest.mark.skipif(not BACKUP_OPTIONS_AVAILABLE, reason='Automatic metadata backups unavailable')
@pytest.mark.parametrize('save_backups', [None, True, False])
def test_init_backup_default_and_explicit_opt_out(save_backups):
    inputs = {'Refresh OMERO Metadata': True, 'Metadata Dry Run': False,
              'Save Metadata Backups': save_backups}
    client = Mock()
    client.getInput.side_effect = inputs.get
    tracker = Mock()
    context = Mock(__enter__=Mock(return_value=tracker), __exit__=Mock(return_value=False))
    refresh = Mock()
    with patch.dict(adapter.__dict__, unwrap=lambda value: value,
                    WorkflowTracker=Mock(return_value=context), refresh_all_metadata=refresh):
        adapter.refresh_metadata_from_init(client, Mock())
    assert refresh.call_args.kwargs['backup_directory'] is None
    assert refresh.call_args.kwargs.get('backup_enabled', True) is (save_backups is not False)
    assert refresh.call_args.kwargs['dry_run'] is False


@pytest.mark.skipif(not BACKUP_OPTIONS_AVAILABLE, reason='Automatic metadata backups unavailable')
def test_bulk_dry_run_never_creates_backup_directory(tmp_path):
    directory = tmp_path / 'unused'
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Plate', 10, 'wf')]),
                    refresh_workflow_metadata=Mock(return_value={'annotations': []})):
        report = adapter.refresh_all_metadata(Mock(), object(), backup_directory=directory)
    assert not directory.exists()
    assert report['backup_directory'] is None
