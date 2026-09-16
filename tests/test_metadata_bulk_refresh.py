"""Bulk maintenance is bounded work, not a cumulative dump of every KV pair."""
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest

from test_metadata_refresh import adapter, connection, planner, tree


AVAILABLE = ('detailed' in adapter.refresh_all_metadata.__code__.co_varnames
             or os.environ.get('BIOMERO_TEST_METADATA_BULK'))
pytestmark = pytest.mark.skipif(not AVAILABLE, reason='Optimized bulk refresh unavailable')


def test_bulk_apply_plans_once_and_keeps_only_compact_outcomes(tmp_path, caplog):
    targets = [('Image', i, 'wf') for i in range(4)]
    plan = {'annotations': [{'action': 'update', 'before_pairs': [['huge', 'x' * 1000]],
                            'after_pairs': [['huge', 'y' * 1000]]}]}
    refresh = Mock(return_value=plan)
    dump = Mock(wraps=json.dump)
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=refresh), patch.object(adapter.json, 'dump', dump), \
            caplog.at_level(logging.INFO):
        report = adapter.refresh_all_metadata(Mock(), object(), dry_run=False,
                                             backup_directory=tmp_path / 'unused')
    assert refresh.call_count == len(targets)
    assert all(call.kwargs['dry_run'] is False for call in refresh.call_args_list)
    assert all(call.kwargs['backup_enabled'] is False for call in refresh.call_args_list)
    assert report['counts']['updated'] == 4
    assert 'before_pairs' not in json.dumps(report)
    assert report['backup_directory'] is None
    assert not (tmp_path / 'unused').exists()
    dump.assert_not_called()
    assert '4/4' in caplog.text


def test_bulk_backups_write_compact_report_once(tmp_path):
    directory = tmp_path / 'backups'
    targets = [('Image', i, 'wf') for i in range(4)]
    refresh = Mock(return_value={'annotations': [{'action': 'update', 'before_pairs': [['k', 'v']]}]})
    dump = Mock(wraps=json.dump)
    with patch.dict(adapter.__dict__, discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=refresh), patch.object(adapter.json, 'dump', dump):
        report = adapter.refresh_all_metadata(Mock(), object(), dry_run=False,
                                             backup_enabled=True, backup_directory=directory)
    assert dump.call_count == 1
    assert json.loads((directory / 'report.json').read_text()) == report
    assert 'before_pairs' not in json.dumps(report)


def test_bulk_preview_counts_changes_without_retaining_full_pairs(caplog):
    refresh = Mock(side_effect=[{'annotations': [{'action': 'update', 'before_pairs': [['k', 'v']]}]},
                               {'annotations': [{'action': 'unchanged'}]}] * 2)
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Image', i, 'wf') for i in range(4)]),
                    refresh_workflow_metadata=refresh), caplog.at_level(logging.INFO):
        report = adapter.refresh_all_metadata(Mock(), object())
    assert all(call.kwargs['dry_run'] is True for call in refresh.call_args_list)
    assert sum(row['changed'] for row in report['results']) == 2
    assert 'before_pairs' not in json.dumps(report)
    assert 'would update=2, unchanged=2' in caplog.text


def test_selected_workflow_preview_retains_exact_diffs():
    wf = '11111111-1111-4111-8111-111111111111'
    plan = {'annotations': [{'namespace': 'biomero/workflow', 'action': 'update', 'before_pairs': [['k', 'old']],
                            'after_pairs': [['k', 'new']]}]}
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Image', i, wf) for i in range(4)]),
                    refresh_workflow_metadata=Mock(return_value=plan)):
        report = adapter.refresh_all_metadata(Mock(), object(), workflow_ids=[wf])
    assert all(row['plan'] == plan for row in report['results'])


def test_apply_unchanged_requires_no_backup_or_write(connection, planner):
    # Fixtures from the adapter harness describe one existing Plate annotation.
    conn, ann = connection
    planner.return_value[0].after = planner.return_value[0].before
    report = adapter.refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf', dry_run=False)
    assert report['annotations'][0]['action'] == 'unchanged'
    planner.assert_called_once()
    ann.save.assert_not_called()
    conn.getAnnotationLinks.assert_not_called()


def test_selected_dry_run_logs_only_human_readable_field_diffs(caplog):
    wf = '11111111-1111-4111-8111-111111111111'
    plan = {'annotations': [{'namespace': 'biomero/workflow', 'action': 'update',
        'before_pairs': [['Same', 'unchanged'], ['Changed', 'old'], ['Removed', 'gone']],
        'after_pairs': [['Same', 'unchanged'], ['Changed', 'new'], ['Added', 'here']]}]}
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Plate', 10, wf)]),
                    refresh_workflow_metadata=Mock(return_value=plan)), caplog.at_level(logging.INFO):
        adapter.refresh_all_metadata(Mock(), object(), workflow_ids=[wf])
    assert 'Changed: old -> new' in caplog.text
    assert '+ Added: here' in caplog.text
    assert '- Removed: gone' in caplog.text
    assert 'Same' not in caplog.text
    assert 'before_pairs' not in caplog.text


def test_bulk_apply_never_logs_field_diffs(caplog):
    plan = {'annotations': [{'namespace': 'biomero/workflow', 'action': 'update',
                            'before_pairs': [['PrivateField', 'old']],
                            'after_pairs': [['PrivateField', 'new']]}]}
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Plate', 10, 'wf')]),
                    refresh_workflow_metadata=Mock(return_value=plan)), caplog.at_level(logging.INFO):
        adapter.refresh_all_metadata(Mock(), object(), dry_run=False)
    assert 'PrivateField' not in caplog.text
    assert '1/1' in caplog.text


PARALLEL_AVAILABLE = ('workers' in adapter.refresh_all_metadata.__code__.co_varnames
                      or os.environ.get('BIOMERO_TEST_METADATA_PARALLEL'))


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
def test_parallel_refresh_is_bounded_isolated_and_preserves_target_order(caplog):
    barrier = threading.Barrier(4)
    lock = threading.Lock()
    connections, trackers, closed = [], [], []
    active = peak = 0

    @contextmanager
    def factory():
        conn, tracker = object(), object()
        with lock:
            connections.append(conn)
            trackers.append(tracker)
        try:
            yield conn, tracker
        finally:
            closed.append(conn)

    def refresh(conn, tracker, kind, ident, wf, **kwargs):
        nonlocal active, peak
        assert conn in connections and tracker in trackers
        assert kwargs['dry_run'] is False
        with lock:
            active += 1
            peak = max(peak, active)
        barrier.wait(timeout=5)
        with lock:
            active -= 1
        return {'annotations': [{'action': 'update'}]}

    targets = [('Image', i, 'wf') for i in range(8)]
    executor = Mock(wraps=ThreadPoolExecutor)
    with patch.dict(adapter.__dict__, discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=Mock(side_effect=refresh), ThreadPoolExecutor=executor), \
            caplog.at_level(logging.INFO):
        report = adapter.refresh_all_metadata(Mock(), object(), dry_run=False,
                                             workers=4, worker_factory=factory)
    assert peak == 4
    assert len(connections) == len(trackers) == len(closed) == 4
    assert report['counts']['updated'] == 8
    assert [r['object_id'] for r in report['results']] == list(range(8))
    assert executor.call_args.kwargs['max_workers'] == 4
    assert '8/8' in caplog.text


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
def test_parallel_refresh_keeps_same_object_in_one_lane_and_continues_failures():
    seen = {}
    closed = []

    @contextmanager
    def factory():
        conn = object()
        try:
            yield conn, object()
        finally:
            closed.append(conn)

    def refresh(conn, tracker, kind, ident, wf, **kwargs):
        previous = seen.setdefault((kind, ident), conn)
        assert previous is conn
        if wf == 'bad':
            raise RuntimeError('failed write')
        return {'annotations': [{'action': 'update'}]}

    targets = [('Image', 1, 'good'), ('Image', 1, 'bad'), ('Plate', 2, 'good')]
    with patch.dict(adapter.__dict__, discover_metadata_targets=Mock(return_value=targets),
                    refresh_workflow_metadata=Mock(side_effect=refresh)):
        report = adapter.refresh_all_metadata(Mock(), object(), dry_run=False,
                                             workers=4, worker_factory=factory)
    assert report['counts']['updated'] == 2
    assert report['counts']['failed'] == 1
    assert report['results'][1]['possibly_partial'] is True
    assert len(closed) == 2


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
def test_failed_worker_initialization_is_reported_without_hanging():
    @contextmanager
    def factory():
        raise RuntimeError('cannot join session')
        yield

    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Image', i, 'wf') for i in range(8)])):
        report = adapter.refresh_all_metadata(Mock(), object(), dry_run=False,
                                             workers=4, worker_factory=factory)
    assert report['counts']['failed'] == 8
    assert all(r['possibly_partial'] is False for r in report['results'])


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
@pytest.mark.parametrize('workers', [0, 9, True, 1.5])
def test_worker_limit_is_validated_before_discovery(workers):
    discover = Mock()
    with patch.dict(adapter.__dict__, discover_metadata_targets=discover):
        with pytest.raises(ValueError, match='workers'):
            adapter.refresh_all_metadata(Mock(), object(), workers=workers)
    discover.assert_not_called()


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
def test_isolated_gateway_joins_parent_session_and_detaches_after_error():
    conn = Mock()
    gateway = conn.clone.return_value
    tracker = Mock(env={'PERSISTENCE_MODULE': 'eventsourcing_sqlalchemy', 'SQLALCHEMY_URL': 'test'})
    worker_tracker = Mock()
    worker_tracker.factory.datastore.scoped_session = None
    context = Mock(__enter__=Mock(return_value=worker_tracker), __exit__=Mock(return_value=False))
    factory = Mock(return_value=context)
    with patch.dict(adapter.__dict__, WorkflowTracker=factory):
        with pytest.raises(RuntimeError, match='operation failed'):
            with adapter.metadata_refresh_worker(conn, tracker, 'session') as (worker_conn, reader):
                assert worker_conn is gateway and reader is worker_tracker
                raise RuntimeError('operation failed')
    gateway.connect.assert_called_once_with(sUuid='session')
    gateway.c.enableKeepAlive.assert_called_once_with(60)
    gateway.close.assert_called_once_with(hard=False)
    worker_tracker.factory.datastore.engine.dispose.assert_called_once()
    assert factory.call_args.kwargs['env']['CREATE_TABLE'] == 'no'


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
def test_join_failure_still_detaches_gateway_and_never_opens_tracker():
    conn, factory = Mock(), Mock()
    conn.clone.return_value.connect.return_value = False
    with patch.dict(adapter.__dict__, WorkflowTracker=factory):
        with pytest.raises(RuntimeError, match='join'):
            with adapter.metadata_refresh_worker(conn, Mock(), 'session'):
                pytest.fail('Failed join must not yield')
    conn.clone.return_value.close.assert_called_once_with(hard=False)
    factory.assert_not_called()


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
def test_shared_event_store_engine_is_not_disposed_by_lane_cleanup():
    conn = Mock()
    reader = Mock()
    context = Mock(__enter__=Mock(return_value=reader), __exit__=Mock(return_value=False))
    with patch.dict(adapter.__dict__, WorkflowTracker=Mock(return_value=context)):
        with adapter.metadata_refresh_worker(conn, Mock(env={}), 'session'):
            pass
    reader.factory.datastore.scoped_session.remove.assert_called_once()
    reader.factory.datastore.engine.dispose.assert_not_called()
    conn.clone.return_value.close.assert_called_once_with(hard=False)


@pytest.mark.skipif(not PARALLEL_AVAILABLE, reason='Parallel metadata refresh unavailable')
def test_ui_defaults_to_four_workers_and_no_backups():
    import ast
    fields = {n.args[0].value: n for n in ast.walk(tree) if isinstance(n, ast.Call)
              and isinstance(n.func, ast.Attribute) and n.func.attr in ('Int', 'Bool')
              and n.args and isinstance(n.args[0], ast.Constant)}
    for name, expected in [('Metadata Workers', 4), ('Save Metadata Backups', False)]:
        default = next(k.value for k in fields[name].keywords if k.arg == 'default')
        assert ast.literal_eval(default) == expected


def test_write_value_error_is_failure_not_safe_skip(connection, planner):
    conn, ann = connection
    ann.save.side_effect = ValueError('database rejected write')
    with pytest.raises(RuntimeError, match='partially updated'):
        adapter.refresh_workflow_metadata(conn, object(), 'Plate', 10, 'wf',
                                          dry_run=False, backup_enabled=False)


def test_long_dry_run_values_are_abbreviated(caplog):
    plan = {'annotations': [{'namespace': 'biomero/workflow', 'action': 'update',
                            'before_pairs': [], 'after_pairs': [['Huge', 'x' * 1000]]}]}
    with caplog.at_level(logging.INFO):
        adapter.metadata_refresh_log_diff('Plate', 10, 'wf', plan)
    assert '+ Huge:' in caplog.text
    assert '1000 characters' in caplog.text
    assert 'x' * 1000 not in caplog.text


def test_unlinked_annotation_is_summarized_not_dumped_field_by_field(caplog):
    plan = {'annotations': [{'namespace': 'biomero/workflow/task/SLURM_Run_Workflow.py',
                            'action': 'unlink', 'before_pairs': [['Unused' + str(i), 'v'] for i in range(246)],
                            'after_pairs': []}]}
    with caplog.at_level(logging.INFO):
        adapter.metadata_refresh_log_diff('Plate', 10, 'wf', plan)
    assert 'unlink' in caplog.text and '246 fields' in caplog.text
    assert 'Unused' not in caplog.text
