"""Bulk maintenance is bounded work, not a cumulative dump of every KV pair."""
import json
import logging
import os
from unittest.mock import Mock, patch

import pytest

from test_metadata_refresh import adapter, connection, planner


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


def test_bulk_preview_counts_changes_without_retaining_full_pairs():
    refresh = Mock(side_effect=[{'annotations': [{'action': 'update', 'before_pairs': [['k', 'v']]}]},
                               {'annotations': [{'action': 'unchanged'}]}] * 2)
    with patch.dict(adapter.__dict__,
                    discover_metadata_targets=Mock(return_value=[('Image', i, 'wf') for i in range(4)]),
                    refresh_workflow_metadata=refresh):
        report = adapter.refresh_all_metadata(Mock(), object())
    assert all(call.kwargs['dry_run'] is True for call in refresh.call_args_list)
    assert sum(row['changed'] for row in report['results']) == 2
    assert 'before_pairs' not in json.dumps(report)


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
