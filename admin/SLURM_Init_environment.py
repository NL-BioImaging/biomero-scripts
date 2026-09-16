#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# Example OMERO.script to instantiate a 'empty' Slurm connection.

"""
BIOMERO SLURM Environment Initialization Script (Admin Only)

This administrative script sets up the complete SLURM environment for BIOMERO
workflow execution including directory structure, job scripts, converters,
and container images.

**ADMIN ONLY**: This script requires OMERO administrator privileges.

This is typically run once during initial BIOMERO-SLURM setup to prepare
the cluster environment for workflow execution.

Authors: Torec Luik, OMERO Team
Institution: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

import omero
import omero.gateway
from omero import scripts
from omero.rtypes import rstring, unwrap
from omero.gateway import BlitzGateway
from biomero import SlurmClient
import logging
import json
from pathlib import Path
from uuid import UUID, uuid4
from eventsourcing.application import AggregateNotFoundError
from omero.sys import ParametersI
from biomero import WorkflowTracker
from biomero.provenance import MetadataAnnotation, NAMESPACE, plan_metadata_refresh
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import partial
from queue import Queue, Empty

logger = logging.getLogger(__name__)
VERSION = "2.9.0"


def format_metadata_summary(report):
    """Summarize result/workflow pairs; detailed plans stay in the worker log."""
    counts = report['counts']
    if report['dry_run']:
        planned = [item for item in report['results'] if item['status'] == 'planned']
        changed = sum(item.get('changed', any(a['action'] != 'unchanged'
                      for a in item.get('plan', {}).get('annotations', []))) for item in planned)
        outcomes = f'Would update: {changed}; Unchanged: {len(planned) - changed}'
        mode = 'dry run'
        note = 'No OMERO metadata was changed.'
    else:
        outcomes = f"Updated: {counts.get('updated', 0)}; Unchanged: {counts.get('unchanged', 0)}"
        mode = 'apply'
        directory = report.get('backup_directory')
        note = (f'Backup run directory: {directory}' if directory else
                'Backups were not requested.' if report.get('backup_enabled') is False else
                'See the backup directory for per-target backups and report.json.')
    return (f"Metadata refresh ({report['view_version']}, {mode}): "
            f"{report['discovered']} result/workflow pairs.\n"
            f"{outcomes}; Skipped: {counts.get('skipped', 0)}; Failed: {counts.get('failed', 0)}.\n"
            f"{note}\nProgress and skip/failure details: activity log (i button).")


def format_image_submission(array_job_id, status):
    """Format the scheduler-native image initialization summary."""
    counts = status.get("counts", {})
    if array_job_id is None:
        first_line = "Image pull array ID: none (all images already valid)"
    else:
        first_line = f"Image pull array ID: {array_job_id}"
    return (
        f"{first_line}\n"
        f"Image status — READY: {counts.get('READY', 0)}, "
        f"RUNNING: {counts.get('RUNNING', 0)}, "
        f"FAILED: {counts.get('FAILED', 0)}"
    )


def metadata_pairs(values):
    """Represent list-valued fields using repeated key/value pairs."""
    return [[str(key), str(item)] for key, value in values.items()
            for item in (value if isinstance(value, list) else [value])]


def _read_values(pairs):
    values = {}
    for key, value in pairs:
        if key in values:
            # Input_Data is historically list-valued, unlike identity fields.
            if key != 'Input_Data':
                raise ValueError(f'Duplicate non-list metadata key: {key}')
            if not isinstance(values[key], list):
                values[key] = [values[key]]
            values[key].append(value)
        else:
            values[key] = value
    return values


def refresh_workflow_metadata(conn, tracker, object_type, object_id, workflow_id,
                              *, view_version='v0', dry_run=True, backup_path=None,
                              backup_enabled=True):
    """Refresh existing maps in place; unlink obsolete internal-task maps.

    Default is a dry run. Applying normally saves a new backup file; disabling
    backups is explicit. Applying always requires administrator
    access so cross-group shared links can be checked. No CSV, canonical/shallow
    annotation, image data or event is modified. Shared annotations are refused.
    Writes are not one transaction: errors propagate and the backup remains.
    Quiesce metadata writers for the target during an administrative refresh.
    """
    if object_type not in ('Image', 'Plate'):
        raise ValueError('Only Image and Plate result targets are supported')
    if not conn.isAdmin():
        raise ValueError('Metadata refresh requires an administrator')
    original_group = conn.SERVICE_OPTS.getOmeroGroup()
    conn.SERVICE_OPTS.setOmeroGroup('-1')
    try:
        target = conn.getObject(object_type, int(object_id))
        if target is None:
            raise ValueError('Result target not found')
        records = []
        rows = []
        for ann in target.listAnnotations():
            ns = ann.getNs() or ''
            if not (ns == NAMESPACE or ns.startswith(NAMESPACE + '/task/')):
                continue
            pairs = ann.getValue()
            if ['Workflow_ID', str(workflow_id)] not in [list(p) for p in pairs]:
                continue
            values = _read_values(pairs)
            rows.append(MetadataAnnotation(ns, values))
            records.append({'annotation_id': ann.getId(), 'namespace': ns,
                            'pairs': [list(p) for p in pairs]})
        plan = plan_metadata_refresh(tracker, workflow_id, rows,
                                     view_version=view_version,
                                     target_key=f'{object_type}:{object_id}')
        actions = []
        for record, change in zip(records, plan):
            pairs = metadata_pairs(change.after.values) if change.after else None
            action = ('unlink' if pairs is None else
                      'unchanged' if pairs == record['pairs'] else 'update')
            actions.append({**record, 'action': action, 'new_pairs': pairs})
        summary = {'object_type': object_type, 'object_id': int(object_id),
                   'workflow_id': str(workflow_id), 'view_version': view_version,
                   'dry_run': dry_run,
                   'annotations': [{'id': a['annotation_id'], 'namespace': a['namespace'],
                                    'action': a['action'],
                                    'before': len(a['pairs']),
                                    'after': len(a['new_pairs'] or []),
                                    'before_pairs': a['pairs'],
                                    'after_pairs': a['new_pairs'] or []}
                                   for a in actions]}
        if dry_run:
            return summary
        if all(action['action'] == 'unchanged' for action in actions):
            return summary
        if backup_enabled and not backup_path:
            raise ValueError('An unused backup_path is required to apply')
        # Preflight all changes before writing any map. Across-group admin
        # visibility prevents accidentally modifying another target's view.
        links = {}
        for action in actions:
            if action['action'] == 'unchanged':
                continue
            aid = action['annotation_id']
            linked = []
            for kind in ('Project', 'Dataset', 'Image', 'Screen', 'Plate',
                         'Well', 'PlateAcquisition', 'Annotation'):
                linked.extend((kind, link) for link in conn.getAnnotationLinks(
                    kind, ann_ids=[aid]))
            if (len(linked) != 1 or linked[0][0] != object_type or
                    linked[0][1].getParent().getId() != int(object_id)):
                raise ValueError(f'Shared or unexpected annotation links: {aid}')
            links[aid] = linked[0][1].getId()
            current = conn.getObject('MapAnnotation', aid)
            if ([list(p) for p in current.getValue()] != action['pairs'] or
                    current.getNs() != action['namespace']):
                raise ValueError(f'Annotation changed during planning: {aid}')
        if backup_enabled:
            with Path(backup_path).open('x', encoding='utf-8') as stream:
                json.dump({**summary, 'actions': actions}, stream, indent=2)
            logger.info('Metadata backup saved: %s', backup_path)
        conn.SERVICE_OPTS.setOmeroGroup(str(target.getDetails().group.id.val))
        # Update retained maps before removing links. No global annotation delete.
        try:
            for action in actions:
                if action['action'] != 'update':
                    continue
                ann = conn.getObject('MapAnnotation', action['annotation_id'])
                if [list(p) for p in ann.getValue()] != action['pairs']:
                    raise ValueError('Annotation changed after preflight')
                ann.setValue(action['new_pairs'])
                ann.save()
            unlink_ids = [links[a['annotation_id']] for a in actions
                          if a['action'] == 'unlink']
            if unlink_ids:
                from omero.cmd import Delete2
                from omero.cmd.graphs import ChildOption
                request = Delete2(
                    targetObjects={object_type + 'AnnotationLink': unlink_ids},
                    childOptions=[ChildOption(excludeType=['MapAnnotation'])])
                handle = conn.c.sf.submit(request, conn.SERVICE_OPTS)
                try:
                    conn._waitOnCmd(handle)
                finally:
                    handle.close()
        except Exception as error:
            # Never classify a failure after writing starts as a safe skip.
            raise RuntimeError('Metadata write failed; target may be partially updated') from error
        return summary
    finally:
        conn.SERVICE_OPTS.setOmeroGroup(original_group)

def discover_metadata_targets(conn):
    """Discover existing workflow views on Images and Plates across groups."""
    if not conn.isAdmin():
        raise ValueError('Metadata refresh requires an administrator')
    original_group = conn.SERVICE_OPTS.getOmeroGroup()
    targets = set()
    conn.SERVICE_OPTS.setOmeroGroup('-1')
    try:
        for kind in ('Image', 'Plate'):
            offset = 0
            while True:
                page = list(conn.getAnnotationLinks(
                    kind, ns=NAMESPACE, params=ParametersI().page(offset, 500)))
                for link in page:
                    for key, value in link.getAnnotation().getValue():
                        if key == 'Workflow_ID' and value:
                            targets.add((kind, link.getParent().getId(), value))
                if len(page) < 500:
                    break
                offset += len(page)
    finally:
        conn.SERVICE_OPTS.setOmeroGroup(original_group)
    return sorted(targets)


def metadata_refresh_log_diff(kind, ident, workflow_id, plan):
    """Log changed fields only; never dump entire metadata maps as JSON."""
    def fields(pairs):
        result = {}
        for key, value in pairs:
            result.setdefault(key, []).append(value)
        return result

    def display(values):
        text = '; '.join(str(value) for value in values)
        return text if len(text) <= 200 else text[:200] + f' ... ({len(text)} characters)'

    for annotation in plan['annotations']:
        if annotation['action'] == 'unchanged':
            continue
        logger.info('Dry-run diff: %s %s, workflow %s, %s (%s)',
                    kind, ident, workflow_id, annotation['namespace'], annotation['action'])
        if annotation['action'] == 'unlink':
            logger.info('  %s %s | unlink annotation (%s fields); original annotation is retained',
                        kind, ident, len(annotation.get('before_pairs', [])))
            continue
        before = fields(annotation.get('before_pairs', []))
        after = fields(annotation.get('after_pairs', []))
        for key in sorted(before.keys() | after.keys()):
            if before.get(key) == after.get(key):
                continue
            if key not in before:
                logger.info('  %s %s | + %s: %s', kind, ident, key, display(after[key]))
            elif key not in after:
                logger.info('  %s %s | - %s: %s', kind, ident, key, display(before[key]))
            else:
                logger.info('  %s %s | ~ %s: %s -> %s', kind, ident, key,
                            display(before[key]), display(after[key]))


def metadata_refresh_target(conn, tracker, kind, ident, workflow_id, *, view_version,
                            dry_run, backup_path, backup_enabled, detailed):
    """Plan once, preflight and apply once, then discard full bulk metadata."""
    item = {'object_type': kind, 'object_id': ident, 'workflow_id': workflow_id}
    try:
        plan = refresh_workflow_metadata(
            conn, tracker, kind, ident, workflow_id, view_version=view_version,
            dry_run=dry_run, backup_path=backup_path, backup_enabled=backup_enabled)
    except AggregateNotFoundError:
        item.update(status='skipped', reason='missing event-store history')
    except ValueError as error:
        item.update(status='skipped', reason=str(error))
    except Exception as error:
        logger.exception('Metadata refresh failed: %s %s, workflow %s', kind, ident, workflow_id)
        item.update(status='failed', reason=str(error) or type(error).__name__,
                    possibly_partial=not dry_run)
    else:
        changed = any(a['action'] != 'unchanged' for a in plan['annotations'])
        if dry_run:
            item.update(status='planned', changed=changed)
            if detailed:
                metadata_refresh_log_diff(kind, ident, workflow_id, plan)
                item['plan'] = plan
        else:
            item.update(status='updated' if changed else 'unchanged')
        if backup_path and changed and not dry_run:
            item['backup'] = str(backup_path)
    if item['status'] == 'skipped':
        logger.info('Metadata refresh skipped: %s %s, workflow %s: %s',
                    kind, ident, workflow_id, item['reason'])
    return item


@contextmanager
def metadata_refresh_worker(conn, tracker, session_id):
    """Own a gateway and history reader in one lane; never kill the parent session."""
    gateway = conn.clone()
    try:
        if not gateway.connect(sUuid=session_id):
            raise RuntimeError('Metadata worker could not join the script session')
        gateway.c.enableKeepAlive(60)
        env = dict(tracker.env)
        # Maintenance only reads existing events; workers must not create tables.
        env['CREATE_TABLE'] = env['WORKFLOWTRACKER_CREATE_TABLE'] = 'no'
        with WorkflowTracker(env=env) as reader:
            try:
                yield gateway, reader
            finally:
                # The SQLAlchemy factory's close() does not dispose its datastore.
                # Remove this thread's scoped session, or dispose its own engine.
                datastore = getattr(reader.factory, 'datastore', None)
                if datastore is not None:
                    if getattr(datastore, 'scoped_session', None) is not None:
                        datastore.scoped_session.remove()
                    elif getattr(datastore, 'engine', None) is not None:
                        datastore.engine.dispose()
    finally:
        gateway.close(hard=False)


def metadata_refresh_lane(chunk, factory, queue, directory, options):
    """Reuse lane-local resources and report every target, including setup failures."""
    completed = 0
    try:
        with factory() as (conn, tracker):
            for index, (kind, ident, workflow_id) in chunk:
                backup = directory / f'{index:06d}-{kind}-{ident}.json' if directory else None
                item = metadata_refresh_target(
                    conn, tracker, kind, ident, workflow_id, backup_path=backup, **options)
                queue.put((index, item))
                completed += 1
    except Exception as error:
        logger.exception('Metadata refresh worker stopped')
        for index, (kind, ident, workflow_id) in chunk[completed:]:
            queue.put((index, {'object_type': kind, 'object_id': ident,
                              'workflow_id': workflow_id, 'status': 'failed',
                              'reason': str(error) or type(error).__name__,
                              'possibly_partial': False}))


def metadata_refresh_outcomes(conn, tracker, targets, directory, options,
                              workers, worker_factory):
    """Keep outstanding jobs bounded to lanes, not one future per OMERO result."""
    if workers == 1:
        for index, (kind, ident, workflow_id) in enumerate(targets):
            backup = directory / f'{index:06d}-{kind}-{ident}.json' if directory else None
            yield index, metadata_refresh_target(
                conn, tracker, kind, ident, workflow_id, backup_path=backup, **options)
        return
    # Serialize multiple workflow views attached to the same object in one lane.
    chunks = [[] for _ in range(workers)]
    object_lanes = {}
    for index, target in enumerate(targets):
        key = target[:2]
        lane = object_lanes.setdefault(key, len(object_lanes) % workers)
        chunks[lane].append((index, target))
    queue = Queue(maxsize=workers * 2)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(metadata_refresh_lane, chunk, worker_factory,
                                   queue, directory, options) for chunk in chunks if chunk]
        for _ in targets:
            while True:
                try:
                    item = queue.get(timeout=1)
                    break
                except Empty:
                    if all(future.done() for future in futures):
                        raise RuntimeError('Metadata workers stopped without reporting every target')
            yield item


def refresh_all_metadata(conn, tracker, *, view_version='v0', dry_run=True,
                         backup_directory=None, workflow_ids=None, backup_enabled=False,
                         workers=1, worker_factory=None):
    """Refresh discoverable views, reporting unavailable histories separately."""
    if not conn.isAdmin():
        raise ValueError('Metadata refresh requires an administrator')
    if view_version != 'v0':
        raise ValueError('View_Version must be v0')
    if type(workers) is not int or not 1 <= workers <= 8:
        raise ValueError('Metadata workers must be an integer between 1 and 8')
    if workers > 1 and worker_factory is None:
        raise ValueError('Parallel metadata workers require an isolated worker_factory')
    selected_workflows = {str(UUID(str(value).strip()))
                          for value in (workflow_ids or [])}
    targets = discover_metadata_targets(conn)
    if selected_workflows:
        targets = [target for target in targets if target[2] in selected_workflows]
    directory = None
    if not dry_run and backup_enabled:
        if backup_directory:
            directory = Path(backup_directory)
            if not directory.is_absolute():
                raise ValueError('Metadata Backup Directory must be an absolute worker path')
        else:
            root = Path('/data/biomero-metadata-backups')
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            directory = root / f'refresh-{uuid4()}'
        # Exclusive directory creation protects previous backups and reports.
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        logger.info('Metadata backups and report will be stored in: %s', directory)
    elif not dry_run:
        logger.info('Metadata backups not requested. No on-disk backups will be created.')
    detailed = dry_run and (0 < len(selected_workflows) <= 3 or len(targets) <= 3)
    report = {'view_version': view_version, 'dry_run': dry_run,
              'backup_enabled': backup_enabled,
              'backup_directory': str(directory) if directory else None,
              'discovered': len(targets), 'results': [],
              'counts': dict(planned=0, updated=0, unchanged=0, skipped=0, failed=0)}
    options = dict(view_version=view_version, dry_run=dry_run,
                   backup_enabled=backup_enabled, detailed=detailed)
    results = {}
    would_update = 0
    logger.info('Metadata refresh started: %s result/workflow pairs; workers=%s; mode=%s; field diffs=%s',
                len(targets), workers, 'dry run' if dry_run else 'apply', detailed)
    for index, item in metadata_refresh_outcomes(
            conn, tracker, targets, directory, options, workers, worker_factory):
        results[index] = item
        report['counts'][item['status']] += 1
        if item['status'] == 'planned' and item['changed']:
            would_update += 1
        if len(results) % 25 == 0 or len(results) == len(targets):
            if dry_run:
                logger.info('Metadata refresh progress: %s/%s; would update=%s, unchanged=%s, skipped=%s, failed=%s',
                            len(results), len(targets), would_update,
                            report['counts']['planned'] - would_update,
                            report['counts']['skipped'], report['counts']['failed'])
            else:
                logger.info('Metadata refresh progress: %s/%s; updated=%s, unchanged=%s, skipped=%s, failed=%s',
                            len(results), len(targets), report['counts']['updated'], report['counts']['unchanged'],
                            report['counts']['skipped'], report['counts']['failed'])
    report['results'] = [results[index] for index in sorted(results)]
    if directory:
        with (directory / 'report.json').open('x', encoding='utf-8') as stream:
            json.dump(report, stream, indent=2)
    return report


def refresh_metadata_from_init(client, conn):
    """Optional metadata-only maintenance; no Slurm connection is required."""
    if not unwrap(client.getInput('Refresh OMERO Metadata')):
        return None
    if not conn.isAdmin():
        raise ValueError('Metadata refresh requires an administrator')
    dry_run = unwrap(client.getInput('Metadata Dry Run'))
    if dry_run is None:
        dry_run = True
    version = unwrap(client.getInput('Metadata View Version')) or 'v0'
    workers = unwrap(client.getInput('Metadata Workers'))
    if workers is None:
        workers = 4
    if type(workers) is not int or not 1 <= workers <= 8:
        raise ValueError('Metadata workers must be an integer between 1 and 8')
    backup = unwrap(client.getInput('Metadata Backup Directory'))
    backup_enabled = unwrap(client.getInput('Save Metadata Backups'))
    if backup_enabled is None:
        backup_enabled = False
    selection = {}
    if unwrap(client.getInput('Filter Metadata by Workflow UUIDs')):
        workflow_ids = unwrap(client.getInput('Metadata Workflow UUIDs'))
        if not workflow_ids:
            raise ValueError('Select at least one workflow UUID when filtering metadata')
        selection['workflow_ids'] = [str(UUID(value.strip())) for value in workflow_ids]
        logger.info('Metadata refresh scope: selected workflow UUIDs %s', selection['workflow_ids'])
    else:
        logger.info('Metadata refresh scope: all workflows; UUID dropdown values are ignored')
    selection['backup_enabled'] = backup_enabled
    client.enableKeepAlive(60)
    with WorkflowTracker() as tracker:
        if workers > 1:
            selection.update(workers=workers, worker_factory=partial(
                metadata_refresh_worker, conn, tracker, client.getSessionId()))
        return refresh_all_metadata(conn, tracker, view_version=version,
                                    dry_run=dry_run, backup_directory=backup, **selection)


def get_metadata_workflow_choices():
    """Populate the native script selector from existing workflow metadata."""
    client = None
    try:
        client = omero.client()
        client.createSession()
        conn = BlitzGateway(client_obj=client)
        # Discovery checks administrator access before querying across groups.
        workflow_ids = set()
        for _, _, value in discover_metadata_targets(conn):
            try:
                workflow_ids.add(str(UUID(str(value).strip())))
            except ValueError:
                continue
        return [rstring(value) for value in sorted(workflow_ids)]
    except Exception:
        logger.warning('Could not load metadata workflow UUID choices', exc_info=True)
        return []
    finally:
        if client is not None:
            client.closeSession()


def runScript():
    """Main entry point for SLURM environment initialization script.
    
    Sets up the complete SLURM environment for BIOMERO workflow execution
    including directory structure, job scripts, converters, and container
    images. This is typically run once during initial setup.
    """

    extra_config_name = "Extra Config file (optional!)"
    init_slurm_name = "Init Slurm"
    rebuild_analytics_name = "Rebuild Analytics Views (skip if only adding new workflows!)"
    rebuild_days_ago_name = "Rebuild From Days Ago"
    rebuild_from_date_name = "Rebuild From Date"
    client = scripts.client(
        'Slurm Init (Admin Only)',
        '''Will initiate the Slurm environment for workflow execution.

        **ADMIN ONLY**: Requires OMERO administrator privileges.

        You can provide a config file location, 
        and/or it will look for default locations:
        /etc/slurm-config.ini
        ~/slurm-config.ini
        ''',
        scripts.Bool(init_slurm_name, grouping="01", default=True),
        scripts.String(extra_config_name, optional=True, grouping="01.1",
                       description="The path to your configuration file on the server. Optional."),
        scripts.Bool(rebuild_analytics_name, grouping="01.2", default=True,
                     description="Drop and rebuild analytics view tables from scratch. "
                                 "Required after BIOMERO upgrades or schema changes. "
                                 "Only safe to uncheck when solely adding new workflow containers "
                                 "to an existing installation with no BIOMERO version change."),
        scripts.Int(rebuild_days_ago_name, optional=True, grouping="01.3",
                    description="Advanced opt-in: limit analytics view rebuild to the last N days of events. "
                                "Only use this if your event history is very large and full rebuilds are too slow. "
                                "Warning: jobs older than this cutoff will not appear in analytics views. "
                                "Leave empty to use whatever is configured in slurm-config.ini or env vars (or full rebuild if nothing is set)."),
        scripts.String(rebuild_from_date_name, optional=True, grouping="01.4",
                       description="Advanced opt-in: limit analytics view rebuild to events from this date onward (YYYY-MM-DD). "
                                   "Only use this if your event history is very large and full rebuilds are too slow. "
                                   "Warning: jobs before this date will not appear in analytics views. "
                                   "Ignored when 'Rebuild From Days Ago' is also set. "
                                   "Leave empty to use whatever is configured in slurm-config.ini or env vars (or full rebuild if nothing is set)."),
        scripts.Bool('Refresh OMERO Metadata', grouping='02', default=False,
                     description='Refresh existing Image and Plate workflow metadata across groups. Independent of Init Slurm.'),
        scripts.Bool('Metadata Dry Run', grouping='02.1', default=True,
                     description='Preview changes without writing. Inspect the report before disabling.'),
        scripts.String('Metadata View Version', grouping='02.2', default='v0',
                       values=[rstring('v0')]),
        scripts.Int('Metadata Workers', grouping='02.2.1', default=4,
                    description='Parallel metadata workers (1-8). Each worker owns its connection and history reader. Use 1 for sequential maintenance.'),
        scripts.Bool('Save Metadata Backups', grouping='02.3', default=False,
                     description='Optionally save original metadata before applying. These are manual-recovery snapshots; no automated restore is provided.'),
        scripts.String('Metadata Backup Directory', optional=True, grouping='02.3.1',
                       description='Optional new absolute worker directory. Leave empty for an automatically created run directory under /data/biomero-metadata-backups. The activity log reports the exact location.'),
        scripts.Bool('Filter Metadata by Workflow UUIDs', grouping='02.4', default=False,
                     description='Restrict the refresh to selected workflows. Leave unchecked for all workflows, regardless of the prefilled UUID dropdown.'),
        scripts.List('Metadata Workflow UUIDs', optional=True, grouping='02.4.1',
                     values=get_metadata_workflow_choices(),
                     description='Used only when Filter Metadata by Workflow UUIDs is checked. Select existing workflows; use [+]/[-] to add/remove selectors and type to search.').ofType(rstring('')),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact='cellularimaging@amsterdamumc.nl',
        authorsInstitutions=[[1]]
    )

    try:
        conn = BlitzGateway(client_obj=client)
        
        # Check if user is admin
        user = conn.getUser()
        is_admin = user.isAdmin()
        user_id = conn.getUserId()
        
        logger.info(f"User ID {user_id} admin status: {is_admin}")
        
        if not is_admin:
            logger.warning("Access denied: Admin privileges required")
            client.setOutput("Message", rstring(
                f"ACCESS DENIED: This initialization script requires OMERO "
                f"administrator privileges. User ID {user_id} is not an admin."
            ))
            return
        
        logger.info("Admin access confirmed, proceeding with initialization")
        message = ""
        init_slurm = unwrap(client.getInput(init_slurm_name))
        reset_view_tables = unwrap(client.getInput(rebuild_analytics_name))
        if reset_view_tables is None:
            reset_view_tables = True  # default: full reset
        rebuild_days_ago = unwrap(client.getInput(rebuild_days_ago_name))
        rebuild_from_date = unwrap(client.getInput(rebuild_from_date_name))
        if init_slurm:
            configfile = unwrap(client.getInput(extra_config_name))
            if not configfile:
                configfile = ''
            with SlurmClient.from_config(configfile=configfile) as slurmClient:
                image_array_id = None
                # Override analytics rebuild window if provided via UI
                if rebuild_days_ago is not None:
                    slurmClient.analytics_rebuild_days_ago = int(rebuild_days_ago)
                elif rebuild_from_date:
                    slurmClient.analytics_rebuild_start_time = rebuild_from_date
                conn.keepAlive()
                # We are kind of duplicating code here, so we can keep the conn alive.
                if slurmClient.validate():
                    # 1. Create directories
                    slurmClient.setup_directories()
                    conn.keepAlive()

                    # 2. Clone git
                    slurmClient.setup_job_scripts()
                    conn.keepAlive()

                    # 3. Stage converters. Their images are submitted together
                    # with workflow images so one concurrency limit covers all
                    # container builds.
                    converter_specs = slurmClient.prepare_converters()
                    conn.keepAlive()

                    # 4. Submit one bounded workflow + converter image array.
                    image_array_id = slurmClient.setup_container_images(
                        extra_image_specs=converter_specs)
                    conn.keepAlive()
                    
                    # 5. Reset db views
                    slurmClient.initialize_analytics_system(reset_tables=reset_view_tables)
                    conn.keepAlive()
                image_status = slurmClient.get_image_pull_status()
                message = (
                    "Slurm directories and scripts are set up.\n" +
                    format_image_submission(image_array_id, image_status) +
                    "\nUse 'SLURM check setup' for per-image failures and "
                    "updated state."
                )
                models, _ = slurmClient.get_all_image_versions_and_data_files()
                filtered_models = {
                    key: value for key, value in models.items() if value}
                logger.info('Validated workflow versions currently available: %s', filtered_models)
                message += f'\nWorkflows with available versions: {len(filtered_models)}.'

        metadata_report = refresh_metadata_from_init(client, conn)
        if metadata_report is not None:
            message += '\n' + format_metadata_summary(metadata_report)
        logger.info('%s', message)
        client.setOutput("Message", rstring(str(message)))

    finally:
        client.closeSession()


if __name__ == '__main__':
    # Some defaults from OMERO; don't feel like reading ice files.
    # Retrieve the value of the OMERODIR environment variable
    OMERODIR = os.environ.get('OMERODIR', '/opt/omero/server/OMERO.server')
    LOGDIR = os.path.join(OMERODIR, 'var', 'log')
    LOGFORMAT = "%(asctime)s %(levelname)-5.5s [%(name)40s] " \
                "[%(process)d] (%(threadName)-10s) %(message)s"
    # Added the process id
    LOGSIZE = 500000000
    LOGNUM = 9
    log_filename = 'biomero.log'
    # OMERO captures stdout as the activity log behind the i button.
    # The separate Message output contains the concise activity result.
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    # Create DEBUG logging to rotating logfile at var/log
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGFORMAT,
                        handlers=[
                            stream_handler,
                            logging.handlers.RotatingFileHandler(
                                os.path.join(LOGDIR, log_filename),
                                maxBytes=LOGSIZE,
                                backupCount=LOGNUM)
                        ])
    
    # Silence some of the DEBUG - Extended for cleaner BIOMERO logs
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('omero.gateway').setLevel(logging.WARNING)  # Silences proxy creation spam
    logging.getLogger('omero.client').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)
    logging.getLogger('paramiko.sftp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('requests_cache').setLevel(logging.WARNING)  # Cache logs
    logging.getLogger('requests-cache').setLevel(logging.WARNING)  # Alt naming
    logging.getLogger('requests_cache.core').setLevel(logging.WARNING)  # Core module
    logging.getLogger('requests_cache.backends').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.base').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.sqlite').setLevel(
        logging.WARNING)
    logging.getLogger('requests_cache.policy').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.policy.actions').setLevel(
        logging.WARNING)
    logging.getLogger('invoke').setLevel(logging.WARNING)
    logging.getLogger('fabric').setLevel(logging.WARNING)  # SSH operations
    logging.getLogger('Ice').setLevel(logging.ERROR)
    logging.getLogger('ZeroC').setLevel(logging.ERROR)

    runScript()
