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
from uuid import UUID
from eventsourcing.application import AggregateNotFoundError
from omero.sys import ParametersI
from biomero import WorkflowTracker
from biomero.provenance import MetadataAnnotation, NAMESPACE, plan_metadata_refresh
import os
import sys

logger = logging.getLogger(__name__)
VERSION = "2.9.0"


def format_metadata_summary(report):
    """Summarize result/workflow pairs; detailed plans stay in the worker log."""
    counts = report['counts']
    if report['dry_run']:
        plans = [item['plan'] for item in report['results'] if item['status'] == 'planned']
        changed = sum(any(a['action'] != 'unchanged' for a in plan['annotations'])
                      for plan in plans)
        outcomes = f'Would update: {changed}; Unchanged: {len(plans) - changed}'
        mode = 'dry run'
        note = 'No OMERO metadata was changed.'
    else:
        outcomes = f"Updated: {counts.get('updated', 0)}; Unchanged: {counts.get('unchanged', 0)}"
        mode = 'apply'
        note = 'See the backup directory for per-target backups and report.json.'
    return (f"Metadata refresh ({report['view_version']}, {mode}): "
            f"{report['discovered']} result/workflow pairs.\n"
            f"{outcomes}; Skipped: {counts.get('skipped', 0)}; Failed: {counts.get('failed', 0)}.\n"
            f"{note}\nFull report and skip/failure details: activity log (i button).")


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
                              *, view_version='v0', dry_run=True, backup_path=None):
    """Refresh existing maps in place; unlink obsolete internal-task maps.

    Default is a dry run. Applying requires a new backup file and administrator
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
                   'annotations': [{'id': a['annotation_id'], 'action': a['action'],
                                    'before': len(a['pairs']),
                                    'after': len(a['new_pairs'] or [])}
                                   for a in actions]}
        if dry_run:
            return summary
        if not backup_path:
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
        with Path(backup_path).open('x', encoding='utf-8') as stream:
            json.dump({**summary, 'actions': actions}, stream, indent=2)
        conn.SERVICE_OPTS.setOmeroGroup(str(target.getDetails().group.id.val))
        # Update retained maps before removing links. No global annotation delete.
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


def refresh_all_metadata(conn, tracker, *, view_version='v0', dry_run=True,
                         backup_directory=None, workflow_id=None):
    """Refresh discoverable views, reporting unavailable histories separately."""
    if not conn.isAdmin():
        raise ValueError('Metadata refresh requires an administrator')
    if view_version not in ('v0', 'v1'):
        raise ValueError('View_Version must be v0 or v1')
    selected_workflow = str(UUID(str(workflow_id).strip())) if workflow_id else None
    targets = discover_metadata_targets(conn)
    if selected_workflow:
        targets = [target for target in targets if target[2] == selected_workflow]
    directory = None
    if not dry_run:
        if not backup_directory or not Path(backup_directory).is_absolute():
            raise ValueError('Applying all views requires an absolute Backup_Directory')
        directory = Path(backup_directory)
        # Exclusive directory creation protects previous backups and reports.
        directory.mkdir(mode=0o700, exist_ok=False)
    report = {'view_version': view_version, 'dry_run': dry_run,
              'discovered': len(targets), 'results': [],
              'counts': dict(planned=0, updated=0, unchanged=0, skipped=0, failed=0)}
    for index, (kind, ident, workflow_id) in enumerate(targets):
        item = {'object_type': kind, 'object_id': ident, 'workflow_id': workflow_id}
        try:
            plan = refresh_workflow_metadata(
                conn, tracker, kind, ident, workflow_id,
                view_version=view_version, dry_run=True)
        except AggregateNotFoundError:
            item.update(status='skipped', reason='missing event-store history')
        except ValueError as error:
            item.update(status='skipped', reason=str(error))
        except Exception as error:
            item.update(status='failed', reason=type(error).__name__)
        else:
            if dry_run:
                item.update(status='planned', plan=plan)
            elif all(a['action'] == 'unchanged' for a in plan['annotations']):
                item.update(status='unchanged')
            else:
                backup = directory / f'{index:06d}-{kind}-{ident}.json'
                try:
                    result = refresh_workflow_metadata(
                        conn, tracker, kind, ident, workflow_id,
                        view_version=view_version, dry_run=False, backup_path=backup)
                    item.update(status='updated', result=result, backup=str(backup))
                except Exception as error:
                    # A write failure may follow earlier writes on this target.
                    # Never report it as an untouched/skipped view.
                    item.update(status='failed', reason=type(error).__name__,
                                backup=str(backup), possibly_partial=True)
        report['results'].append(item)
        report['counts'][item['status']] += 1
        if directory:
            with (directory / 'report.json').open('w', encoding='utf-8') as stream:
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
    backup = unwrap(client.getInput('Metadata Backup Directory'))
    workflow_id = unwrap(client.getInput('Metadata Workflow UUID'))
    selection = {'workflow_id': str(UUID(workflow_id.strip()))} if workflow_id and workflow_id.strip() else {}
    client.enableKeepAlive(60)
    with WorkflowTracker() as tracker:
        return refresh_all_metadata(conn, tracker, view_version=version,
                                    dry_run=dry_run, backup_directory=backup, **selection)


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
                       values=[rstring('v0'), rstring('v1')]),
        scripts.String('Metadata Backup Directory', optional=True, grouping='02.3',
                       description='New absolute directory on private durable worker storage, required when applying changes.'),
        scripts.String('Metadata Workflow UUID', optional=True, grouping='02.4',
                       description='Only refresh existing Image and Plate metadata for this workflow UUID. Leave blank for all workflows.'),
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
            logger.info('Full metadata refresh report:\n%s', json.dumps(metadata_report, indent=2))
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
