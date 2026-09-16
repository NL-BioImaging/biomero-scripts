#!/usr/bin/env python
"""Administrative refresh of existing workflow metadata annotations."""
import json
from pathlib import Path
from uuid import UUID

from eventsourcing.application import AggregateNotFoundError

from omero import scripts
from omero.constants.namespaces import NSDYNAMIC
from omero.gateway import BlitzGateway
from omero.rtypes import rstring
from omero.sys import ParametersI

from biomero import WorkflowTracker
from biomero.provenance import MetadataAnnotation, NAMESPACE, plan_metadata_refresh

VERSION = "2.9.0"


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
                                     view_version=view_version)
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
                         backup_directory=None):
    """Refresh discoverable views, reporting unavailable histories separately."""
    if not conn.isAdmin():
        raise ValueError('Metadata refresh requires an administrator')
    if view_version not in ('v0', 'v1'):
        raise ValueError('View_Version must be v0 or v1')
    targets = discover_metadata_targets(conn)
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


def runScript():
    """Refresh selected or all existing metadata views; never import data."""
    client = scripts.client(
        'Refresh BIOMERO Metadata (Admin Only)',
        'Refresh existing metadata views. Dry run by default. '
        'Applying requires a private, durable backup path on the worker.',
        scripts.String('Data_Type', optional=False, grouping='1',
                       values=[rstring('Image'), rstring('Plate')], default='Plate'),
        scripts.Long('ID', optional=True, grouping='2'),
        scripts.String('Workflow_ID', optional=True, grouping='3'),
        scripts.String('View_Version', optional=False, grouping='4',
                       values=[rstring('v0'), rstring('v1')], default='v0'),
        scripts.Bool('Dry_Run', optional=False, grouping='5', default=True),
        scripts.String('Backup_Path', optional=True, grouping='6',
                       description='New absolute file path on durable worker storage; '
                                   'required when Dry_Run is false.'),
        scripts.Bool('All_Existing', optional=False, grouping='7', default=False,
                     description='Discover all Image and Plate workflow views across groups.'),
        scripts.String('Backup_Directory', optional=True, grouping='8',
                       description='New absolute directory on durable worker storage; '
                                   'required when applying All_Existing.'),
        namespaces=[NSDYNAMIC], version=VERSION,
        authors=['Torec Luik'], institutions=['Amsterdam UMC'],
        contact='cellularimaging@amsterdamumc.nl')
    try:
        conn = BlitzGateway(client_obj=client)
        if not conn.isAdmin():
            client.setOutput('Message', rstring(
                'Access denied: administrator privileges are required.'))
            return
        inputs = client.getInputs(unwrap=True)
        all_existing = inputs.get('All_Existing', False)
        if all_existing and (inputs.get('ID') is not None or inputs.get('Workflow_ID')):
            raise ValueError('Omit ID and Workflow_ID when All_Existing is selected')
        if not all_existing and (inputs.get('ID') is None or not inputs.get('Workflow_ID')):
            raise ValueError('Provide ID and Workflow_ID, or select All_Existing')
        dry_run = inputs.get('Dry_Run', True)
        backup = inputs.get('Backup_Path', '').strip() or None
        if not all_existing and not dry_run and (not backup or not Path(backup).is_absolute()):
            raise ValueError('Applying requires an absolute Backup_Path on durable storage')
        client.enableKeepAlive(60)
        # Tracking persistence comes from the worker's configured environment.
        # Metadata maintenance does not connect to or submit work on Slurm.
        with WorkflowTracker() as tracker:
            if all_existing:
                result = refresh_all_metadata(
                    conn, tracker, view_version=inputs.get('View_Version', 'v0'),
                    dry_run=dry_run,
                    backup_directory=inputs.get('Backup_Directory', '').strip() or None)
            else:
                workflow_id = str(UUID(inputs['Workflow_ID'].strip()))
                result = refresh_workflow_metadata(
                    conn, tracker, inputs['Data_Type'], inputs['ID'], workflow_id,
                    view_version=inputs.get('View_Version', 'v0'),
                    dry_run=dry_run, backup_path=backup)
        client.setOutput('Message', rstring(json.dumps(result, indent=2)))
    finally:
        client.closeSession()


if __name__ == '__main__':
    runScript()
