# Refreshing workflow metadata

BIOMERO core renders and plans metadata views as plain data structures.
The scripts layer owns OMERO connections, annotation persistence and backups.
The dedicated admin script `admin/SLURM_Refresh_Metadata.py` updates existing
metadata. Result import scripts only write metadata for newly imported results.
It supports result Images and Plates, regardless of which result script created
their annotations.

Run **Refresh BIOMERO Metadata (Admin Only)** with these inputs:

- `Data_Type`: `Image` or `Plate`.
- `ID`: the existing result object's ID (single-result mode).
- `Workflow_ID`: the UUID recorded in its workflow metadata (single-result mode).
- `View_Version`: `v0` (default) or `v1`.
- `Dry_Run`: true by default; inspect the returned plan before applying.
- `Backup_Path`: a new absolute file path on private, durable worker storage;
  required when applying. This is a worker path, not a browser download path.
- `All_Existing`: discover workflow views on all accessible result Images and
  Plates across groups. Leave `ID` and `Workflow_ID` empty in this mode;
  `Data_Type` is ignored. This option is off by default.
- `Backup_Directory`: a new absolute directory on private, durable worker storage;
  required instead of `Backup_Path` when applying `All_Existing`. Its parent
  directory must already exist.

For a deployment-wide update, select `All_Existing`, choose the desired
`View_Version`, and inspect the dry-run report first. Discovery uses existing
`biomero/workflow` annotations; it does not create metadata on unannotated objects.
Each object/workflow pair is processed independently. Missing event-store
history or ambiguous/incomplete snapshots are skipped and reported, leaving
those views unchanged. A write failure is reported separately as potentially
partial, and processing continues with the next target. Shared annotations are
not modified automatically.

Applying bulk changes creates separate per-target backup files and a cumulative
`report.json` in the new backup directory. Reusing an existing directory is
refused. Bulk scope never implies permission to reconstruct missing history,
advance historical snapshots to current state, or discard unknown annotations.

Administrator privileges are checked before accessing workflow history. The
script uses the worker's tracking-database configuration and does not submit
Slurm jobs or re-import data.

The helper can also be called from the scripts runtime, with `admin` on the
Python import path:

```python
from SLURM_Refresh_Metadata import refresh_workflow_metadata

# conn is an administrator's BlitzGateway; tracker is WorkflowTracker.
plan = refresh_workflow_metadata(
    conn, tracker, "Plate", plate_id, workflow_uuid, view_version="v1")
# Inspect the dry-run plan before applying. Use a private backup location.
result = refresh_workflow_metadata(
    conn, tracker, "Plate", plate_id, workflow_uuid,
    view_version="v1", dry_run=False,
    backup_path="/private/backups/plate-metadata-before-refresh.json")
```

The default is a dry run and the legacy-compatible `v0` view. The explicit `v1`
view additionally omits verbose job command, environment and result-message
fields. Neither policy reduces full CSV provenance or changes event history.
See BIOMERO's developer documentation, **Workflow metadata views**, for the
rendering policies and historical snapshot resolution.

The updater edits retained MapAnnotations in place, preserving their IDs,
namespaces and creation events. Obsolete internal-task annotations are unlinked
from the selected object, not globally deleted. Reapplying a view is idempotent.
The backup contains original key/value pairs and annotation IDs, including
unlinked annotations; protect it like other provenance containing execution
details. Restore retained values with `MapAnnotationWrapper.setValue` and
`save`; unlinked original annotations can be linked to the object again.

Ambiguous or incomplete snapshots, conflicting identities, shared annotations,
or duplicate non-list keys are refused rather than guessed. Repeated
`Input_Data` keys retain the legacy list representation. Unknown namespaces
and additional custom keys are preserved. Existing CSV references for oversized
values are retained.

Run refreshes while metadata writers for the selected result are idle. The
updater checks for intervening changes, but multiple OMERO writes are not one
transaction. Errors propagate; inspect the backup and current annotations
before retrying a partially applied refresh. Image data, full CSV attachments,
shallow/canonical metadata and events are never rewritten by this API.
