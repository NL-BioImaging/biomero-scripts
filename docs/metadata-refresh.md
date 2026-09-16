# Refreshing workflow metadata

BIOMERO core renders and plans metadata views as plain data structures.
The scripts layer owns OMERO connections, annotation persistence and backups.
The admin script `admin/SLURM_Init_environment.py` optionally updates existing
metadata. Result import scripts only write metadata for newly imported results.
It supports result Images and Plates, regardless of which result script created
their annotations.

Run **Slurm Init (Admin Only)** with these inputs:

- `Refresh OMERO Metadata`: off by default; discover existing Image and Plate
  workflow metadata across groups.
- `Metadata View Version`: `v0` (default) or `v1`.
- `Metadata Dry Run`: true by default; inspect the report before disabling it.
- `Metadata Backup Directory`: a new absolute directory on private, durable
  worker storage; required when applying. Its parent must already exist.
- `Metadata Workflow UUID`: optional; restrict refresh to existing Image and Plate
  annotations for one workflow. Leave blank for all workflows. Discovery still
  scans workflow annotations, but only matching results are replayed and planned.

Uncheck `Init Slurm` for metadata-only maintenance, without cluster setup or
analytics rebuilding. Analytics projections and OMERO annotations are separate
views of the event store, controlled by separate options.

For a deployment-wide update, select `Refresh OMERO Metadata`, choose the desired
view version, and inspect the dry-run report first. Discovery uses existing
`biomero/workflow` annotations; it does not create metadata on unannotated objects.
Each object/workflow pair is processed independently. Missing event-store
history or ambiguous/incomplete snapshots are skipped and reported, leaving
those views unchanged. A write failure is reported separately as potentially
partial, and processing continues with the next target. Shared annotations are
not modified automatically.

The activity Message and stdout show a compact summary, including how many
result/workflow pairs would change, are unchanged, were skipped or failed.
The full per-target report is retained in the worker's `biomero.log`; applying
changes also writes `report.json` in the backup directory. Library INFO detail
is kept in the worker log rather than stdout; warnings remain visible in the UI.

Applying bulk changes creates separate per-target backup files and a cumulative
`report.json` in the new backup directory. Reusing an existing directory is
refused. Bulk scope never implies permission to reconstruct missing history,
advance historical snapshots to current state, or discard unknown annotations.

Administrator privileges are checked before accessing workflow history. The
metadata refresh uses the worker's tracking-database configuration and does not
submit Slurm jobs or re-import data.

The helper can also be called from the scripts runtime, with `admin` on the
Python import path:

```python
from SLURM_Init_environment import refresh_workflow_metadata

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
Both retain recorded per-result shallow/full storage provenance alongside the
import-task metadata, including execution location, tool/container identity and
canonical biocodes or a manifest reference. Import Results records these facts
on new imports; this admin script only re-renders the historical snapshot. Older
snapshots lacking those facts cannot acquire them through a view refresh alone.
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
