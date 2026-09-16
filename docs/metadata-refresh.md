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
- `Metadata View Version`: `v0` (the only supported view).
- `Metadata Dry Run`: true by default; inspect the report before disabling it.
- `Save Metadata Backups`: false by default. Enable it to save the original
  annotation values and links before changing each target. These are optional
  inspection/manual-recovery snapshots, not an automated restore mechanism.
- `Metadata Backup Directory`: optional override for a new absolute worker
  directory. Leave empty to create a unique run directory under
  `/data/biomero-metadata-backups` on the worker's durable shared storage.
  Missing parent directories are created automatically. No directory needs
  to be supplied for normal use.
- `Filter Metadata by Workflow UUIDs`: false by default. Leave unchecked to
  refresh all workflows; the UUID dropdown's preselected value is ignored.
- `Metadata Workflow UUIDs`: searchable selectors populated from
  existing Image and Plate workflow metadata. When filtering is enabled,
  select one or more workflows; use `[+]`/`[-]` to add or remove selectors.
  At least one UUID is required when filtering is enabled. Discovery still
  scans workflow annotations, but only matching results are replayed and planned.

Uncheck `Init Slurm` for metadata-only maintenance, without cluster setup or
analytics rebuilding. Analytics projections and OMERO annotations are separate
views of the event store, controlled by separate options.

Before a deployment-wide update, dry-run one to three selected workflow UUIDs
and inspect their field diffs. Then disable filtering and dry-run mode to apply
the same view across OMERO. Discovery uses existing
`biomero/workflow` annotations; it does not create metadata on unannotated objects.
Each object/workflow pair is processed independently. Missing event-store
history or ambiguous/incomplete snapshots are skipped and reported, leaving
those views unchanged. A write failure is reported separately as potentially
partial, and processing continues with the next target. Shared annotations are
not modified automatically.

The activity Message shows a compact summary, including how many
result/workflow pairs would change, are unchanged, were skipped or failed.
The activity log behind the info button and the worker's `biomero.log` show
human-readable field diffs for dry runs of up to three selected workflows
(or up to three result/workflow pairs). Only added, removed and changed fields
are logged; unchanged fields are omitted. Long values are abbreviated.
An `unlink` removes only the result's link, not the annotation itself.
Bulk sweeps log progress counts and skip/failure outcomes rather than full
metadata maps. Dry runs write neither OMERO metadata nor backup files.

When backups are enabled, applying bulk changes creates separate per-target JSON
snapshots and a compact outcome `report.json`, written once on completion, in
the new run directory. The exact
directory is reported in both the activity Message and detailed log; each saved
snapshot is logged. Reusing an explicitly supplied existing directory is
refused. With backups disabled, no snapshot or report file is written;
progress and outcomes remain in the normal activity and worker logs. Bulk scope never
implies permission to reconstruct missing history,
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
    conn, tracker, "Plate", plate_id, workflow_uuid, view_version="v0")
# Inspect the dry-run plan before applying. Use a private backup location.
result = refresh_workflow_metadata(
    conn, tracker, "Plate", plate_id, workflow_uuid,
    view_version="v0", dry_run=False,
    backup_path="/private/backups/plate-metadata-before-refresh.json")
```

The default is a dry run and the legacy-compatible `v0` view, which is the only
supported view. Job command, environment and result-message fields are retained.
The view does not reduce full CSV provenance or change event history.
It retains recorded per-result shallow/full storage provenance alongside the
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
details. There is currently no automated restore script. These are inspection
and manual-recovery snapshots: retained values can be restored with
`MapAnnotationWrapper.setValue` and `save`, and unlinked original annotations
can be linked to the object again.

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
