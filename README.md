# <img src="https://raw.githubusercontent.com/NL-BioImaging/OMERO.biomero/refs/tags/v1.2.1/webapp/src/img/biomero-logo.svg" alt="BIOMERO" height="28" style="height:28px; width:auto; vertical-align:middle;"> BIOMERO.scripts
[![Test BIOMERO scripts](https://github.com/NL-BioImaging/biomero-scripts/actions/workflows/tests.yml/badge.svg)](https://github.com/NL-BioImaging/biomero-scripts/actions/workflows/tests.yml)
> 🚀 **This package is part of <img src="https://raw.githubusercontent.com/NL-BioImaging/OMERO.biomero/refs/tags/v1.2.1/webapp/src/img/biomero-logo.svg" alt="BIOMERO" height="16" style="height:16px; width:auto; vertical-align:middle;"> BIOMERO 2.0** — For complete deployment and FAIR infrastructure setup, start with the [**NL-BIOMERO Documentation**](https://nl-bioimaging.github.io/NL-BIOMERO/) 📖

OMERO scripts for exporting image data, running containerized workflows on Slurm,
and importing results with recorded provenance. They work with the
[BIOMERO library](https://github.com/NL-BioImaging/biomero) and the
[OMERO.biomero web interface](https://github.com/NL-BioImaging/OMERO.biomero).

## Documentation

The scripts reference is published with BIOMERO, whose release series these
scripts follow. Deployment and administration are documented in NL-BIOMERO.

- [Scripts reference](https://nl-bioimaging.github.io/biomero/scripts.html):
  installation, workflow execution, result options and logging.
- [NL-BIOMERO deployment](https://nl-bioimaging.github.io/NL-BIOMERO/):
  containers, worker configuration and shared storage.
- [Detached workflows](https://nl-bioimaging.github.io/NL-BIOMERO/master/sysadmin/detached-workflows.html)
  and [remote shallowing](https://nl-bioimaging.github.io/NL-BIOMERO/master/sysadmin/remote-shallower.html).
- [Metadata administration](https://nl-bioimaging.github.io/NL-BIOMERO/master/sysadmin/metadata-refresh.html):
  previewing and refreshing existing workflow annotations.
- [Metadata developer reference](https://nl-bioimaging.github.io/biomero/developer/metadata-views.html):
  event-store views and the scripts persistence adapter.

## Script catalogue

| Directory | Entry points | Purpose |
| --- | --- | --- |
| `__workflows/` | `SLURM_Run_Workflow.py`, `SLURM_Run_Workflow_Batched.py` | Run an analysis and retrieve results; optionally split inputs into batches. |
| `_data/` | Image/File Transfer, Remote Conversion, Get Update | Export inputs, convert formats and monitor jobs. |
| `_data/` | `SLURM_Get_Results.py`, `SLURM_Import_Results.py` | Upload results through OMERO or import in place with BIOMERO.importer. |
| `admin/` | Slurm Init, Check Setup, Shallow Storage Migration, Cownary, Tail Logs | Initialize and inspect the cluster, migrate managed storage metadata, maintain workflow metadata and diagnose execution. |

CellPose Segmentation and Example Minimal Slurm Script are examples, not the
standard workflow entry points; NL-BIOMERO does not install them by default.

## Installation and compatibility

Use the released NL-BIOMERO containers for a coordinated installation. For a
custom deployment, follow the [scripts installation guide](https://nl-bioimaging.github.io/biomero/scripts.html#installation-and-upgrades)
and install the scripts on the OMERO server and detached worker.

Deploy matching core and scripts releases together. This release requires core's
`biomero.provenance` and `biomero.maintenance` APIs even when optional features
are disabled. Prerelease tag counters can differ between repositories; use the
component references supplied by the NL-BIOMERO release.

Detached execution and shallow storage are opt-in for existing deployments:
missing or false feature flags leave them disabled. Within enabled shallow
storage, remote normalization is preferred; set `BIOMERO_REMOTE_SHALLOW_ZARR=false`
to use importer-side normalization. NL-BIOMERO's demo enables these features.

Canonical indexing records the backing store's actual OME-Zarr profile: 0.4/v2
or 0.5/v3. Existing v2 stores are reused as v2, and importing an existing v3
Image or Plate does not require conversion to v2. V3 indexing requires the
generic semantic-guard API from the matching v3-capable shallower/importer;
older installations retain v2-only indexing. Mixed-format Plate inventories
are rejected rather than recorded under an incorrect profile.

Fresh exports for Zarr-consuming workflows default to OME-NGFF 0.5 / Zarr v3;
an explicit 0.4 choice remains available. This is an export preference, not a
request to rewrite existing managed stores: native Zarr workflows reuse v2 and
v3 sources in their existing format.

TIFF-consuming workflows keep using the legacy HPC converter. Their transfer
route requires actual OME-NGFF 0.4 / Zarr v2 material: compatible v2 sources are
reused, while v3 sources are exported temporarily from the selected OMERO
pixels as v2. Shallow selections also export their selected pixels, including
label views, instead of reconstructing a different image. These temporary
exports are not indexed or promoted to canonical storage; the original store
and its annotations remain unchanged. Deploy the matching OMERO.biomero web
change after these scripts so the web default and transfer safeguards agree.

Result file attachments use the existing on-disk paths. Workflow-qualified
attachment names are stored in OMERO metadata, so in-place image registrations
and attachment links remain valid. Directory-backed Zarr stores are available
through result registration or the bulk results ZIP; individual file
attachments skip those directories and report the skipped count.

## Storage provenance and repair

Shallow-result provenance uses the shared schema-2 manifest parser from
`biomero-schema>=0.3.0b1,<0.4` (also required by the matching BIOMERO core).
Plate annotations collect the source identities from the manifest's image
bindings; Image annotations select their own image node. Both v2 and v3
interchange profiles are read without converting the store. Unsupported
manifest versions, inconsistent bindings or ambiguous references produce a
metadata warning rather than guessed provenance.

The import records storage evidence before rendering workflow annotations.
If this step or annotation persistence fails, the script's `Message` reports
`WARNING` with the complete/reduced/incomplete counts and points to the activity
log. The imported data remains available; a completed import does not imply
that all its metadata was written.

For an existing result whose storage evidence is missing, an administrator can
use `backfill_import_storage_provenance()` from the installed import script.
Run this in the worker's Python environment, with an authenticated administrator
`conn` in the result's group and the normal workflow event-store configuration:

```python
import json
import runpy
from pathlib import Path
from types import SimpleNamespace
from biomero import WorkflowTracker

scripts_root = Path("/opt/omero/server/OMERO.server/lib/scripts/biomero")
adapter = runpy.run_path(str(scripts_root / "_data/SLURM_Import_Results.py"))
backfill = adapter["backfill_import_storage_provenance"]
maintenance = runpy.run_path(str(scripts_root / "admin/SLURM_Init_environment.py"))
refresh = maintenance["refresh_workflow_metadata"]
# Select the existing result and the workflow that imported it.
result_type, result_id, workflow_id = "Plate", 123, "your-workflow-uuid"

with WorkflowTracker() as tracker:
    client = SimpleNamespace(workflowTracker=tracker)
    preview = backfill(conn, client, result_type, result_id, workflow_id)
    print(json.dumps(preview, indent=2))
    # After reviewing the preview, append its evidence:
    # backfill(conn, client, result_type, result_id, workflow_id, dry_run=False)

    # After recording, preview the corresponding OMERO annotation changes:
    # plan = refresh(conn, tracker, result_type, result_id, workflow_id,
    #                refresh_storage=True)
    # print(json.dumps(plan, indent=2))
    # Apply with a new backup filename after reviewing that plan:
    # refresh(conn, tracker, result_type, result_id, workflow_id,
    #         refresh_storage=True, dry_run=False,
    #         backup_path="/data/biomero-metadata-backups/result-before-repair.json")
```

The default is a read-only preview. Apply validates the matching workflow
annotation, managed storage evidence and unique import task, then appends the
observed evidence to the event store. Identical evidence is not recorded twice.
Missing files, mismatched workflows, conflicting recorded evidence and ambiguous
results must be resolved before applying. Source and result pixels are never read, rewritten or
renormalized by this operation.

The explicit `refresh_storage=True` option requires a coordinated core build
whose `plan_metadata_refresh()` supports that option. It advances only the
import-task snapshot needed to expose newly recorded storage evidence, including
its paired job maps. Other task snapshots remain pinned. Normal metadata
refresh, including the Slurm Init UI, preserves the old snapshots and therefore
does not expose later evidence by itself. See
[metadata administration](https://nl-bioimaging.github.io/NL-BIOMERO/master/sysadmin/metadata-refresh.html)
for backup and refresh controls. Recording evidence and refreshing maps are two
separate steps. If refresh fails, retry it; repeating the backfill is also safe.

## Result import failures and retries

Importer-enabled finalization succeeds only when every image import order is
confirmed complete. An import failure, polling timeout or lost database
connection fails the script and preserves the Slurm results, downloaded archive
and staged files. Requested cleanup runs only after successful finalization.

Recovery is automatic on compatible storage; no additional feature flag is
needed. Before preparing an import, the script acquires a filesystem lock and
tests durable writes using disposable metadata in the same directory. The check
covers file synchronization, replacement of an existing file and POSIX directory
synchronization. It does not read or copy image pixels.

On compatible storage, each new workflow-result import plan creates one small
`.biomero-result-import.json` in its staged result directory. It saves the
destination, rename mapping, options and order UUIDs before queueing any imports.
It does not create a record per image, hash pixels, scan historical OMERO objects
or migrate already-imported data. Rerun **Import Results** for the same workflow/job
with the same import settings to resume those orders. Pending orders are polled;
completed orders are reused without registering their images again. Concurrent
finalizers are rejected using filesystem locks, which are released on process
exit. Recovery storage must support locks across worker processes, atomic file
replacement, file synchronization and (on POSIX) directory synchronization.

If the preflight explicitly reports an unsupported lock or durable-write
operation, a fresh attempt uses the legacy import path and logs that saved-order
recovery is unavailable. This fallback happens before preparing a destination,
renaming results or submitting orders. It adds no recovery-record writes or
recovery database lookups. File/directory synchronization rejected with `EINVAL` also
uses this fallback. Busy locks, permission errors, disk-full errors, storage
outages and failures after recovery has begun do not fall back. An existing
recovery record requires supported storage; it cannot be bypassed. Without
recovery, review prior orders before retrying. Failure reporting, database
acknowledgement and preservation of failed results remain active in both paths.

An order marked failed can already contain partial OMERO registrations. Retrying
does not automatically submit a replacement: review its order UUID, importer log
and destination objects first. Interrupted preparation (renaming or destination
creation), unreadable recovery records, changed settings and older staged imports
with orders but no recovery record also require review. Preserve the record and
files; deleting the record to bypass the check can create duplicate imports.
Historical imports are inspected only when explicitly retried.
This protects image registration; it does not make optional ROI creation and
attachment postprocessing exactly-once operations.

Use an importer release that checks database write acknowledgement and recovers
interrupted shallow transactions before reading a store. These recovery changes
support v2 and v3 and do not change canonical-store format or identity policy.

## Development

Tests live on the separate
[test-suite branch](https://github.com/NL-BioImaging/biomero-scripts/tree/test-suite),
so they are not exposed as scripts by OMERO. CI runs that harness against the
pull request's source revision. See its README for local test instructions.

## License

See [LICENSE](LICENSE). These scripts build on OME's copyleft-licensed scripts.

# About #
This section provides machine-readable information about your scripts.
It will be used to help generate a landing page and links for your work.
Please modify **all** values on **each** branch to describe your scripts.

###### Repository name ######
BIOMERO.scripts repository

###### Minimum version ######
5.6

###### Maximum version ######
5.6

###### Owner(s) ######
T.T. Luik


###### Institution ######
Amsterdam UMC

###### URL ######
https://nl-bioimaging.github.io/biomero/

###### Email ######
t.t.luik@amsterdamumc.nl

###### Description ######
These scripts are to be used with the [BIOMERO library](https://github.com/NL-BioImaging/biomero).

They show how to use the library to run workflows directly from OMERO on a Slurm cluster.
