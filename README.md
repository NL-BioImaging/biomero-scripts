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
