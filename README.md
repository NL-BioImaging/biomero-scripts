BIOMERO.scripts
==================

These scripts provide a comprehensive OMERO integration for running bioimage analysis workflows on SLURM clusters. 

### Key Features
- Multi-format support: TIFF, OME-TIFF, and ZARR
- Automatic data export from OMERO to SLURM clusters
- Intelligent format conversion with optimization
- Comprehensive workflow tracking and monitoring
- Automatic result import back to OMERO
- Configurable output organization options

These scripts work together with the [BIOMERO library](https://github.com/NL-BioImaging/biomero) to enable seamless bioimage analysis workflows directly from OMERO.

## Deploy with NL-BIOMERO

For the easiest deployment and integration with other FAIR infrastructure, consider using the NL-BIOMERO stack:

- **NL-BIOMERO deployment repo**: https://github.com/Cellular-Imaging-Amsterdam-UMC/NL-BIOMERO
- **OMERO.biomero OMERO.web plugin**: https://github.com/Cellular-Imaging-Amsterdam-UMC/OMERO.biomero
- **Pre-built BIOMERO processor container**: https://hub.docker.com/r/cellularimagingcf/biomero

The NL-BIOMERO stack provides Docker Compose configurations that automatically set up OMERO.web with the OMERO.biomero plugin, databases, and all necessary dependencies.

# Overview

In the figure below we show our **BIOMERO** framework, for **B**io**I**mage analysis in **OMERO**. 

BIOMERO consists of the Python library [BIOMERO](https://github.com/NL-BioImaging/biomero) and the integrations within OMERO through the scripts in this repository.

![OMERO-Figure1_Overview_v5](https://github.com/NL-BioImaging/biomero/assets/68958516/ff437ed2-d4b7-48b4-a7e3-12f1dbf00981)

## BIOMERO 2.0 Web Interface

In addition to these command-line scripts, **BIOMERO 2.0** introduces a modern web-based user interface through the [OMERO.biomero](https://github.com/Cellular-Imaging-Amsterdam-UMC/OMERO.biomero) web plugin. This plugin provides:

- **Interactive Workflow Management**: Browse and launch workflows with a modern web interface
- **Real-time Progress Tracking**: Monitor job progress with live updates
- **Workflow History**: View past executions with full tracking and metadata  
- **Dashboard Overview**: Get an overview of all your workflows at a glance

For new users, we recommend the NL-BIOMERO stack with the web interface for the complete experience. These scripts remain fully supported for advanced users who need custom scripting capabilities.

## Script Architecture

### Main Workflow Scripts (`__workflows/`)
- **`SLURM_Run_Workflow.py`**: Primary workflow orchestrator with ZARR support
- **`SLURM_Run_Workflow_Batched.py`**: Batch processing variant for multiple datasets
- **`SLURM_CellPose_Segmentation.py`**: Specialized CellPose segmentation workflow

### Data Management Scripts (`_data/`)
- **`_SLURM_Image_Transfer.py`**: Export data from OMERO to SLURM (with cleanup)
- **`SLURM_Remote_Conversion.py`**: Intelligent format conversion on SLURM
- **`SLURM_Get_Results.py`**: Import workflow results back to OMERO
- **`SLURM_Get_Update.py`**: Monitor and update workflow status

### Administrative Scripts (`admin/`)
- **`SLURM_Init_environment.py`**: Initialize SLURM environment
- **`SLURM_check_setup.py`**: Validate BIOMERO configuration

### Workflow Process
1. **Export**: Selected data transferred from OMERO to SLURM cluster
2. **Convert**: Smart format conversion (with ZARR no-op optimization)
3. **Process**: Computational workflows executed on SLURM
4. **Monitor**: Job progress tracking and status updates
5. **Import**: Results imported back to OMERO with configurable organization
6. **Cleanup**: Temporary artifacts automatically removed

Installation
------------

1. Change into the scripts location of your OMERO installation

        cd /opt/omero/server/OMERO.server/lib/scripts/

2. Clone the repository with a unique name (e.g. "biomero")

         git clone https://github.com/NL-BioImaging/biomero-scripts.git biomero

3. Update your list of installed scripts by examining the list of scripts
   in OMERO.insight or OMERO.web, or by running the following command

        <path>/<to>/<bin>/omero script list

4. Install system requirements _on the_ **PROCESSOR** _nodes_:
    - `python3 -m pip install biomero ezomero==1.1.1 tifffile==2020.9.3 omero-metadata==0.12.0` 
    - the [OMERO CLI Zarr plugin](https://github.com/ome/omero-cli-zarr), e.g. 
    `python3 -m pip install omero-cli-zarr==0.5.3` && `yum install -y blosc-devel`
    - the [bioformats2raw-0.7.0](https://github.com/glencoesoftware/bioformats2raw/releases/download/v0.7.0/bioformats2raw-0.7.0.zip), e.g. `unzip -d /opt bioformats2raw-0.7.0.zip && export PATH="$PATH:/opt/bioformats2raw-0.7.0/bin"`

These examples work on Linux CentOS (i.e. the official OMERO containers); for Windows, or other Linux package managers, check with the original repositories (OMERO CLI ZARR and BioFormats2RAW) for more details on installation.

Requirements
---------

Just to reiterate, you need all these requirements installed to run all these scripts, on the OMERO  **PROCESSOR** node:

- Python libraries:
  - biomero (latest version, or at least matching the version number of this repository)
  - ezomero==1.1.1
  - tifffile==2020.9.3
  - omero-metadata==0.12.0
  - omero-cli-zarr==0.5.3 (see below)
- the [OMERO CLI Zarr plugin](https://github.com/ome/omero-cli-zarr), e.g. 
    `python3 -m pip install omero-cli-zarr==0.5.3` && `yum install -y blosc-devel`
- the [bioformats2raw-0.7.0](https://github.com/glencoesoftware/bioformats2raw/releases/download/v0.7.0/bioformats2raw-0.7.0.zip), e.g. `unzip -d /opt bioformats2raw-0.7.0.zip && export PATH="$PATH:/opt/bioformats2raw-0.7.0/bin"`


Upgrading
---------

1. Change into the repository location cloned into during installation

        cd /opt/omero/server/OMERO.server/lib/scripts/<UNIQUE_NAME>

2. Update the repository to the latest version

        git pull --rebase

3. Update your list of installed scripts by examining the list of scripts
   in OMERO.insight or OMERO.web, or by running the following command

        <path>/<to>/<bin>/omero script list

Use the BIOMERO.scripts
-----

This repository provides example OMERO scripts for using [BIOMERO](https://github.com/NL-BioImaging/biomero). These scripts do not work without installing that client on your OMERO servers/processors that will run these scripts.

Always start with initiating the Slurm environment at least once, for example using [admin/SLURM Init environment](https://github.com/NL-BioImaging/biomero-scripts/blob/master/admin/SLURM_Init_environment.py). This might take a while to download all container images if you configured a lot.

For example, [__workflows/SLURM Run Workflow](https://github.com/NL-BioImaging/biomero-scripts/blob/master/__workflows/SLURM_Run_Workflow.py) should provide an easy way to send data to Slurm, run the configured and chosen workflow, poll Slurm until jobs are done (or errors) and retrieve the results when the job is done. This workflow script uses some of the other scripts, like

-  [`_data/SLURM Image Transfer`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/_data/_SLURM_Image_Transfer.py): to export your selected images / dataset / screen as ZARR files to a Slurm dir.
- [`_data/SLURM Get Results`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/_data/SLURM_Get_Results.py): to import your Slurm job results back into OMERO as a zip, dataset or attachment.

Other example OMERO scripts are:
- [`_data/SLURM Get Update`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/_data/SLURM_Get_Update.py): to run while you are waiting on a job to finish on Slurm; it will try to get a `%` progress from your job's logfile. Depends on your job/workflow logging a `%` of course.

- [`__workflows/SLURM Run Workflow Batched`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/__workflows/SLURM_Run_Workflow_Batched.py): This will allow you to run several `__workflows/SLURM Run Workflow` in parallel, by batching your input images into smaller chunks (e.g. turn 64 images into 2 batches of 32 images each). It will then poll all these jobs.

- [`__workflows/SLURM CellPose Segmentation`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/__workflows/SLURM_CellPose_Segmentation.py): This is a more primitive script that only runs the actual workflow `CellPose` (if correctly configured). You will need to manually transfer data first (with `_data/SLURM Image Transfer`) and manually retrieve data afterward (with `_data/SLURM Get Results`).

Logging Configuration
-----

**BIOMERO.scripts already have comprehensive DEBUG logging enabled by default!** All scripts are configured with:

- **DEBUG level logging** to rotating log files (`biomero.log` in `/opt/omero/server/OMERO.server/var/log/`)
- **INFO level logging** to stdout (visible in OMERO.web script output)
- **Rotating log files** (500MB max, 9 backups) to prevent disk space issues
- **Pre-silenced verbose libraries** (omero.gateway.utils, paramiko.transport, invoke) at WARNING level

### Current Logging Setup

Each script automatically configures logging like this:

```Python
if __name__ == '__main__':
    # Comprehensive DEBUG logging to rotating biomero.log file
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)  # Only INFO+ to stdout
    logging.basicConfig(level=logging.DEBUG,  # Full DEBUG to file
                        format="%(asctime)s %(levelname)-5.5s [%(name)40s] "
                               "[%(process)d] (%(threadName)-10s) %(message)s",
                        handlers=[
                            stream_handler,
                            logging.handlers.RotatingFileHandler(
                                os.path.join(LOGDIR, 'biomero.log'),
                                maxBytes=500000000, backupCount=9)
                        ])

    # Silence verbose libraries
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)
    logging.getLogger('invoke').setLevel(logging.WARNING)
    
    runScript()
```

### Reducing Log Verbosity (If Needed)

If the default DEBUG logging is too verbose, you can modify any script to use less logging:

```Python
# Change DEBUG to INFO for less verbose logging
logging.basicConfig(level=logging.INFO, ...)

# Or silence additional libraries
logging.getLogger('biomero').setLevel(logging.INFO)
logging.getLogger('fabric').setLevel(logging.WARNING)
```

### Log File Locations

- **Main logs**: `/opt/omero/server/OMERO.server/var/log/biomero.log*`
- **OMERO logs**: Standard OMERO logging locations
- **Rotation**: Logs rotate when reaching 500MB, keeping 9 backups


Legal
-----

See [LICENSE](LICENSE). Note this is copy-left, as we copied from OME's scripts with copy-left license.


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

## Usage Examples

### Running a Standard Workflow (TIFF)
1. Select your images, datasets, or plates in OMERO
2. Run the **SLURM Run Workflow** script
3. Choose your desired workflow (e.g., CellPose, StarDist)
4. Configure workflow parameters
5. Select output organization options
6. Execute - data will be automatically exported, processed, and imported back

### Using ZARR Format (New in v2.0.0-alpha.7)
1. Select your data in OMERO
2. Run the **SLURM Run Workflow** script
3. **✅ Check "Use ZARR Format"** for workflows that support native ZARR input
4. Choose your ZARR-compatible workflow
5. Configure parameters and output options
6. Execute - conversion step will be skipped for efficiency

### Manual Data Export
For advanced users who need custom processing:
1. Use **SLURM Image Transfer** to export data in your preferred format
2. Use **SLURM Remote Conversion** if format conversion is needed
3. Process data using custom workflows on SLURM
4. Use **SLURM Get Results** to import results back to OMERO

### Monitoring and Debugging
- **SLURM Check Setup**: Validate your BIOMERO configuration
- **SLURM Get Update**: Monitor job progress and retrieve logs
- **SLURM Init Environment**: Initialize or update SLURM environment
