BIOMERO Scripts
==================

These scripts are to be used within BIOMERO (an extension to OMERO), together with the [BIOMERO library](https://github.com/NL-BioImaging/biomero).

Together, BIOMERO allows you to run BioImage analysis workflows directly from OMERO on a Slurm cluster, through SSH.

!!*NOTE*: Do not install [Example Minimal Slurm Script](https://github.com/NL-BioImaging/biomero-scripts/blob/master/Example_Minimal_Slurm_Script.py) if you do not trust your users with your Slurm cluster. It has literal Command Injection for the SSH user as a **FEATURE**. 


# Overview

In the figure below we show our **BIOMERO** framework, for **B**io**I**mage analysis in **OMERO**. 

BIOMERO consists of the Python library [BIOMERO](https://github.com/NL-BioImaging/biomero) and the integrations within OMERO through the scripts in this repository.

![OMERO-Figure1_Overview_v5](https://github.com/NL-BioImaging/biomero/assets/68958516/ff437ed2-d4b7-48b4-a7e3-12f1dbf00981)

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

Use the BIOMERO scripts
-----

This repository provides example OMERO scripts for using [BIOMERO](https://github.com/NL-BioImaging/biomero). These scripts do not work without installing that client on your OMERO servers/processors that will run these scripts.

Always start with initiating the Slurm environment at least once, for example using [init/Slurm Init environment](https://github.com/NL-BioImaging/biomero-scripts/blob/master/init/SLURM_Init_environment.py). This might take a while to download all container images if you configured a lot.

For example, [workflows/Slurm Run Workflow](https://github.com/NL-BioImaging/biomero-scripts/blob/master/workflows/SLURM_Run_Workflow.py) should provide an easy way to send data to Slurm, run the configured and chosen workflow, poll Slurm until jobs are done (or errors) and retrieve the results when the job is done. This workflow script uses some of the other scripts, like

-  [`data/Slurm Image Transfer`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/data/_SLURM_Image_Transfer.py): to export your selected images / dataset / screen as ZARR files to a Slurm dir.
- [`data/Slurm Get Results`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/data/SLURM_Get_Results.py): to import your Slurm job results back into OMERO as a zip, dataset or attachment.

Other example OMERO scripts are:
- [`data/Slurm Get Update`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/data/SLURM_Get_Update.py): to run while you are waiting on a job to finish on Slurm; it will try to get a `%` progress from your job's logfile. Depends on your job/workflow logging a `%` of course.

- [`workflows/Slurm Run Workflow Batched`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/workflows/SLURM_Run_Workflow_Batched.py): This will allow you to run several `workflows/Slurm Run Workflow` in parallel, by batching your input images into smaller chunks (e.g. turn 64 images into 2 batches of 32 images each). It will then poll all these jobs.

- [`workflows/Slurm CellPose Segmentation`](https://github.com/NL-BioImaging/biomero-scripts/blob/master/workflows/SLURM_CellPose_Segmentation.py): This is a more primitive script that only runs the actual workflow `CellPose` (if correctly configured). You will need to manually transfer data first (with `Slurm Image Transfer`) and manually retrieve data afterward (with `Slurm Get Results`).

Enable logging (of the BIOMERO library)
-----

Note that you can just enable more logging of your OMERO scripts (including the BIOMERO library) by changing the logger in the __init__ of your scripts:

```Python
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        stream=sys.stdout)
    runScript()
```

You can even turn on logging.DEBUG.


Legal
-----

See [LICENSE](LICENSE). Note this is copy-left, as we copied from OME's scripts with copy-left license.


# About #
This section provides machine-readable information about your scripts.
It will be used to help generate a landing page and links for your work.
Please modify **all** values on **each** branch to describe your scripts.

###### Repository name ######
BIOMERO Scripts repository

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
