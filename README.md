OMERO Slurm Scripts
==================

These scripts are to be used with the [Omero Slurm Client library](https://github.com/NL-BioImaging/omero-slurm-client).

They show how to use the library to run workflows directly from Omero on a Slurm cluster.

Installation
------------

1. Change into the scripts location of your OMERO installation

        cd OMERO_DIST/lib/scripts

2. Clone the repository with a unique name (e.g. "slurm")

         git clone https://github.com/NL-BioImaging/omero-slurm-scripts.git slurm

3. Update your list of installed scripts by examining the list of scripts
   in OMERO.insight or OMERO.web, or by running the following command

        path/to/bin/omero script list

Upgrading
---------

1. Change into the repository location cloned into during installation

        cd OMERO_DIST/lib/scripts/UNIQUE_NAME

2. Update the repository to the latest version

        git pull --rebase

3. Update your list of installed scripts by examining the list of scripts
   in OMERO.insight or OMERO.web, or by running the following command

        path/to/bin/omero script list

Use the Omero Slurm scripts
-----

We have provided example Omero scripts of how to use the [Omero Slurm Client](https://github.com/NL-BioImaging/omero-slurm-client). These scripts do not work without installing that client on your Omero servers/processors.


For example, [workflows/Slurm Run Workflow](https://github.com/NL-BioImaging/omero-slurm-scripts/blob/master/workflows/SLURM_Run_Workflow.py) should provide an easy way to send data to Slurm, run the configured and chosen workflow, poll Slurm until jobs are done (or errors) and retrieve the results when the job is done. This workflow script uses some of the other scripts, like

-  [`data/Slurm Image Transfer`](https://github.com/NL-BioImaging/omero-slurm-scripts/blob/master/data/_SLURM_Image_Transfer.py): to export your selected images / dataset / screen as TIFF files to a Slurm dir.
- [`data/Slurm Get Results`](https://github.com/NL-BioImaging/omero-slurm-scripts/blob/master/data/SLURM_Get_Results.py): to import your Slurm job results back into Omero as a zip, dataset or attachment.

Other example Omero scripts are:
- [`data/Slurm Get Update`](https://github.com/NL-BioImaging/omero-slurm-scripts/blob/master/data/SLURM_Get_Update.py): to run while you are waiting on a job to finish on Slurm; it will try to get a `%` progress from your job's logfile. Depends on your job/workflow logging a `%` of course.

- [`workflows/Slurm Run Workflow Batched`](https://github.com/NL-BioImaging/omero-slurm-scripts/blob/master/workflows/SLURM_Run_Workflow_Batched.py): This will allow you to run several `workflows/Slurm Run Workflow` in parallel, by batching your input images into smaller chunks (e.g. turn 64 images into 2 batches of 32 images each). It will then poll all these jobs.

- [`workflows/Slurm CellPose Segmentation`](https://github.com/NL-BioImaging/omero-slurm-scripts/blob/master/workflows/SLURM_CellPose_Segmentation.py): This is a more primitive script that only runs the actual workflow `CellPose` (if correctly configured). You will need to manually transfer data first (with `Slurm Image Transfer`) and manually retrieve data afterward (with `Slurm Get Results`).


Legal
-----

See [LICENSE](LICENSE). Note this is copy-left, as we copied from OME with copy-left.


# About #
This section provides machine-readable information about your scripts.
It will be used to help generate a landing page and links for your work.
Please modify **all** values on **each** branch to describe your scripts.

###### Repository name ######
OMERO Slurm Scripts repository

###### Minimum version ######
5.6

###### Maximum version ######
5.6

###### Owner(s) ######
T.T. Luik


###### Institution ######
Amsterdam UMC

###### URL ######
https://nl-bioimaging.github.io/omero-slurm-client/

###### Email ######
t.t.luik@amsterdamumc.nl

###### Description ######
These scripts are to be used with the [Omero Slurm Client library](https://github.com/NL-BioImaging/omero-slurm-client).

They show how to use the library to run workflows directly from Omero on a Slurm cluster.
