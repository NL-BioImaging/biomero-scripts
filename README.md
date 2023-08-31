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
