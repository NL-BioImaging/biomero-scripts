#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2024 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# Example OMERO.script to convert remote data on Slurm

import omero
import omero.gateway
from omero import scripts
from omero.gateway import BlitzGateway
from omero.rtypes import rstring
from biomero import SlurmClient, constants
import logging
import os
import sys

logger = logging.getLogger(__name__)

CONV_OPTIONS_SOURCE = ['zarr']
CONV_OPTIONS_TARGET = ['tiff']
INPUT_DATA = "Input data"
SOURCE = "Source format"
TARGET = "Target format"


def runScript():
    """
    The main entry point of the script
    """
    with SlurmClient.from_config() as slurmClient:
        name_descr = f"Name of folder where images are stored, as provided\
            with {constants.IMAGE_EXPORT_SCRIPT}"
        conversion_descr = "Convert from X to Y"
        _, _datafiles = slurmClient.get_image_versions_and_data_files(
            'cellpose')
        client = scripts.client(
            'SLURM Remote Conversion',
            f'''Use Slurm to convert data on your remote slurm cluster.
            
            By default BIOMERO only supplies ZARR to TIFF conversion.
            
            1. First transfer data (as ZARR) to Slurm.
            
            2. Second, run this script to convert to TIFF.
            
            3. Third, run some workflow that works with TIFF input data, like cellpose.
            
            **NOTE!** This step is normally handeled automatically by Slurm_Run_Workflow.
            Only use these modular scripts if you have a good reason to do so.
            
            Connection ready? << {slurmClient.validate()} >>
            ''',
            scripts.String(INPUT_DATA, grouping="01",
                           description=name_descr,
                           values=_datafiles),
            scripts.String(SOURCE, grouping="02.1",
                           description=conversion_descr,
                           values=CONV_OPTIONS_SOURCE,
                           default='zarr'),
            scripts.String(TARGET, grouping="02.2",
                           description=conversion_descr,
                           values=CONV_OPTIONS_TARGET,
                           default='tiff'),            
            namespaces=[omero.constants.namespaces.NSDYNAMIC],
            version="1.9.1",
            authors=["Torec Luik"],
            institutions=["Amsterdam UMC"],
            contact='cellularimaging@amsterdamumc.nl',
            authorsInstitutions=[[1]]
        )

        try:
            scriptParams = client.getInputs(unwrap=True)

            message = ""
            logger.info(f"Converting: {scriptParams}\n")

            zipfile = scriptParams[INPUT_DATA]
            convert_from = scriptParams[SOURCE]
            convert_to = scriptParams[TARGET]
            slurmJob = slurmClient.run_conversion_workflow_job(
                    zipfile, convert_from, convert_to)
            logger.info(f"Conversion job submitted: {slurmJob}")
            if not slurmJob.ok:
                logger.warning(f"Error converting data: {slurmJob.get_error()}")
            else:
                try:
                    conn = BlitzGateway(client_obj=client)
                    slurmJob.wait_for_completion(slurmClient, conn)
                    if not slurmJob.completed():
                        raise Exception(
                            f"Conversion is not completed: {slurmJob}")
                    else:
                        slurmJob.cleanup(slurmClient)
                        msg = f"Converted {zipfile} from {convert_from} to {convert_to}"
                        logger.info(msg)
                        message += msg
                except Exception as e:
                    message += f" ERROR WITH CONVERTING DATA: {e}"
                    raise e

            client.setOutput("Message", rstring(str(message)))

        finally:
            client.closeSession()


if __name__ == '__main__':
    # Some defaults from OMERO; don't feel like reading ice files.
    # Retrieve the value of the OMERODIR environment variable
    OMERODIR = os.environ.get('OMERODIR', '/opt/omero/server/OMERO.server')
    LOGDIR = os.path.join(OMERODIR, 'var', 'log')
    LOGFORMAT = "%(asctime)s %(levelname)-5.5s [%(name)40s] " \
                "[%(process)d] (%(threadName)-10s) %(message)s"
    # Added the process id
    LOGSIZE = 500000000
    LOGNUM = 9
    log_filename = 'biomero.log'
    # Create a stream handler with INFO level (for OMERO.web output)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    # Create DEBUG logging to rotating logfile at var/log
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGFORMAT,
                        handlers=[
                            stream_handler,
                            logging.handlers.RotatingFileHandler(
                                os.path.join(LOGDIR, log_filename),
                                maxBytes=LOGSIZE,
                                backupCount=LOGNUM)
                        ])
       
    # Silence some of the DEBUG
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)

    runScript()
