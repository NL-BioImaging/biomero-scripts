#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2024 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt

"""
BIOMERO SLURM Remote Data Conversion Script

This script provides intelligent data format conversion on SLURM clusters
with optimization for same-format operations (no-op conversion).

Key Features:
- Support for ZARR and TIFF format conversion
- Smart no-op logic: skips conversion when source equals target format
- Integration with BIOMERO workflow tracking system
- Automatic cleanup of temporary files
- Robust error handling and logging

The script is typically called automatically by SLURM_Run_Workflow.py
but can be used standalone for specific conversion needs.

Supported Conversions:
- ZARR → TIFF (full conversion using bioformats2raw + raw2ometiff)
- ZARR → ZARR (no-op, data unzipped only)
- TIFF → TIFF (no-op, data unzipped only)

Authors: Torec Luik, OMERO Team
Institution: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

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
CONV_OPTIONS_TARGET = ['tiff', 'zarr']
INPUT_DATA = "Input data"
SOURCE = "Source format"
TARGET = "Target format"
CLEANUP = "Cleanup?"

# Version constant for easy version management
VERSION = "2.0.0-alpha.8"


def runScript():
    """
    Main entry point for SLURM remote data conversion script.
    
    This function orchestrates data format conversion on SLURM clusters
    with intelligent optimization for same-format operations:
    
    1. Validates available data files and conversion options
    2. Sets up OMERO script parameters for user input
    3. Checks if conversion is needed (source != target format)
    4. If conversion needed: submits conversion job to SLURM
    5. If no conversion needed: performs unzip-only operation
    6. Updates workflow tracking with results
    
    The script automatically handles:
    - ZARR to TIFF conversion using bioformats tools
    - No-op operations for same-format requests (optimization)
    - Workflow task tracking and status updates
    - Error handling and cleanup
    
    Raises:
        Exception: Various exceptions during conversion process,
                  all tracked in workflow status
    """
    with SlurmClient.from_config() as slurmClient:
        name_descr = f"Name of folder where images are stored, as provided\
            with {constants.IMAGE_EXPORT_SCRIPT}"
        conversion_descr = "Convert from X to Y"
        cleanup_descr = "Cleanup logfile (default) or not? Turn off for debugging."
        _, _datafiles = slurmClient.get_image_versions_and_data_files(
            'cellpose')
        script_name = 'SLURM Remote Conversion'
        script_descr = f'''Use Slurm to convert data on your remote slurm cluster.
            By default BIOMERO only supplies ZARR to TIFF conversion.
            1. First transfer data (as ZARR) to Slurm.
            2. Second, run this script to convert to TIFF.
            3. Third, run some workflow that works with TIFF input data, like cellpose.
            **NOTE!** This step is normally handeled automatically by Slurm_Run_Workflow.
            Only use these modular scripts if you have a good reason to do so.
            Connection ready? << {slurmClient.validate()} >>
            '''

        script_version = VERSION
        client = scripts.client(
            script_name,
            script_descr,
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
            scripts.Bool(CLEANUP, grouping="03",
                         description=cleanup_descr,
                         default=True),
            scripts.String("Parent_Workflow_ID", grouping="04",
                           description="Internal parameter for parent wf",
                           optional=True),
            namespaces=[omero.constants.namespaces.NSDYNAMIC],
            version=script_version,
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
            cleanup = scriptParams[CLEANUP]
            parent_wf_id = scriptParams.get("Parent_Workflow_ID")

            # Connect to Omero
            conn = BlitzGateway(client_obj=client)
            user = conn.getUserId()
            group = conn.getGroupFromContext().id
            
            # Check if running as part of parent workflow or standalone
            is_subtask = parent_wf_id is not None
            if is_subtask:
                # Running as part of parent workflow - don't create new wf
                wf_id = parent_wf_id
                logger.info(f"Running as subtask of workflow: {wf_id}")
            else:
                # Running standalone - create new workflow
                wf_id = slurmClient.workflowTracker.initiate_workflow(
                    script_name,
                    "\n".join([script_descr, script_version]),
                    user,
                    group
                )
                logger.info(f"Running as standalone workflow: {wf_id}")
            try:
                # Check if conversion is needed (no-op if source == target)
                if convert_from == convert_to:
                    msg = f"No conversion needed: {zipfile} already in " \
                          f"{convert_to} format"
                    logger.info(msg)
                    message += msg
                    # Only complete workflow if running standalone
                    if not is_subtask:
                        slurmClient.workflowTracker.complete_workflow(wf_id)
                else:
                    slurmJob = slurmClient.run_conversion_workflow_job(
                        zipfile, convert_from, convert_to, wf_id)
                    logger.info(f"Conversion job submitted: {slurmJob}")
                    if not slurmJob.ok:
                        logger.error(
                            f"Error converting data: {slurmJob.get_error()}")
                        # Only fail workflow if running standalone
                        if not is_subtask:
                            slurmClient.workflowTracker.fail_workflow(
                                wf_id, "Conversion job submission failed")
                    else:
                        slurmJob.wait_for_completion(slurmClient, conn)
                        if not slurmJob.completed():
                            log_msg = f"Conversion is not completed: " \
                                      f"{slurmJob}"
                            slurmClient.workflowTracker.fail_task(
                                slurmJob.task_id, "Conversion failed")
                            # Only fail workflow if running standalone
                            if not is_subtask:
                                slurmClient.workflowTracker.fail_workflow(
                                    wf_id, "Conversion failed")
                            raise Exception(log_msg)
                        else:
                            if cleanup:
                                slurmJob.cleanup(slurmClient)
                            msg = f"Converted {zipfile} from {convert_from} " \
                                  f"to {convert_to}"
                            logger.info(msg)
                            message += msg
                            slurmClient.workflowTracker.complete_task(
                                slurmJob.task_id, msg)
                            # Only complete workflow if running standalone
                            if not is_subtask:
                                slurmClient.workflowTracker.complete_workflow(
                                    wf_id)
            except Exception as e:
                message += f" ERROR WITH CONVERTING DATA: {e}"
                logger.error(message)
                # Only fail workflow if running standalone
                if not is_subtask:
                    slurmClient.workflowTracker.fail_workflow(wf_id, str(e))
                raise e

            client.setOutput("Message", rstring(str(message)))
            # slurmClient.workflowTracker.complete_workflow(wf_id)
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
