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
from omero.rtypes import rstring, robject, wrap, unwrap
from omero.constants.namespaces import NSCREATED
from biomero import SlurmClient, constants
import logging
import os
import sys

logger = logging.getLogger(__name__)

CONV_OPTIONS_SOURCE = ['zarr']
CONV_OPTIONS_TARGET = ['tiff', 'zarr']

# Version constant for easy version management
VERSION = "2.8.0"


def resolve_log_fallback_target(client, conn):
    """Resolve a container to link the conversion log to.

    Reads the ``LOG_FALLBACK_TARGET`` parameter (a ``"DataType:id"`` string
    forwarded by SLURM_Run_Workflow, e.g. ``"Dataset:42"`` or ``"Plate:7``).
    When run standalone the user can type e.g. ``Dataset:42`` in the UI field.

    Args:
        client: OMERO script client for parameter access.
        conn: OMERO BlitzGateway connection.

    Returns:
        list: A list with one OMERO container object, or empty list if nothing
            could be resolved.
    """
    raw = unwrap(client.getInput(constants.results.LOG_FALLBACK_TARGET))
    if raw and ":" in str(raw):
        data_type, _, obj_id = str(raw).partition(":")
        data_type = data_type.strip()
        obj_id = obj_id.strip()
        if data_type and obj_id:
            try:
                obj = conn.getObject(data_type, obj_id)
                if obj is not None:
                    logger.info(
                        f"Conversion log fallback target: {data_type}:{obj_id}")
                    return [obj]
                logger.warning(
                    f"Conversion log fallback target {data_type}:{obj_id} "
                    f"not found in OMERO")
            except Exception as e:
                logger.warning(
                    f"Could not load conversion log fallback target '{raw}': {e}")
    return []


def upload_conversion_log_to_omero(client, conn, slurmClient, slurmJob, wf_id,
                                   targets=None):
    """Fetch the conversion job log from Slurm and attach it to OMERO.

    The conversion runs as a Slurm array job that writes ``omero-<jobid>_*.log``
    (one per task). We combine those into a single file and attach it to OMERO
    as a file annotation, exposing a download URL on the script output.

    The annotation is linked to every container in ``targets`` so it is
    findable via the normal OMERO search / activity panel. When ``targets``
    is empty the annotation is uploaded but unlinked (orphaned).

    This is best-effort and must NEVER raise: it runs on both the success and
    the failure path. Especially on failure the conversion log is the only clue
    we can show the user, so we always try to get it into OMERO before any
    cleanup removes it from Slurm.

    Args:
        client: OMERO script client (for setOutput).
        conn: OMERO BlitzGateway connection.
        slurmClient: Active SLURM client.
        slurmJob: The conversion SlurmJob (carries job_id and log_file glob).
        wf_id: Workflow UUID for the description.
        targets: Optional list of OMERO container objects to link the log to.
            Forwarded from :func:`resolve_log_fallback_target`.

    Returns:
        str: A short status string to append to the script message.
    """
    try:
        job_id = slurmJob.job_id
        if job_id is None or int(job_id) < 0:
            # Job never made it onto Slurm; there is no remote log to fetch.
            logger.warning(
                "No valid Slurm job id for conversion; skipping log upload")
            return " (no conversion log: job not submitted)"

        _, local_path, _ = slurmClient.get_conversion_logfile_from_slurm(
            job_id, logfile=slurmJob.log_file)

        mimetype = "text/plain"
        namespace = NSCREATED + "/SLURM/SLURM_REMOTE_CONVERSION"
        description = f"Conversion log from SLURM job {job_id}"
        if wf_id:
            description += f" (Workflow {wf_id})"

        annotation = conn.createFileAnnfromLocalFile(
            local_path, mimetype=mimetype, ns=namespace, desc=description)

        # Link to every resolved container so the log is findable in OMERO.
        for target in (targets or []):
            try:
                target.linkAnnotation(annotation)
            except Exception as link_e:
                logger.warning(
                    f"Could not link conversion log to {target}: {link_e}")

        # Build a download URL so the log is reachable from the activity panel.
        obj_id = annotation.getFile().getId()
        try:
            config = conn.getConfigService()
            web_host = (config.getConfigValue(
                "omero.client.web.host") or "").rstrip("/")
        except Exception:
            web_host = ""
        url = f"{web_host}/webclient/get_original_file/{obj_id}/"

        client.setOutput("Conversion_Log", robject(annotation._obj))
        client.setOutput("URL", wrap({"type": "URL", "href": url}))
        logger.info(
            f"Uploaded conversion log {local_path} to OMERO (file {obj_id}), "
            f"linked to {len(targets or [])} container(s)")
        return f" Uploaded conversion log (SLURM job {job_id})."
    except Exception as e:
        # Never let log upload break the conversion result handling.
        logger.warning(f"Failed to upload conversion log to OMERO: {e}")
        return f" (failed to upload conversion log: {e})"


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
            scripts.String(constants.conversion.INPUT_DATA, grouping="01",
                           description=name_descr,
                           values=_datafiles),
            scripts.String(constants.conversion.SOURCE_FORMAT, grouping="02.1",
                           description=conversion_descr,
                           values=CONV_OPTIONS_SOURCE,
                           default='zarr'),
            scripts.String(constants.conversion.TARGET_FORMAT, grouping="02.2",
                           description=conversion_descr,
                           values=CONV_OPTIONS_TARGET,
                           default='tiff'),
            scripts.Bool(constants.CLEANUP, grouping="03",
                         description=cleanup_descr,
                         default=True),
            scripts.String(constants.conversion.PARENT_WORKFLOW_ID, grouping="04",
                           description="Internal parameter for parent wf",
                           optional=True),
            scripts.String(constants.results.LOG_FALLBACK_TARGET, grouping="05",
                           description="Container to attach the conversion log to: \"DataType:id\" "
                                       "e.g. \"Dataset:42\" or \"Plate:7\". "
                                       "Forwarded automatically by Slurm_Run_Workflow. "
                                       "When running standalone, enter the container where you "
                                       "want the conversion log to be findable in OMERO.",
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

            zipfile = scriptParams[constants.conversion.INPUT_DATA]
            convert_from = scriptParams[constants.conversion.SOURCE_FORMAT]
            convert_to = scriptParams[constants.conversion.TARGET_FORMAT]
            cleanup = scriptParams[constants.CLEANUP]
            parent_wf_id = scriptParams.get(constants.conversion.PARENT_WORKFLOW_ID)

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
                    log_targets = resolve_log_fallback_target(client, conn)
                    if not slurmJob.ok:
                        logger.error(
                            f"Error converting data: {slurmJob.get_error()}")
                        # Upload whatever log exists so the user can see why
                        # submission failed (best-effort, never raises).
                        message += upload_conversion_log_to_omero(
                            client, conn, slurmClient, slurmJob, wf_id,
                            targets=log_targets)
                        slurmClient.workflowTracker.fail_task(
                            slurmJob.task_id,
                            "Conversion job submission failed")
                        # Only fail workflow if running standalone
                        if not is_subtask:
                            slurmClient.workflowTracker.fail_workflow(
                                wf_id, "Conversion job submission failed")
                        raise Exception(
                            f"Conversion job submission failed: "
                            f"{slurmJob.get_error()}")
                    else:
                        slurmJob.wait_for_completion(slurmClient, conn)
                        # Always pull the conversion log into OMERO BEFORE any
                        # cleanup removes it from Slurm - especially important
                        # when the conversion failed.
                        message += upload_conversion_log_to_omero(
                            client, conn, slurmClient, slurmJob, wf_id,
                            targets=log_targets)
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
                error_msg = f" ERROR WITH CONVERTING DATA: {e}"
                message += error_msg
                logger.error(
                    f"Conversion script exception: {e} | Full error: {message}")

                # Only fail workflow if running standalone
                if not is_subtask:
                    slurmClient.workflowTracker.fail_workflow(wf_id, str(e))

                # Always set output message with error for parent detection
                client.setOutput("Message", rstring(str(message)))
                raise e

            # Set successful output message
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

    # Silence some of the DEBUG - Extended for cleaner BIOMERO logs
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('omero.gateway').setLevel(
        logging.WARNING)  # Silences proxy creation spam
    logging.getLogger('omero.client').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)
    logging.getLogger('paramiko.sftp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('requests_cache').setLevel(logging.WARNING)  # Cache logs
    logging.getLogger('requests-cache').setLevel(logging.WARNING)  # Alt naming
    logging.getLogger('requests_cache.core').setLevel(
        logging.WARNING)  # Core module
    logging.getLogger('requests_cache.backends').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.base').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.sqlite').setLevel(
        logging.WARNING)
    logging.getLogger('requests_cache.policy').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.policy.actions').setLevel(
        logging.WARNING)
    logging.getLogger('invoke').setLevel(logging.WARNING)
    logging.getLogger('fabric').setLevel(logging.WARNING)  # SSH operations
    logging.getLogger('Ice').setLevel(logging.ERROR)
    logging.getLogger('ZeroC').setLevel(logging.ERROR)

    runScript()
