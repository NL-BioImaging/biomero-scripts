#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt

"""
BIOMERO SLURM Workflow Runner.

This script provides a comprehensive OMERO interface for running computational
workflows on SLURM clusters. It supports both traditional TIFF and modern ZARR
data formats, with automatic data transfer, conversion, and result import.

The script orchestrates the complete workflow lifecycle from data export
through result import, with intelligent optimization for ZARR format handling
and automatic cleanup of temporary artifacts.

Workflow Process:
    1. Export selected data (Images/Datasets/Plates) to SLURM cluster
    2. Convert data format if needed (with ZARR bypass option)
    3. Execute selected computational workflows on SLURM
    4. Monitor job completion and retrieve results
    5. Import results back to OMERO with specified organization
    6. Clean up temporary artifacts

Key Features:
    - Multi-format support: TIFF, OME-TIFF, and ZARR
    - Smart format conversion with no-op optimization
    - Configurable workflow execution with parameter validation
    - Comprehensive workflow tracking and logging
    - Automatic temporary file cleanup
    - Dynamic import script selection via IMPORTER_ENABLED environment variable

Import Script Selection:
    - If IMPORTER_ENABLED=true: Uses SLURM_Import_Results.py (biomero-importer integration)
    - If IMPORTER_ENABLED=false or unset: Uses SLURM_Get_Results.py (standard import)
    - Both scripts support the same API including workflow UUID tracking

Usage:
    Select data in OMERO, choose workflows and parameters, then run the script.
    Results are automatically imported based on selected output options and
    environment configuration.

Authors:
    Torec Luik (Amsterdam UMC)
    OMERO Team (University of Dundee)

License:
    GPL v2+ (see LICENSE.txt)
"""

from __future__ import print_function
import sys
import os
from uuid import UUID
import omero
from omero.grid import JobParams
from omero.rtypes import rstring, unwrap, rlong, rbool, rlist, robject, wrap
from omero.constants.namespaces import NSCREATED
from omero.sys import Parameters
from omero.gateway import BlitzGateway
import omero.scripts as omscripts
import datetime
from biomero import SlurmClient, constants
import logging
import os
import time as timesleep
from paramiko import SSHException

logger = logging.getLogger(__name__)

# Check if importer is enabled via environment variable
IMPORTER_ENABLED = os.getenv("IMPORTER_ENABLED", "false").lower() == "true"

EXPORT_SCRIPTS = [constants.IMAGE_EXPORT_SCRIPT]
# Dynamically choose import script based on IMPORTER_ENABLED
if IMPORTER_ENABLED:
    IMPORT_SCRIPTS = ["SLURM_Import_Results.py"]
    logger.info("Using SLURM_Import_Results.py (importer-enabled workflow)")
else:
    IMPORT_SCRIPTS = [constants.IMAGE_IMPORT_SCRIPT]
    logger.info("Using SLURM_Get_Results.py (standard workflow)")
CONVERSION_SCRIPTS = [constants.CONVERSION_SCRIPT]
DATATYPES = [rstring(constants.transfer.DATA_TYPE_DATASET),
             rstring(constants.transfer.DATA_TYPE_IMAGE),
             rstring(constants.transfer.DATA_TYPE_PLATE),
             rstring(constants.transfer.DATA_TYPE_SCREEN)]
OUTPUT_OPTIONS = [constants.workflow.OUTPUT_RENAME,
                  constants.workflow.OUTPUT_PARENT,
                  constants.workflow.OUTPUT_NEW_DATASET,
                  constants.workflow.OUTPUT_NEW_SCREEN,
                  constants.workflow.OUTPUT_ATTACH,
                  constants.workflow.OUTPUT_CSV_TABLE,
                  constants.workflow.OUTPUT_ATTACH_FILE_OUTPUTS]
VERSION = "2.8.0"


def validate_importer_write_access(slurmClient: SlurmClient, conn: BlitzGateway, client: omscripts.client) -> None:
    """
    Check if we have write access to importer storage before starting workflow.
    
    This performs an early validation to ensure we can write to the permanent
    storage location that will be needed if using the importer workflow. This
    allows us to fail fast with a clear error message before doing expensive
    SLURM operations.
    
    Args:
        slurmClient: Active SLURM client connection
        conn: OMERO BlitzGateway connection
        client: OMERO script client needed for script execution
        
    Raises:
        RuntimeError: If write access validation fails
    """
    if not IMPORTER_ENABLED:
        logger.info("Importer not enabled, skipping write access check")
        return
        
    logger.info("Validating importer write access before starting workflow")
    
    try:
        group_name = conn.getGroupFromContext().getName()
        
        # Call the import results script with write access check enabled
        svc = conn.getScriptService()
        scripts = svc.getScripts()
        
        # Find the import script
        script_matches = [(unwrap(s.id), unwrap(s.getVersion()), unwrap(s.getName()))
                         for s in scripts if unwrap(s.getName()) in IMPORT_SCRIPTS]
        
        if not script_matches:
            raise RuntimeError(f"Import script not found: {IMPORT_SCRIPTS}")
            
        script_id, script_version, script_name = script_matches[0]
        
        # Get a valid job ID from the list instead of using a dummy value
        # We need this because the script validates job IDs against completed jobs
        try:
            # Get list of completed jobs to find a valid job ID for validation
            dummy_job_id = "validation_check"
            
            # Try to get available job list from slurmClient
            if hasattr(slurmClient, 'list_completed_jobs'):
                completed_jobs_list = slurmClient.list_completed_jobs()
                if completed_jobs_list:
                    # Use the first available job ID for validation
                    dummy_job_id = completed_jobs_list[0]
                    logger.info(f"Using job ID '{dummy_job_id}' for write access validation")
                else:
                    logger.warning("No completed jobs found, using fallback job ID")
                    dummy_job_id = "1"  # Basic fallback
            
        except Exception as job_list_error:
            logger.warning(f"Could not get completed jobs list for validation: {job_list_error}")
            # If we can't get the job list, we'll have to skip this validation
            # or use a very basic fallback
            dummy_job_id = "1"
        
        # Prepare inputs for write access validation (DRY-RUN ONLY)
        inputs = {
            constants.results.TEST_WRITE_PERMISSIONS_ONLY: rbool(True),
            constants.results.OUTPUT_SLURM_JOB_ID: rstring(dummy_job_id),
            constants.results.OUTPUT_COMPLETED_JOB: rbool(True),
            constants.results.OUTPUT_ATTACH_PROJECT: rbool(False),
            constants.results.OUTPUT_ATTACH_DATASET: rbool(False),
            constants.results.OUTPUT_ATTACH_PLATE: rbool(False),
            constants.results.OUTPUT_ATTACH_OG_IMAGES: rbool(False),
            constants.results.OUTPUT_ATTACH_NEW_DATASET: rbool(False),
            constants.results.OUTPUT_ATTACH_NEW_SCREEN: rbool(False),
            constants.results.OUTPUT_ATTACH_TABLE: rbool(False)
        }
        
        logger.info(f"Calling {script_name} for write access validation with job ID: {dummy_job_id}")
        
        try:
            rv, job = runOMEROScript(client, svc, script_id, inputs)
            
            # Check if the script executed successfully
            if rv is None or job is None:
                raise RuntimeError("No response from write access validation script")
            
            # Get job status ID for logging
            job_status_id = None
            try:
                job_status = job.getStatus()
                job_status_id = unwrap(job_status.getId())
                logger.debug(f"Write access validation job status ID: {job_status_id}")
            except Exception as job_error:
                logger.warning(f"Could not get job status: {job_error}")
            
            # Get message content
            message = ""
            if 'Message' in rv:
                message = unwrap(rv['Message'])
                if message:
                    logger.info(f"Write access validation message: {message}")
            
            # Check for any failure indicators
            failure_indicators = [
                "Script execution failed:",
                "Permission denied", 
                "FAILED:",
                "RuntimeError:",
                "Exception:",
                "ERROR:"
            ]
            
            # Fail if: status indicates error (6=Error, 9=Cancelled) OR message contains failure indicators
            if (job_status_id in [6, 9] or 
                any(indicator in message for indicator in failure_indicators)):
                error_message = message or f"Script failed with status ID: {job_status_id}"
                raise RuntimeError(error_message)
            
            # If we got here without failures, treat as success
            logger.info("Write access validation completed successfully")
            return
                
        except RuntimeError:
            # Re-raise RuntimeError as-is (these are our validation failure messages)
            raise
        except Exception as script_error:
            # Handle unexpected script execution errors
            raise RuntimeError(f"Unexpected error during write access validation: {str(script_error)}")
            
    except Exception as e:
        # The validation worked, but found an issue that prevents workflow execution
        error_msg = f"Write access validation failed - workflow cannot proceed.\\n" + \
                   f"{str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def runScript():
    """Main entry point for the SLURM workflow execution script.

    Orchestrates the complete workflow lifecycle including SLURM client setup,
    OMERO script parameter configuration, user input validation, and the full
    data processing pipeline from export through import.

    The processing pipeline includes:
        - Export data from OMERO to SLURM
        - Convert data formats if needed (with ZARR optimization)
        - Execute selected computational workflows
        - Monitor job completion
        - Import results back to OMERO
        - Handle cleanup and error recovery

    Uses the biomero framework to maintain persistent connections to both
    OMERO and SLURM for robust data transfer and job management.

    Raises:
        Exception: Various exceptions during workflow execution, all logged
            and handled gracefully.
    """
    # --------------------------------------------
    # :: Slurm Client ::
    # --------------------------------------------
    # Start by setting up the Slurm Client from configuration files.
    # We will use the client to connect via SSH to Slurm to send data and
    # commands.
    with SlurmClient.from_config() as slurmClient:
        # --------------------------------------------
        # :: Script definition ::
        # --------------------------------------------
        # Script name, description and parameters are defined here.
        # These parameters will be recognised by the Insight and web clients
        # and populated with the currently selected Image(s)/Dataset(s)
        params = JobParams()
        params.authors = ["Torec Luik"]
        params.version = VERSION
        params.description = f'''Script to run a workflow on the Slurm cluster.

        This runs a script remotely on your Slurm cluster.
        Connection ready? << {slurmClient.validate()} >>

        Select one or more of the workflows below to run them on the given
        Datasets / Images / Plates.

        Parameters for workflows are automatically generated from their Github.
        Versions are only those currently available on your Slurm cluster.

        Results will be imported back into OMERO with the selected settings.

        If you need different Slurm settings (like memory increase), ask your
        OMERO admin.
        '''
        params.name = 'Slurm Workflow'
        params.contact = 'cellularimaging@amsterdamumc.nl'
        params.institutions = ["Amsterdam UMC"]
        params.authorsInstitutions = [[1]]
        # Default script parameters that we want to know for all workflows:
        # input and output.
        email_descr = "Do you want an email if your job is done or cancelled?"

        ome_zarr_versions = [rstring(constants.transfer.OME_ZARR_VERSION_0_4),
                             rstring(constants.transfer.OME_ZARR_VERSION_0_5)]

        input_list = [
            omscripts.String(
                constants.transfer.DATA_TYPE, optional=False, grouping="01.1",
                description="The data you want to work with.",
                values=DATATYPES,
                default=constants.transfer.DATA_TYPE_DATASET),
            omscripts.List(
                constants.transfer.IDS, optional=False, grouping="01.2",
                description="List of Dataset IDs or Image IDs").ofType(
                    rlong(0)),
            omscripts.Bool(constants.workflow.EMAIL, grouping="01.3",
                           description=email_descr,
                           default=True),
            omscripts.Bool(constants.workflow.USE_ZARR_FORMAT, grouping="01.4",
                           description="Skip TIFF conversion and run "
                           "workflows directly on ZARR data (experimental). "
                           "Use this for workflows that support ZARR input.",
                           default=False),
            omscripts.String(
                            constants.transfer.OME_VERSION, grouping="01.4.1",
                            description="Ome-zarr version", values=ome_zarr_versions,
                            default=constants.transfer.OME_ZARR_VERSION_0_4),
            omscripts.Bool(constants.workflow.SELECT_IMPORT,
                           optional=False,
                           grouping="02",
                           description="Select one or more options below:",
                           default=True),
            omscripts.String(constants.workflow.OUTPUT_RENAME,
                             optional=True,
                             grouping="02.7",
                             description="A new name for the imported images. You can use variables {original_file}, {original_ext}, {file}, and {ext}. E.g. {original_file}_nuclei_mask.{ext} or {file}_processed.{original_ext}",
                             default=constants.workflow.NO),
            omscripts.Bool(constants.workflow.OUTPUT_PARENT,
                           optional=True, grouping="02.2",
                           description="Attach zip to parent project/plate",
                           default=True),
            omscripts.Bool(constants.workflow.OUTPUT_ATTACH,
                           optional=True,
                           grouping="02.4",
                           description="Attach all resulting images to original images as attachments",
                           default=False),
            omscripts.String(constants.workflow.OUTPUT_NEW_DATASET, optional=True,
                             grouping="02.5",
                             description="Name for the new dataset w/ result images",
                             default=constants.workflow.NO),
            omscripts.String(constants.workflow.OUTPUT_NEW_SCREEN, optional=True,
                             grouping="02.5.1",
                             description="Name for the new screen w/ result images",
                             default=constants.workflow.NO),
            omscripts.Bool(constants.workflow.OUTPUT_DUPLICATES,
                           optional=True,
                           grouping="02.6",
                           description="If a dataset already matches this name, still make a new one?",
                           default=False),
            omscripts.Long(constants.results.OUTPUT_ATTACH_NEW_DATASET_ID,
                           optional=True,
                           grouping="02.61",
                           description="Pinpoint an exact Dataset by OMERO ID. If provided, this ID wins over name lookup and Allow duplicate settings."),
            omscripts.Long(constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID,
                           optional=True,
                           grouping="02.62",
                           description="Pinpoint an exact Screen by OMERO ID. If provided, this ID wins over name lookup and Allow duplicate settings."),
            omscripts.Bool(constants.workflow.OUTPUT_CSV_TABLE,
                           optional=False,
                           grouping="02.8",
                           description="Any resulting csv files will be added as OMERO.table to parent dataset/plate",
                           default=True),
            omscripts.Bool(constants.workflow.OUTPUT_ATTACH_FILE_OUTPUTS,
                           optional=True,
                           grouping="02.85",
                           description="Attach individual non-image output files (arrays, model weights, configs) as OMERO file annotations. Useful for bilayers workflows with 'array', 'file', or 'executable' output types.",
                           default=False),
            omscripts.Bool(constants.CLEANUP,
                           optional=True,
                           grouping="02.9", 
                           description="Cleanup temporary files after completion (default: True). Turn off for debugging.",
                           default=True)

        ]
        # Generate script parameters for all our workflows
        (wf_versions, _) = slurmClient.get_all_image_versions_and_data_files()
        na = ["Not Available!"]
        _workflow_params = {}
        _workflow_file_params = {}  # file-attachment params keyed by param_id
        _workflow_available_versions = {}
        # All currently configured workflows
        workflows = wf_versions.keys()
        for group_incr, wf in enumerate(workflows):
            # increment per wf, determines UI order
            new_position = group_incr+3
            if new_position > 9:
                parameter_group = f"{new_position}"
            else:
                parameter_group = f"0{new_position}"
            _workflow_available_versions[wf] = wf_versions.get(
                wf, na)
            # Get the workflow parameters (dynamically) from their repository
            _all_wf_params = slurmClient.get_workflow_parameters(wf)
            _workflow_params[wf] = {k: v for k, v in _all_wf_params.items() if not v['file_attachment']}
            _workflow_file_params[wf] = {k: v for k, v in _all_wf_params.items() if v['file_attachment']}
            descriptor = slurmClient.generic_descriptor_from_github(wf)
            wf_descr = descriptor['description']
            # Build value-choices lookup from the descriptor (scoped per wf,
            # so param name collisions across workflows are not an issue)
            value_choices_map = {
                inp['id']: [rstring(v) for v in inp['value-choices']]
                for inp in descriptor.get('inputs', [])
                if inp.get('value-choices')
            }
            # Main parameter to select this workflow for execution
            wf_ = omscripts.Bool(wf, grouping=parameter_group, default=False,
                                 description=wf_descr)
            input_list.append(wf_)
            # Select an available container image version to execute on Slurm
            version_descr = f"Version of the container of {wf}"
            wf_v = omscripts.String(f"{wf}_Version",
                                    grouping=f"{parameter_group}.0",
                                    description=version_descr,
                                    values=_workflow_available_versions[wf])
            input_list.append(wf_v)
            # Create a script parameter for all workflow parameters
            for param_incr, (k, param) in enumerate(_workflow_params[
                    wf].items()):
                # Convert the parameter type to om(ero)type
                omtype_param = slurmClient.convert_param_type_to_omtype(
                    param["type"],
                    param["default"],
                    param["name"],
                    description=param["description"],
                    default=param["default"],
                    grouping=f"{parameter_group}.{param_incr+1}",
                    optional=True,  # always optional: params from other workflows must not block this one
                    **({"values": value_choices_map[k]} if k in value_choices_map else {})
                )
                # To allow 'duplicate' params, add the wf to uniqueify them
                # we have to remove this prefix later again, before passing
                # them to BIOMERO (as the wf will not understand these params)
                omtype_param._name = f"{wf}_|_{omtype_param._name}"
                input_list.append(omtype_param)
            # File-attachment params: exposed as List(Long) of OMERO FileAnnotation IDs
            # They live under a FILE_ prefix so run_workflow can tell them apart.
            num_reg = len(_workflow_params[wf])
            for fp_incr, (k, fp) in enumerate(_workflow_file_params[wf].items()):
                fmt_str = ", ".join(fp['format']) if fp['format'] else "any"
                fp_param = omscripts.List(
                    f"{wf}_|_FILE_{k}",
                    optional=True,  # required-ness is enforced per selected workflow at runtime
                    grouping=f"{parameter_group}.{num_reg + fp_incr + 1}",
                    description=(
                        f"[{fp['type'].capitalize()} attachment] {fp['description']}"
                        f" Accepted formats: {fmt_str}."
                        f" Provide one or more OMERO FileAnnotation IDs."
                    ),
                ).ofType(rlong(0))
                input_list.append(fp_param)
        # Finish setting up the Omero script UI
        inputs = {
            f"{p._name}": p for p in input_list
        }
        params.inputs = inputs
        # Reload instead of caching
        params.namespaces = [omero.constants.namespaces.NSDYNAMIC]
        client = omscripts.client(params)

        # --------------------------------------------
        # :: Workflow execution ::
        # --------------------------------------------
        # Here we actually run the chosen workflows on the chosen data
        # on Slurm.
        # Steps:
        # 1. Push selected data to Slurm
        # 2. Unpack data on Slurm
        # 3. Create Slurm jobs for all workflows
        # 4. Check Slurm job statuses
        # 5. When completed, pull and upload data to Omero
        try:

            # Display startup banner - only shown during actual execution
            # Dynamic centering for version string to handle varying lengths
            version_text = f"SLURM Workflow Runner {VERSION}"
            total_width = 62  # Inner width of the box
            padding = (total_width - len(version_text)) // 2
            right_pad = total_width - len(version_text) - padding
            padded_version = f"{' ' * padding}{version_text}{' ' * right_pad}"

            logger.info(f"""
            ╔══════════════════════════════════════════════════════════════╗
            ║  ██████  ██  ██████  ███    ███ ███████ ██████   ██████      ║
            ║  ██   ██ ██ ██    ██ ████  ████ ██      ██   ██ ██    ██     ║
            ║  ██████  ██ ██    ██ ██ ████ ██ █████   ██████  ██    ██     ║
            ║  ██   ██ ██ ██    ██ ██  ██  ██ ██      ██   ██ ██    ██     ║
            ║  ██████  ██  ██████  ██      ██ ███████ ██   ██  ██████      ║
            ║                                                              ║
            ║{padded_version}║
            ╚══════════════════════════════════════════════════════════════╝
            """)

            # log_string will be output in the Omero Web UI
            UI_messages = ""
            errormsg = None
            wf_id = None  # Define wf_id early
            # Check if user actually selected (a version of) a workflow to run
            selected_workflows = {wf_name: unwrap(
                client.getInput(wf_name)) for wf_name in workflows}
            if not any(selected_workflows.values()):
                errormsg = "ERROR: Please select at least 1 workflow!"
                client.setOutput("Message", rstring(errormsg))
                raise ValueError(errormsg)
            version_errors = ""
            for wf, selected in selected_workflows.items():
                selected_version = unwrap(client.getInput(f"{wf}_Version"))
                logger.debug(f"{wf}, {selected}, {selected_version}")
                if selected and not selected_version:
                    version_errors += f"ERROR: No version for '{wf}'! \n"
            if version_errors:
                raise ValueError(version_errors)
            # Check if user actually selected the output option
            selected_output = {}
            dataset_id_override = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_ID))
            screen_id_override = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID))
            for output_option in OUTPUT_OPTIONS:
                selected_op = unwrap(client.getInput(output_option))
                if (not selected_op) or (
                    selected_op == constants.workflow.NO) or (
                        type(selected_op) == list and constants.workflow.NO in selected_op):
                    # Still treat as selected if an explicit ID override was given
                    if output_option == constants.workflow.OUTPUT_NEW_DATASET and dataset_id_override:
                        selected_output[output_option] = True
                    elif output_option == constants.workflow.OUTPUT_NEW_SCREEN and screen_id_override:
                        selected_output[output_option] = True
                    else:
                        selected_output[output_option] = False
                else:
                    selected_output[output_option] = True
                    logger.debug(
                        f"Selected: {output_option} >> [{selected_op}]")
            if not any(selected_output.values()):
                errormsg = "ERROR: Please select at least 1 output method!"
                client.setOutput("Message", rstring(errormsg))
                raise ValueError(errormsg)
            else:
                logger.info(f"Output options chosen: {selected_output}")

            # Connect to Omero
            conn = BlitzGateway(client_obj=client)
            conn.SERVICE_OPTS.setOmeroGroup(-1)
            # retrieve user data: email for Slurm, user, group
            email = getOmeroEmail(client, conn)
            user = conn.getUserId()
            group = conn.getGroupFromContext().id
            # Get ZARR format preference
            use_zarr_format = unwrap(client.getInput(constants.workflow.USE_ZARR_FORMAT))
            if use_zarr_format:
                ome_zarr_version = unwrap(client.getInput(constants.transfer.OME_VERSION))
            else:
                ome_zarr_version = constants.transfer.OME_ZARR_VERSION_0_4 # backward compatible

            logger.debug(f"User: {user} - Group: {group} - Email: {email}")
            logger.debug(f"Use ZARR format: {use_zarr_format}")
            # Start tracking the workflow on a unique ID
            wf_id = slurmClient.workflowTracker.initiate_workflow(
                params.name,
                "\n".join([params.description, VERSION]),
                user,
                group
            )

            # Early validation: Check importer write access if needed
            # This prevents expensive SLURM operations if we can't write results
            validate_importer_write_access(slurmClient, conn, client)

            # Gather all inputs and parameters to store in the launcher task
            launcher_params = {}
            for k in inputs:
                launcher_params[k] = unwrap(client.getInput(k))

            selected_wfs = [wf_name for wf_name in workflows if unwrap(client.getInput(wf_name))]
            launcher_params["workflows"] = selected_wfs
            launcher_params["email"] = email
            launcher_params["zipfile"] = createFileName(client, conn, wf_id)
            launcher_params["use_zarr_format"] = use_zarr_format
            launcher_params["ome_zarr_version"] = ome_zarr_version

            for wf_name in selected_wfs:
                launcher_params[f"wf_params_{wf_name}"] = _workflow_params[wf_name]
                launcher_params[f"wf_file_params_{wf_name}"] = _workflow_file_params[wf_name]

            output_settings = {
                "DATA_TYPE": unwrap(client.getInput(constants.transfer.DATA_TYPE)),
                "IDS": unwrap(client.getInput(constants.transfer.IDS)),
                "OUTPUT_PARENT": unwrap(client.getInput(constants.workflow.OUTPUT_PARENT)),
                "OUTPUT_RENAME": unwrap(client.getInput(constants.workflow.OUTPUT_RENAME)),
                "OUTPUT_RENAME_SELECTED": selected_output[constants.workflow.OUTPUT_RENAME],
                "OUTPUT_NEW_DATASET": unwrap(client.getInput(constants.workflow.OUTPUT_NEW_DATASET)),
                "OUTPUT_NEW_DATASET_SELECTED": selected_output[constants.workflow.OUTPUT_NEW_DATASET],
                "OUTPUT_NEW_SCREEN": unwrap(client.getInput(constants.workflow.OUTPUT_NEW_SCREEN)),
                "OUTPUT_NEW_SCREEN_SELECTED": selected_output[constants.workflow.OUTPUT_NEW_SCREEN],
                "OUTPUT_DUPLICATES": unwrap(client.getInput(constants.workflow.OUTPUT_DUPLICATES)),
                "OUTPUT_ATTACH": unwrap(client.getInput(constants.workflow.OUTPUT_ATTACH)),
                "OUTPUT_CSV_TABLE": unwrap(client.getInput(constants.workflow.OUTPUT_CSV_TABLE)),
                "OUTPUT_ATTACH_FILE_OUTPUTS": unwrap(client.getInput(constants.workflow.OUTPUT_ATTACH_FILE_OUTPUTS)),
                "OUTPUT_ATTACH_NEW_DATASET_ID": unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_ID)),
                "OUTPUT_ATTACH_NEW_SCREEN_ID": unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID)),
                "CLEANUP": unwrap(client.getInput(constants.CLEANUP)),
            }
            launcher_params["output_settings"] = output_settings

            # Register the launcher task in the event store
            task_id = slurmClient.workflowTracker.add_task_to_workflow(
                wf_id,
                'SLURM_Run_Workflow.py',
                VERSION,
                unwrap(client.getInput(constants.transfer.IDS)),
                launcher_params
            )

            UI_messages = "Workflow initialization complete. Your job has been queued and will be processed in the background. You can safely close this browser tab; results will be imported automatically."
            client.setOutput("Message", rstring(UI_messages))

        except Exception as e:
            # Only mark workflow as failed if we actually started one
            logger.error("Exception in wf: ", exc_info=True)
            if 'wf_id' in locals() and wf_id is not None:
                logger.debug(
                    f"Workflow failed {wf_id} {slurmClient.workflowTracker.repository.get(wf_id)}")
                try:
                    slurmClient.workflowTracker.fail_workflow(wf_id, str(e))
                except Exception as db_e:
                    logger.error(
                        f"Database error marking workflow {wf_id} as failed: {db_e}")
            client.setOutput("Message", rstring(
                f"{UI_messages} ERROR: {str(e)}"))
            raise  # Re-raise the exception after handling

        finally:
            client.closeSession()


def upload_job_log_to_omero(client, conn, slurmClient, slurm_job_id, wf_id):
    """Fetch a workflow job's Slurm log and attach it to OMERO (best-effort).

    Normally the workflow log is uploaded by the result-import step, but that
    only runs when the job COMPLETED. When a job FAILED / CANCELLED / TIMEOUT
    the import step is skipped, yet the log is exactly what the user needs to
    diagnose the failure. This pulls ``omero-<jobid>.log`` from Slurm and
    attaches it as a file annotation with a download URL.

    Best-effort: never raises, so it cannot mask the original failure.

    Args:
        client: OMERO script client (for setOutput).
        conn: OMERO BlitzGateway connection.
        slurmClient: Active SLURM client.
        slurm_job_id: The Slurm job ID whose log should be uploaded.
        wf_id: Workflow UUID for the description.

    Returns:
        str: A short status string to append to the UI messages.
    """
    try:
        if slurm_job_id is None or int(slurm_job_id) < 0:
            return ""
        _, local_path, _ = slurmClient.get_logfile_from_slurm(
            str(slurm_job_id))
        mimetype = "text/plain"
        namespace = NSCREATED + "/SLURM/SLURM_RUN_WORKFLOW"
        description = f"Log from SLURM job {slurm_job_id}"
        if wf_id:
            description += f" (Workflow {wf_id})"
        annotation = conn.createFileAnnfromLocalFile(
            local_path, mimetype=mimetype, ns=namespace, desc=description)
        obj_id = annotation.getFile().getId()
        try:
            config = conn.getConfigService()
            web_host = (config.getConfigValue(
                "omero.client.web.host") or "").rstrip("/")
        except Exception:
            web_host = ""
        url = f"{web_host}/webclient/get_original_file/{obj_id}/"
        client.setOutput("Job_Log", robject(annotation._obj))
        client.setOutput("URL", wrap({"type": "URL", "href": url}))
        logger.info(
            f"Uploaded log for failed job {slurm_job_id} to OMERO "
            f"(file {obj_id})")
        return f" Uploaded log for SLURM job {slurm_job_id}."
    except Exception as e:
        logger.warning(
            f"Failed to upload job log {slurm_job_id} to OMERO: {e}")
        return f" (failed to upload log for job {slurm_job_id}: {e})"


def run_workflow(slurmClient: SlurmClient,
                 workflow_params,
                 file_params,
                 client,
                 conn,
                 UI_messages: str,
                 zipfile,
                 email,
                 name,
                 wf_id,
                 output_settings):
    """Execute a specific workflow on the SLURM cluster.

    Submits a named workflow to SLURM with user-specified parameters and
    monitors initial job submission status. Handles parameter extraction
    from OMERO UI inputs, file-attachment transfers to HPC, and workflow
    tracking state.

    Args:
        slurmClient (SlurmClient): Active SLURM client connection.
        workflow_params: Dictionary of regular workflow parameters.
        file_params: Dictionary of file-attachment params (annotation IDs).
        client: OMERO script client for parameter access.
        conn: OMERO BlitzGateway connection (needed for file transfers).
        UI_messages (str): Accumulated user interface messages.
        zipfile: Name of input data file on SLURM (determines data_path).
        email: User email for job notifications.
        name: Workflow name to execute.
        wf_id: Workflow UUID for tracking.

    Returns:
        tuple: (UI_messages, slurm_job_id, wf_id, task_id) containing
            updated messages, SLURM job ID, workflow ID, and task ID.

    Raises:
        SSHException: If job submission or status checking fails.
    """
    global wf_failed  # Declare global at the top of the function
    logger.info(f"Submitting workflow: {name}")
    workflow_version = unwrap(client.getInput(f"{name}_Version"))

    # Extract regular workflow parameters
    kwargs = {}
    for k in workflow_params:
        kwargs[k] = unwrap(client.getInput(f"{name}_|_{k}"))

    # Transfer file-attachment inputs to HPC and resolve their Slurm paths
    if file_params:
        logger.info('''
        # ------------------------------------------------
        # :: 1b. Transfer file attachments to Slurm ::
        # ------------------------------------------------
        ''')
        svc = conn.getScriptService()
        scripts = svc.getScripts()
        file_transfer_scripts = [constants.FILE_TRANSFER_SCRIPT]
        ft_matches = [(unwrap(s.id), unwrap(s.getName()))
                      for s in scripts if unwrap(s.getName()) in file_transfer_scripts]
        if not ft_matches:
            logger.warning(
                f"File transfer script {file_transfer_scripts} not found — "
                f"skipping file-attachment transfer for {name}."
            )
        else:
            ft_script_id, ft_script_name = ft_matches[0]
            for slot_index, (param_id, fp) in enumerate(file_params.items(), start=1):
                param_slot = f"p{slot_index}"
                raw = unwrap(client.getInput(f"{name}_|_FILE_{param_id}"))
                # Normalise: single int or list of ints → always a list
                if raw is None:
                    ann_ids = []
                elif isinstance(raw, (list, tuple)):
                    ann_ids = [int(v) for v in raw if v is not None]
                else:
                    ann_ids = [int(raw)]

                if not ann_ids:
                    if not fp['optional']:
                        err = f"Required file attachment '{param_id}' has no annotation ID — cannot run workflow."
                        logger.error(err)
                        raise ValueError(err)
                    logger.debug(f"No annotation ID supplied for optional param '{param_id}', skipping.")
                    continue

                collected_paths = []
                for ann_id in ann_ids:
                    logger.info(
                        f"Transferring file attachment {ann_id} for "
                        f"param slot '{param_slot}'"
                    )
                    ft_inputs = {
                        constants.file_transfer.FILE_ANNOTATION_ID: rlong(ann_id),
                        constants.file_transfer.FOLDER: rstring(zipfile),
                        constants.file_transfer.PARAM_SLOT: rstring(param_slot),
                        constants.CLEANUP: client.getInput(constants.CLEANUP) or rbool(True),
                    }
                    if fp.get('format'):
                        ft_inputs[constants.file_transfer.FORMAT] = rstring(
                            ",".join(fp['format'])
                        )
                    try:
                        ft_task_id = slurmClient.workflowTracker.add_task_to_workflow(
                            wf_id, ft_script_name, VERSION,
                            ann_id, {k: unwrap(v) for k, v in ft_inputs.items()}
                        )
                        slurmClient.workflowTracker.start_task(ft_task_id)
                    except Exception as db_e:
                        logger.error(f"DB error adding file-transfer task: {db_e}")
                        raise
                    ft_rv, _ = runOMEROScript(client, svc, ft_script_id, ft_inputs,
                                               slurmClient=slurmClient)
                    slurm_path = unwrap(ft_rv.get('Slurm_Path')) if ft_rv else None
                    ft_msg = unwrap(ft_rv.get('Message', None)) if ft_rv else ''
                    if slurm_path:
                        collected_paths.append(slurm_path)
                        UI_messages += (
                            f"Transferred {fp['type']} '{param_id}' "
                            f"as {param_slot} to {slurm_path}. "
                        )
                        slurmClient.workflowTracker.complete_task(ft_task_id, ft_msg or slurm_path)
                    else:
                        err = (
                            f"File transfer for '{param_id}' "
                            f"({param_slot}, ann {ann_id}) returned no path: {ft_msg}"
                        )
                        logger.warning(err)
                        slurmClient.workflowTracker.fail_task(ft_task_id, err)
                        raise ValueError(err)

                if collected_paths:
                    # Attachments are routed into param-specific dirs on SLURM
                    # (e.g. .../data/in/<param_id>/). Pass that directory to
                    # the workflow CLI flag for consistent handling across
                    # single and multiple file-count params.
                    kwargs[param_id] = os.path.dirname(collected_paths[0])

    logger.debug(f"Workflow parameters (incl. file paths): {kwargs}")
    try:
        # Pre-flight: verify the job script exists on Slurm before submitting
        configured_job = slurmClient.slurm_model_jobs.get(name.lower())
        job_script = (
            f"{slurmClient.slurm_script_path}/{configured_job}"
            if configured_job
            else f"{slurmClient.slurm_script_path}/jobs/{name}.sh"
        )
        check_result = slurmClient.run(f'test -f "{job_script}"', warn=True)
        if check_result.exited != 0:
            raise FileNotFoundError(
                f"Job script not found on Slurm: {job_script}. "
                f"Please generate the job script for '{name}' first "
                f"(use 'Validate Slurm Setup' or check your slurm-scripts repo)."
            )

        cp_result, slurm_job_id, wf_id, task_id = slurmClient.run_workflow(
            workflow_name=name,
            workflow_version=workflow_version,
            input_data=zipfile,
            email=email,
            time=None,
            wf_id=wf_id,
            output_settings=output_settings,
            **kwargs)
        logger.debug(cp_result.stdout)
        if not cp_result.ok:
            logger.warning(f"Error running {name} job: {cp_result.stderr}")
            slurmClient.workflowTracker.fail_task(
                task_id, "Job submission failed")
            wf_failed = True
        else:
            UI_messages += f"Submitted {name} to Slurm\
                as batch job {slurm_job_id}."

            job_status_dict, poll_result = slurmClient.check_job_status(
                [slurm_job_id])
            job_state = job_status_dict[slurm_job_id]
            logger.debug(
                f"{job_state}, {poll_result.stdout}")
            if not poll_result.ok:
                logger.warning(
                    f"Error checking job status: {poll_result.stderr}")
            else:
                log_msg = f"\n{job_state}"
                logger.info(log_msg)
                try:
                    slurmClient.workflowTracker.update_task_status(task_id,
                                                                   job_state)
                except Exception as db_e:
                    logger.error(
                        f"Database error updating task {task_id} status: {db_e}")
                    raise
    except Exception as e:
        error_msg = f"Failed to submit workflow {name}: {e}"
        logger.error(error_msg)
        UI_messages += f" ERROR: {error_msg}"
        raise SSHException(UI_messages)
    return UI_messages, slurm_job_id, wf_id, task_id


def getOmeroEmail(client, conn):
    """Retrieve the authenticated user's email address from OMERO.

    Attempts to get the email address of the currently authenticated OMERO
    user if email notifications are enabled in the script parameters.

    Args:
        client: OMERO script client for parameter access.
        conn: OMERO BlitzGateway connection.

    Returns:
        str or None: User's email address if available and enabled,
            None otherwise.
    """
    if unwrap(client.getInput(constants.workflow.EMAIL)):
        try:
            # Retrieve information about the authenticated user
            user = conn.getUser()
            use_email = user.getEmail()
            if use_email == "None":
                logger.debug("No email given for this user")
                use_email = None
        except omero.gateway.OMEROError as e:
            logger.warning(f"Error retrieving email {e}")
            use_email = None
    else:
        use_email = None
    logger.info(f"Using email {use_email}")
    return use_email


def convertDataOnSLURM(client: omscripts.client,
                       conn: BlitzGateway,
                       slurmClient: SlurmClient,
                       zipfile: str,
                       source_format: str,
                       target_format: str,
                       wf_id: UUID):
    """
    Convert data format on SLURM cluster using the remote conversion script.

    This function delegates format conversion to the SLURM_Remote_Conversion.py
    script, which includes smart no-op logic for same-format conversions
    (e.g., zarr->zarr operations are skipped for efficiency).

    Args:
        client: OMERO script client for parameter access
        conn: OMERO BlitzGateway connection
        slurmClient: Active SLURM client connection
        zipfile: Name of the data file on SLURM to convert
        source_format: Input data format (e.g., 'zarr', 'tiff')
        target_format: Desired output format (e.g., 'zarr', 'tiff')
        wf_id: Workflow UUID for tracking

    Raises:
        Exception: If conversion script not found or conversion fails
    """
    svc = conn.getScriptService()
    scripts = svc.getScripts()
    # force just one script, why is it an array?
    script_id, biomero, script_name = [(unwrap(s.id), unwrap(s.getVersion()), unwrap(s.getName()))
                                       for s in scripts if unwrap(s.getName()) in CONVERSION_SCRIPTS][0]
    if not script_id:
        raise Exception(f"Conversion script not found: {CONVERSION_SCRIPTS}")

    # Compute the parent container for the conversion log — same logic as
    # importResultsToOmero so the log is always linked to a findable container.
    first_id = unwrap(client.getInput(constants.transfer.IDS))[0]
    data_type = unwrap(client.getInput(constants.transfer.DATA_TYPE))
    parent_id = first_id
    parent_data_type = data_type
    if data_type == constants.transfer.DATA_TYPE_IMAGE:
        q = conn.getQueryService()
        _img_params = Parameters()
        _img_params.map = {"image_id": rlong(first_id)}
        _resultPlates = q.projection(
            "SELECT DISTINCT p.id FROM Plate p"
            " JOIN p.wells w JOIN w.wellSamples ws JOIN ws.image i"
            " WHERE i.id = :image_id",
            _img_params, conn.SERVICE_OPTS)
        _resultDatasets = q.projection(
            "SELECT DISTINCT d.id FROM Dataset d"
            " JOIN d.imageLinks dil JOIN dil.child i"
            " WHERE i.id = :image_id",
            _img_params, conn.SERVICE_OPTS)
        if len(_resultPlates) > len(_resultDatasets):
            parent_id = _resultPlates[0][0]
            parent_data_type = constants.transfer.DATA_TYPE_PLATE
        elif _resultDatasets:
            parent_id = _resultDatasets[0][0]
            parent_data_type = constants.transfer.DATA_TYPE_DATASET
    _conv_fallback_id = unwrap(parent_id)
    inputs = {
        constants.conversion.INPUT_DATA: rstring(zipfile),
        constants.conversion.SOURCE_FORMAT: rstring(source_format),
        constants.conversion.TARGET_FORMAT: rstring(target_format),
        constants.CLEANUP: client.getInput(constants.CLEANUP) or rbool(True),
        constants.conversion.PARENT_WORKFLOW_ID: rstring(str(wf_id)),
        constants.results.LOG_FALLBACK_TARGET: rstring(
            f"{parent_data_type}:{_conv_fallback_id}"),
    }
    persist_dict = {key: unwrap(value) for key, value in inputs.items()}
    logger.debug(f"{inputs}, {script_id}")
    try:
        task_id = slurmClient.workflowTracker.add_task_to_workflow(
            wf_id,
            script_name,
            VERSION,
            zipfile,
            persist_dict
        )
        slurmClient.workflowTracker.start_task(task_id)
    except Exception as db_e:
        raise
    # Execute the conversion script with comprehensive error detection
    try:
        rv, job = runOMEROScript(client, svc, script_id, inputs)

        # Determine if the script actually succeeded using job status
        success = False
        msg = ""
        
        # Get job status
        job_status_id = None
        if job:
            try:
                job_status = job.getStatus()
                job_status_id = unwrap(job_status.getId())
                logger.debug(f"Conversion script job status ID: {job_status_id}")
                # Success if job finished (status ID 8)
                success = job_status_id in (7, 8)
            except Exception as job_error:
                logger.warning(f"Could not get job status: {job_error}")
                success = False

        if rv and isinstance(rv, dict):
            # Extract the Message content if available
            if 'Message' in rv:
                try:
                    message_content = rv['Message'].getValue()

                    # Check for error indicators in the message
                    error_indicators = [
                        'ERROR WITH CONVERTING DATA', 'Error converting data', 'Conversion failed', 'Exception']
                    if any(indicator in message_content for indicator in error_indicators):
                        success = False
                        msg = f"Conversion failed - error in message: {message_content}"
                    else:
                        success = True
                        msg = message_content
                except Exception as msg_e:
                    success = False
                    msg = f"Failed to extract message from result: {msg_e}"
                    logger.error(f"Error extracting message: {msg_e}")
            else:
                # No Message key - check for other error indicators
                result_str = str(rv)
                if any(key in result_str.lower() for key in ['error', 'exception', 'failed']):
                    success = False
                    msg = f"Conversion failed - errors in result: {rv}"
                    logger.error(f"Error indicators found in result: {rv}")
                else:
                    success = True
                    msg = f"Conversion completed (no message): {rv}"
                    logger.info(f"Conversion assumed successful: {rv}")
        else:
            # Empty or None result typically indicates failure
            success = False
            msg = f"Conversion script returned empty/invalid result: {rv}"
            logger.error(f"Empty/invalid result: {rv}")

        if success:
            try:
                slurmClient.workflowTracker.complete_task(task_id, msg)
            except Exception as db_e:
                logger.error(
                    f"Database error completing conversion task {task_id}: {db_e}")
                raise
        else:
            try:
                slurmClient.workflowTracker.fail_task(task_id, msg)
            except Exception as db_e:
                logger.error(
                    f"Database error failing conversion task {task_id}: {db_e}")
                raise
            raise Exception(f"Conversion script failed: {msg}")

    except Exception as script_e:
        # Script execution failed entirely
        error_msg = f"Conversion script execution failed: {script_e}"
        try:
            slurmClient.workflowTracker.fail_task(task_id, error_msg)
        except Exception as db_e:
            logger.error(
                f"Database error failing conversion task {task_id}: {db_e}")
        raise Exception(error_msg)
    return rv, task_id


def exportImageToSLURM(client: omscripts.client,
                       conn: BlitzGateway,
                       slurmClient: SlurmClient,
                       zipfile: str,
                       wf_id: UUID,
                       ome_zarr_version: str):
    """
    Export selected OMERO data to SLURM cluster for processing.

    This function delegates to the _SLURM_Image_Transfer.py script to export
    selected images, datasets, or plates from OMERO to the SLURM cluster.
    The exported data is automatically transferred and unpacked on SLURM,
    with temporary file annotations cleaned up after successful transfer.

    Args:
        client: OMERO script client for accessing user inputs
        conn: OMERO BlitzGateway connection
        slurmClient: Active SLURM client connection
        zipfile: Target filename for the exported data
        wf_id: Workflow UUID for tracking
        ome_zarr_version: Version of OME-Zarr format to use
    Returns:
        tuple: (export_result_dict, task_id) containing script results and task ID

    Raises:
        ValueError: If export script not found
        Exception: If export process fails
    """
    svc = conn.getScriptService()
    scripts = svc.getScripts()
    # force just one script, why is it an array?
    script_id, biomero, script_name = [(unwrap(s.id), unwrap(s.getVersion()), unwrap(s.getName()))
                                       for s in scripts if unwrap(s.getName()) in EXPORT_SCRIPTS][0]
    if not script_id:
        raise ValueError(
            f"Cannot export images to Slurm: scripts ({EXPORT_SCRIPTS})\
                not found in ({[unwrap(s.getName()) for s in scripts]}) ")
    # TODO: export nucleus channel only? that is individual channels,
    # but filtered...
    # Might require metadata: what does the WF want? What is in which channel?
    inputs = {
        constants.transfer.DATA_TYPE: client.getInput(
            constants.transfer.DATA_TYPE),
        constants.transfer.IDS: client.getInput(constants.transfer.IDS),
        constants.transfer.SETTINGS: rbool(True),
        constants.transfer.CHANNELS: rbool(False),
        constants.transfer.CHANNELS_GREY: rbool(False),
        constants.transfer.MERGED: rbool(True),
        constants.transfer.Z: rstring(constants.transfer.Z_MAXPROJ),
        constants.transfer.T: rstring(constants.transfer.T_DEFAULT),
        constants.transfer.FORMAT: rstring(
            constants.transfer.FORMAT_OMEZARR),
        constants.transfer.OME_VERSION: rstring(ome_zarr_version),
        constants.transfer.FOLDER: rstring(zipfile),
        constants.CLEANUP: client.getInput(constants.CLEANUP) or rbool(True)
    }
    persist_dict = {key: unwrap(value) for key, value in inputs.items()}
    logger.debug(f"{inputs}, {script_id}")
    try:
        task_id = slurmClient.workflowTracker.add_task_to_workflow(
            wf_id,
            script_name,
            VERSION,
            persist_dict[constants.transfer.IDS],
            persist_dict
        )
        slurmClient.workflowTracker.start_task(task_id)
    except Exception as db_e:
        logger.error(
            f"Database error adding export task to workflow {wf_id}: {db_e}")
        raise
    try:
        rv, job = runOMEROScript(client, svc, script_id, inputs)

        success = False
        msg = ""

        # Check job status for success/failure.
        # OMERO uses 8 for finished, 6 for error, 9 for cancelled.
        job_status_id = None
        if job:
            try:
                job_status = job.getStatus()
                job_status_id = unwrap(job_status.getId())
                logger.debug(f"Export script job status ID: {job_status_id}")
                success = job_status_id in (7, 8)
            except Exception as job_error:
                logger.warning(f"Could not get job status: {job_error}")
                success = False

        if rv and isinstance(rv, dict):
            if 'Message' in rv:
                try:
                    message_content = rv['Message'].getValue()
                    error_indicators = [
                        'ERROR:',
                        'FAILED:',
                        'Critical error:',
                        'ZARR export failed',
                        'Data transfer to SLURM failed',
                        'Data unpacking on SLURM failed',
                        'No files exported',
                    ]
                    if any(indicator in message_content for indicator in error_indicators):
                        success = False
                        msg = f"Export failed - error in message: {message_content}"
                    else:
                        msg = message_content
                except Exception as msg_error:
                    success = False
                    msg = f"Failed to extract export message: {msg_error}"
                    logger.error(msg)
            else:
                success = False
                msg = f"Export script returned no Message output: {rv}"
        else:
            success = False
            msg = f"Export script returned empty/invalid result: {rv}"

        if not success:
            raise RuntimeError(msg or f"Export script failed with status {job_status_id}")

        # The export sub-script can report success before a later consumer proves
        # the target folder is actually discoverable on SLURM. Validate that the
        # expected folder is present now so the workflow fails at the transfer step.
        _, available_data_files = slurmClient.get_image_versions_and_data_files('cellpose')
        if zipfile not in available_data_files:
            error_msg = (
                f"Exported data folder '{zipfile}' is not available on SLURM after transfer. "
                f"Export message: {msg}"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        try:
            slurmClient.workflowTracker.complete_task(task_id, msg)
        except Exception as db_e:
            logger.error(
                f"Database error completing export task {task_id}: {db_e}")
            raise
    except Exception as export_error:
        error_msg = f"Export script execution failed: {export_error}"
        try:
            slurmClient.workflowTracker.fail_task(task_id, error_msg)
        except Exception as db_e:
            logger.error(
                f"Database error failing export task {task_id}: {db_e}")
        raise RuntimeError(error_msg)
    return rv, task_id


def runOMEROScript(client: omscripts.client, svc, script_id, inputs,
                   slurmClient=None):
    """
    Execute an OMERO script and return its results and job information.

    Args:
        client: OMERO script client
        svc: OMERO script service
        script_id: ID of the script to execute
        inputs: Dictionary of input parameters for the script
        slurmClient: Optional SlurmClient; when provided, polls wfProgress
            on each iteration so IMPORTING/IMPORTED events written by the
            sub-script are visible in real time.

    Returns:
        tuple: (results_dict, job_info) where job_info contains status information
    """
    rv = None
    job = None

    script_id = int(script_id)
    # params = svc.getParams(script_id) # we can dynamically get them

    # The last parameter is how long to wait as an RInt
    proc = svc.runScript(script_id, inputs, None)
    try:
        cb = omero.scripts.ProcessCallbackI(client, proc)
        # Snapshot position once so we only process NEW events each iteration
        next_position = 1
        if slurmClient is not None and slurmClient.wfProgress is not None:
            try:
                next_position = slurmClient.wfProgress.recorder.max_tracking_id(
                    application_name='WorkflowTracker'
                ) + 1
            except Exception:
                next_position = 1
        while not cb.block(1000):  # ms.
            if slurmClient is not None and slurmClient.wfProgress is not None:
                try:
                    slurmClient.bring_listener_uptodate(
                        slurmClient.wfProgress, start=next_position
                    )
                except Exception as e:
                    logger.debug(f"wfProgress poll skipped (events already processed by runner): {e}")
                finally:
                    try:
                        new_position = slurmClient.wfProgress.recorder.max_tracking_id(
                            application_name='WorkflowTracker'
                        ) + 1
                        if new_position > next_position:
                            logger.debug(f"Import subscript progress: picked up {new_position - next_position} new workflow event(s) (events {next_position}-{new_position - 1})")
                        next_position = new_position
                    except Exception:
                        pass
        cb.close()
        rv = proc.getResults(0)
        job = proc.getJob()  # Get job status information
    finally:
        proc.close(False)
    return rv, job


def importResultsToOmero(client: omscripts.client,
                         conn: BlitzGateway,
                         slurmClient: SlurmClient,
                         slurm_job_id: int,
                         selected_output: list,
                         wf_id: UUID) -> str:
    """
    Import workflow results from SLURM back into OMERO.

    This function dynamically delegates to either SLURM_Get_Results.py or 
    SLURM_Import_Results.py based on the IMPORTER_ENABLED environment variable
    to retrieve completed workflow results from SLURM and import them into OMERO
    according to the user's selected output options.

    Import Script Selection:
        - If IMPORTER_ENABLED=true: Uses SLURM_Import_Results.py (biomero-importer)
        - If IMPORTER_ENABLED=false/unset: Uses SLURM_Get_Results.py (standard)

    Args:
        client: OMERO script client for accessing user inputs
        conn: OMERO BlitzGateway connection
        slurmClient: Active SLURM client connection
        slurm_job_id: SLURM job ID of the completed workflow
        selected_output: Dictionary of selected output organization options
        wf_id: Workflow UUID for tracking

    Returns:
        str: Import result message

    Raises:
        Exception: If import script not found or import fails
    """
    global wf_failed  # Declare global at the top of the function
    if conn.keepAlive():
        svc = conn.getScriptService()
        scripts = svc.getScripts()
    else:
        msg = f"Lost connection with OMERO. Slurm done @ {slurm_job_id}"
        logger.error(msg)
        raise ConnectionError(msg)
    # Force one script
    script_id, script_version, script_name = [(unwrap(s.id), unwrap(s.getVersion()), unwrap(s.getName()))
                                              for s in scripts if unwrap(s.getName()) in IMPORT_SCRIPTS][0]
    first_id = unwrap(client.getInput(constants.transfer.IDS))[0]
    data_type = unwrap(client.getInput(constants.transfer.DATA_TYPE))
    logger.debug(f"{script_id}, {first_id}, {data_type}")
    inputs = {
        constants.results.OUTPUT_COMPLETED_JOB: rbool(True),
        constants.results.OUTPUT_SLURM_JOB_ID: rstring(str(slurm_job_id)),
        constants.CLEANUP: client.getInput(constants.CLEANUP) or rbool(True),
        constants.results.WORKFLOW_UUID: rstring(str(wf_id))
    }

    # Get a 'parent' dataset or plate of input images
    parent_id = first_id
    parent_data_type = data_type
    if data_type == constants.transfer.DATA_TYPE_IMAGE:
        q = conn.getQueryService()
        params = Parameters()
        params.map = {"image_id": rlong(first_id)}
        logger.debug(params)
        resultPlates = q.projection(
            "SELECT DISTINCT p.id FROM Plate p "
            " JOIN p.wells w "
            " JOIN w.wellSamples ws "
            " JOIN ws.image i "
            " WHERE i.id = :image_id",
            params,
            conn.SERVICE_OPTS
        )
        resultDatasets = q.projection(
            "SELECT DISTINCT d.id FROM Dataset d "
            " JOIN d.imageLinks dil "
            " JOIN dil.child i "
            " WHERE i.id = :image_id",
            params,
            conn.SERVICE_OPTS
        )
        logger.debug(f"Projects:{resultDatasets} Plates:{resultPlates}")
        if len(resultPlates) > len(resultDatasets):
            parent_id = resultPlates[0][0]
            parent_data_type = constants.transfer.DATA_TYPE_PLATE
        else:
            parent_id = resultDatasets[0][0]
            parent_data_type = constants.transfer.DATA_TYPE_DATASET

    logger.debug(f"Determined parent to be {parent_data_type}:{parent_id}")

    # Always forward the input's parent container as a guaranteed fallback target
    # for the job log, independent of the chosen output option. The import scripts
    # force-link the log here when no richer target resolves, so it is never left
    # as an unlinked (invisible) annotation. parent_data_type is already the input
    # container itself for Plate/Screen/Dataset inputs, or the resolved parent for
    # Image inputs.
    # parent_id may be a plain int (direct container input) or an RLong returned
    # by the projection query above; unwrap so the forwarded string is numeric.
    _fallback_parent_id = unwrap(parent_id)
    inputs[constants.results.LOG_FALLBACK_TARGET] = rstring(
        f"{parent_data_type}:{_fallback_parent_id}")

    if selected_output[constants.workflow.OUTPUT_PARENT]:
        # For now, there is no attaching to Dataset or Screen...
        # If we need that, build it ;) (in Get_Result script)
        if (parent_data_type == constants.transfer.DATA_TYPE_DATASET or
                parent_data_type == constants.transfer.DATA_TYPE_PROJECT):
            logger.debug(f"Adding to dataset {parent_id}")
            projects = get_project_name_ids(conn, parent_id)
            if projects:
                inputs[constants.results.OUTPUT_ATTACH_PROJECT] = rbool(True)
                inputs[constants.results.OUTPUT_ATTACH_PROJECT_ID] = rlist(projects)
                inputs[constants.results.OUTPUT_ATTACH_DATASET] = rbool(False)
            else:
                logger.warning(
                    f"Dataset {parent_id} has no parent project; "
                    "falling back to attaching directly to the dataset")
                datasets = get_dataset_name_ids(conn, parent_id)
                inputs[constants.results.OUTPUT_ATTACH_PROJECT] = rbool(False)
                inputs[constants.results.OUTPUT_ATTACH_DATASET] = rbool(True)
                inputs[constants.results.OUTPUT_ATTACH_DATASET_ID] = rlist(datasets)
            inputs[constants.results.OUTPUT_ATTACH_PLATE] = rbool(False)
        elif parent_data_type == constants.transfer.DATA_TYPE_PLATE:
            logger.debug(f"Adding to plate {parent_id}")
            plates = get_plate_name_ids(conn, parent_id)
            inputs[constants.results.OUTPUT_ATTACH_PROJECT] = rbool(
                False)
            inputs[constants.results.OUTPUT_ATTACH_PLATE] = rbool(
                True)
            inputs[constants.results.OUTPUT_ATTACH_PLATE_ID] = rlist(
                plates)
        else:
            raise ValueError(f"Cannot handle {parent_data_type}")
    else:
        inputs[constants.results.OUTPUT_ATTACH_PROJECT] = rbool(
            False)
        inputs[constants.results.OUTPUT_ATTACH_PLATE] = rbool(
            False)

    if selected_output[constants.workflow.OUTPUT_RENAME]:
        inputs[
            constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME
        ] = rbool(True)
        inputs[
            constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME_NAME
        ] = client.getInput(constants.workflow.OUTPUT_RENAME)
    else:
        inputs[
            constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME
        ] = rbool(False)

    if selected_output[constants.workflow.OUTPUT_NEW_DATASET]:
        inputs[constants.results.OUTPUT_ATTACH_NEW_DATASET] = rbool(
            True)
        inputs[
            constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME
        ] = client.getInput(constants.workflow.OUTPUT_NEW_DATASET)
        # duplicate dataset name check
        inputs[
            constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE
        ] = client.getInput(constants.workflow.OUTPUT_DUPLICATES)
        # Forward explicit dataset ID if provided (wins over name lookup)
        dataset_id_override = client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_ID)
        if dataset_id_override is not None:
            inputs[constants.results.OUTPUT_ATTACH_NEW_DATASET_ID] = dataset_id_override

    else:
        inputs[constants.results.OUTPUT_ATTACH_NEW_DATASET] = rbool(
            False)

    if selected_output[constants.workflow.OUTPUT_NEW_SCREEN]:
        inputs[constants.results.OUTPUT_ATTACH_NEW_SCREEN] = rbool(
            True)
        inputs[
            constants.results.OUTPUT_ATTACH_NEW_SCREEN_NAME
        ] = client.getInput(constants.workflow.OUTPUT_NEW_SCREEN)
        # duplicate screen name check
        inputs[
            constants.results.OUTPUT_ATTACH_NEW_SCREEN_DUPLICATE
        ] = client.getInput(constants.workflow.OUTPUT_DUPLICATES)
        # Forward explicit screen ID if provided (wins over name lookup)
        screen_id_override = client.getInput(constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID)
        if screen_id_override is not None:
            inputs[constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID] = screen_id_override

    else:
        inputs[constants.results.OUTPUT_ATTACH_NEW_SCREEN] = rbool(
            False)

    if selected_output[constants.workflow.OUTPUT_ATTACH]:
        inputs[
            constants.results.OUTPUT_ATTACH_OG_IMAGES
        ] = rbool(True)
    else:
        inputs[
            constants.results.OUTPUT_ATTACH_OG_IMAGES
        ] = rbool(False)

    if selected_output[constants.workflow.OUTPUT_CSV_TABLE]:
        inputs[
            constants.results.OUTPUT_ATTACH_TABLE
        ] = rbool(True)
        if parent_data_type == constants.transfer.DATA_TYPE_DATASET:
            inputs[
                constants.results.OUTPUT_ATTACH_TABLE_DATASET
            ] = rbool(True)
            inputs[
                constants.results.OUTPUT_ATTACH_TABLE_DATASET_ID
            ] = rlist(get_dataset_name_ids(conn, parent_id))
        else:
            inputs[
                constants.results.OUTPUT_ATTACH_TABLE_DATASET
            ] = rbool(False)
        if parent_data_type == constants.transfer.DATA_TYPE_PLATE:
            inputs[
                constants.results.OUTPUT_ATTACH_TABLE_PLATE
            ] = rbool(True)
            inputs[
                constants.results.OUTPUT_ATTACH_TABLE_PLATE_ID
            ] = rlist(get_plate_name_ids(conn, parent_id))
        else:
            inputs[
                constants.results.OUTPUT_ATTACH_TABLE_PLATE
            ] = rbool(False)
    else:
        inputs[
            constants.results.OUTPUT_ATTACH_TABLE
        ] = rbool(False)

    # Explicitly set all import params for provenance/reproducibility.
    # These match the defaults in the import scripts but are forwarded here
    # so they are always recorded in task metadata regardless of how the
    # import script is invoked. Run_Workflow owns the canonical values.
    inputs[constants.results.IMPORT_LABEL_ZARRS] = rbool(True)
    inputs[constants.results.IMPORT_ONLY_LABELS] = rbool(True)
    inputs[constants.results.TEST_WRITE_PERMISSIONS_ONLY] = rbool(False)

    # Forward file-output annotation flag + destinations (mirrors CSV table wiring exactly)
    if selected_output.get(constants.workflow.OUTPUT_ATTACH_FILE_OUTPUTS, False):
        inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS] = rbool(True)
        if parent_data_type == constants.transfer.DATA_TYPE_DATASET:
            inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_DATASET] = rbool(True)
            inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_DATASET_ID] = rlist(get_dataset_name_ids(conn, parent_id))
        else:
            inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_DATASET] = rbool(False)
        if parent_data_type == constants.transfer.DATA_TYPE_PLATE:
            inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_PLATE] = rbool(True)
            inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_PLATE_ID] = rlist(get_plate_name_ids(conn, parent_id))
        else:
            inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_PLATE] = rbool(False)
    else:
        inputs[constants.results.OUTPUT_ATTACH_FILE_OUTPUTS] = rbool(False)

    # Wait for Slurm Accounting to update
    wait_for_job_completion(slurmClient, slurm_job_id)

    logger.info(f"Running import script {script_id} with inputs: {inputs}")
    persist_dict = {key: unwrap(value) for key, value in inputs.items()}
    task_id = slurmClient.workflowTracker.add_task_to_workflow(
        wf_id,
        script_name,
        VERSION,
        {constants.transfer.IDS: unwrap(
            client.getInput(constants.transfer.IDS))},
        persist_dict
    )
    # Add task_id to inputs so Import_Results can update task status during import
    inputs["Task_ID"] = rstring(str(task_id))
    slurmClient.workflowTracker.start_task(task_id)
    rv, job = runOMEROScript(client, svc, script_id, inputs, slurmClient=slurmClient)
    
    # Check job status for success/failure
    job_status_id = None
    script_failed = False
    if job:
        try:
            job_status = job.getStatus()
            job_status_id = unwrap(job_status.getId())
            logger.debug(f"Import script job status ID: {job_status_id}")
            # Script failed if status is Error (6) or Cancelled (9)
            script_failed = job_status_id in [6, 9]
        except Exception as job_error:
            logger.warning(f"Could not get job status: {job_error}")
            script_failed = True  # Assume failure if we can't get status
    
    try:
        msg = unwrap(rv['Message'])

        # When a sub-script raises an exception, OMERO's runner catches it and
        # still completes the job (status != 6), but the script sets the Message
        # to "FAILED: ...".  Check both the job status flag AND the message prefix.
        # Use str() conversion to guard against unwrap() returning an OMERO
        # RStringI object instead of a plain Python str (which would make
        # isinstance(msg, str) silently return False).
        msg_str = str(msg) if msg is not None else ""
        message_failed = msg_str.startswith("FAILED:")
        if script_failed or message_failed:
            logger.error(f"Import script failed (status={job_status_id}, msg_prefix={'FAILED' if message_failed else 'ok'}): {msg_str}")
            slurmClient.workflowTracker.fail_task(task_id, f"Import failed: {msg_str}")
            wf_failed = True
            raise RuntimeError(f"Import script failed: {msg_str}")
        else:
            logger.info(f"Import script succeeded: {msg_str}")
            slurmClient.workflowTracker.complete_task(task_id, msg_str)
    except KeyError as e:
        error_msg = "No message returned from import script"
        logger.error(error_msg)
        slurmClient.workflowTracker.fail_task(task_id, error_msg)
        wf_failed = True
        raise RuntimeError(error_msg)
    return rv


def wait_for_job_completion(slurmClient, slurm_job_id, timeout=500, interval=15):
    """Wait for SLURM job completion by polling at regular intervals.

    Continuously polls the SLURM accounting system to determine when a job
    has completed, with configurable timeout and polling intervals.

    Args:
        slurmClient: SLURM client used to query job status.
        slurm_job_id: ID of the SLURM job to wait for.
        timeout (int): Maximum wait time in seconds. Defaults to 500.
        interval (int): Polling interval in seconds. Defaults to 15.

    Raises:
        TimeoutError: If job does not complete within timeout period.
    """
    start_time = timesleep.time()

    while True:
        # Check if the job is completed
        if str(slurm_job_id) in slurmClient.list_completed_jobs():
            return  # Job is complete, exit the function

        # Check if we've hit the timeout
        elapsed_time = timesleep.time() - start_time
        if elapsed_time > timeout:
            raise TimeoutError(
                f"Job {slurm_job_id} not found in Slurm Accounting within {timeout} seconds.")

        # Wait for the next interval before checking again
        timesleep.sleep(interval)


def get_project_name_ids(conn, parent_id):
    """Get formatted project name/ID strings for a given dataset.

    Args:
        conn: OMERO BlitzGateway connection.
        parent_id: Dataset ID to find parent projects for.

    Returns:
        list: Formatted strings of "ID: Name" for each project.
    """
    # Note different implementation XD
    # Call it 'legacy code', at version 1 already ;)
    projects = [rstring('%d: %s' % (d.id, d.getName()))
                for d in conn.getObjects(constants.transfer.DATA_TYPE_PROJECT,
                                         opts={'dataset': parent_id})]
    logger.debug(projects)
    return projects


def get_dataset_name_ids(conn, parent_id):
    """Get formatted dataset name/ID strings for given dataset IDs.

    Args:
        conn: OMERO BlitzGateway connection.
        parent_id: Dataset ID to retrieve information for.

    Returns:
        list: Formatted strings of "ID: Name" for each dataset.
    """
    dataset = [rstring('%d: %s' % (d.id, d.getName()))
               for d in conn.getObjects(constants.transfer.DATA_TYPE_DATASET,
                                        [parent_id])]
    logger.debug(dataset)
    return dataset


def get_plate_name_ids(conn, parent_id):
    """Get formatted plate name/ID strings for given plate IDs.

    Args:
        conn: OMERO BlitzGateway connection.
        parent_id: Plate ID to retrieve information for.

    Returns:
        list: Formatted strings of "ID: Name" for each plate.
    """
    plates = [rstring('%d: %s' % (d.id, d.getName()))
              for d in conn.getObjects(constants.transfer.DATA_TYPE_PLATE,
                                       [parent_id])]
    logger.debug(plates)
    return plates


def createFileName(client: omscripts.client, conn: BlitzGateway,
                   wf_id: UUID) -> str:
    """Generate a unique filename for workflow data transfer.

    Creates a compact folder name with a BIOMERO prefix and workflow UUID.

    Args:
        client: OMERO script client for parameter access.
        conn: OMERO BlitzGateway connection.
        wf_id: Workflow UUID for filename uniqueness.

    Returns:
        str: Generated filename in the form ``biomero_<uuid>``.

    """
    full_filename = f"biomero_{wf_id}"
    logger.debug("Filename: " + full_filename)
    return full_filename


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
