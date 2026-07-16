#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt

"""
BIOMERO SLURM Batched Workflow Execution Script

This script provides batch processing capabilities for running workflows
on SLURM clusters, allowing users to process multiple datasets efficiently
with automatic job management and result importing.

Key Features:
- Batch processing of multiple datasets/images/plates
- Configurable batch sizes to optimize cluster resource usage
- Automatic workflow parameter discovery from GitHub repositories
- Support for all available workflow versions on SLURM cluster
- Intelligent job scheduling and status monitoring
- Automatic result importing back to OMERO
- Email notifications for job completion/failure
- Comprehensive error handling and logging

Batch Processing Workflow:
1. Divide selected data into configurable batch sizes
2. Submit each batch as separate SLURM jobs
3. Monitor job execution and status
4. Import results back to OMERO upon completion
5. Provide comprehensive status reporting

This script is designed for processing large datasets where individual
job submission would be inefficient. It complements SLURM_Run_Workflow.py
by adding batch processing capabilities.

Authors: Torec Luik, OMERO Team
Institution: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

from __future__ import print_function
import sys
import os
import datetime
import omero
from omero.grid import JobParams
from omero.rtypes import rstring, unwrap, rlong, rlist, robject
from omero.gateway import BlitzGateway
import omero.util.script_utils as script_utils
import omero.scripts as omscripts
from biomero import SlurmClient, constants
import logging
from itertools import islice
import time as timesleep
import pprint

logger = logging.getLogger(__name__)


PROC_SCRIPTS = [constants.RUN_WF_SCRIPT]
DATATYPES = [rstring(constants.transfer.DATA_TYPE_DATASET),
             rstring(constants.transfer.DATA_TYPE_IMAGE),
             rstring(constants.transfer.DATA_TYPE_PLATE)]
OUTPUT_OPTIONS = [constants.workflow.OUTPUT_RENAME,
                  constants.workflow.OUTPUT_PARENT,
                  constants.workflow.OUTPUT_NEW_DATASET,
                  constants.workflow.OUTPUT_NEW_SCREEN,
                  constants.workflow.OUTPUT_ATTACH,
                  constants.workflow.OUTPUT_CSV_TABLE,
                  constants.workflow.OUTPUT_ATTACH_FILE_OUTPUTS]

# Version constant for easy version management
VERSION = "2.8.0"


def runScript():
    """Main entry point for SLURM batched workflow execution script.

    Orchestrates batch processing of workflows on SLURM clusters by dividing
    selected datasets into manageable batches, submitting jobs, monitoring
    execution, and importing results back to OMERO.

    The function handles:
        - SLURM client setup and validation
        - Dynamic workflow parameter discovery
        - Batch size optimization and job submission
        - Job status monitoring and error handling
        - Result importing and user notification
        - Comprehensive workflow tracking

    Batch processing improves efficiency for large datasets by optimizing
    cluster resource usage and reducing individual job overhead.
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
        params.description = f'''Script to run workflows on slurm
        cluster, in batches.

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
        params.name = 'Slurm Workflow (Batched)'
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
                default=constants.transfer.DATA_TYPE_IMAGE),
            omscripts.List(
                constants.transfer.IDS, optional=False, grouping="01.2",
                description="List of Dataset IDs or Image IDs").ofType(
                    rlong(0)),
            omscripts.Bool(constants.workflow.EMAIL, grouping="01.3",
                           description=email_descr,
                           default=True),
            omscripts.Int(constants.workflow_batched.BATCH_SIZE,
                          optional=False, grouping="01.4",
                          description="Number of images to send to 1 slurm job",
                          default=32),
            omscripts.Bool(constants.workflow.USE_ZARR_FORMAT, grouping="01.5",
                           description="Skip TIFF conversion and run "
                           "workflows directly on ZARR data (experimental). "
                           "Use this for workflows that support ZARR input.",
                           default=False),
            omscripts.String(
                            constants.transfer.OME_VERSION, grouping="01.5.1",
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
                           default=False),
            omscripts.Bool(constants.workflow.OUTPUT_ATTACH,
                           optional=True,
                           grouping="02.4",
                           description="Attach all resulting images to original images as attachments",
                           default=False),
            omscripts.String(constants.workflow.OUTPUT_NEW_DATASET,
                             optional=True,
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
                           description="If a dataset already matches this name, still make a new one? (Note: for batched runs this is always False to share one dataset)",
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
                           default=False)
        ]
        # Generate script parameters for all our workflows
        (wf_versions, _) = slurmClient.get_all_image_versions_and_data_files()
        na = ["Not Available!"]
        _workflow_params = {}
        _workflow_file_params = {}
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
            # Main parameter to select this workflow for execution
            descriptor = slurmClient.generic_descriptor_from_github(wf)
            wf_descr = descriptor['description']
            # Build value-choices lookup from the descriptor (scoped per wf,
            # so param name collisions across workflows are not an issue)
            value_choices_map = {
                inp['id']: [rstring(v) for v in inp['value-choices']]
                for inp in descriptor.get('inputs', [])
                if inp.get('value-choices')
            }
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
                    optional=param['optional'],
                    **({"values": value_choices_map[k]} if k in value_choices_map else {})
                )
                # To allow 'duplicate' params, add the wf to uniqueify them
                # we have to remove this prefix later again, before passing
                # them to BIOMERO (as the wf will not understand these params)
                omtype_param._name = f"{wf}_|_{omtype_param._name}"
                input_list.append(omtype_param)
            # File-attachment params: exposed as List(Long) of OMERO FileAnnotation IDs
            # They live under a FILE_ prefix so SLURM_Run_Workflow can tell them apart.
            num_reg = len(_workflow_params[wf])
            for fp_incr, (k, fp) in enumerate(_workflow_file_params[wf].items()):
                fmt_str = ", ".join(fp['format']) if fp['format'] else "any"
                fp_param = omscripts.List(
                    f"{wf}_|_FILE_{k}",
                    optional=True,
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
            p._name: p for p in input_list
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
        # 1. Split data into batches
        # 2. Run (omero) workflow script per batch
        # 3. Track (omero) jobs, join log outputs
        try:
            # log_string will be output in the Omero Web UI
            UI_messages = {
                'Message': [],
                'File_Annotation': []
            }
            errormsg = None
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
                        type(selected_op) == list and
                        constants.workflow.NO in selected_op):
                    # Still treat as selected if an explicit ID override was given
                    if output_option == constants.workflow.OUTPUT_NEW_DATASET and dataset_id_override:
                        selected_output[output_option] = True
                    elif output_option == constants.workflow.OUTPUT_NEW_SCREEN and screen_id_override:
                        selected_output[output_option] = True
                    else:
                        selected_output[output_option] = False
                else:
                    selected_output[output_option] = True
            if not any(selected_output.values()):
                errormsg = "ERROR: Please select at least 1 output method!"
                client.setOutput("Message", rstring(errormsg))
                raise ValueError(errormsg)
            else:
                logger.info(f"Output options chosen: {selected_output}")

            # Connect to Omero
            conn = BlitzGateway(client_obj=client)
            conn.SERVICE_OPTS.setOmeroGroup(-1)
            svc = conn.getScriptService()
            # Find script
            scripts = svc.getScripts()
            script_ids = [unwrap(s.id)
                          for s in scripts if unwrap(
                              s.getName()) in PROC_SCRIPTS]
            if not script_ids:
                raise ValueError(
                    f"Cannot process workflows: scripts ({PROC_SCRIPTS})\
                        not found in ({[unwrap(s.getName()) for s in scripts]}) ")
            script_id = script_ids[0]  # go away with your list...

            user = conn.getUserId()
            group = conn.getGroupFromContext().id
            # Start tracking the workflow on a unique ID
            wf_id = slurmClient.workflowTracker.initiate_workflow(
                params.name,
                "\n".join([params.description, params.version]),
                user,
                group
            )

            batch_size = unwrap(client.getInput(constants.workflow_batched.BATCH_SIZE))
            data_ids = unwrap(client.getInput(constants.transfer.IDS))
            data_type = unwrap(client.getInput(constants.transfer.DATA_TYPE))
            script_params = client.getInputs(unwrap=True)
            inputs = client.getInputs()

            # Resolve input image/plate IDs
            if data_type == constants.transfer.DATA_TYPE_IMAGE:
                image_ids = data_ids
            elif data_type == constants.transfer.DATA_TYPE_DATASET:
                objects, log_message = script_utils.get_objects(conn, script_params)
                images = []
                for ds in objects:
                    images.extend(list(ds.listChildren()))
                if not images:
                    raise ValueError(f"No image found in dataset(s) {data_ids}")
                image_ids = [img.id for img in images]
            elif data_type == constants.transfer.DATA_TYPE_PLATE:
                objects, log_message = script_utils.get_objects(conn, script_params)
                output_screen = unwrap(client.getInput(constants.workflow.OUTPUT_NEW_SCREEN))
                screen_id_override = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID))
                use_screen_output = (output_screen and output_screen != constants.workflow.NO) or screen_id_override
                if use_screen_output:
                    image_ids = [plate.id for plate in objects]
                else:
                    images = []
                    wells = []
                    for plate in objects:
                        wells.extend(list(plate.listChildren()))
                    for well in wells:
                        nr_samples = well.countWellSample()
                        for index in range(0, nr_samples):
                            image = well.getImage(index)
                            images.append(image)
                    if not images:
                        raise ValueError(f"No image found in plate(s) {data_ids}")
                    image_ids = [img.id for img in images]
            else:
                raise ValueError(f"Not recognized input data: {data_type}")

            # Gather all inputs and parameters to store in the launcher task
            launcher_params = {}
            for k in inputs:
                launcher_params[k] = unwrap(client.getInput(k))

            selected_wfs = [wf_name for wf_name in workflows if unwrap(client.getInput(wf_name))]
            launcher_params["workflows"] = selected_wfs
            launcher_params["batch_size"] = batch_size
            launcher_params["IDS"] = image_ids

            for wf_name in selected_wfs:
                launcher_params[f"wf_params_{wf_name}"] = _workflow_params[wf_name]
                launcher_params[f"wf_file_params_{wf_name}"] = _workflow_file_params[wf_name]

            output_settings = {
                "DATA_TYPE": unwrap(client.getInput(constants.transfer.DATA_TYPE)),
                "IDS": image_ids,
                "OUTPUT_PARENT": unwrap(client.getInput(constants.workflow.OUTPUT_PARENT)),
                "OUTPUT_RENAME": unwrap(client.getInput(constants.workflow.OUTPUT_RENAME)),
                "OUTPUT_NEW_DATASET": unwrap(client.getInput(constants.workflow.OUTPUT_NEW_DATASET)),
                "OUTPUT_NEW_SCREEN": unwrap(client.getInput(constants.workflow.OUTPUT_NEW_SCREEN)),
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
                'SLURM_Run_Workflow_Batched.py',
                VERSION,
                image_ids,
                launcher_params
            )

            msg = "Workflow batch initialization complete. Your jobs have been queued and will be processed in the background. You can safely close this browser tab; results will be imported automatically."
            client.setOutput("Message", rstring(msg))

        except Exception as e:
            logger.error("Exception in batch wf: ", exc_info=True)
            if 'wf_id' in locals() and wf_id is not None:
                try:
                    slurmClient.workflowTracker.fail_workflow(wf_id, str(e))
                except Exception as db_e:
                    logger.error(f"Database error marking workflow {wf_id} as failed: {db_e}")
            client.setOutput("Message", rstring(f"ERROR: {str(e)}"))
            raise
        finally:
            client.closeSession()


def chunk(lst, n):
    """Yield successive n-sized chunks from lst."""
    it = iter(lst)
    return iter(lambda: tuple(islice(it, n)), ())


def add_batch_supervisor_metadata_for_batch(conn, slurmClient, supervisor_wf_id, batch_index, batch_ids, total_batches, base_inputs, effective_type=None):
    """Add batch supervisor metadata to output objects from a single completed batch.

    For image workflows: annotates output images found via annotation query.
    For plate workflows: annotates output plates found via annotation query.

    Args:
        conn: OMERO BlitzGateway connection
        slurmClient: SlurmClient for accessing workflow tracking
        supervisor_wf_id: UUID of the supervisor (batched) workflow
        batch_index: Index of this specific batch
        batch_ids: List of input object IDs (plate IDs or image IDs) for this batch
        total_batches: Total number of batches
        base_inputs: Base input parameters used for all batches
        effective_type: The data type actually sent to sub-jobs (e.g. 'Plate' or 'Image')
    """
    try:
        import ezomero
        logger.debug(
            f"Adding batch supervisor metadata for batch {batch_index + 1}/{total_batches}")

        is_plate_mode = (effective_type == constants.transfer.DATA_TYPE_PLATE)

        if is_plate_mode:
            # Plate mode: find output plates via annotation query (same as image mode but for Plate objects).
            object_type = "Plate"
            ids_key = "Input_Plate_IDs"
            output_objects = find_output_plates_for_batch(conn, batch_ids)
        else:
            # Image mode: find output images via annotation query.
            object_type = "Image"
            ids_key = "Input_Image_IDs"
            output_objects = find_output_images_for_batch(conn, batch_ids)

        if output_objects:
            batch_supervisor_dict = {
                'Batch_Supervisor_Workflow_ID': str(supervisor_wf_id),
                'Batch_Index': str(batch_index + 1),  # 1-indexed for users
                'Total_Batches': str(total_batches),
                ids_key: ', '.join(map(str, batch_ids[:10])) + ('...' if len(batch_ids) > 10 else '')
            }

            for obj_id in output_objects:
                try:
                    map_ann_id = ezomero.post_map_annotation(
                        conn=conn,
                        object_type=object_type,
                        object_id=obj_id,
                        kv_dict=batch_supervisor_dict,
                        ns="biomero/workflow/batch",
                        across_groups=False
                    )
                    logger.debug(
                        f"Added batch supervisor metadata to {object_type} {obj_id}")
                except Exception as e:
                    logger.warning(
                        f"Failed to add batch supervisor metadata to {object_type} {obj_id}: {e}")

            logger.debug(
                f"Added batch supervisor metadata to {len(output_objects)} {object_type}(s)")
        else:
            logger.warning(
                f"No output {object_type}(s) found for batch {batch_index + 1}")

    except Exception as e:
        logger.error(
            f"Error adding batch supervisor metadata for batch {batch_index + 1}: {e}")
        raise


def find_output_plates_for_batch(conn, input_plate_ids):
    """Find output plates created by a batch using input plate IDs.

    Args:
        conn: OMERO BlitzGateway connection
        input_plate_ids: List of input plate IDs for this batch

    Returns:
        List of output plate IDs created by this batch
    """
    try:
        query_service = conn.getQueryService()

        import datetime
        from omero.rtypes import rtime
        cutoff = datetime.datetime.now(
            datetime.timezone.utc) - datetime.timedelta(minutes=5)
        params = omero.sys.ParametersI()
        params.add("cutoff", rtime(int(cutoff.timestamp() * 1000)))

        like_conditions = []
        for i, plate_id in enumerate(input_plate_ids):
            param_name = f"id_{i}"
            like_conditions.append(f"mv.value like :{param_name}")
            params.addString(param_name, f"%{plate_id}%")

        like_clause = " and ".join(like_conditions)

        hql = f"""
        select distinct
            plate.id as output_plate_id,
            ma.id as annotation_id,
            mv.value as input_data,
            ev.time as creation_time
        from Plate plate
        join plate.annotationLinks l
        join l.child ma
        join ma.details.creationEvent ev
        join ma.mapValue mvName
        join ma.mapValue mv
        where ma.ns = 'biomero/workflow/task/SLURM_Get_Results.py'
          and mvName.name = 'Name'
          and (mvName.value = 'SLURM_Get_Results.py' or mvName.value = 'SLURM_Import_Results.py')
          and mv.name = 'Input_Data'
          and ev.time > :cutoff
          and ({like_clause})
        """

        results = query_service.projection(hql, params)

        output_plate_ids = [result[0].val for result in results]
        logger.debug(
            f"Found {len(output_plate_ids)} output plates for batch with {len(input_plate_ids)} input plates")
        return output_plate_ids

    except Exception as e:
        logger.error(f"Error finding output plates for batch: {e}")
        return []


def find_output_images_for_batch(conn, input_image_ids):
    """Find output images created by a batch using input IDs.

    Args:
        conn: OMERO BlitzGateway connection
        input_image_ids: List of input image IDs for this batch

    Returns:
        List of output image IDs created by this batch
    """
    try:
        # Search for SLURM_Get_Results.py task annotations (optimized with time constraint)
        query_service = conn.getQueryService()

        # Proper OMERO parameter initialization and time handling
        import datetime
        from omero.rtypes import rtime

        # Very tight time window - batch supervisor runs immediately after completion
        # Only look for SLURM_Get_Results annotations created in last 5 minutes
        cutoff = datetime.datetime.now(
            datetime.timezone.utc) - datetime.timedelta(minutes=5)
        params = omero.sys.ParametersI()
        params.add("cutoff", rtime(int(cutoff.timestamp() * 1000)))

        # Add LIKE conditions for each input ID
        like_conditions = []
        for i, img_id in enumerate(input_image_ids):
            param_name = f"id_{i}"
            like_conditions.append(f"mv.value like :{param_name}")
            params.addString(param_name, f"%{img_id}%")

        like_clause = " and ".join(like_conditions)

        # The WORKING HQL query - finds output images linked to SLURM Import/Get Results annotations
        # that match our batch input IDs and were created recently
        hql = f"""
        select distinct
            img.id as output_image_id,
            ma.id as annotation_id,
            mv.value as input_data,
            ev.time as creation_time
        from Image img
        join img.annotationLinks l
        join l.child ma
        join ma.details.creationEvent ev
        join ma.mapValue mvName
        join ma.mapValue mv
        where ma.ns = 'biomero/workflow/task/SLURM_Get_Results.py'
          and mvName.name = 'Name'
          and (mvName.value = 'SLURM_Get_Results.py' or mvName.value = 'SLURM_Import_Results.py')
          and mv.name = 'Input_Data'
          and ev.time > :cutoff
          and ({like_clause})
        """

        results = query_service.projection(hql, params)

        output_image_ids = []
        for result in results:
            img_id = result[0].val
            ann_id = result[1].val
            input_data = result[2].val

            output_image_ids.append(img_id)

        logger.debug(
            f"Found {len(output_image_ids)} output images for batch with {len(input_image_ids)} input images")
        return output_image_ids

    except Exception as e:
        logger.error(f"Error finding output images for batch: {e}")
        return []


def is_recent_and_matching_settings(conn, img_id, output_settings):
    """Check if the task is recent and has matching output settings.

    Args:
        conn: OMERO BlitzGateway connection  
        img_id: Image ID to check
        output_settings: Expected output settings

    Returns:
        Boolean indicating if this is a recent matching task
    """
    try:
        # Get annotations for this image to check timing and settings
        query_service = conn.getQueryService()
        params = omero.sys.Parameters()
        params.map = {"img_id": omero.rtypes.rlong(img_id)}

        # Get task annotations with Created_On timestamp
        hql = """
        SELECT mv.name, mv.value
        FROM Image img
        JOIN img.annotationLinks ial
        JOIN ial.child ann  
        JOIN ann.mapValue mv
        WHERE img.id = :img_id
        AND TYPE(ann) = MapAnnotation
        AND (mv.name = 'Created_On' OR mv.name LIKE 'Param_%')
        """

        results = query_service.projection(hql, params, conn.SERVICE_OPTS)

        # Check if created recently (within last 2 hours)
        import datetime
        now = datetime.datetime.now(datetime.timezone.utc)
        two_hours_ago = now - datetime.timedelta(hours=2)

        is_recent = False
        settings_match = True  # Assume match unless we find a mismatch

        for result in results:
            name = result[0].val
            value = result[1].val

            if name == 'Created_On':
                try:
                    iso_value = value.replace('Z', '+00:00')
                    created_time = datetime.datetime.fromisoformat(iso_value)
                    is_recent = created_time > two_hours_ago
                except (ValueError, AttributeError):
                    is_recent = True  # Default to true if we can't parse

            # Check output settings match
            elif name.startswith('Param_') and output_settings:
                param_name = name.replace('Param_', '')
                if param_name in output_settings:
                    expected = str(output_settings[param_name])
                    if str(value) != expected:
                        settings_match = False
                        break

        return is_recent and settings_match

    except Exception as e:
        logger.debug(f"Error checking recent settings for image {img_id}: {e}")
        return True  # Default to true if we can't check


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
