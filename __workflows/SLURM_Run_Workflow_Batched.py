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
                  constants.workflow.OUTPUT_ATTACH,
                  constants.workflow.OUTPUT_CSV_TABLE]

# Version constant for easy version management
VERSION = "2.1.0"


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
            omscripts.Bool(constants.workflow.SELECT_IMPORT,
                           optional=False,
                           grouping="02",
                           description="Select one or more options below:",
                           default=True),
            omscripts.String(constants.workflow.OUTPUT_RENAME,
                             optional=True,
                             grouping="02.7",
                             description="A new name for the imported images. You can use variables {original_file} and {ext}. E.g. {original_file}NucleiLabels.{ext}",
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
            omscripts.Bool(constants.workflow.OUTPUT_CSV_TABLE,
                           optional=False,
                           grouping="02.8",
                           description="Any resulting csv files will be added as OMERO.table to parent dataset/plate",
                           default=True)
        ]
        # Generate script parameters for all our workflows
        (wf_versions, _) = slurmClient.get_all_image_versions_and_data_files()
        na = ["Not Available!"]
        _workflow_params = {}
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
            _workflow_params[wf] = slurmClient.get_workflow_parameters(
                wf)
            # Main parameter to select this workflow for execution
            json_descriptor = slurmClient.pull_descriptor_from_github(wf)
            wf_descr = json_descriptor['description']
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
                # Convert the parameter from cy(tomine)type to om(ero)type
                omtype_param = slurmClient.convert_cytype_to_omtype(
                    param["cytype"],
                    param["default"],
                    param["name"],
                    description=param["description"],
                    default=param["default"],
                    grouping=f"{parameter_group}.{param_incr+1}",
                    optional=param['optional']
                )
                # To allow 'duplicate' params, add the wf to uniqueify them
                # we have to remove this prefix later again, before passing
                # them to BIOMERO (as the wf will not understand these params)
                omtype_param._name = f"{wf}_|_{omtype_param._name}"
                input_list.append(omtype_param)
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
            for output_option in OUTPUT_OPTIONS:
                selected_op = unwrap(client.getInput(output_option))
                if (not selected_op) or (
                    selected_op == constants.workflow.NO) or (
                        type(selected_op) == list and
                        constants.workflow.NO in selected_op):
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
            logger.info('''
            # --------------------------------------------
            # :: 1. Split data into batches ::
            # --------------------------------------------
            ''')
            batch_size = unwrap(client.getInput(
                constants.workflow_batched.BATCH_SIZE))
            data_ids = unwrap(client.getInput(constants.transfer.IDS))
            data_type = unwrap(client.getInput(constants.transfer.DATA_TYPE))
            script_params = client.getInputs(unwrap=True)
            inputs = client.getInputs()
            if data_type == constants.transfer.DATA_TYPE_IMAGE:
                image_ids = data_ids
            elif data_type == constants.transfer.DATA_TYPE_DATASET:
                objects, log_message = script_utils.get_objects(conn,
                                                                script_params)
                UI_messages['Message'].extend(
                    [log_message])
                images = []
                for ds in objects:
                    images.extend(list(ds.listChildren()))
                if not images:
                    error = f"No image found in dataset(s) {data_ids} / {objects}"
                    UI_messages['Message'].extend(
                        [error])
                    raise ValueError(error)
                else:
                    image_ids = [img.id for img in images]
                    inputs[constants.transfer.DATA_TYPE] = rstring(
                        constants.transfer.DATA_TYPE_IMAGE)
            elif data_type == constants.transfer.DATA_TYPE_PLATE:
                objects, log_message = script_utils.get_objects(conn,
                                                                script_params)
                UI_messages['Message'].extend(
                    [log_message])
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
                    error = f"No image found in plate(s) {data_ids} / {objects}"
                    UI_messages['Message'].extend(
                        [error])
                    raise ValueError(error)
                else:
                    image_ids = [img.id for img in images]
                    inputs[constants.transfer.DATA_TYPE] = rstring(
                        constants.transfer.DATA_TYPE_IMAGE)
            else:
                raise ValueError(f"Not recognized input data: {data_type}. \
                    Expected one of {DATATYPES}")
            batch_ids = chunk(image_ids, batch_size)
            batch_ids_list = list(batch_ids)  # Convert generator to list
            logger.info(
                f"Created {len(batch_ids_list)} batches from {len(image_ids)} images (batch size: {batch_size})")

            # For batching, ensure we write to 1 dataset, not 1 for each batch
            inputs[constants.workflow.OUTPUT_DUPLICATES] = omscripts.rbool(
                False)
            processes = {}
            remaining_batches = {i: b for i, b in enumerate(batch_ids_list)}
            logger.info("#--------------------------------------------#")
            logger.info(f"Batch Size: {batch_size}")
            logger.info(f"Total items: {len(image_ids)}")
            formatted_batches = pprint.pformat(remaining_batches,
                                               depth=2,
                                               compact=True)
            logger.info(f"Batches: {formatted_batches}")
            logger.info("#--------------------------------------------#")

            logger.info('''
            # --------------------------------------------
            # :: 2. Run workflow(s) per batch ::
            # --------------------------------------------
            ''')
            task_ids = {}
            logger.info(
                f"Submitting {len(remaining_batches)} batch jobs at {datetime.datetime.now()}")
            for i, batch in remaining_batches.items():
                inputs[constants.transfer.IDS] = rlist([rlong(x)
                                                        # override ids
                                                        for x in batch])

                persist_dict = {key: unwrap(value)
                                for key, value in inputs.items()}
                # persist_dict[constants.transfer.IDS] = [unwrap(value) for value in persist_dict[constants.transfer.IDS]]
                script_id = int(script_id)
                # The last parameter is how long to wait as an RInt
                proc = svc.runScript(script_id, inputs, None)
                processes[i] = proc
                omero_job_id = proc.getJob()._id
                logger.debug(f"Batch {i+1}: Started OMERO job {omero_job_id}")
                # TODO: don't do this, use a different domain (driven design)
                # Now, views will listen to events and not know what they get
                task_id = slurmClient.workflowTracker.add_task_to_workflow(
                    wf_id,
                    PROC_SCRIPTS[0],
                    params.version,
                    persist_dict[constants.transfer.IDS],
                    persist_dict
                )
                task_ids[i] = task_id
                slurmClient.workflowTracker.start_task(task_id)
                # slurmClient.workflowTracker.add_job_id(task_id, unwrap(omero_job_id))

            logger.info('''
            # --------------------------------------------
            # :: 3. Track all the batch jobs ::
            # --------------------------------------------
            ''')
            finished = []
            # Check if any tasks failed by tracking failure status
            wf_failed = False
            try:
                # 4. Poll results
                current_batch_being_processed = None
                while remaining_batches:
                    # loop the remaining processes
                    for i, batch in remaining_batches.items():
                        current_batch_being_processed = i
                        task_id = task_ids[i]
                        process = processes[i]
                        try:
                            return_code = process.poll()
                            logger.debug(
                                f"Process {process} polled: {return_code}")
                        except Exception as poll_e:
                            logger.error(
                                f"Error polling process for batch {i}: {poll_e}")
                            raise

                        try:
                            # TODO don't
                            slurmClient.workflowTracker.update_task_status(
                                task_id,
                                f"{return_code}")
                        except Exception as status_e:
                            logger.error(
                                f"Error updating task status for batch {i}, task {task_id}: {status_e}")
                            raise

                        if return_code:  # None if not finished
                            try:
                                results = process.getResults(
                                    0)  # 0 ms; RtypeDict
                                if 'Message' in results:
                                    result_msg = results['Message'].getValue()
                                    logger.info(result_msg)
                                    UI_messages['Message'].extend(
                                        [f">> Batch {i}: ",
                                         result_msg])

                                if 'File_Annotation' in results:
                                    UI_messages['File_Annotation'].append(
                                        results['File_Annotation'].getValue())
                            except Exception as results_e:
                                logger.error(
                                    f"Error getting results for batch {i}: {results_e}")
                                raise

                            finished.append(i)
                            if return_code.getValue() == 0:
                                msg = f"Batch {i} - [{remaining_batches[i]}] finished."
                                logger.info(
                                    msg)
                                UI_messages['Message'].extend(
                                    [msg])
                                try:
                                    slurmClient.workflowTracker.complete_task(
                                        task_id, result_msg + msg)
                                except Exception as complete_e:
                                    logger.error(
                                        f"Error completing task {task_id} for batch {i}: {complete_e}")
                                    raise

                                # Add batch supervisor metadata for this completed batch
                                try:
                                    add_batch_supervisor_metadata_for_batch(
                                        conn, slurmClient, wf_id, i, batch, len(batch_ids_list), inputs)
                                except Exception as e:
                                    logger.warning(
                                        f"Failed to add batch supervisor metadata for batch {i}: {e}")
                            else:
                                msg = f"Batch {i} - [{remaining_batches[i]}] failed!"
                                logger.info(
                                    msg)
                                UI_messages['Message'].extend(
                                    [msg])
                                try:
                                    slurmClient.workflowTracker.fail_task(
                                        task_id, "Batch failed")
                                except Exception as fail_e:
                                    logger.error(
                                        f"Error failing task {task_id} for batch {i}: {fail_e}")
                                    raise
                                wf_failed = True
                        else:
                            pass

                    # clear out our tracking list, to end while loop at some point
                    for i in finished:
                        del remaining_batches[i]
                    finished = []
                    # wait for 10 seconds before checking again
                    conn.keepAlive()  # keep connection alive w/ omero/ice
                    timesleep.sleep(10)
            except Exception as e:
                logger.error(
                    f"Exception in batch monitoring loop for batch {current_batch_being_processed}: {e}")
                # Mark the workflow as failed due to monitoring exception
                wf_failed = True
                # Re-raise the exception to ensure it's not silently ignored
                raise
            finally:
                logger.info('''
                # ====================================
                # :: Finished all batches ::
                # ====================================
                ''')
                for proc in processes.values():
                    proc.close(False)  # stop the scripts

            # 7. Script output
            client.setOutput("Message",
                             rstring("\n".join(UI_messages['Message'])))

            if wf_failed:
                slurmClient.workflowTracker.fail_workflow(
                    wf_id, "One or more workflow batches failed")
            else:
                slurmClient.workflowTracker.complete_workflow(wf_id)

            for i, ann in enumerate(UI_messages['File_Annotation']):
                client.setOutput(f"File_Annotation_{i}", robject(ann))
        finally:
            client.closeSession()


def chunk(lst, n):
    """Yield successive n-sized chunks from lst."""
    it = iter(lst)
    return iter(lambda: tuple(islice(it, n)), ())


def add_batch_supervisor_metadata_for_batch(conn, slurmClient, supervisor_wf_id, batch_index, batch_image_ids, total_batches, base_inputs):
    """Add batch supervisor metadata to images from a single completed batch.

    This function finds output images by matching the input image IDs and output settings
    that were used for this specific batch, then adds supervisor workflow metadata.

    Args:
        conn: OMERO BlitzGateway connection
        slurmClient: SlurmClient for accessing workflow tracking
        supervisor_wf_id: UUID of the supervisor (batched) workflow
        batch_index: Index of this specific batch
        batch_image_ids: List of input image IDs for this batch
        total_batches: Total number of batches
        base_inputs: Base input parameters used for all batches
    """
    try:
        logger.debug(
            f"Adding batch supervisor metadata for batch {batch_index + 1}/{total_batches}")

        # Extract the output settings that were used for all batches
        output_settings = {}
        if constants.workflow.OUTPUT_NEW_DATASET in base_inputs:
            output_settings['New Dataset'] = unwrap(
                base_inputs[constants.workflow.OUTPUT_NEW_DATASET])
        if constants.workflow.OUTPUT_DUPLICATES in base_inputs:
            output_settings['Allow duplicate?'] = unwrap(
                base_inputs[constants.workflow.OUTPUT_DUPLICATES])

        # Find the corresponding output images for this batch
        output_images = find_output_images_for_batch(
            conn, batch_image_ids)

        if output_images:
            # Add batch supervisor metadata to found images
            batch_supervisor_dict = {
                'Batch_Supervisor_Workflow_ID': str(supervisor_wf_id),
                'Batch_Index': str(batch_index + 1),  # 1-indexed for users
                'Total_Batches': str(total_batches),
                # First 10 IDs
                'Input_Image_IDs': ', '.join(map(str, batch_image_ids[:10])) + ('...' if len(batch_image_ids) > 10 else '')
            }

            for img_id in output_images:
                try:
                    import ezomero
                    map_ann_id = ezomero.post_map_annotation(
                        conn=conn,
                        object_type="Image",
                        object_id=img_id,
                        kv_dict=batch_supervisor_dict,
                        ns="biomero/workflow/batch",
                        across_groups=False
                    )
                    logger.debug(
                        f"Added batch supervisor metadata to image {img_id}")
                except Exception as e:
                    logger.warning(
                        f"Failed to add batch supervisor metadata to image {img_id}: {e}")

            logger.debug(
                f"Added batch supervisor metadata to {len(output_images)} images")
        else:
            logger.warning(
                f"No output images found for batch {batch_index + 1}")

    except Exception as e:
        logger.error(
            f"Error adding batch supervisor metadata for batch {batch_index + 1}: {e}")
        raise


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

        # The WORKING HQL query - finds output images linked to SLURM_Get_Results annotations
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
          and mvName.value = 'SLURM_Get_Results.py'
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
