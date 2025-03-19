#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# Example OMERO.script to run multiple segmentation images on Slurm.

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


def runScript():
    """
    The main entry point of the script
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
        params.version = "2.0.0-alpha.4"
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
            logger.info(
                f"\nGenerated these parameters for {wf} descriptors:\n")
            for param_incr, (k, param) in enumerate(_workflow_params[
                    wf].items()):
                logger.debug(f"{param_incr}, {k}, {param}")
                logger.info(param)
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
                logger.info(f"{wf}, {selected}, {selected_version}")
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
                    logger.info(
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

            # For batching, ensure we write to 1 dataset, not 1 for each batch
            inputs[constants.workflow.OUTPUT_DUPLICATES] = omscripts.rbool(
                False)
            processes = {}
            remaining_batches = {i: b for i, b in enumerate(batch_ids)}
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
            logger.info(f"Starting batch scripts at {datetime.datetime.now()}")
            for i, batch in remaining_batches.items():
                inputs[constants.transfer.IDS] = rlist([rlong(x)
                                                        # override ids
                                                        for x in batch])
                persist_dict = {key: unwrap(value) for key, value in inputs.items()}
                # persist_dict[constants.transfer.IDS] = [unwrap(value) for value in persist_dict[constants.transfer.IDS]]
                script_id = int(script_id)
                # The last parameter is how long to wait as an RInt
                proc = svc.runScript(script_id, inputs, None)
                processes[i] = proc
                omero_job_id = proc.getJob()._id
                logger.info(f"Started script {script_id} at\
                    {datetime.datetime.now()}:\
                    Omero Job ID {omero_job_id}")
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
            try:
                # 4. Poll results
                while remaining_batches:
                    logger.debug(f"Remaining batches: {remaining_batches}")
                    # loop the remaining processes
                    for i, batch in remaining_batches.items():
                        task_id = task_ids[i]
                        process = processes[i]
                        return_code = process.poll()
                        logger.debug(
                            f"Process {process} polled: {return_code}")
                        # TODO don't
                        slurmClient.workflowTracker.update_task_status(
                            task_id, 
                            f"{return_code}")
                        if return_code:  # None if not finished
                            results = process.getResults(0)  # 0 ms; RtypeDict
                            if 'Message' in results:
                                result_msg = results['Message'].getValue()
                                logger.info(result_msg)
                                UI_messages['Message'].extend(
                                    [f">> Batch {i}: ",
                                     result_msg])

                            if 'File_Annotation' in results:
                                UI_messages['File_Annotation'].append(
                                    results['File_Annotation'].getValue())

                            finished.append(i)
                            if return_code.getValue() == 0:
                                msg = f"Batch {i} - [{remaining_batches[i]}] finished."
                                logger.info(
                                    msg)
                                UI_messages['Message'].extend(
                                    [msg])
                                slurmClient.workflowTracker.complete_task(
                                    task_id, result_msg + msg)
                            else:
                                msg = f"Batch {i} - [{remaining_batches[i]}] failed!"
                                logger.info(
                                    msg)
                                UI_messages['Message'].extend(
                                    [msg])
                                slurmClient.workflowTracker.fail_task(
                                    task_id, "Batch failed")
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
                logger.warning(e)
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
            slurmClient.workflowTracker.complete_workflow(wf_id)
            for i, ann in enumerate(UI_messages['File_Annotation']):
                client.setOutput(f"File_Annotation_{i}", robject(ann))
        finally:
            client.closeSession()


def chunk(lst, n):
    """Yield successive n-sized chunks from lst."""
    it = iter(lst)
    return iter(lambda: tuple(islice(it, n)), ())


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
