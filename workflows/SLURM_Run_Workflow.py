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
from uuid import UUID
import omero
from omero.grid import JobParams
from omero.rtypes import rstring, unwrap, rlong, rbool, rlist
from omero.gateway import BlitzGateway
import omero.scripts as omscripts
import datetime
from biomero import SlurmClient, constants
import logging
import time as timesleep
from paramiko import SSHException

logger = logging.getLogger(__name__)

EXPORT_SCRIPTS = [constants.IMAGE_EXPORT_SCRIPT]
IMPORT_SCRIPTS = [constants.IMAGE_IMPORT_SCRIPT]
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
        params.version = "1.14.0"
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
            omscripts.Bool(constants.workflow.OUTPUT_DUPLICATES,
                           optional=True,
                           grouping="02.6",
                           description="If a dataset already matches this name, still make a new one?",
                           default=False),
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
            json_descriptor = slurmClient.pull_descriptor_from_github(wf)
            wf_descr = json_descriptor['description']
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
            # log_string will be output in the Omero Web UI
            UI_messages = ""
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
                logger.debug(f"{wf}, {selected}, {selected_version}")
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
                        type(selected_op) == list and constants.workflow.NO in selected_op):
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
            
            logger.debug(f"User: {user} - Group: {group} - Email: {email}")
            # Start tracking the workflow on a unique ID
            wf_id = slurmClient.workflowTracker.initiate_workflow(
                params.name,
                "\n".join([params.description, params.version]),
                user,
                group
            )
            
            logger.info('''
            # --------------------------------------------
            # :: 1. Push selected data to Slurm ::
            # --------------------------------------------
            ''')
            # Generate a filename for the input data
            zipfile = createFileName(client, conn)
            # Send data to Slurm, zipped, over SSH
            # Uses _SLURM_Image_Transfer script from Omero
            rv, task_id = exportImageToSLURM(client, conn, slurmClient, 
                                             zipfile, wf_id)
            logger.debug(f"Ran data export: {rv.keys()}, {rv}")
            if 'Message' in rv:
                logger.info(rv['Message'].getValue())  # log
            UI_messages += "Exported data to Slurm. "

            logger.info('''
            # --------------------------------------------
            # :: 2. Convert data on Slurm ::
            # --------------------------------------------
            ''')
            # Note: Moved unzipping data to transfer script, removed from here            
            slurm_job_ids = {}
            task_ids = {}
            # Quick git pull on Slurm for latest version of job scripts
            update_result = slurmClient.update_slurm_scripts()
            logger.debug(update_result.__dict__)
            slurmJob = slurmClient.run_conversion_workflow_job(
                zipfile, 'zarr', 'tiff', wf_id)
            logger.info(f"Conversion job: {slurmJob}")
            if not slurmJob.ok:
                logger.warning(f"Error converting data: {slurmJob.get_error()}")
            else:
                try:
                    slurmJob.wait_for_completion(slurmClient, conn)
                    if not slurmJob.completed():
                        log_msg = f"Conversion is not completed: {slurmJob}"
                        slurmClient.workflowTracker.fail_task(slurmJob.task_id, 
                                                              "Conversion failed")
                        raise Exception(log_msg)
                    else:
                        slurmJob.cleanup(slurmClient)
                        msg = f"Conversion completed: {slurmJob}"
                        slurmClient.workflowTracker.complete_task(
                            slurmJob.task_id, msg)
                except Exception as e:
                    UI_messages += f" ERROR WITH CONVERTING DATA: {e}"
                    raise e

                logger.info('''
                # --------------------------------------------
                # :: 3. Create Slurm jobs for all workflows ::
                # --------------------------------------------
                ''')
                for wf_name in workflows:
                    if unwrap(client.getInput(wf_name)):
                        UI_messages, slurm_job_id, wf_id, task_id = run_workflow(
                            slurmClient,
                            _workflow_params[wf_name],
                            client,
                            UI_messages,
                            zipfile,
                            email,
                            wf_name,
                            wf_id)
                        slurm_job_ids[wf_name] = slurm_job_id
                        task_ids[slurm_job_id] = task_id

                # 4. Poll SLURM results
                slurm_job_id_list = [
                    x for x in slurm_job_ids.values() if x >= 0]
                logger.debug(slurm_job_id_list)
                while slurm_job_id_list:
                    # Query all jobids we care about
                    try:
                        job_status_dict, _ = slurmClient.check_job_status(
                            slurm_job_id_list)
                    except Exception as e:
                        UI_messages += f" ERROR WITH JOB: {e}"

                    for slurm_job_id, job_state in job_status_dict.items():
                        logger.debug(f"Job {slurm_job_id} is {job_state}.")
                        progress = slurmClient.get_active_job_progress(
                            slurm_job_id)
                        task_id = task_ids[slurm_job_id]
                        slurmClient.workflowTracker.update_task_status(
                            task_id, 
                            job_state) 
                        slurmClient.workflowTracker.update_task_progress(
                            task_id, 
                            progress)                        
                        if job_state == "TIMEOUT":
                            log_msg = f"Job {slurm_job_id} is TIMEOUT."
                            UI_messages += log_msg
                            # TODO resubmit with longer timeout? add an option?
                            # new_job_id = slurmClient.resubmit_job(
                            #     slurm_job_id)
                            # log_msg = f"Job {slurm_job_id} has been
                            # resubmitted ({new_job_id})."
                            logger.warning(log_msg)
                            # log_string += log_msg
                            slurm_job_id_list.remove(slurm_job_id)
                            slurmClient.workflowTracker.fail_task(task_id, 
                                                                  f"Slurm job state {job_state}")
                            # slurm_job_id_list.append(new_job_id)
                        elif job_state == "COMPLETED":
                            # 5. Retrieve SLURM images
                            # 6. Store results in OMERO
                            log_msg = f"Job {slurm_job_id} is COMPLETED."
                            slurmClient.workflowTracker.complete_task(task_id, 
                                                                      log_msg)
                            rv_imp = importResultsToOmero(
                                client, conn, slurmClient, 
                                slurm_job_id, selected_output,
                                wf_id)

                            if rv_imp:
                                try:
                                    if rv_imp['Message']:
                                        log_msg = f"{rv_imp['Message'].getValue()}"
                                except KeyError:
                                    log_msg += "Data import status unknown."
                                try:
                                    if rv_imp['URL']:
                                        client.setOutput(
                                            "URL", rv_imp['URL'])
                                except KeyError:
                                    log_msg += "|No URL|"
                                try:
                                    if rv_imp["File_Annotation"]:
                                        client.setOutput("File_Annotation",
                                                            rv_imp[
                                                                "File_Annotation"])
                                except KeyError:
                                    log_msg += "|No Annotation|"
                            else:
                                log_msg = "Attempted to import images to\
                                    Omero."
                            logger.info(log_msg)
                            UI_messages += log_msg
                            slurm_job_id_list.remove(slurm_job_id)
                        elif (job_state.startswith("CANCELLED")
                                or job_state == "FAILED"):
                            # Remove from future checks
                            log_msg = f"Job {slurm_job_id} is {job_state}."
                            log_msg += f"You can get the logfile using `Slurm Get Update` on job {slurm_job_id}"
                            logger.warning(log_msg)
                            UI_messages += log_msg
                            slurm_job_id_list.remove(slurm_job_id)
                            slurmClient.workflowTracker.fail_task(task_id, 
                                                                  f"Slurm job state {job_state}")
                        elif (job_state == "PENDING"
                                or job_state == "RUNNING"):
                            # expected
                            log_msg = f"Job {slurm_job_id} is busy..."
                            logger.debug(log_msg)
                            continue
                        else:
                            log_msg = f"Oops! State of job {slurm_job_id}\
                                is unknown: {job_state}. Stop tracking."
                            logger.warning(log_msg)
                            UI_messages += log_msg
                            slurm_job_id_list.remove(slurm_job_id)
                            slurmClient.workflowTracker.fail_task(task_id, 
                                                                  f"Slurm job state {job_state}")

                    # wait for 10 seconds before checking again
                    conn.keepAlive()  # keep the connection alive
                    timesleep.sleep(10)

            # 7. Script output
            slurmClient.workflowTracker.complete_workflow(wf_id)
            client.setOutput("Message", rstring(UI_messages))
        finally:
            client.closeSession()


def run_workflow(slurmClient: SlurmClient,
                 workflow_params,
                 client,
                 UI_messages: str,
                 zipfile,
                 email,
                 name,
                 wf_id):
    logger.info(f"Running {name}")
    workflow_version = unwrap(
        client.getInput(f"{name}_Version"))
    kwargs = {}
    for k in workflow_params:
        # Undo the added uniquefying prefix {name} |
        # That is only for the OMERO UI, not for the wf
        kwargs[k] = unwrap(client.getInput(f"{name}_|_{k}"))  # kwarg dict
    logger.info(f"Run workflow with: {kwargs}")
    try:
        cp_result, slurm_job_id, wf_id, task_id = slurmClient.run_workflow(
            workflow_name=name,
            workflow_version=workflow_version,
            input_data=zipfile,
            email=email,
            time=None,
            wf_id=wf_id,
            **kwargs)
        logger.debug(cp_result.stdout)
        if not cp_result.ok:
            logger.warning(f"Error running {name} job: {cp_result.stderr}")
        else:
            UI_messages += f"Submitted {name} to Slurm\
                as batch job {slurm_job_id}."

            job_status_dict, poll_result = slurmClient.check_job_status(
                [slurm_job_id])
            job_state = job_status_dict[slurm_job_id]
            logger.debug(
                f"{job_state}, {poll_result.stdout}")
            if not poll_result.ok:
                logger.warning(f"Error checking job status: {poll_result.stderr}")
            else:
                log_msg = f"\n{job_state}"
                logger.info(log_msg)
                slurmClient.workflowTracker.update_task_status(task_id, 
                                                               job_state)
    except Exception as e:
        UI_messages += f" ERROR WITH JOB: {e}"
        logger.warning(UI_messages)
        raise SSHException(UI_messages)
    return UI_messages, slurm_job_id, wf_id, task_id


def getOmeroEmail(client, conn):
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


def exportImageToSLURM(client: omscripts.client,
                       conn: BlitzGateway,
                       slurmClient: SlurmClient,
                       zipfile: str,
                       wf_id: UUID):
    svc = conn.getScriptService()
    scripts = svc.getScripts()
    # force just one script, why is it an array?
    script_id, script_version, script_name = [(unwrap(s.id), unwrap(s.getVersion()), unwrap(s.getName())) 
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
        constants.transfer.MERGED: rbool(True),
        constants.transfer.Z: rstring(constants.transfer.Z_MAXPROJ),
        constants.transfer.T: rstring(constants.transfer.T_DEFAULT),
        constants.transfer.FORMAT: rstring(
            constants.transfer.FORMAT_ZARR),
        constants.transfer.FOLDER: rstring(zipfile)
    }
    persist_dict = {key: unwrap(value) for key, value in inputs.items()}
    logger.debug(f"{inputs}, {script_id}")
    task_id = slurmClient.workflowTracker.add_task_to_workflow(
        wf_id,
        script_name,
        script_version,
        persist_dict[constants.transfer.IDS],
        persist_dict
    )
    slurmClient.workflowTracker.start_task(task_id)
    rv = runOMEROScript(client, svc, script_id, inputs)
    slurmClient.workflowTracker.complete_task(task_id, unwrap(rv['Message']))
    return rv, task_id


def runOMEROScript(client: omscripts.client, svc, script_id, inputs):
    rv = None
    
    script_id = int(script_id)
    # params = svc.getParams(script_id) # we can dynamically get them

    # The last parameter is how long to wait as an RInt
    proc = svc.runScript(script_id, inputs, None)
    try:
        cb = omero.scripts.ProcessCallbackI(client, proc)
        while not cb.block(1000):  # ms.
            pass
        cb.close()
        rv = proc.getResults(0)
    finally:
        proc.close(False)
    return rv


def importResultsToOmero(client: omscripts.client,
                         conn: BlitzGateway,
                         slurmClient: SlurmClient,
                         slurm_job_id: int,
                         selected_output: list,
                         wf_id: UUID) -> str:
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
        constants.results.OUTPUT_SLURM_JOB_ID: rstring(str(slurm_job_id))
    }

    # Get a 'parent' dataset or plate of input images
    parent_id = first_id
    parent_data_type = data_type
    if data_type == constants.transfer.DATA_TYPE_IMAGE:
        datasets = [d.id for d in conn.getObjects(
            constants.transfer.DATA_TYPE_DATASET, opts={'image': first_id})]
        plates = [d.id for d in conn.getObjects(
            constants.transfer.DATA_TYPE_PLATE, opts={'image': first_id})]
        logger.debug(f"Datasets:{datasets} Plates:{plates}")
        if len(plates) > len(datasets):
            parent_id = plates[0]
            parent_data_type = constants.transfer.DATA_TYPE_PLATE
        else:
            parent_id = datasets[0]
            parent_data_type = constants.transfer.DATA_TYPE_DATASET

    logger.debug(f"Determined parent to be {parent_data_type}:{parent_id}")

    if selected_output[constants.workflow.OUTPUT_PARENT]:
        # For now, there is no attaching to Dataset or Screen...
        # If we need that, build it ;) (in Get_Result script)
        if (parent_data_type == constants.transfer.DATA_TYPE_DATASET or
                parent_data_type == constants.transfer.DATA_TYPE_PROJECT):
            logger.debug(f"Adding to dataset {parent_id}")
            projects = get_project_name_ids(conn, parent_id)
            inputs[constants.results.OUTPUT_ATTACH_PROJECT_ID] = rlist(
                projects)
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

    else:
        inputs[constants.results.OUTPUT_ATTACH_NEW_DATASET] = rbool(
            False)

    if selected_output[constants.workflow.OUTPUT_ATTACH]:
        inputs[
            constants.results.OUTPUT_ATTACH_OG_IMAGES
        ] = rbool(True)
    else:
        inputs[
            constants.results.OUTPUT_ATTACH_OG_IMAGES
        ] = rbool(False)

    if selected_output[constants.workflow.OUTPUT_PARENT]:
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

    # Wait for Slurm Accounting to update
    wait_for_job_completion(slurmClient, slurm_job_id)

    logger.info(f"Running import script {script_id} with inputs: {inputs}")
    persist_dict = {key: unwrap(value) for key, value in inputs.items()}
    task_id = slurmClient.workflowTracker.add_task_to_workflow(
        wf_id,
        script_name,
        script_version,
        {constants.transfer.IDS: unwrap(client.getInput(constants.transfer.IDS))},
        persist_dict
    )
    slurmClient.workflowTracker.start_task(task_id)
    rv = runOMEROScript(client, svc, script_id, inputs)
    try:
        msg = unwrap(rv['Message'])
    except KeyError as e:
        slurmClient.workflowTracker.fail_task(task_id, 
                                              "Import failed")
        raise e
    slurmClient.workflowTracker.complete_task(task_id, msg)
    return rv


def wait_for_job_completion(slurmClient, slurm_job_id, timeout=500, interval=15):
    """
    Waits for the slurm job to complete by polling at regular intervals.

    Parameters:
        slurmClient: The SLURM client used to query job status.
        slurm_job_id: The ID of the SLURM job to wait for.
        timeout (int): Maximum time to wait for the job to complete (in seconds).
        interval (int): Time between each check (in seconds).

    Raises:
        TimeoutError: If the job does not complete within the timeout period.
    """
    start_time = timesleep.time()
    
    while True:
        # Check if the job is completed
        if str(slurm_job_id) in slurmClient.list_completed_jobs():
            return  # Job is complete, exit the function
        
        # Check if we've hit the timeout
        elapsed_time = timesleep.time() - start_time
        if elapsed_time > timeout:
            raise TimeoutError(f"Job {slurm_job_id} not found in Slurm Accounting within {timeout} seconds.")
        
        # Wait for the next interval before checking again
        timesleep.sleep(interval)


def get_project_name_ids(conn, parent_id):
    # Note different implementation XD
    # Call it 'legacy code', at version 1 already ;)
    projects = [rstring('%d: %s' % (d.id, d.getName()))
                for d in conn.getObjects(constants.transfer.DATA_TYPE_PROJECT,
                                         opts={'dataset': parent_id})]
    logger.debug(projects)
    return projects


def get_dataset_name_ids(conn, parent_id):
    dataset = [rstring('%d: %s' % (d.id, d.getName()))
               for d in conn.getObjects(constants.transfer.DATA_TYPE_DATASET,
                                        [parent_id])]
    logger.debug(dataset)
    return dataset


def get_plate_name_ids(conn, parent_id):
    plates = [rstring('%d: %s' % (d.id, d.getName()))
              for d in conn.getObjects(constants.transfer.DATA_TYPE_PLATE,
                                       [parent_id])]
    logger.debug(plates)
    return plates


def createFileName(client: omscripts.client, conn: BlitzGateway) -> str:
    opts = {}
    data_type = unwrap(client.getInput(constants.transfer.DATA_TYPE))
    if data_type == constants.transfer.DATA_TYPE_IMAGE:
        # get parent dataset
        opts['image'] = unwrap(client.getInput(constants.transfer.IDS))[0]
        objparams = ['%d_%s' % (d.id, d.getName())
                     for d in conn.getObjects(
                         constants.transfer.DATA_TYPE_DATASET, opts=opts)]
    elif data_type == constants.transfer.DATA_TYPE_DATASET:
        objparams = ['%d_%s' % (d.id, d.getName())
                     for d in conn.getObjects(
                         constants.transfer.DATA_TYPE_DATASET,
                         unwrap(client.getInput(constants.transfer.IDS)))]
    elif data_type == constants.transfer.DATA_TYPE_PLATE:
        objparams = ['%d_%s' % (d.id, d.getName())
                     for d in conn.getObjects(
                         constants.transfer.DATA_TYPE_PLATE,
                         unwrap(client.getInput(constants.transfer.IDS)))]
    else:
        raise ValueError(f"Can't handle {data_type}")

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = "_".join(objparams)
    # Replace spaces with underscores in the filename
    filename = filename.replace(" ", "_")
    full_filename = f"{filename}_{timestamp}"
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

    # Silence some of the DEBUG
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)

    runScript()
