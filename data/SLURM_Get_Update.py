#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# Example OMERO.script to get a Slurm job update.

import omero
import omero.gateway
from omero import scripts
import omero.util.script_utils as script_utils
from omero.constants.namespaces import NSCREATED
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, robject, unwrap, wrap
from omero_slurm_client import SlurmClient
import logging

logger = logging.getLogger(__name__)

SLURM_JOB_ID = "SLURM Job Id"
SLURM_JOB_ID_OLD = "SLURM Job Id (old)"
RUN_ON_GPU_NS = "GPU"
RUNNING_JOB = "Running Job"
COMPLETED_JOB = "Completed Job"


def getUserProjects():
    try:
        client = omero.client()
        client.createSession()
        conn = omero.gateway.BlitzGateway(client_obj=client)
        conn.SERVICE_OPTS.setOmeroGroup(-1)
        objparams = [rstring('%d: %s' % (d.id, d.getName()))
                     for d in conn.getObjects('Project')]
        #  if type(d) == omero.model.ProjectI
        if not objparams:
            objparams = [rstring('<No objects found>')]
        return objparams
    except Exception as e:
        return ['Exception: %s' % e]
    finally:
        client.closeSession()


def runScript():
    """
    The main entry point of the script
    """
    with SlurmClient.from_config() as slurmClient:

        _slurmjobs = slurmClient.list_active_jobs()
        _alljobs = slurmClient.list_all_jobs()
        _oldjobs = [job for job in _alljobs if job not in _slurmjobs]
        _projects = getUserProjects()
        client = scripts.client(
            'Slurm Get Update',
            '''Retrieve an update about your SLURM job.

            Will download the logfile if you select a completed job.
            ''',
            scripts.Bool(RUNNING_JOB, optional=True,
                         grouping="01", default=True),
            scripts.String(SLURM_JOB_ID, optional=True, grouping="01.1",
                           values=_slurmjobs),
            scripts.Bool(COMPLETED_JOB, optional=True,
                         default=False, grouping="02"),
            scripts.String(SLURM_JOB_ID_OLD, optional=True, grouping="02.1",
                           values=_oldjobs),
            scripts.List("Project", optional=False, grouping="02.5",
                         description="Project to attach workflow results to",
                         values=_projects),
            namespaces=[omero.constants.namespaces.NSDYNAMIC],
        )

        try:
            scriptParams = client.getInputs(unwrap=True)

            message = ""
            print(f"Request: {scriptParams}\n")

            # Job id
            slurm_job_id = unwrap(client.getInput(SLURM_JOB_ID))
            slurm_job_id_old = unwrap(
                client.getInput(SLURM_JOB_ID_OLD)).strip()

            # Job State
            if unwrap(client.getInput(RUNNING_JOB)):
                message = check_job(slurmClient, message, slurm_job_id)
            if unwrap(client.getInput(COMPLETED_JOB)):
                message = check_job(slurmClient, message, slurm_job_id_old)

            # Job log
            if unwrap(client.getInput(RUNNING_JOB)):
                try:
                    update = slurmClient.get_active_job_progress(slurm_job_id)
                    message += update
                except Exception as e:
                    message += f" Tailing logfile failed: {e}\n"

            if unwrap(client.getInput(COMPLETED_JOB)):
                try:
                    # Pull log from Slurm to server
                    tup = slurmClient.get_logfile_from_slurm(
                        slurm_job_id_old)
                    (dir, export_file, result) = tup
                    print(f"Pulled logfile {result.__dict__}")
                    # Upload logfile to Omero as Original File
                    output_display_name = f"Job logfile '{result.local}'"
                    namespace = NSCREATED + "/SLURM/SLURM_GET_UPDATE"
                    mimetype = 'text/plain'
                    obj = client.upload(export_file, type=mimetype)
                    obj_id = obj.id.val
                    url = f"get_original_file/{obj_id}/"
                    client.setOutput("URL", wrap({"type": "URL", "href": url}))
                    # Attach logfile (OriginalFile) to Project
                    conn = BlitzGateway(client_obj=client)
                    project_ids = unwrap(client.getInput("Project"))
                    print(project_ids)
                    project_id = project_ids[0].split(":")[0]
                    print(project_id)
                    project = conn.getObject("Project", project_id)
                    tup = script_utils.create_link_file_annotation(
                        conn, export_file, project, output=output_display_name,
                        namespace=namespace, mimetype=mimetype)
                    (file_annotation, ann_message) = tup
                    if len(project_ids) > 1:
                        # link to the other given projects too
                        for project_id in project_ids[1:]:
                            project_id = project_id.split(":")[0]
                            project = conn.getObject("Project", project_id)
                            # link it to project.
                            project.linkAnnotation(file_annotation)
                    # Script output
                    message += ann_message
                    client.setOutput("File_Annotation",
                                     robject(file_annotation._obj))
                except Exception as e:
                    message += f" Importing logfile failed: {e}\n"

            client.setOutput("Message", rstring(str(message)))

        finally:
            client.closeSession()


def check_job(slurmClient, message, slurm_job_id):
    try:
        job_status_dict, poll_result = slurmClient.check_job_status(
            slurm_job_ids=[slurm_job_id])
        print(job_status_dict, poll_result.stdout)
        if not poll_result.ok:
            print("Error checking job status:", poll_result.stderr)
            message += f"\nError checking job status: {poll_result.stderr}"
        else:
            message += f"\n{job_status_dict}"
    except Exception as e:
        message += f" Show job failed: {e}"
    return message


if __name__ == '__main__':
    runScript()
