#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# This script is used to run the CellPose segmentation algorithm on a Slurm
# cluster, using data exported from an Omero server.
#
# This script requires the SlurmClient and Fabric Python modules to be
# installed, as well as access to a Slurm cluster running the
# CellPose Singularity image.

from __future__ import print_function
import omero
import os
import sys
from omero.grid import JobParams
from omero.rtypes import rstring, unwrap
import omero.scripts as omscripts
from biomero import SlurmClient, constants
import logging

logger = logging.getLogger(__name__)

_DEFAULT_MAIL = "No"
_DEFAULT_TIME = "00:15:00"


def runScript():
    """
    The main entry point of the script
    """

    with SlurmClient.from_config() as slurmClient:

        params = JobParams()
        params.authors = ["Torec Luik"]
        params.version = "1.9.0"
        params.description = f'''Script to run CellPose on slurm cluster.
        First run the {constants.IMAGE_EXPORT_SCRIPT} script to export your data
        to the cluster.

        Specifically will run:
        https://hub.docker.com/r/torecluik/t_nucleisegmentation-cellpose


        This runs a script remotely on the Slurm cluster.
        Connection ready? {slurmClient.validate()}
        '''
        params.name = 'Slurm Cellpose Segmentation'
        params.contact = 'cellularimaging@amsterdamumc.nl'
        params.institutions = ["Amsterdam UMC"]
        params.authorsInstitutions = [[1]]

        _versions, _datafiles = slurmClient.get_image_versions_and_data_files(
            'cellpose')
        _workflow_params = slurmClient.get_workflow_parameters('cellpose')
        logger.debug(_workflow_params)
        name_descr = f"Name of folder where images are stored, as provided\
            with {constants.IMAGE_EXPORT_SCRIPT}"
        dur_descr = "Maximum time the script should run for. \
            Max is 8 hours. Notation is hh:mm:ss"
        email_descr = "Provide an e-mail if you want a mail \
            when your job is done or cancelled."
        input_list = [
            omscripts.String(constants.transfer.FOLDER, grouping="01",
                             description=name_descr,
                             values=_datafiles),
            omscripts.Bool("Slurm Job Parameters",
                           grouping="02", default=True),
            omscripts.String("Duration", grouping="02.2",
                             description=dur_descr,
                             default=_DEFAULT_TIME),
            omscripts.String(constants.workflow.EMAIL, grouping="02.3",
                             description=email_descr,
                             default=_DEFAULT_MAIL)
        ]

        for wf, group, versions, wfparams in [
            ["CellPose",
             "03",
             _versions,
             _workflow_params],
        ]:
            wf_ = omscripts.Bool(wf, grouping=group, default=True)
            input_list.append(wf_)
            version_descr = f"Version of the Singularity Image of {wf}"
            wf_v = omscripts.String(f"{wf}_Version", grouping=f"{group}.0",
                                    description=version_descr,
                                    values=versions)
            input_list.append(wf_v)
            for i, (k, param) in enumerate(wfparams.items()):
                logger.debug(i, k, param)
                logging.info(param)
                p = slurmClient.convert_cytype_to_omtype(
                    param["cytype"],
                    param["default"],
                    param["name"],
                    description=param["description"],
                    default=param["default"],
                    grouping=f"03.{i+1}",
                    optional=param['optional']
                )
                input_list.append(p)
        inputs = {
            p._name: p for p in input_list
        }
        params.inputs = inputs
        params.namespaces = [omero.constants.namespaces.NSDYNAMIC]
        client = omscripts.client(params)

        # Unpack script input values
        cellpose_version = unwrap(client.getInput("CellPose_Version"))
        zipfile = unwrap(client.getInput(constants.transfer.FOLDER))
        email = unwrap(client.getInput(constants.workflow.EMAIL))
        if email == _DEFAULT_MAIL:
            email = None
        time = unwrap(client.getInput("Duration"))
        kwargs = {}
        for i, k in enumerate(_workflow_params):
            kwargs[k] = unwrap(client.getInput(k))  # kwarg dict
        logger.debug(kwargs)

        try:
            # 3. Call SLURM (segmentation)
            unpack_result = slurmClient.unpack_data(zipfile)
            logger.debug(unpack_result.stdout)
            if not unpack_result.ok:
                logger.warning(f"Error unpacking data:{unpack_result.stderr}")
            else:
                # Quick git pull on Slurm for latest version of job scripts
                try:
                    update_result = slurmClient.update_slurm_scripts()
                    logger.debug(update_result.__dict__)
                except Exception as e:
                    logger.warning(f"Error updating SLURM scripts:{e}")

                cp_result, slurm_job_id = slurmClient.run_workflow(
                    workflow_name='cellpose',
                    workflow_version=cellpose_version,
                    input_data=zipfile,
                    email=email,
                    time=time,
                    **kwargs
                )
                if not cp_result.ok:
                    logger.warning(f"Error running CellPose job: {cp_result.stderr}")
                else:
                    print_result = f"Submitted to Slurm as\
                        batch job {slurm_job_id}."
                    # 4. Poll SLURM results
                    try:
                        tup = slurmClient.check_job_status(
                            [slurm_job_id])
                        (job_status_dict, poll_result) = tup
                        logger.debug(f"{poll_result.stdout},{job_status_dict}")
                        if not poll_result.ok:
                            logger.warning("Error checking job status:", 
                                           poll_result.stderr)
                        else:
                            print_result += f"\n{job_status_dict}"
                    except Exception as e:
                        print_result += f" ERROR WITH JOB: {e}"
                        logger.warning(print_result)

            # 7. Script output
            logger.info(print_result)
            client.setOutput("Message", rstring(print_result))
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
