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
from omero.grid import JobParams
from omero.rtypes import rstring, unwrap
import omero.scripts as omscripts
from omero_slurm_client import SlurmClient
import logging

logger = logging.getLogger(__name__)

_IMAGE_EXPORT_SCRIPT = "_SLURM_Image_transfer.py"
_DEFAULT_MAIL = "No"
_DEFAULT_TIME = "00:15:00"


def runScript():
    """
    The main entry point of the script
    """

    with SlurmClient.from_config() as slurmClient:

        params = JobParams()
        params.authors = ["Torec Luik"]
        params.version = "0.0.4"
        params.description = f'''Script to run CellPose on slurm cluster.
        First run the {_IMAGE_EXPORT_SCRIPT} script to export your data
        to the cluster.

        Specifically will run:
        https://hub.docker.com/r/torecluik/t_nucleisegmentation-cellpose


        This runs a script remotely on the Slurm cluster.
        Connection ready? {slurmClient.validate()}
        '''
        params.name = 'Slurm Cellpose Segmentation'
        params.contact = 't.t.luik@amsterdamumc.nl'
        params.institutions = ["Amsterdam UMC"]
        params.authorsInstitutions = [[1]]

        _versions, _datafiles = slurmClient.get_image_versions_and_data_files(
            'cellpose')
        _workflow_params = slurmClient.get_workflow_parameters('cellpose')
        print(_workflow_params)
        name_descr = f"Name of folder where images are stored, as provided\
            with {_IMAGE_EXPORT_SCRIPT}"
        dur_descr = "Maximum time the script should run for. \
            Max is 8 hours. Notation is hh:mm:ss"
        email_descr = "Provide an e-mail if you want a mail \
            when your job is done or cancelled."
        input_list = [
            omscripts.String("Folder_Name", grouping="01",
                             description=name_descr,
                             values=_datafiles),
            omscripts.Bool("Slurm Job Parameters",
                           grouping="02", default=True),
            omscripts.String("Duration", grouping="02.2",
                             description=dur_descr,
                             default=_DEFAULT_TIME),
            omscripts.String("E-mail", grouping="02.3",
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
                print(i, k, param)
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
        cellpose_version = unwrap(client.getInput("Version"))
        zipfile = unwrap(client.getInput("Folder_Name"))
        email = unwrap(client.getInput("E-mail"))
        if email == _DEFAULT_MAIL:
            email = None
        time = unwrap(client.getInput("Duration"))
        kwargs = {}
        for i, k in enumerate(_workflow_params):
            kwargs[k] = unwrap(client.getInput(k))  # kwarg dict
        print(kwargs)

        try:
            # 3. Call SLURM (segmentation)
            unpack_result = slurmClient.unpack_data(zipfile)
            print(unpack_result.stdout)
            if not unpack_result.ok:
                print("Error unpacking data:", unpack_result.stderr)
            else:
                update_result = slurmClient.update_slurm_scripts()
                print(update_result.stdout)
                if not update_result.ok:
                    print("Error updating SLURM scripts:",
                          update_result.stderr)
                else:
                    cp_result, slurm_job_id = slurmClient.run_cellpose(
                        cellpose_version,
                        zipfile,
                        email=email,
                        time=time,
                        **kwargs
                    )
                    if not cp_result.ok:
                        print("Error running CellPose job:", cp_result.stderr)
                    else:
                        print_result = f"Submitted to Slurm as\
                            batch job {slurm_job_id}."
                        # 4. Poll SLURM results
                        try:
                            tup = slurmClient.check_job_status(
                                [slurm_job_id])
                            (job_status_dict, poll_result) = tup
                            print(poll_result.stdout, job_status_dict)
                            if not poll_result.ok:
                                print("Error checking job status:",
                                      poll_result.stderr)
                            else:
                                print_result += f"\n{job_status_dict}"
                        except Exception as e:
                            print_result += f" ERROR WITH JOB: {e}"
                            print(print_result)

            # 7. Script output
            client.setOutput("Message", rstring(print_result))
        finally:
            client.closeSession()


if __name__ == '__main__':
    runScript()
