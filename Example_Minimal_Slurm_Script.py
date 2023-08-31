#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#

from __future__ import print_function
from omero.grid import JobParams
from omero.rtypes import rstring
import omero.scripts as omscripts
import subprocess
from omero_slurm_client import SlurmClient
import logging

logger = logging.getLogger(__name__)

_PYCMD = "Python_Command"
_DEFAULT_CMD = "import numpy as np; arr = np.array([1,2,3,4,5]);\
    print(arr.mean())"
_RUNPY = "Run_Python"
_RUNSLRM = "Check_SLURM_Status"
_SQUEUE = "Check_Queue"
_SINFO = "Check_Cluster"
_SOTHER = "Run_Other_Command"
_SCMD = "Linux_Command"
_DEFAULT_SCMD = "ls -la"


def runScript():
    """
    The main entry point of the script
    """

    with SlurmClient.from_config() as slurmClient:

        params = JobParams()
        params.authors = ["Torec Luik"]
        params.version = "0.0.3"
        params.description = f'''Example script to run on slurm cluster

        Runs a script remotely on SLURM.

        Connection ready? {slurmClient.validate()}
        '''
        params.name = 'Minimal Slurm Script'
        params.contact = 't.t.luik@amsterdamumc.nl'
        params.institutions = ["Amsterdam UMC"]
        params.authorsInstitutions = [[1]]

        input_list = [
            omscripts.Bool(_RUNPY, grouping="01", default=True),
            omscripts.String(_PYCMD, optional=False, grouping="01.1",
                             description="The Python command to run on slurm",
                             default=_DEFAULT_CMD),
            omscripts.Bool(_RUNSLRM, grouping="02", default=False),
            omscripts.Bool(_SQUEUE, grouping="02.1", default=False),
            omscripts.Bool(_SINFO, grouping="02.2", default=False),
            omscripts.Bool(_SOTHER, grouping="03", default=False),
            omscripts.String(_SCMD, optional=False, grouping="03.1",
                             description="The linux command to run on slurm",
                             default=_DEFAULT_SCMD),
        ]
        inputs = {
            p._name: p for p in input_list
        }
        params.inputs = inputs
        client = omscripts.client(params)

        try:
            scriptParams = client.getInputs(unwrap=True)
            print(f"Params: {scriptParams}")
            print(f"Validating slurm connection:\
                {slurmClient.validate()} for {slurmClient.__dict__}")

            print(f"Running py cmd: {scriptParams[_PYCMD]}")
            print_result = []
            cmdlist = []
            if scriptParams[_RUNSLRM]:
                if scriptParams[_SQUEUE]:
                    cmdlist.append("squeue -u $USER")
                if scriptParams[_SINFO]:
                    cmdlist.append("sinfo")
            if scriptParams[_SOTHER]:
                cmdlist.append(scriptParams[_SCMD])
            if scriptParams[_RUNPY]:
                cmdlist.append("module load Anaconda3 && " +
                               f"python -c '{scriptParams[_PYCMD]}'")
            try:
                # run a list of commands
                for cmd in cmdlist:
                    results = slurmClient.run(cmd)
                    print(f"Ran slurm {results}")
            except subprocess.CalledProcessError as e:
                results = f"Error {e.__dict__}"
                print(results)
            finally:
                print_result.append(f"{results.stdout}")
            client.setOutput("Message", rstring("".join(print_result)))
        finally:
            client.closeSession()


if __name__ == '__main__':
    runScript()
