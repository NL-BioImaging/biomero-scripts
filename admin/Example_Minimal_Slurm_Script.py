#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt

"""
BIOMERO Minimal SLURM Script Example (Admin Only)

This example script demonstrates basic SLURM integration capabilities
and serves as a template for developing custom BIOMERO-SLURM workflows.

**ADMIN ONLY**: This script requires OMERO administrator privileges.

Key Features:
- Simple Python command execution on SLURM cluster
- SLURM cluster status checking (squeue, sinfo)
- Custom Linux command execution
- Basic SLURM client validation and connection testing
- Minimal example structure for custom script development

Example Capabilities:
- Run Python: Execute Python commands remotely on cluster
- Check SLURM Status: Query cluster queue and node information
- Run Linux Commands: Execute arbitrary shell commands on cluster
- Connection Testing: Validate SLURM client configuration

Educational Purpose:
This script serves as:
- Introduction to BIOMERO-SLURM integration
- Template for custom workflow development
- Testing tool for SLURM connectivity
- Example of OMERO script parameter handling

Use this script as a starting point for developing more complex
workflows or for testing basic SLURM cluster connectivity.

Authors: Torec Luik, OMERO Team
Institution: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""


from __future__ import print_function
from omero.grid import JobParams
from omero.rtypes import rstring
from omero.gateway import BlitzGateway
import omero.scripts as omscripts
import subprocess
from biomero import SlurmClient
import logging
import os
import sys

# Version constant for easy version management
VERSION = "2.0.0-alpha.8"

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
    """Main entry point for minimal SLURM script example.
    
    Demonstrates basic SLURM integration capabilities including Python
    command execution, cluster status checking, and custom Linux commands.
    Serves as template for developing custom BIOMERO-SLURM workflows.
    
    The function provides:
        - Python command execution on SLURM cluster
        - SLURM cluster status queries (squeue, sinfo)
        - Custom Linux command execution
        - SLURM client validation and testing
        - Basic parameter handling examples
    """

    with SlurmClient.from_config() as slurmClient:

        params = JobParams()
        params.authors = ["Torec Luik"]
        params.version = VERSION
        params.description = f'''Admin-only example script for SLURM cluster

        **ADMIN ONLY**: Requires OMERO administrator privileges.
        Runs a script remotely on SLURM.

        Connection ready? {slurmClient.validate()}
        '''
        params.name = 'Minimal Slurm Script'
        params.contact = 'cellularimaging@amsterdamumc.nl'
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
            logger.info(f"Params: {scriptParams}")
            
            # Check if user is admin - simple approach using BlitzGateway
            conn = BlitzGateway(client_obj=client)
            user = conn.getUser()
            is_admin = user.isAdmin()
            user_id = conn.getUserId()
            
            logger.info(f"User ID {user_id} admin status: {is_admin}")
            
            if not is_admin:
                logger.warning("Access denied: Admin privileges required")
                client.setOutput("Message", rstring(
                    f"ACCESS DENIED: This script requires OMERO administrator "
                    f"privileges. User ID {user_id} is not an admin."
                ))
                return
            
            logger.info("Admin access confirmed, proceeding with SLURM ops")
            logger.info(f"Validating slurm connection:\
                {slurmClient.validate()} for {slurmClient.__dict__}")

            logger.info(f"Running py cmd: {scriptParams[_PYCMD]}")
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
                    logger.info(f"Ran slurm {results}")
            except subprocess.CalledProcessError as e:
                results = f"Error {e.__dict__}"
                logger.info(results)
            finally:
                if results:
                    print_result.append(f"{results}")
            client.setOutput("Message", rstring("".join(print_result)))
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
