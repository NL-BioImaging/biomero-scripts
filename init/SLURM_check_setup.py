#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# Example OMERO.script to check wf creation progress.

import omero
import omero.gateway
from omero import scripts
from omero.rtypes import rstring
from biomero import SlurmClient
import logging
import os
import sys

logger = logging.getLogger(__name__)


def runScript():
    """
    The main entry point of the script
    """

    client = scripts.client(
        'Slurm Check Setup',
        '''Check Slurm setup, e.g. available workflows.
        ''',
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
    )

    try:
        message = ""
        with SlurmClient.from_config() as slurmClient:
            message = f"Connected: {slurmClient.validate()}" + \
                    f"\n Slurm: {slurmClient}\n"
            models, data = slurmClient.get_all_image_versions_and_data_files()
            
            # Initialize pending models dictionary
            pending = {}
            # Iterate through slurm_model_repos to identify pending models
            for model, repo in slurmClient.slurm_model_repos.items():
                _, version = slurmClient.extract_parts_from_url(repo)
                if model not in models:
                    pending[model] = version
                else:
                    # Check versions for pending items
                    if version not in models[model]:
                        if model not in pending:
                            pending[model] = [version]
                        else:
                            pending[model].append(version)    
            
            message += f"\n>> Available Models: {models}"
            message += f"\n>>> Pending Models: {pending}"
            message += f"\n>> Available Data: {data}"
            logger.info(message)
            # TODO get sing.log too

        client.setOutput("Message", rstring(str(message)))

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
    runScript()
