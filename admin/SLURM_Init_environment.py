#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# Example OMERO.script to instantiate a 'empty' Slurm connection.

import omero
import omero.gateway
from omero import scripts
from omero.rtypes import rstring, unwrap
from omero.gateway import BlitzGateway
from biomero import SlurmClient
import logging
import os
import sys

logger = logging.getLogger(__name__)
VERSION = "2.0.0-alpha.4"


def runScript():
    """
    The main entry point of the script
    """

    extra_config_name = "Extra Config file (optional!)"
    client = scripts.client(
        'Slurm Init',
        '''Will initiate the Slurm environment for workflow execution.

        You can provide a config file location, 
        and/or it will look for default locations:
        /etc/slurm-config.ini
        ~/slurm-config.ini
        ''',
        scripts.Bool("Init Slurm", grouping="01", default=True),
        scripts.String(extra_config_name, optional=True, grouping="01.1",
                       description="The path to your configuration file on the server. Optional."),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact='cellularimaging@amsterdamumc.nl',
        authorsInstitutions=[[1]]
    )

    try:
        conn = BlitzGateway(client_obj=client)
        message = ""
        init_slurm = unwrap(client.getInput("Init Slurm"))
        if init_slurm:
            configfile = unwrap(client.getInput(extra_config_name))
            if not configfile:
                configfile = ''
            with SlurmClient.from_config(configfile=configfile) as slurmClient:
                conn.keepAlive()
                # We are kind of duplicating code here, so we can keep the conn alive.
                if slurmClient.validate():
                    # 1. Create directories
                    slurmClient.setup_directories()
                    conn.keepAlive()

                    # 2. Clone git
                    slurmClient.setup_job_scripts()
                    conn.keepAlive()

                    # 3. Setup converters
                    slurmClient.setup_converters()
                    conn.keepAlive()

                    # 4. Download workflow images
                    slurmClient.setup_container_images()
                    conn.keepAlive()
                message = "Slurm is almost set up. " + \
                    "It will now download and build " + \
                    "all the requested workflow images." + \
                    " This might take a while!" + \
                    " You can check progress with the " + \
                    "'SLURM check setup' script. "
                models, _ = slurmClient.get_all_image_versions_and_data_files()
                filtered_models = {key: value for key, value in models.items() if any(v != '' for v in value)}
                
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
                message += f">> These workflows are already available now: {filtered_models}. \nThese are still pending: {pending}"

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
    
    # Silence some of the DEBUG
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)

    runScript()
