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
from biomero import SlurmClient
import logging
import os
import sys

logger = logging.getLogger(__name__)


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
                       description="The path to your configuration file. Optional."),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
    )

    try:
        message = ""
        init_slurm = unwrap(client.getInput("Init Slurm"))
        if init_slurm:
            configfile = unwrap(client.getInput(extra_config_name))
            if not configfile:
                configfile = ''
            with SlurmClient.from_config(configfile=configfile,
                                         init_slurm=True) as slurmClient:
                slurmClient.validate(validate_slurm_setup=True)
                message = "Slurm is setup:"
                models, data = slurmClient.get_all_image_versions_and_data_files()
                message += f"Models: {models}\nData:{data}"

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
