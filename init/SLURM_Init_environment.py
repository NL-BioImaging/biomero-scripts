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
from omero_slurm_client import SlurmClient
import logging

logger = logging.getLogger(__name__)


def runScript():
    """
    The main entry point of the script
    """

    client = scripts.client(
        'Slurm Init',
        '''Will initiate the Slurm environment for workflow execution.

        You can provide a config file location, 
        and/or it will look for default locations:
        /etc/slurm-config.ini
        ~/slurm-config.ini
        ''',
        scripts.Bool("Init Slurm", grouping="01", default=True),
        scripts.String("Config file", optional=True, grouping="01.1",
                       description="The path to your configuration file. Optional."),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
    )

    try:
        message = ""
        init_slurm = unwrap(client.getInput("Init Slurm"))
        if init_slurm:
            configfile = unwrap(client.getInput("Config file"))
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
    runScript()
