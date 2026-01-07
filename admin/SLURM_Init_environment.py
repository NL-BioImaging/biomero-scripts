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

"""
BIOMERO SLURM Environment Initialization Script (Admin Only)

This administrative script sets up the complete SLURM environment for BIOMERO
workflow execution including directory structure, job scripts, converters,
and container images.

**ADMIN ONLY**: This script requires OMERO administrator privileges.

This is typically run once during initial BIOMERO-SLURM setup to prepare
the cluster environment for workflow execution.

Authors: Torec Luik, OMERO Team
Institution: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

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
VERSION = "2.1.0"


def runScript():
    """Main entry point for SLURM environment initialization script.
    
    Sets up the complete SLURM environment for BIOMERO workflow execution
    including directory structure, job scripts, converters, and container
    images. This is typically run once during initial setup.
    """

    extra_config_name = "Extra Config file (optional!)"
    client = scripts.client(
        'Slurm Init (Admin Only)',
        '''Will initiate the Slurm environment for workflow execution.

        **ADMIN ONLY**: Requires OMERO administrator privileges.

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
        
        # Check if user is admin
        user = conn.getUser()
        is_admin = user.isAdmin()
        user_id = conn.getUserId()
        
        logger.info(f"User ID {user_id} admin status: {is_admin}")
        
        if not is_admin:
            logger.warning("Access denied: Admin privileges required")
            client.setOutput("Message", rstring(
                f"ACCESS DENIED: This initialization script requires OMERO "
                f"administrator privileges. User ID {user_id} is not an admin."
            ))
            return
        
        logger.info("Admin access confirmed, proceeding with initialization")
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
                    
                    # 5. Reset db views
                    slurmClient.initialize_analytics_system(reset_tables=True)
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
    
    # Silence some of the DEBUG - Extended for cleaner BIOMERO logs
    logging.getLogger('omero.gateway.utils').setLevel(logging.WARNING)
    logging.getLogger('omero.gateway').setLevel(logging.WARNING)  # Silences proxy creation spam
    logging.getLogger('omero.client').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)
    logging.getLogger('paramiko.sftp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('requests_cache').setLevel(logging.WARNING)  # Cache logs
    logging.getLogger('requests-cache').setLevel(logging.WARNING)  # Alt naming
    logging.getLogger('requests_cache.core').setLevel(logging.WARNING)  # Core module
    logging.getLogger('requests_cache.backends').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.base').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.backends.sqlite').setLevel(
        logging.WARNING)
    logging.getLogger('requests_cache.policy').setLevel(logging.WARNING)
    logging.getLogger('requests_cache.policy.actions').setLevel(
        logging.WARNING)
    logging.getLogger('invoke').setLevel(logging.WARNING)
    logging.getLogger('fabric').setLevel(logging.WARNING)  # SSH operations
    logging.getLogger('Ice').setLevel(logging.ERROR)
    logging.getLogger('ZeroC').setLevel(logging.ERROR)

    runScript()
