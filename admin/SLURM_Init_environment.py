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
VERSION = "2.8.2"


def format_image_submission(array_job_id, status):
    """Format the scheduler-native image initialization summary."""
    counts = status.get("counts", {})
    if array_job_id is None:
        first_line = "Image pull array ID: none (all images already valid)"
    else:
        first_line = f"Image pull array ID: {array_job_id}"
    return (
        f"{first_line}\n"
        f"Image status — READY: {counts.get('READY', 0)}, "
        f"RUNNING: {counts.get('RUNNING', 0)}, "
        f"FAILED: {counts.get('FAILED', 0)}"
    )


def runScript():
    """Main entry point for SLURM environment initialization script.
    
    Sets up the complete SLURM environment for BIOMERO workflow execution
    including directory structure, job scripts, converters, and container
    images. This is typically run once during initial setup.
    """

    extra_config_name = "Extra Config file (optional!)"
    init_slurm_name = "Init Slurm"
    rebuild_analytics_name = "Rebuild Analytics Views (skip if only adding new workflows!)"
    rebuild_days_ago_name = "Rebuild From Days Ago"
    rebuild_from_date_name = "Rebuild From Date"
    client = scripts.client(
        'Slurm Init (Admin Only)',
        '''Will initiate the Slurm environment for workflow execution.

        **ADMIN ONLY**: Requires OMERO administrator privileges.

        You can provide a config file location, 
        and/or it will look for default locations:
        /etc/slurm-config.ini
        ~/slurm-config.ini
        ''',
        scripts.Bool(init_slurm_name, grouping="01", default=True),
        scripts.String(extra_config_name, optional=True, grouping="01.1",
                       description="The path to your configuration file on the server. Optional."),
        scripts.Bool(rebuild_analytics_name, grouping="01.2", default=True,
                     description="Drop and rebuild analytics view tables from scratch. "
                                 "Required after BIOMERO upgrades or schema changes. "
                                 "Only safe to uncheck when solely adding new workflow containers "
                                 "to an existing installation with no BIOMERO version change."),
        scripts.Int(rebuild_days_ago_name, optional=True, grouping="01.3",
                    description="Advanced opt-in: limit analytics view rebuild to the last N days of events. "
                                "Only use this if your event history is very large and full rebuilds are too slow. "
                                "Warning: jobs older than this cutoff will not appear in analytics views. "
                                "Leave empty to use whatever is configured in slurm-config.ini or env vars (or full rebuild if nothing is set)."),
        scripts.String(rebuild_from_date_name, optional=True, grouping="01.4",
                       description="Advanced opt-in: limit analytics view rebuild to events from this date onward (YYYY-MM-DD). "
                                   "Only use this if your event history is very large and full rebuilds are too slow. "
                                   "Warning: jobs before this date will not appear in analytics views. "
                                   "Ignored when 'Rebuild From Days Ago' is also set. "
                                   "Leave empty to use whatever is configured in slurm-config.ini or env vars (or full rebuild if nothing is set)."),
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
        init_slurm = unwrap(client.getInput(init_slurm_name))
        reset_view_tables = unwrap(client.getInput(rebuild_analytics_name))
        if reset_view_tables is None:
            reset_view_tables = True  # default: full reset
        rebuild_days_ago = unwrap(client.getInput(rebuild_days_ago_name))
        rebuild_from_date = unwrap(client.getInput(rebuild_from_date_name))
        if init_slurm:
            configfile = unwrap(client.getInput(extra_config_name))
            if not configfile:
                configfile = ''
            with SlurmClient.from_config(configfile=configfile) as slurmClient:
                image_array_id = None
                # Override analytics rebuild window if provided via UI
                if rebuild_days_ago is not None:
                    slurmClient.analytics_rebuild_days_ago = int(rebuild_days_ago)
                elif rebuild_from_date:
                    slurmClient.analytics_rebuild_start_time = rebuild_from_date
                conn.keepAlive()
                # We are kind of duplicating code here, so we can keep the conn alive.
                if slurmClient.validate():
                    # 1. Create directories
                    slurmClient.setup_directories()
                    conn.keepAlive()

                    # 2. Clone git
                    slurmClient.setup_job_scripts()
                    conn.keepAlive()

                    # 3. Stage converters. Their images are submitted together
                    # with workflow images so one concurrency limit covers all
                    # container builds.
                    converter_specs = slurmClient.prepare_converters()
                    conn.keepAlive()

                    # 4. Submit one bounded workflow + converter image array.
                    image_array_id = slurmClient.setup_container_images(
                        extra_image_specs=converter_specs)
                    conn.keepAlive()
                    
                    # 5. Reset db views
                    slurmClient.initialize_analytics_system(reset_tables=reset_view_tables)
                    conn.keepAlive()
                image_status = slurmClient.get_image_pull_status()
                message = (
                    "Slurm directories and scripts are set up.\n" +
                    format_image_submission(image_array_id, image_status) +
                    "\nUse 'SLURM check setup' for per-image failures and "
                    "updated state."
                )
                models, _ = slurmClient.get_all_image_versions_and_data_files()
                filtered_models = {
                    key: value for key, value in models.items() if value}
                message += (
                    f"\nValidated workflow versions currently available: "
                    f"{filtered_models}"
                )

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
