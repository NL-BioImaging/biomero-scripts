#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt

"""
BIOMERO SLURM Setup Validation Script (Admin Only)

This administrative script provides comprehensive validation and monitoring
of SLURM cluster setup for BIOMERO workflow execution.

**ADMIN ONLY**: This script requires OMERO administrator privileges.

Key Features:
- Validate SLURM cluster connectivity and configuration
- Check availability of workflow container images
- Monitor pending image downloads and builds
- Verify converter tool availability
- Display available data files on cluster
- Retrieve and display cluster setup logs

Setup Validation:
- BIOMERO version information
- SLURM connection status
- Available workflow models and versions
- Pending model downloads/builds
- Converter tool availability
- Data file inventory
- Container build logs (sing.log)

Administrative Use:
- Initial setup verification after installation
- Troubleshooting connection or configuration issues
- Monitoring workflow image availability
- Checking setup progress during initialization

This script is primarily used by OMERO administrators to verify
and monitor the BIOMERO-SLURM integration setup.

Authors: Torec Luik, OMERO Team
Institution: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

import omero
import omero.gateway
from omero import scripts
from omero.rtypes import rstring, wrap
from omero.gateway import BlitzGateway
from biomero import SlurmClient
import logging
import os
import sys
import pkg_resources

# Version constant for easy version management
VERSION = "2.0.0-alpha.7"

logger = logging.getLogger(__name__)


def runScript():
    """Main entry point for SLURM setup validation script.
    
    Performs comprehensive validation of BIOMERO-SLURM integration
    including connectivity testing, workflow availability checking,
    converter verification, and setup log retrieval.
    
    Validation includes:
        - BIOMERO version information
        - SLURM cluster connectivity
        - Available workflow models and pending downloads
        - Converter tool availability
        - Data file inventory
        - Container build log retrieval and attachment
    
    Results are displayed to user and optionally attached as log files
    to OMERO for administrative review.
    """

    client = scripts.client(
        'Slurm Check Setup (Admin Only)',
        '''Check Slurm setup, e.g. available workflows.
        
        **ADMIN ONLY**: Requires OMERO administrator privileges.
        ''',
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact='cellularimaging@amsterdamumc.nl',
        authorsInstitutions=[[1]]
    )

    try:
        # Check if user is admin
        conn = BlitzGateway(client_obj=client)
        user = conn.getUser()
        is_admin = user.isAdmin()
        user_id = conn.getUserId()
        
        logger.info(f"User ID {user_id} admin status: {is_admin}")
        
        if not is_admin:
            logger.warning("Access denied: Admin privileges required")
            client.setOutput("Message", rstring(
                f"ACCESS DENIED: This administrative script requires OMERO "
                f"administrator privileges. User ID {user_id} is not an admin."
            ))
            return
        
        logger.info("Admin access confirmed, proceeding with setup check")
        message = ""
        with SlurmClient.from_config() as slurmClient:
            bio_version = pkg_resources.get_distribution("biomero").version
            message = f"== BIOMERO v{bio_version} =="
            message += f"\nConnected: {slurmClient.validate()}" + \
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
            
            message += f"\n>> Available Models: {models}."
            message += f"\n>>> Pending Models: {pending}."
            # Check converters:
            converters = slurmClient.list_available_converter_versions()
            message += f"\n>> Available Converters: {converters}."
            message += f"\n>> Available Data: {data}."
            
            logger.info(message)
            # misuse get_logfile to get this sing.log
            tup = slurmClient.get_logfile_from_slurm(slurm_job_id='',
                                                     logfile=f"{slurmClient.slurm_images_path}/sing.log")
            (dir, export_file, result) = tup
            # little script hack here so we don't have to adjust the client
            export_file = os.path.join(dir, "sing.log")
            logger.debug(f"Pulled logfile {result.__dict__}")
            # Upload logfile to Omero as Original File
            mimetype = 'text/plain'
            obj = client.upload(export_file, type=mimetype)
            obj_id = obj.id.val
            url = f"get_original_file/{obj_id}/"
            client.setOutput("URL", wrap({"type": "URL", "href": url}))
            message += f"\n>>Also pulled the singularity log, click the URL button above to view ({url})"

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
