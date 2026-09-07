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
- Report per-image structured status and log locations

Setup Validation:
- BIOMERO version information
- SLURM connection status
- Available workflow models and versions
- READY, RUNNING, and FAILED image counts with concise failure reasons
- Converter tool availability
- Data file inventory
- Per-image array logs and structured status

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
from omero.rtypes import rstring
from omero.gateway import BlitzGateway
from biomero import SlurmClient
import logging
import os
import sys
import pkg_resources

# Version constant for easy version management
VERSION = "2.9.0"

logger = logging.getLogger(__name__)


def format_image_pull_status(status):
    """Format exact READY/RUNNING/FAILED counts and concise failures."""
    counts = status.get("counts", {})
    lines = [
        "Image initialization:",
        f"  READY: {counts.get('READY', 0)}",
        f"  RUNNING: {counts.get('RUNNING', 0)}",
        f"  FAILED: {counts.get('FAILED', 0)}",
    ]
    failed = [
        image for image in status.get("images", [])
        if image.get("state") == "FAILED"
    ]
    if failed:
        lines.append("  Failure details:")
        for image in failed:
            reason = image.get("reason") or "unknown failure"
            exit_code = image.get("exit_code")
            exit_text = (
                f" (exit {exit_code})" if exit_code is not None else "")
            lines.append(
                f"    {image.get('name')}:{image.get('version')} - "
                f"{reason}{exit_text}")
    return "\n".join(lines)


def runScript():
    """Main entry point for SLURM setup validation script.
    
    Performs comprehensive validation of BIOMERO-SLURM integration
    including connectivity testing, workflow availability checking,
    converter verification, and structured image initialization status.
    
    Validation includes:
        - BIOMERO version information
        - SLURM cluster connectivity
        - Exact READY, RUNNING, and FAILED image counts
        - Converter tool availability
        - Data file inventory
        - Per-image array log and status location
    
    Results are displayed to the user with concise per-image failure reasons.
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
            models = {name: versions for name, versions in models.items()
                      if versions}
            image_status = slurmClient.get_image_pull_status()
            message += "\n" + format_image_pull_status(image_status)
            message += f"\n>> Available Models: {models}."
            # Check converters:
            converters = slurmClient.list_available_converter_versions()
            message += f"\n>> Available Converters: {converters}."
            message += f"\n>> Available Data: {data}."
            message += (
                f"\n>> Per-image logs and structured status: "
                f"{slurmClient.slurm_script_path}/image-pulls"
            )
            
            logger.info(message)

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
