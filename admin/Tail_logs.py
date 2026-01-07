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
from omero.rtypes import rstring, rlong
import omero.scripts as omscripts
import subprocess
import logging
import os
import sys
from omero.gateway import BlitzGateway
import omero

# Define log path and tail lines constants
_LOG_PATH = "Log_Path"
_DEFAULT_LOG_PATH = "/opt/omero/server/OMERO.server/var/log/biomero.log"
_TAIL_LINES = "Tail_Lines"
_DEFAULT_TAIL_LINES = 5000

# Version constant for easy version management
VERSION = "2.1.0"

logger = logging.getLogger(__name__)


def runScript():
    """
    Main entry point to check for admin privileges and tail log file.
    """
    # Connect to OMERO and check if the user has admin privileges
    client = omscripts.client(
        'Tail Biomero Log (Admin Only)',
        '''Fetches the last N lines from biomero.log.
       
        **ADMIN ONLY**: This script requires OMERO administrator privileges.
        ''',
        omscripts.String(_LOG_PATH, grouping="01",
                         default=rstring(_DEFAULT_LOG_PATH),
                         description="Path to the biomero log file to tail."),
        omscripts.Long(_TAIL_LINES, optional=True, grouping="01.1",
                         default=rlong(_DEFAULT_TAIL_LINES),
                         description="The number of lines to tail from the log file."),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact='t.t.luik@amsterdamumc.nl',
        authorsInstitutions=[[1]]
    )

    try:
        # Verify admin privileges
        conn = BlitzGateway(client_obj=client)
        user = conn.getUser()
        is_admin = user.isAdmin()
        user_id = conn.getUserId()

        logger.info(f"User ID {user_id} admin status: {is_admin}")

        if not is_admin:
            logger.warning("Access denied: Admin privileges required")
            client.setOutput("Message", rstring(
                f"ACCESS DENIED: This script requires OMERO administrator privileges. "
                f"User ID {user_id} is not an admin."
            ))
            return

        logger.info("Admin access confirmed, proceeding with log tailing")

        # Retrieve input parameters for log path and tail lines
        scriptParams = client.getInputs(unwrap=True)
        log_path = scriptParams.get(_LOG_PATH, _DEFAULT_LOG_PATH)
        tail_lines = scriptParams.get(_TAIL_LINES, _DEFAULT_TAIL_LINES)

        # Check if the log file exists
        if os.path.exists(log_path):
            try:
                print(
                    f"Fetching the last {tail_lines} lines from {log_path}...")
                # Run the tail command to get the last N lines
                cmd = ["tail", "-n", str(tail_lines), log_path]
                result = subprocess.run(
                    cmd, capture_output=True, text=True, check=True)
                print(result.stdout)  # Print the output to stdout
            except subprocess.CalledProcessError as e:
                print(f"Error running tail command: {e}", file=sys.stderr)
        else:
            print(f"Error: Log file not found at {log_path}.", file=sys.stderr)

        # Send completion message to OMERO
        client.setOutput("Message", rstring("Log tailing complete"))

    except Exception as e:
        # If something goes wrong, log the error and return a message to OMERO
        logger.error(f"Error occurred: {str(e)}")
        client.setOutput("Message", rstring(f"Error: {str(e)}"))

    finally:
        client.closeSession()


if __name__ == '__main__':
    # Some defaults from OMERO; don't feel like reading ice files.
    # Retrieve the value of the OMERODIR environment variable
    OMERODIR = os.environ.get('OMERODIR', '/opt/omero/server/OMERO.server')
    LOGDIR = os.path.join(OMERODIR, 'var', 'log')
    LOGFORMAT = "%(asctime)s %(levelname)-5.5s [%(name)40s] " \
                "[%(process)d] (%(threadName)-10s) %(message)s"
    LOGSIZE = 500000000
    LOGNUM = 9
    log_filename = 'biomero.log'

    # Create a stream handler with INFO level (for OMERO.web output)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)

    # Set up the logging format and handlers
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGFORMAT,
                        handlers=[
                            stream_handler,
                            logging.handlers.RotatingFileHandler(
                                os.path.join(LOGDIR, log_filename),
                                maxBytes=LOGSIZE,
                                backupCount=LOGNUM)
                        ])

    # Run the script
    runScript()
