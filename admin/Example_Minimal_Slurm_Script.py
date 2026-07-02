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
from omero.rtypes import rstring
import omero.scripts as omscripts
import omero
import re
import subprocess
from biomero import SlurmClient
from omero.gateway import BlitzGateway
import logging
import os
import sys

logger = logging.getLogger(__name__)

_MAX_CMD_LENGTH = 500

# Block destructive or dangerous command patterns even for admins
_DANGEROUS_PATTERNS = [
    r"\brm\b",           # rm (remove files)
    r"\brmdir\b",        # rmdir
    r"\bmkfs\b",         # format filesystem
    r"\bdd\b",           # disk dump/overwrite
    r"\bkill\b",         # kill processes
    r"\bpkill\b",        # kill by name
    r"\bscancel\b",      # cancel Slurm jobs
    r"\breboot\b",       # reboot node
    r"\bshutdown\b",     # shutdown node
    r"\bpoweroff\b",     # poweroff node
    r"\btruncate\b",     # truncate files
    r"\bchmod\b",        # change permissions
    r"\bchown\b",        # change ownership
    r">\s*/",            # redirect to root path
    r"\|\s*sh\b",        # pipe to shell
    r"\|\s*bash\b",      # pipe to bash
    r"\beval\b",         # eval arbitrary code
    r"\bexec\b",         # replace process
]


def _check_command_safety(cmd: str) -> str | None:
    """Return an error message if the command matches a dangerous pattern."""
    if len(cmd) > _MAX_CMD_LENGTH:
        return (f"Command too long ({len(cmd)} chars, max {_MAX_CMD_LENGTH}). "
                f"Refusing to run.")
    for pattern in _DANGEROUS_PATTERNS:
        if re.search(pattern, cmd, re.IGNORECASE):
            return (f"Command blocked: matches dangerous pattern '{pattern}'. "
                    f"If you need this, run it directly on the Slurm cluster.")
    return None


_RUNSLRM = "Check_SLURM_Status"
_SQUEUE = "Check_Queue"
_SINFO = "Check_Cluster"
_SOTHER = "Run_Other_Command"
_SCMD = "Linux_Command"
_DEFAULT_SCMD = "ls -la"

VERSION = "2.8.0"


def runScript():
    """
    The main entry point of the script
    """
    client = omscripts.client(
        'Slurm SSH Command (Admin Only)',
        '''Run SSH commands on the Slurm cluster.

        **ADMIN ONLY**: This script requires OMERO administrator privileges.
        ''',
        omscripts.Bool(_RUNSLRM, grouping="01", default=True,
                       description="Run Slurm status commands"),
        omscripts.Bool(_SQUEUE, grouping="01.1", default=True,
                       description="Show job queue (squeue -u $USER)"),
        omscripts.Bool(_SINFO, grouping="01.2", default=False,
                       description="Show cluster info (sinfo)"),
        omscripts.Bool(_SOTHER, grouping="02", default=False,
                       description="Run a custom Linux command on Slurm"),
        omscripts.String(_SCMD, optional=True, grouping="02.1",
                         description="The Linux command to run on Slurm",
                         default=_DEFAULT_SCMD),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact='cellularimaging@amsterdamumc.nl',
        authorsInstitutions=[[1]]
    )

    try:
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

        scriptParams = client.getInputs(unwrap=True)
        logger.info(f"Params: {scriptParams}")

        user_name = user.getName()

        with SlurmClient.from_config() as slurmClient:
            logger.info(f"Slurm connection valid: {slurmClient.validate()}")

            cmdlist = []
            if scriptParams.get(_RUNSLRM):
                if scriptParams.get(_SQUEUE):
                    cmdlist.append("squeue -u $USER")
                if scriptParams.get(_SINFO):
                    cmdlist.append("sinfo")
            if scriptParams.get(_SOTHER):
                custom_cmd = scriptParams.get(_SCMD, "").strip()
                safety_error = _check_command_safety(custom_cmd)
                if safety_error:
                    logger.warning(
                        f"BLOCKED command from admin user {user_name} "
                        f"(id={user_id}): '{custom_cmd}' — {safety_error}"
                    )
                    client.setOutput("Message", rstring(
                        f"Command blocked: {safety_error}"
                    ))
                    return
                cmdlist.append(custom_cmd)

            print_result = []
            try:
                for cmd in cmdlist:
                    # Audit: log who ran what
                    logger.info(
                        f"AUDIT: user={user_name} (id={user_id}) "
                        f"running command: {cmd}"
                    )
                    results = slurmClient.run(cmd)
                    logger.info(f"Ran slurm: {results}")
                    print_result.append(str(results))
            except Exception as e:
                logger.error(f"Command error: {e}")
                print_result.append(f"Error: {e}")

            client.setOutput("Message", rstring("\n".join(print_result)))

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