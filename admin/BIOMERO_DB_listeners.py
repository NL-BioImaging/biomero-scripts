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

import time
import omero
import omero.gateway
from omero import scripts
from omero.rtypes import rstring, unwrap
from omero.gateway import BlitzGateway
from biomero import SlurmClient, WorkflowTracker, NoOpWorkflowTracker
import logging
import os
import sys

logger = logging.getLogger(__name__)
VERSION = "1.14.0"


def runScript():
    """
    The main entry point of the script
    """
    client = scripts.client(
        'BIOMERO DB listener',
        '''Will keep the BIOMERO DB up to date by listening to the workflowTracker(s)
        ''',
        scripts.Bool("Job Accounting", grouping="01", default=True),
        scripts.Bool("Job Progress", grouping="02", default=True),
        scripts.Bool("Workflow Analytics", grouping="03", default=True),
        scripts.Bool("Keep listening", grouping="04", default=True),
        scripts.Int("Refresh rate (seconds)", grouping="04.1", default=30),
        scripts.Bool("Repopulate all tables", grouping="05", default=True),
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
        enable_job_accounting = unwrap(client.getInput("Job Accounting"))
        if enable_job_accounting: 
            message += "|| Enabled Job Accounting listening"
        enable_job_progress = unwrap(client.getInput("Job Progress"))
        if enable_job_progress: 
            message += "|| Enabled Job Progress listening"
        enable_workflow_analytics = unwrap(client.getInput("Workflow Analytics"))
        if enable_workflow_analytics: 
            message += "|| Enabled Workflow Analytics listening"
        keep_listening = unwrap(client.getInput("Keep listening"))
        if keep_listening:
            refresh_rate = unwrap(client.getInput("Refresh rate (seconds)"))
            message += f"|| Will keep listening while script runs ({refresh_rate}s)"
        repop = unwrap(client.getInput("Repopulate all tables"))
        if repop:
            message += "|| Will repopulate all view tables"
        logger.info("Listening for BIOMERO DB: "+message)
        with SlurmClient(track_workflows=True,
                         enable_job_accounting=enable_job_accounting,
                         enable_job_progress=enable_job_progress,
                         enable_workflow_analytics=enable_workflow_analytics
                         ) as slurmClient:
            slurmClient.initialize_analytics_system(repop)
            logger.info("Repopulated BIOMERO DB view tables")
            listeners = []
            for listener in [slurmClient.jobAccounting, 
                             slurmClient.jobProgress,
                             slurmClient.wfProgress, 
                             slurmClient.workflowAnalytics]:
                if not isinstance(listener, NoOpWorkflowTracker):
                    listeners.append(listener)
            while keep_listening:
                conn.keepAlive()
                for listener in listeners:
                    next_up = listener.recorder.max_tracking_id(
                        WorkflowTracker.__name__) + 1
                    logger.debug(f"Updating BIOMERO DB listener: {listener}, from {next_up}")
                    slurmClient.bring_listener_uptodate(listener, 
                                                        start=next_up)
                logger.debug("Updated BIOMERO DB view tables")
                time.sleep(refresh_rate)

        message += "Stopping listening"
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
    logging.getLogger('invoke').setLevel(logging.INFO)

    runScript()
