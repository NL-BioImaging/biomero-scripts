#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Torec Luik, Amsterdam UMC
# Licensed under the GNU General Public License version 2; see LICENSE.
#
"""Ask a running BIOMERO workflow to cancel.

Records a cancel request on the workflow
(``WorkflowTracker.request_workflow_cancel``).
Whoever executes the workflow, the detached supervisor or the inline script,
sees the request at its next step, cancels the Slurm jobs of the workflow and
marks the workflow FAILED with the reason. Detached runs have no OMERO script
process of their own to cancel, which is why this script exists.
"""

from uuid import UUID
from omero.rtypes import rstring
import omero.scripts as omscripts
import logging
import logging.handlers
import os
import sys
from omero.gateway import BlitzGateway
import omero
from biomero import WorkflowTracker, constants

_DEFAULT_REASON = "Cancelled by user"
_MAX_REASON_LENGTH = 200

# Version constant for easy version management
VERSION = "2.9.0b29"

logger = logging.getLogger(__name__)


def request_cancel(tracker: WorkflowTracker,
                   workflow_id: UUID,
                   requester_id: int,
                   is_admin: bool,
                   reason: str) -> str:
    """Record a cancel request, if the requester may cancel this workflow.

    Args:
        tracker (WorkflowTracker): Tracker holding the workflow.
        workflow_id (UUID): The workflow to cancel.
        requester_id (int): OMERO user ID of whoever asks.
        is_admin (bool): Whether that user is an OMERO administrator.
        reason (str): Why, recorded with the request (cut to
            ``_MAX_REASON_LENGTH`` characters).

    Returns:
        str: The message for the user. A workflow that already finished, or
            that already has a cancel request, records nothing new.

    Raises:
        PermissionError: If the requester is neither admin nor the workflow's
            owner.
    """
    workflow = tracker.repository.get(workflow_id)
    if not (is_admin or workflow.user == requester_id):
        # Raised, and logged once by the caller with its traceback.
        raise PermissionError(
            f"Workflow {workflow_id} belongs to another user; only its owner "
            f"or an administrator can cancel it.")
    if tracker.is_workflow_finished(workflow_id):
        logger.info(f"Workflow {workflow_id} already finished; no cancel "
                    f"recorded")
        return f"Workflow {workflow_id} already finished; nothing to cancel."
    reason = reason[:_MAX_REASON_LENGTH]
    if tracker.is_workflow_cancel_requested(workflow_id):
        logger.info(f"Cancel of workflow {workflow_id} was already requested")
        return f"Cancel of workflow {workflow_id} was already requested."
    tracker.request_workflow_cancel(workflow_id, reason)
    logger.info(f"User {requester_id} requested cancel of workflow "
                f"{workflow_id}: {reason}")
    return (f"Cancel requested for workflow {workflow_id}. It stops at its "
            f"next step and shows as FAILED with this reason. (Batched "
            f"workflows that run inline, not detached, do not honour cancel.)")


def runScript() -> None:
    """Main entry point: cancel the workflow named in the inputs.

    Reads the workflow ID and reason from the script inputs and sets the
    ``Message`` output to the outcome. When the request fails, the error is
    set as the ``Message`` and raised again, so OMERO shows the script as
    failed.

    Raises:
        Exception: Whatever stopped the request, such as a ``PermissionError``
            for a workflow of another user or a ``ValueError`` for a workflow
            ID that is not a UUID.
    """
    client = omscripts.client(
        "Cancel BIOMERO Workflow",
        """Cancels a running BIOMERO workflow, including detached ones.

        The workflow stops at its next step, its Slurm jobs are cancelled and
        it is marked FAILED with the reason. Only the workflow's owner or an
        OMERO administrator can cancel it.
        """,
        omscripts.String(constants.cancel_workflow.WORKFLOW_ID,
                         grouping="01", optional=False,
                         description="UUID of the workflow, as shown in the "
                                     "BIOMERO workflow overview."),
        omscripts.String(constants.cancel_workflow.REASON,
                         grouping="02", optional=True,
                         default=_DEFAULT_REASON,
                         description="Why the workflow is cancelled."),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact="t.t.luik@amsterdamumc.nl",
        authorsInstitutions=[[1]]
    )
    try:
        conn = BlitzGateway(client_obj=client)
        inputs = client.getInputs(unwrap=True)
        workflow_id = UUID(
            str(inputs[constants.cancel_workflow.WORKFLOW_ID]).strip())
        reason = (inputs.get(constants.cancel_workflow.REASON)
                  or _DEFAULT_REASON)
        with WorkflowTracker() as tracker:
            message = request_cancel(
                tracker, workflow_id, conn.getUserId(),
                conn.getUser().isAdmin(), reason)
        client.setOutput(constants.results.MESSAGE_OUTPUT, rstring(message))
    except Exception as e:
        logger.error(f"Could not cancel the workflow: {e}", exc_info=True)
        client.setOutput(constants.results.MESSAGE_OUTPUT,
                         rstring(f"Error: {e}"))
        # Show the red X in OMERO, as the other scripts do on failure.
        raise
    finally:
        client.closeSession()


if __name__ == "__main__":
    OMERODIR = os.environ.get("OMERODIR", "/opt/omero/server/OMERO.server")
    LOGDIR = os.path.join(OMERODIR, "var", "log")
    LOGFORMAT = "%(asctime)s %(levelname)-5.5s [%(name)40s] " \
                "[%(process)d] (%(threadName)-10s) %(message)s"
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format=LOGFORMAT,
                        handlers=[
                            stream_handler,
                            logging.handlers.RotatingFileHandler(
                                os.path.join(LOGDIR, "biomero.log"),
                                maxBytes=500000000, backupCount=9)
                        ])
    runScript()
