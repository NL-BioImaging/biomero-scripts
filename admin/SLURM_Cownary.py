#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.

"""Run a fixed lolcow job as an end-to-end Slurm cownary (admin only)."""

import logging
import logging.handlers
import os
import re
import sys

import omero
import omero.scripts as omscripts
from biomero import SlurmClient
from omero.gateway import BlitzGateway
from omero.rtypes import rstring


logger = logging.getLogger(__name__)

VERSION = "2.9.0"

_LOLCOW_IMAGE = "docker://godlovedc/lolcow"
_COWNARY_SBATCH_COMMAND = (
    "sbatch --parsable --wait{job_parameters} --nodes=1 --ntasks=1 "
    "--job-name=biomero-cownary "
    "--output=biomero-cownary-%j.log "
    f"--wrap='hostname && singularity run {_LOLCOW_IMAGE}'"
)
_COWNARY_MARKERS = ("^__^", "(oo)\\_______")
_COWNARY_PROTECTED_SBATCH_FLAGS = {
    "--array",
    "--chdir",
    "--error",
    "--input",
    "--job-name",
    "--nodes",
    "--ntasks",
    "--output",
    "--parsable",
    "--wait",
    "--wrap",
}


def _cownary_job_parameters(slurm_client):
    """Return configured sbatch options without changing cownary scope."""
    parameters = []
    seen_flags = set()
    for configured in getattr(slurm_client, "slurm_global_job_params", []):
        parameter = str(configured).strip()
        flag = parameter.split("=", 1)[0]
        if not parameter.startswith("--"):
            logger.warning("Ignoring invalid global sbatch parameter: %s", parameter)
            continue
        if flag in _COWNARY_PROTECTED_SBATCH_FLAGS:
            logger.warning(
                "Ignoring global sbatch parameter protected by the cownary: %s",
                parameter,
            )
            continue
        if flag not in seen_flags:
            parameters.append(f" {parameter}")
            seen_flags.add(flag)

    default_partition = getattr(
        slurm_client, "slurm_default_partition", None
    )
    if default_partition and "--partition" not in seen_flags:
        parameters.insert(0, f" --partition={default_partition}")

    return "".join(parameters)


def _cownary_environment(slurm_client):
    """Return BIOMERO's configured container environment for the job."""
    environment = {}
    tmpdir = getattr(slurm_client, "apptainer_tmpdir", None)
    cachedir = getattr(slurm_client, "apptainer_cachedir", None)
    bind_path = getattr(slurm_client, "slurm_data_bind_path", None)

    if tmpdir:
        environment["APPTAINER_TMPDIR"] = f'"{tmpdir}"'
        environment["SINGULARITY_TMPDIR"] = f'"{tmpdir}"'
    if cachedir:
        environment["APPTAINER_CACHEDIR"] = f'"{cachedir}"'
        environment["SINGULARITY_CACHEDIR"] = f'"{cachedir}"'
    if bind_path:
        environment["APPTAINER_BINDPATH"] = f'"{bind_path}"'

    return environment


def _parse_job_id(stdout):
    """Extract a job ID from ``sbatch --parsable`` output."""
    match = re.fullmatch(r"\s*(\d+)(?:;[^\s;]+)?\s*", stdout or "")
    if not match:
        raise ValueError(
            "Slurm did not return a valid job ID: "
            f"{(stdout or '').strip() or '<empty output>'}"
        )
    return match.group(1)


def run_cownary(slurm_client):
    """Run the fixed cownary in the configured Slurm data directory.

    Returns:
        tuple[bool, str]: Whether the job returned a recognizable cow and the
    complete user-facing report.
    """
    with slurm_client.cd(slurm_client.slurm_data_path):
        submit_command = _COWNARY_SBATCH_COMMAND.format(
            job_parameters=_cownary_job_parameters(slurm_client)
        )
        submission = slurm_client.run(
            submit_command,
            env=_cownary_environment(slurm_client),
            hide=True,
            warn=True,
        )
        try:
            job_id = _parse_job_id(getattr(submission, "stdout", ""))
        except ValueError as exc:
            stderr = getattr(submission, "stderr", "").strip()
            details = f"\nSlurm error: {stderr}" if stderr else ""
            return False, f"COWNARY FAILED\n{exc}{details}"

        log_file = f"biomero-cownary-{job_id}.log"
        log_result = None
        cleanup_result = None
        try:
            log_result = slurm_client.run(
                f'cat "{log_file}"', hide=True, warn=True
            )
        finally:
            # This is the only remote deletion performed by this script. The
            # name is derived from sbatch's validated numeric job ID.
            cleanup_result = slurm_client.run(
                f'rm -f "{log_file}"', hide=True, warn=True
            )

    output = getattr(log_result, "stdout", "").rstrip()
    has_cow = all(marker in output for marker in _COWNARY_MARKERS)
    success = bool(submission.ok and log_result.ok and has_cow)
    node = output.splitlines()[0].strip() if output else "unknown"

    report = [
        "COWNARY PASSED" if success else "COWNARY FAILED",
        f"Slurm job: {job_id}",
        f"Slurm node: {node}",
        "",
        "--- lolcow output ---",
        output or "<no output>",
    ]

    if not submission.ok:
        exit_code = getattr(submission, "exited", "unknown")
        report.append(f"\nSlurm job exit status: {exit_code}")
        stderr = getattr(submission, "stderr", "").strip()
        if stderr:
            report.append(f"Slurm error: {stderr}")
    if not log_result.ok:
        stderr = getattr(log_result, "stderr", "").strip()
        report.append(f"\nCould not read the canary log: {stderr or 'unknown error'}")
    elif not has_cow:
        report.append("\nThe cownary completed but no recognizable cow was returned.")
    if cleanup_result is not None and not cleanup_result.ok:
        cleanup_error = getattr(cleanup_result, "stderr", "").strip()
        logger.warning(
            "Could not remove cownary log %s: %s",
            log_file,
            cleanup_error or "unknown error",
        )
        report.append("\nWarning: the cownary log could not be removed.")

    logger.info("Cownary job %s finished; success=%s", job_id, success)
    logger.info("Cownary report:\n%s", "\n".join(report))
    return success, "\n".join(report)


def runScript():
    """OMERO script entry point."""
    client = omscripts.client(
        "Slurm Cownary (Admin Only)",
        """Run a fixed lolcow Slurm job to verify SSH, scheduling, shared
        storage, and Singularity execution.

        **ADMIN ONLY**: This script requires OMERO administrator privileges.
        It accepts no command or path input.
        """,
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version=VERSION,
        authors=["Torec Luik"],
        institutions=["Amsterdam UMC"],
        contact="cellularimaging@amsterdamumc.nl",
        authorsInstitutions=[[1]],
    )

    try:
        conn = BlitzGateway(client_obj=client)
        user = conn.getUser()
        user_id = conn.getUserId()

        if not user.isAdmin():
            logger.warning(
                "Cownary access denied for non-admin user ID %s",
                user_id,
            )
            client.setOutput(
                "Message",
                rstring(
                    "ACCESS DENIED: This script requires OMERO administrator "
                    f"privileges. User ID {user_id} is not an admin."
                ),
            )
            return

        logger.info(
            "AUDIT: admin user=%s (id=%s) running fixed Slurm cownary",
            user.getName(),
            user_id,
        )
        with SlurmClient.from_config() as slurm_client:
            _success, message = run_cownary(slurm_client)
        client.setOutput("Message", rstring(message))
    except Exception as exc:
        logger.exception("Cownary failed unexpectedly")
        client.setOutput(
            "Message",
            rstring(
                "COWNARY FAILED\n"
                f"{type(exc).__name__}: {exc}"
            ),
        )
    finally:
        client.closeSession()


if __name__ == "__main__":
    OMERODIR = os.environ.get("OMERODIR", "/opt/omero/server/OMERO.server")
    LOGDIR = os.path.join(OMERODIR, "var", "log")
    LOGFORMAT = (
        "%(asctime)s %(levelname)-5.5s [%(name)40s] "
        "[%(process)d] (%(threadName)-10s) %(message)s"
    )
    LOGSIZE = 500000000
    LOGNUM = 9
    log_filename = "biomero.log"

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.DEBUG,
        format=LOGFORMAT,
        handlers=[
            stream_handler,
            logging.handlers.RotatingFileHandler(
                os.path.join(LOGDIR, log_filename),
                maxBytes=LOGSIZE,
                backupCount=LOGNUM,
            ),
        ],
    )

    logging.getLogger("omero.gateway.utils").setLevel(logging.WARNING)
    logging.getLogger("paramiko.transport").setLevel(logging.WARNING)

    runScript()
