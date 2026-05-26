#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2024 T T Luik
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
BIOMERO SLURM File Attachment Transfer Script

Transfers a single OMERO FileAnnotation to a SLURM cluster, placing it
under the correct parameter input directory so the workflow container
receives it as a CLI flag.

The destination on SLURM is:
  {slurm_data_path}/{Folder_Name}/data/in/{filename}

The resolved absolute SLURM path is returned as the ``Slurm_Path`` output,
which ``SLURM_Run_Workflow.py`` injects as the CLI argument value for the
corresponding workflow parameter.

Inputs:
    Annotation_ID (Long): OMERO FileAnnotation ID supplied by the user.
    Folder_Name (String): The job data folder on SLURM (same value used
        when the images were transferred, e.g. ``SLURM_IMAGES_1234567890``).
    Cleanup? (Bool, optional): Remove the local temp file after transfer
        (default True).

Outputs:
    Slurm_Path (String): Absolute SLURM path of the transferred file.
    Message (String): Human-readable status / error message.

Authors: Torec Luik
Institutions: Amsterdam UMC
Contact: cellularimaging@amsterdamumc.nl
"""

import os
import sys
import logging
import logging.handlers
import tempfile

import omero.scripts as scripts
import omero
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong

from biomero import SlurmClient, constants

logger = logging.getLogger(__name__)

VERSION = "2.7.0"


# ---------------------------------------------------------------------------
# Core transfer logic
# ---------------------------------------------------------------------------

def transfer_file_to_slurm(
    conn: BlitzGateway,
    annotation_id: int,
    folder_name: str,
    slurmClient: SlurmClient,
    cleanup: bool = True,
    allowed_formats: list = None,
) -> tuple[str, str]:
    """Download an OMERO FileAnnotation and upload it to the SLURM cluster.

    Args:
        conn: Active OMERO BlitzGateway connection.
        annotation_id: ID of the OMERO FileAnnotation to transfer.
        folder_name: Name of the job data folder on SLURM. Must match the
            folder name used for the image transfer so both end up in the
            same data/in/ directory. Created automatically if absent.
        slurmClient: Authenticated SlurmClient instance.
        cleanup: If True, delete the local temp file after transfer.
        allowed_formats: Optional list of accepted file extensions (without
            leading dot, e.g. ["csv", "unix"]). If provided and the file's
            extension is not in the list, a ValueError is raised before any
            data transfer takes place.

    Returns:
        (slurm_path, message): Absolute path of the file on SLURM and a
        human-readable status message.

    Raises:
        ValueError: If the annotation or its underlying file cannot be found,
            or if the file extension does not match ``allowed_formats``.
        Exception: On SSH / SCP failure.
    """
    # ------------------------------------------------------------------
    # 1. Fetch the FileAnnotation and its underlying OriginalFile
    # ------------------------------------------------------------------
    ann = conn.getObject("FileAnnotation", annotation_id)
    if ann is None:
        raise ValueError(
            f"FileAnnotation {annotation_id} not found or not accessible."
        )
    orig_file = ann.getFile()
    if orig_file is None:
        raise ValueError(
            f"FileAnnotation {annotation_id} has no associated OriginalFile."
        )
    filename = orig_file.getName()
    file_size = orig_file.getSize()

    # ------------------------------------------------------------------
    # Validate file extension against allowed formats (if specified)
    # ------------------------------------------------------------------
    if allowed_formats:
        ext = os.path.splitext(filename)[1].lstrip('.').lower()
        normalised = [f.lstrip('.').lower() for f in allowed_formats if f]
        if normalised and ext not in normalised:
            raise ValueError(
                f"File '{filename}' has extension '.{ext}' but this parameter "
                f"expects one of: {normalised}. "
                f"Please select a file with a supported format."
            )

    logger.info(
        f"Downloading FileAnnotation {annotation_id} "
        f"('{filename}', {file_size} bytes) …"
    )

    # ------------------------------------------------------------------
    # 2. Download file bytes to a local temp file
    # ------------------------------------------------------------------
    tmp_dir = tempfile.mkdtemp(prefix="biomero_ft_")
    local_path = os.path.join(tmp_dir, filename)
    try:
        with open(local_path, "wb") as fh:
            for chunk in ann.getFileInChunks():
                fh.write(chunk)
        logger.info(f"File saved locally to {local_path}")

        # ------------------------------------------------------------------
        # 3. Create destination directory on SLURM
        # ------------------------------------------------------------------
        dest_dir = f"{slurmClient.slurm_data_path}/{folder_name}/data/in"
        mkdir_result = slurmClient.run_commands(
            [f'mkdir -p "{dest_dir}"']
        )
        if not mkdir_result.ok:
            raise Exception(
                f"Failed to create remote directory '{dest_dir}': "
                f"{mkdir_result.stderr}"
            )
        logger.info(f"Remote directory ready: {dest_dir}")

        # ------------------------------------------------------------------
        # 4. Transfer file to SLURM
        # ------------------------------------------------------------------
        slurmClient.put(local=local_path, remote=dest_dir)
        slurm_path = f"{dest_dir}/{filename}"
        logger.info(f"File transferred to SLURM: {slurm_path}")
        message = (
            f"Successfully transferred '{filename}' to SLURM "
            f"into '{folder_name}/data/in/'."
        )
        return slurm_path, message

    finally:
        if cleanup and os.path.exists(local_path):
            os.remove(local_path)
            os.rmdir(tmp_dir)
            logger.debug(f"Cleaned up local temp file {local_path}")


# ---------------------------------------------------------------------------
# OMERO script entry point
# ---------------------------------------------------------------------------

def run_script():
    """OMERO script entry point for _SLURM_File_Transfer."""

    with SlurmClient.from_config() as slurmClient:

        client = scripts.client(
            "_SLURM_File_Transfer",
            f"""Transfer an OMERO FileAnnotation to the SLURM cluster.

Places the file under:
  {{slurm_data_path}}/{{Folder_Name}}/data/in/{{filename}}

Returns the resolved SLURM path so SLURM_Run_Workflow can pass it as a
CLI flag to the workflow container.

This runs a script remotely on your SLURM cluster.
Connection ready? {slurmClient.validate()}""",

            scripts.Long(
                constants.file_transfer.FILE_ANNOTATION_ID,
                optional=False,
                grouping="1",
                description="OMERO FileAnnotation ID of the file to transfer.",
            ),

            scripts.String(
                constants.file_transfer.FOLDER,
                optional=False,
                grouping="2",
                description=(
                    "SLURM job data folder name (e.g. SLURM_IMAGES_1234567890). "
                    "Must match the folder name used for the image transfer so "
                    "images and file attachments share the same data/in/ directory. "
                    "The directory is created automatically if it does not yet exist."
                ),
            ),

            scripts.Bool(
                constants.CLEANUP,
                grouping="3",
                description=(
                    "Remove the local temporary file after transfer. "
                    "Uncheck for debugging."
                ),
                default=True,
            ),

            scripts.String(
                constants.file_transfer.FORMAT,
                optional=True,
                grouping="4",
                description=(
                    "Comma-separated list of accepted file extensions "
                    "(without leading dot, e.g. 'csv,parquet'). "
                    "When provided, the transfer is rejected if the selected "
                    "file does not match. Leave empty to skip validation."
                ),
            ),

            version=VERSION,
            authors=["Torec Luik"],
            institutions=["Amsterdam UMC"],
            contact="cellularimaging@amsterdamumc.nl",
            authorsInstitutions=[[1]],
            namespaces=[omero.constants.namespaces.NSDYNAMIC],
        )

        try:
            conn = BlitzGateway(client_obj=client)
            script_params = client.getInputs(unwrap=True)

            annotation_id = script_params[constants.file_transfer.FILE_ANNOTATION_ID]
            folder_name = script_params[constants.file_transfer.FOLDER]
            cleanup = script_params.get(constants.CLEANUP, True)
            formats_raw = script_params.get(constants.file_transfer.FORMAT, "")
            allowed_formats = (
                [f.strip() for f in formats_raw.split(",") if f.strip()]
                if formats_raw else None
            )

            slurm_path, message = transfer_file_to_slurm(
                conn=conn,
                annotation_id=annotation_id,
                folder_name=folder_name,
                slurmClient=slurmClient,
                cleanup=cleanup,
                allowed_formats=allowed_formats,
            )

            client.setOutput("Message", rstring(message))
            client.setOutput("Slurm_Path", rstring(slurm_path))

        except Exception as exc:
            logger.exception("File transfer failed")
            client.setOutput("Message", rstring(f"ERROR: {exc}"))

        finally:
            client.closeSession()


if __name__ == "__main__":
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

    run_script()
