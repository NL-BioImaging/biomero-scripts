#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt

"""
BIOMERO SLURM Results Import Script

This script handles the import of completed workflow results from SLURM
clusters back into OMERO with flexible organization options and metadata
preservation.

Key Features:
- Import results from completed SLURM jobs
- Flexible result organization (attach to original data, new datasets, etc.)  
- Support for multiple file formats (TIFF, OME-TIFF, PNG)
- CSV data import as OMERO tables
- Metadata preservation and linking
- Configurable naming and dataset organization
- Comprehensive error handling and logging
- Automatic workflow UUID extraction from SLURM logs

NEW: Importer-Only Dataset Import Workflow:
- Dataset imports: Requires biomero-importer and IMPORTER_ENABLED=true
- CSV tables: Direct OMERO table import
- File attachments, zip uploads: Standard attachment workflows
- Script will fail if dataset import is requested but importer is not enabled/available

Workflow ID Handling:
- If workflow_id is provided and valid: Uses provided UUID
- If not provided or invalid: Automatically extracts from SLURM log file 
- Final fallback: Generates random UUID
- UUIDs enable proper metadata linking and workflow tracking

Import Options:
- Attach results to original images as attachments
- Create new datasets with custom naming
- Import into parent dataset/plate structure  
- Convert CSV files to OMERO tables
- Rename imported images with custom patterns

File Support:
- Images: TIFF, OME-TIFF, PNG formats (importer-enabled for dataset imports)
- Tables: CSV files converted to OMERO.tables
- Metadata: Preserved through import process and exported to CSV for importer

CLEANUP BEHAVIOR:
When cleanup is enabled, this script removes:
- SLURM server: Original job results directory, temporary zip files, job logs
- Local: Temporary files from zip operations (if any were created)

PERMANENT STORAGE (NEVER CLEANED):
- Permanent storage: .analyzed/uuid/timestamp/ directories and ALL contents
- Complete workflow preservation: images, CSVs, metadata, logs, analysis results
- Enables both data preservation and biomero-importer access to image files
- This is completely separate from temporary SLURM files

This script is typically called automatically by SLURM_Run_Workflow.py
but can be used standalone for manual result importing.

Authors: Torec Luik, OMERO Team
Institution: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

import csv
import glob
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import omero
import omero.gateway
from omero import scripts
from omero.constants.namespaces import NSCREATED
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, robject, unwrap, wrap
from omero_metadata.populate import ParsingContext

from biomero import SlurmClient, constants

logger = logging.getLogger(__name__)

# Check if importer is enabled via environment variable
IMPORTER_ENABLED = os.getenv("IMPORTER_ENABLED", "false").lower() == "true"
UUID_PATTERN = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

try:
    from biomero_importer.utils.ingest_tracker import (
        initialize_ingest_tracker,
        log_ingestion_step,
        STAGE_NEW_ORDER,
        _mask_url,
    )
    IMPORTER_AVAILABLE = True
except ImportError:
    IMPORTER_AVAILABLE = False
    def _mask_url(url): return url  # Fallback function if import fails

# Check both environment setting and module availability
if IMPORTER_ENABLED and not IMPORTER_AVAILABLE:
    logger.error(
        "IMPORTER_ENABLED is true but biomero-importer module is not available")
    raise RuntimeError(
        "IMPORTER_ENABLED is true but biomero-importer is not installed or available")

if not IMPORTER_ENABLED:
    logger.warning(
        "IMPORTER_ENABLED is false - dataset imports will not be supported")

# Version constant for easy version management
VERSION = "2.3.0"

OBJECT_TYPES = (
    'Plate',
    'Screen',
    'Dataset',
    'Project',
    'Image',
)

_LOGFILE_PATH_PATTERN_GROUP = "DATA_PATH"
_LOGFILE_PATH_PATTERN = "Running [\w-]+? Job w\/ .+? \| .+? \| (?P<DATA_PATH>.+?) \|.*"
SUPPORTED_IMAGE_EXTENSIONS = [
    # TIFF formats
    '.tif', '.tiff', '.ome.tif', '.ome.tiff',
    # Common formats
    '.png', '.jpg', '.jpeg', '.gif', '.bmp',
    # Microscopy formats
    '.lsm', '.czi', '.nd2', '.oib', '.oif', '.vsi', '.scn',
    '.svs', '.ims', '.lif', '.pic', '.flex', '.zvi',
    # Zarr and OME formats
    '.zarr', '.ome.zarr',
    # Other scientific formats
    '.dm3', '.dm4', '.ser', '.img', '.hdr', '.sdt',
    '.psd', '.fits', '.dcm', '.dicom',
    # Multi-page formats
    '.stk', '.lei', '.mrc', '.rec', '.st',
    # Proprietary formats
    '.mvd2', '.afi', '.exp', '.ipw', '.raw',
    '.nrrd', '.nhdr', '.am', '.amiramesh',
    # Video formats that Bio-Formats supports
    '.avi', '.mov', '.flv', '.swf'
]
SUPPORTED_TABLE_EXTENSIONS = ['.csv']

# Importer integration configuration - use importer APIs instead of env vars
if IMPORTER_ENABLED:
    try:
        from biomero_importer.utils.initialize import load_settings
        IMPORTER_CONFIG = load_settings(
            "/opt/omero/server/config-importer/settings.yml")
    except (ImportError, Exception):
        IMPORTER_CONFIG = None
else:
    IMPORTER_CONFIG = None


def getOriginalFilename(name: str) -> str:
    """Extract original filename from processed file path.

    Extracts the base filename from a processed file path by removing workflow
    processing suffixes.

    Args:
        name: Path/name of processed file.

    Returns:
        Original filename if pattern matches, otherwise input name.

    Example:
        >>> getOriginalFilename("/../../Cells Apoptotic.png_merged_z01_t01.tiff")
        "Cells Apoptotic.png"
    """
    match = re.match(pattern=".+\/(.+\.[A-Za-z]+).+\.[tiff|png]", string=name)
    if match:
        name = match.group(1)

    return name


def saveCSVToOmeroAsTable(
    conn: BlitzGateway,
    folder: str,
    client: Any,
    data_type: str,
    object_id: int,
    wf_id: Optional[str] = None
) -> str:
    """Save CSV files from folder to OMERO as OMERO.tables.

    Searches for CSV files in the specified folder and converts them to
    OMERO.tables attached to the specified OMERO object. Falls back to
    file attachments if table creation fails.

    Args:
        conn: OMERO BlitzGateway connection.
        folder: Path to folder containing CSV files.
        client: OMERO script client for job ID access.
        data_type: Type of OMERO object ('Dataset', 'Plate', etc.).
        object_id: ID of OMERO object to attach tables to.
        wf_id: Workflow ID for metadata. Defaults to None.

    Returns:
        Status message describing import results.
    """
    message = ""

    # Get a list of all CSV files in the folder
    all_files = glob.iglob(folder+'**/**', recursive=True)
    csv_files = [f for f in all_files if os.path.isfile(f)
                 and any(f.endswith(ext) for ext in SUPPORTED_TABLE_EXTENSIONS)]
    logger.info(f"Found the following table files in {folder}: {csv_files}")
    # namespace = NSCREATED + "/BIOMERO/SLURM_GET_RESULTS"
    job_id = unwrap(client.getInput(
        constants.results.OUTPUT_SLURM_JOB_ID)).strip()

    if not csv_files:
        return "No table files found in the folder."

    for csv_file in csv_files:
        try:
            # We use the omero-metadata plugin to populate a table
            # See https://pypi.org/project/omero-metadata/
            # It has ways to automatically detect types based on column names
            # or a "# header" line.
            # That is up to the user to format their csv.
            #
            # Default is StringColumn for everything.
            # d: DoubleColumn, for floating point numbers
            # l: LongColumn, for integer numbers
            # s: StringColumn, for text
            # b: BoolColumn, for true/false
            # plate, well, image, dataset, roi to specify objects
            #
            # e.g. # header image,dataset,d,l,s
            # e.g. # header s,s,d,l,s
            # e.g. # header well,plate,s,d,l,d
            csv_name = os.path.basename(csv_file)
            csv_path = os.path.join(folder, csv_file)

            objecti = getattr(omero.model, data_type + 'I')
            omero_object = objecti(int(object_id), False)

            # Split name and extension
            name_parts = os.path.splitext(csv_name)
            table_name = f"{name_parts[0]}"
            if job_id:
                table_name += f"_{job_id}"
            if wf_id:
                table_name += f"_{wf_id}"
            # Add back extension
            table_name += name_parts[1]

            # ParsingContext could be provided with 'column_types' kwarg here,
            # if you know them already somehow.
            ctx = ParsingContext(client, omero_object, "",
                                 table_name=table_name)

            with open(csv_path, 'rt', encoding='utf-8-sig') as f1:
                ctx.preprocess_from_handle(f1)
                with open(csv_path, 'rt', encoding='utf-8-sig') as f2:
                    ctx.parse_from_handle_stream(f2)

            # Add the FileAnnotation to the script message
            message += f"\nCSV file {csv_name} data added as table for {data_type}: {object_id}"
        except Exception as e:
            logger.warning(f"Error processing CSV file {csv_name}: {e}")
            # If an exception is caught, attach the CSV file as an attachment
            try:
                mimetype = "text/csv"
                namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
                description = f"CSV file {csv_name} from SLURM job {job_id}"
                if wf_id:
                    description += f" from Workflow {wf_id}"
                origFName = os.path.join(folder, table_name)
                csv_file_attachment = conn.createFileAnnfromLocalFile(
                    csv_path, origFilePathAndName=origFName,
                    mimetype=mimetype,
                    ns=namespace, desc=description)

                # Ensure the OMERO object is fully loaded
                if data_type == 'Dataset':
                    omero_object = conn.getObject("Dataset", int(object_id))
                elif data_type == 'Project':
                    omero_object = conn.getObject("Project", int(object_id))
                elif data_type == 'Plate':
                    omero_object = conn.getObject("Plate", int(object_id))
                else:
                    raise ValueError(f"Unsupported data_type: {data_type}")
                omero_object.linkAnnotation(csv_file_attachment)
                message += f"\nCSV file {csv_name} failed to attach as table."
                message += f"\nCSV file {csv_name} instead attached as an attachment to {data_type}: {object_id}"
            except Exception as attachment_error:
                message += f"\nError attaching CSV file {csv_name} as an attachment to OMERO: {attachment_error}"
                message += f"\nOriginal error: {e}"

    return message


def saveImagesToOmeroAsAttachments(
    conn: BlitzGateway,
    folder: str,
    client: Any,
    metadata_files: Optional[List[str]] = None,
    wf_id: Optional[str] = None
) -> str:
    """Save image from a (unzipped) folder to OMERO as attachments.

    Args:
        conn: Connection to OMERO.
        folder: Unzipped folder path.
        client: OMERO client to attach output.
        metadata_files: List of metadata CSV file paths to attach. Defaults to None.
        wf_id: Workflow ID for metadata. Defaults to None.

    Returns:
        Message to add to script output.
    """
    all_files = glob.iglob(folder+'**/**', recursive=True)
    files = [f for f in all_files if os.path.isfile(f)
             and any(f.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS)]
    # more_files = [f for f in os.listdir(f"{folder}/out") if os.path.isfile(f)
    #               and f.endswith('.tiff')]  # out folder
    # files += more_files
    logger.info(f"Found the following files in {folder}: {files}")
    namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
    job_id = unwrap(client.getInput(
        constants.results.OUTPUT_SLURM_JOB_ID)).strip()
    msg = ""
    message = ""  # Initialize message variable to fix scoping issue
    for name in files:
        og_name = getOriginalFilename(name)
        images = conn.getObjects("Image", attributes={
                                 "name": f"{og_name}"})  # Can we get in 1 go?

        if images:
            try:
                original_name = name  # Store original name
                # Rename file with workflow ID before first extension
                if wf_id:
                    filename = os.path.basename(name)
                    first_dot = filename.find('.')
                    if first_dot != -1:
                        # Insert wf_id before the first dot
                        new_name = os.path.join(
                            os.path.dirname(name),
                            f"{filename[:first_dot]}.{wf_id}{filename[first_dot:]}"
                        )
                        try:
                            os.rename(name, new_name)
                            name = new_name  # Update name for the rest of the process
                        except OSError as e:
                            logger.warning(
                                f"Could not rename file {name}: {e}")

                # Create annotation with renamed file
                ext = os.path.splitext(name)[1][1:]
                file_ann = conn.createFileAnnfromLocalFile(
                    name, mimetype=f"image/{ext}",
                    ns=namespace, desc=f"Result from job {job_id}" + (f" (Workflow {wf_id})" if wf_id else "") + f" | analysis {folder}")

                # Restore original filename after annotation is created
                if name != original_name:
                    try:
                        os.rename(name, original_name)
                    except OSError as e:
                        logger.warning(
                            f"Could not restore original filename {original_name}: {e}")

                logger.info(f"Attaching {name} to image {og_name}")
                # image = load_image(conn, image_id)
                for image in images:
                    image.linkAnnotation(file_ann)
                    if metadata_files:
                        message = upload_metadata_csv_to_omero(
                            client, conn, message, job_id, [image], metadata_files, wf_id)

                client.setOutput("File_Annotation", robject(file_ann._obj))
            except Exception as e:
                msg = f"Issue attaching file {name} to OMERO {og_name}: {e}"
                logger.warning(msg)
        else:
            msg = f"No images ({og_name}) found to attach {name} to: {images}"
            logger.info(msg)

    message = f"\nTried attaching result images to OMERO original images!\n{msg}"

    return message


def rename_import_file(client: Any, name: str, og_name: str) -> str:
    """Rename import file using pattern substitution.

    Generates a new filename using the rename pattern from client input,
    supporting variables for original and result file components.

    Args:
        client: OMERO script client for accessing rename settings.
        name: Result file path/name.
        og_name: Original file path/name.

    Returns:
        New formatted filename.

    Note:
        Supports variables: {original_file}, {original_ext}, {file}, {ext}
        Handles double extensions like '.ome.tiff' correctly.
    """
    pattern = unwrap(client.getInput(
        constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME_NAME))

    # Handle double extensions (e.g., .ome.tiff, .ome.zarr)
    from pathlib import PurePath

    # Get result file info (name parameter)
    result_filename = os.path.basename(name)
    # e.g. ['.ome', '.tiff']
    result_suffixes = PurePath(result_filename).suffixes
    result_ext_combo = ''.join(result_suffixes)  # e.g. '.ome.tiff'
    if result_ext_combo:
        result_file_base = result_filename[:-len(result_ext_combo)]
        ext = result_ext_combo[1:]  # Remove leading dot for {ext}
    else:
        result_file_base = os.path.splitext(result_filename)[0]
        ext = os.path.splitext(result_filename)[
            1][1:]  # Fallback for no extension

    # Get original file info (og_name parameter) - clean up any trailing dots
    original_filename = os.path.basename(
        og_name).rstrip('.')  # Remove trailing dots
    original_suffixes = PurePath(original_filename).suffixes
    original_ext_combo = ''.join(original_suffixes)
    if original_ext_combo:
        original_file = original_filename[:-len(original_ext_combo)]
        # Remove leading dot for {original_ext}
        original_ext = original_ext_combo[1:]
    else:
        original_file = os.path.splitext(original_filename)[0]
        original_ext = os.path.splitext(original_filename)[
            1][1:]  # Fallback for no extension

    # Create variables for pattern formatting
    variables = {
        'original_file': original_file,      # Original file basename without extension
        # Original file extension (handles double extensions)
        'original_ext': original_ext,
        'file': result_file_base,            # Result file basename without extension
        # Result file extension (handles double extensions)
        'ext': ext
    }

    name = pattern.format(**variables)
    logger.info(f"New name: {name}")
    return name


def getUserPlates() -> List[Any]:
    """Get OMERO Plates that user has access to.

    Returns:
        List of plate IDs and names formatted as strings.

    Raises:
        Exception: If connection or query fails.
    """
    try:
        client = omero.client()
        client.createSession()
        conn = omero.gateway.BlitzGateway(client_obj=client)
        conn.SERVICE_OPTS.setOmeroGroup(-1)
        objparams = [rstring('%d: %s' % (d.id, d.getName()))
                     for d in conn.getObjects('Plate')
                     if type(d) == omero.gateway.PlateWrapper]
        #  if type(d) == omero.model.ProjectI
        if not objparams:
            objparams = [rstring('<No objects found>')]
        return objparams
    except Exception as e:
        return ['Exception: %s' % e]
    finally:
        client.closeSession()


def getUserDatasets() -> List[Any]:
    """Get OMERO Datasets that user has access to.

    Returns:
        List of dataset IDs and names formatted as strings.

    Raises:
        Exception: If connection or query fails.
    """
    try:
        client = omero.client()
        client.createSession()
        conn = omero.gateway.BlitzGateway(client_obj=client)
        conn.SERVICE_OPTS.setOmeroGroup(-1)
        objparams = [rstring('%d: %s' % (d.id, d.getName()))
                     for d in conn.getObjects('Dataset')
                     if type(d) == omero.gateway.DatasetWrapper]
        #  if type(d) == omero.model.ProjectI
        if not objparams:
            objparams = [rstring('<No objects found>')]
        return objparams
    except Exception as e:
        return ['Exception: %s' % e]
    finally:
        client.closeSession()


def getUserProjects() -> List[Any]:
    """Get OMERO Projects that user has access to.

    Returns:
        List of project IDs and names formatted as strings.

    Raises:
        Exception: If connection or query fails.
    """
    try:
        client = omero.client()
        client.createSession()
        conn = omero.gateway.BlitzGateway(client_obj=client)
        conn.SERVICE_OPTS.setOmeroGroup(-1)
        objparams = [rstring('%d: %s' % (d.id, d.getName()))
                     for d in conn.getObjects('Project')
                     if type(d) == omero.gateway.ProjectWrapper]
        #  if type(d) == omero.model.ProjectI
        if not objparams:
            objparams = [rstring('<No objects found>')]
        return objparams
    except Exception as e:
        return ['Exception: %s' % e]
    finally:
        client.closeSession()


def cleanup_tmp_files_locally(message: str, folder: str, log_file: str) -> str:
    """ Cleanup zip and unzipped files/folders

    Args:
        message (String): Script output
        folder (String): Path of folder/zip to remove
        log_file (String): Path to the logfile to remove

    Returns
        String: Script output
    """
    try:
        # Cleanup
        os.remove(log_file)
        os.remove(f"{folder}.zip")
        shutil.rmtree(folder)

        # TODO: remove these from "permanent move"
        # Remove the zip file after extraction (importer expects extracted files)
        os.remove(target_zip_path)

        # Clean up temp directory only if we extracted fresh (not using existing zip)
        if not existing_zip_path:
            temp_dir = os.path.dirname(existing_zip_path)
            if os.path.exists(temp_dir) and 'biomero_import_' in temp_dir:
                shutil.rmtree(temp_dir)

    except Exception as e:
        logger.error(f"Failed to cleanup tmp files: {e}")
        message += f" Failed to cleanup tmp files: {e}"

    return message


def upload_log_to_omero(
    client: Any,
    conn: BlitzGateway,
    message: str,
    slurm_job_id: str,
    projects: List[Any],
    file: str,
    wf_id: Optional[str] = None
) -> str:
    """Upload a log/text file to OMERO.

    Creates file annotation and attaches to specified projects.

    Args:
        client: OMERO client.
        conn: Open connection to OMERO.
        message: Current script output message.
        slurm_job_id: ID of the SLURM job the file came from.
        projects: OMERO projects to attach file to.
        file: Path to file to upload.
        wf_id: Workflow ID for metadata. Defaults to None.

    Returns:
        Updated message with operation results.

    Raises:
        RuntimeError: If upload fails.
    """
    try:
        # upload log and link to project(s)
        logger.info(f"Uploading {file} and attaching to {projects}")
        mimetype = "text/plain"
        namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
        description = f"Log from SLURM job {slurm_job_id}"
        if wf_id:
            description += f" (Workflow {wf_id})"
        annotation = conn.createFileAnnfromLocalFile(
            file, mimetype=mimetype,
            ns=namespace, desc=description)
        # Already have other output in this script
        # But you could add this as output if you wanted log instead
        # client.setOutput("File_Annotation", robject(annotation._obj))

        # For now, we choose to add as a weblink button
        obj_id = annotation.getFile().getId()
        url = f"get_original_file/{obj_id}/"
        client.setOutput("URL", wrap({"type": "URL", "href": url}))

        for project in projects:
            project.linkAnnotation(annotation)  # link it to project.
        message += f"Attached {file} to {projects}"
    except Exception as e:
        message += f" Uploading file failed: {e}"
        logger.warning(message)
        raise RuntimeError(message)

    return message


def upload_metadata_csv_to_omero(
    client: Any,
    conn: BlitzGateway,
    message: str,
    slurm_job_id: str,
    projects: List[Any],
    metadata_files: List[str],
    wf_id: Optional[str] = None
) -> str:
    """Upload metadata CSV to OMERO as file attachments.

    Creates file annotation for metadata CSV and attaches to specified projects.
    Renames the file to include workflow ID for better organization.

    Args:
        client: OMERO client.
        conn: Open connection to OMERO.
        message: Current script output message.
        slurm_job_id: ID of the SLURM job the metadata came from.
        projects: OMERO projects to attach metadata to.
        metadata_files: List of metadata CSV file paths.
        wf_id: Workflow ID for metadata. Defaults to None.

    Returns:
        Updated message with operation results.

    Raises:
        RuntimeError: If upload fails.
    """
    if not metadata_files or not projects:
        return message

    try:
        # Use the first metadata file (they should all have the same content)
        source_metadata_file = metadata_files[0]

        # Create a renamed copy with workflow ID
        source_dir = os.path.dirname(source_metadata_file)
        if wf_id:
            renamed_metadata_file = os.path.join(
                source_dir, f"metadata_{wf_id}.csv")
        else:
            renamed_metadata_file = os.path.join(
                source_dir, f"metadata_{slurm_job_id}.csv")

        # Copy file with new name only if it doesn't already exist (don't modify original that importer might use)
        if not os.path.exists(renamed_metadata_file):
            import shutil
            shutil.copy2(source_metadata_file, renamed_metadata_file)
            logger.debug(
                f"Created renamed metadata copy: {renamed_metadata_file}")
        else:
            logger.debug(
                f"Using existing renamed metadata copy: {renamed_metadata_file}")

        # Upload metadata CSV and link to project(s)
        logger.info(
            f"Uploading {renamed_metadata_file} and attaching to {projects}")
        mimetype = "text/csv"
        namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
        description = f"Workflow metadata from SLURM job {slurm_job_id}"
        if wf_id:
            description += f" (Workflow {wf_id})"

        annotation = conn.createFileAnnfromLocalFile(
            renamed_metadata_file, mimetype=mimetype,
            ns=namespace, desc=description)

        # Refresh objects to avoid UnloadedEntityException (works for Projects, Plates, Images)
        for obj in projects:
            # Detect object type dynamically
            if hasattr(obj, '_obj') and hasattr(obj._obj, '__class__'):
                obj_class_name = obj._obj.__class__.__name__
                if 'Project' in obj_class_name:
                    object_type = "Project"
                elif 'Plate' in obj_class_name:
                    object_type = "Plate"
                elif 'Image' in obj_class_name:
                    object_type = "Image"
                else:
                    # Fallback - try to use the wrapper type
                    object_type = type(obj).__name__.replace(
                        'Wrapper', '').replace('_', '')
            else:
                object_type = "Project"  # Default fallback

            refreshed_obj = conn.getObject(object_type, obj.getId())
            if refreshed_obj:
                refreshed_obj.linkAnnotation(annotation)
            else:
                logger.warning(
                    f"Could not refresh {object_type} {obj.getId()} for annotation linking")

        message += f"\nAttached metadata CSV {os.path.basename(renamed_metadata_file)} to {len(projects)} projects"
        logger.info(
            f"Successfully attached metadata CSV to {len(projects)} projects")

    except Exception as e:
        message += f" Uploading metadata CSV failed: {e}"
        logger.warning(f"Metadata CSV upload failed: {e}")
        raise RuntimeError(f"Metadata CSV upload failed: {e}")

    return message


def upload_zip_to_omero(
    client: Any,
    conn: BlitzGateway,
    message: str,
    slurm_job_id: str,
    projects: List[Any],
    folder: str,
    wf_id: Optional[str] = None
) -> str:
    """Upload a zip to OMERO (without unpacking).

    Creates zip file annotation and attaches to specified projects.

    Args:
        client: OMERO client.
        conn: Open connection to OMERO.
        message: Current script output message.
        slurm_job_id: ID of the SLURM job the zip came from.
        projects: OMERO projects to attach zip to.
        folder: Path/name of zip (without zip extension).
        wf_id: Workflow ID for metadata. Defaults to None.

    Returns:
        Updated message with operation results.

    Raises:
        RuntimeError: If upload fails.
    """
    try:
        # upload zip and link to project(s)
        logger.info(f"Uploading {folder}.zip and attaching to {projects}")
        mimetype = "application/zip"
        namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
        description = f"Results from SLURM job {slurm_job_id}"
        if wf_id:
            description += f" (Workflow {wf_id})"
        zip_annotation = conn.createFileAnnfromLocalFile(
            f"{folder}.zip", mimetype=mimetype,
            ns=namespace, desc=description)

        client.setOutput("File_Annotation", robject(zip_annotation._obj))

        for project in projects:
            project.linkAnnotation(zip_annotation)  # link it to project.
        message += f"Attached zip {folder} to {projects}"
    except Exception as e:
        message += f" Uploading zip failed: {e}"
        logger.warning(message)
        raise RuntimeError(message)

    return message


def get_importer_group_base_path(group_name: str) -> str:
    """Get the base path for a group in importer storage.

    Uses frontend group mappings from biomero-config.json, falling back to
    base_dir + group_name if no specific mapping exists.

    Args:
        group_name: Name of the OMERO group.

    Returns:
        Base path for the group in importer storage.

    Raises:
        RuntimeError: If importer configuration is not available or improperly configured.
    """
    if not IMPORTER_CONFIG:
        logger.error(f"IMPORTER_CONFIG is None for group '{group_name}'")
        raise RuntimeError(
            f"Importer configuration not available for group '{group_name}'. "
            "Please ensure biomero-importer is properly configured."
        )

    # Get base_dir from settings.yml as fallback
    base_dir = IMPORTER_CONFIG.get('base_dir', '/data')

    try:
        # Load frontend group mappings from biomero-config.json
        import json
        with open('/opt/omero/server/biomero-config.json', 'r') as f:
            frontend_config = json.load(f)

        group_mappings = frontend_config.get('group_mappings', {})

        # Look up group by groupName to get folder mapping
        for group_id, mapping in group_mappings.items():
            if mapping.get('groupName') == group_name:
                folder_name = mapping.get('folder')
                if folder_name:
                    logger.info(
                        f"Found group mapping: {group_name} -> {folder_name}")
                    return os.path.join(base_dir, folder_name)

        # No specific mapping found - group has access to everything under base_dir/group_name
        fallback_path = os.path.join(base_dir, group_name)
        logger.info(
            f"No group mapping found for '{group_name}', using fallback: {fallback_path}")
        return fallback_path

    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Could not load frontend group mappings: {e}")
        # Final fallback to base_dir + group_name
        final_fallback = os.path.join(base_dir, group_name)
        logger.info(
            f"Using base_dir fallback for group '{group_name}': {final_fallback}")
        return final_fallback


def get_workflow_results_path(group_name: str, wf_id: Optional[str]) -> str:
    """Get the full path for workflow results in PERMANENT comprehensive storage.

    Creates path structure: /base_path/.analyzed/uuid/timestamp/
    - .analyzed: Marker directory for analyzed workflow results 
    - uuid: Workflow ID for organization
    - timestamp: Ensures uniqueness for multiple imports from same workflow

    IMPORTANT: This creates PERMANENT storage for ALL workflow data (images, CSVs,
    metadata, logs, etc.) that should NEVER be cleaned up. This serves both data
    preservation and enables importer access to image files.

    Args:
        group_name: Name of the OMERO group.
        wf_id: Workflow ID. Can be None for unknown workflows.

    Returns:
        Full path for storing complete workflow results with timestamp for uniqueness.

    Raises:
        ValueError: If wf_id is invalid or contains illegal characters.
    """

    # Validate workflow ID before using it as directory name
    if not wf_id:
        logger.error("Empty wf_id provided")
        raise ValueError("wf_id cannot be empty")

    # Check for invalid characters that shouldn't be in directory names
    import re
    # Windows and general invalid chars + spaces and brackets
    invalid_chars = r'[<>:"/\\|?*\s\[\]]'
    if re.search(invalid_chars, wf_id):
        logger.error(
            f"Invalid characters detected in wf_id: '{wf_id}'")
        logger.error(
            "This appears to contain log content rather than a valid UUID")
        raise ValueError(
            f"wf_id contains invalid directory name characters: '{wf_id[:100]}...'")

    # Additional length check
    if len(wf_id) > 255:  # Typical max filename length
        logger.error(
            f"wf_id suspiciously long ({len(wf_id)} chars): '{wf_id[:100]}...'")
        raise ValueError(
            f"wf_id too long for directory name: {len(wf_id)} characters")

    base_path = get_importer_group_base_path(group_name)

    if not base_path:
        logger.error(f"No base path returned for group: {group_name}")
        raise ValueError(
            f"No importer path configured for group: {group_name}")

    # Add timestamp directory to ensure uniqueness for multiple imports from same workflow
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results_path = os.path.join(
        base_path, '.analyzed', wf_id, timestamp)
    return results_path


def extract_slurm_results_zip(
    slurmClient: SlurmClient,
    slurm_job_id: str,
    local_tmp_storage: str,
    group_name: str,
    wf_id: str,
    message: str,
) -> Tuple[str, str, str, str, str]:
    """Extract and copy SLURM results zip once for reuse by multiple workflows.

    Performs the expensive network operations (data location extraction, zipping on SLURM,
    copying locally) exactly once to avoid duplication.

    Args:
        slurmClient: BIOMERO SlurmClient instance.
        slurm_job_id: SLURM job ID.
        local_tmp_storage: Local temporary storage path.
        group_name: Name of the OMERO group.
        wf_id: Workflow ID.
        message: Message to be returned in the tuple.

    Returns:
        slurm_data_path, permanent_storage_path, temporary_zip_file_path, filename, message

    Raises:
        RuntimeError: If any extraction step fails.
    """
    # Extract data location from SLURM log
    logger.info("Extracting data location from SLURM logfile...")
    slurm_data_path = slurmClient.extract_data_location_from_log(slurm_job_id)
    if not slurm_data_path:
        logger.error(
            f"Failed to extract data location from job {slurm_job_id}")
        raise RuntimeError(
            f"Could not extract data location from SLURM job {slurm_job_id}")

    logger.info(f"Extracted data location: {slurm_data_path}")

    # Create and copy zip archive from SLURM
    filename = f"{slurm_job_id}_out"
    logger.info(f"Creating and copying data archive from SLURM...")
    logger.info(f"Zipping data from {slurm_data_path} as {filename}")

    # Zip data on SLURM server
    zip_result = slurmClient.zip_data_on_slurm_server(
        slurm_data_path, filename)
    if not zip_result.ok:
        logger.error(f"SLURM zip failed. stderr: {zip_result.stderr}")
        raise RuntimeError(
            f"Failed to zip data on SLURM server: {zip_result.stderr}")

    # Validate zip content on SLURM
    zip_output = zip_result.stdout or ""
    if ("No files to process" in zip_output or
        "Files: 0" in zip_output or
            "Size:       0" in zip_output):
        error_msg = ("SLURM data transfer failed: No files were transferred to SLURM (zip contains no data). "
                     "This usually indicates the original image export failed - check SLURM_Image_Transfer logs for ZARR/OME-TIFF export errors.")
        logger.error(error_msg)
        logger.error(f"Zip output indicates empty transfer: {zip_output}")
        raise RuntimeError(error_msg)

    logger.info("Successfully zipped data on SLURM server")

    # Copy zip from SLURM to local storage
    logger.info(f"Copying zip file to local storage: {local_tmp_storage}")
    try:
        copy_result = slurmClient.copy_zip_locally(local_tmp_storage, filename)

        # Validate copied zip file
        temporary_zip_file_path = f"{local_tmp_storage.rstrip('/')}/{filename}.zip"
        if not os.path.exists(temporary_zip_file_path):
            error_msg = f"Copied zip file not found: {temporary_zip_file_path}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        zip_size = os.path.getsize(temporary_zip_file_path)
        if zip_size < 1024:  # Less than 1KB
            error_msg = (f"SLURM transfer failed: Zip file is too small ({zip_size} bytes), indicating no meaningful data was transferred. "
                         f"This usually means the original image export failed - check SLURM_Image_Transfer logs for errors.")
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        # Validate zip can be opened and contains files
        try:
            import zipfile
            with zipfile.ZipFile(temporary_zip_file_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                if not file_list:
                    error_msg = "SLURM transfer failed: Zip file contains no files. Check if the original image export completed successfully."
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                # Check for meaningful files (not just directories or empty files)
                meaningful_files = [
                    f for f in file_list if not f.endswith('/') and f.strip()]
                if not meaningful_files:
                    error_msg = "SLURM transfer failed: Zip contains only directories, no actual files. Check if the original image export completed successfully."
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                logger.info(
                    f"Zip validation successful: {len(meaningful_files)} files found")

        except zipfile.BadZipFile as zip_e:
            error_msg = f"SLURM transfer failed: Copied file is not a valid zip: {zip_e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.info("Successfully copied and validated zip from SLURM server")

        # Move all results to permanent storage (pass existing zip to avoid re-extraction)
        logger.info("Moving ALL results to permanent storage...")
        permanent_storage_path = move_results_to_permanent_storage(
            slurmClient, slurm_job_id, group_name, wf_id, temporary_zip_file_path)
        message += f"\nALL results moved to permanent storage: {permanent_storage_path}"
        logger.info(
            f"Comprehensive permanent storage created at: {permanent_storage_path}")

        return slurm_data_path, permanent_storage_path, temporary_zip_file_path, filename, message

    except Exception as e:
        logger.error(f"Failed to copy/validate zip: {e}")
        raise RuntimeError(f"Copy zip failed: {e}")


def move_results_to_permanent_storage(
    slurmClient: SlurmClient,
    slurm_job_id: str,
    group_name: str,
    wf_id: str,
    existing_zip_path: str
) -> str:
    """Move ALL SLURM results to permanent storage for preservation and importer access.

    IMPORTANT: This creates PERMANENT storage for ALL workflow results (images, CSVs, 
    metadata, etc.) that should NEVER be cleaned up. This serves dual purposes:
    1. Data preservation: Complete workflow results archived permanently
    2. Importer access: Images available for biomero-importer processing

    Creates structure: /storage/group/.analyzed/uuid/timestamp/
    - .analyzed: Marker directory for analyzed/processed results  
    - uuid: Workflow UUID for organization
    - timestamp: Ensures uniqueness for multiple imports from same workflow
    - Contents: ALL result files including images, CSVs, metadata, logs, etc.

    This permanent storage preserves the complete scientific record and enables
    future analysis while providing importer access to image data.

    Args:
        slurmClient: BIOMERO SlurmClient instance.
        slurm_job_id: SLURM job ID.
        group_name: OMERO group name.
        wf_id: Workflow UUID.
        existing_zip_path: Path to already extracted zip file.

    Returns:
        Path to the extracted results in PERMANENT storage.

    Raises:
        RuntimeError: If move operation fails at any stage.
    """
    # Get importer storage path
    target_path = get_workflow_results_path(group_name, wf_id)
    logger.info(
        f"Creating PERMANENT storage for ALL workflow results at: {target_path}")

    # Ensure target directory exists
    os.makedirs(target_path, exist_ok=True)

    # Extract zip to permanent storage
    target_zip_path = os.path.join(
        target_path, os.path.basename(existing_zip_path))

    try:
        # Copy zip to target location if it's not already there
        if existing_zip_path != target_zip_path:
            shutil.copy2(existing_zip_path, target_zip_path)

        # Extract zip in permanent storage
        import zipfile
        with zipfile.ZipFile(target_zip_path, "r") as zip_file:
            zip_file.extractall(target_path)

        logger.info(
            f"ALL workflow results moved to PERMANENT storage: {target_path}")
        logger.info(
            " COMPREHENSIVE PERMANENT storage created - preserves complete workflow results")
        return target_path

    except Exception as e:
        raise RuntimeError(f"Failed to move results to PERMANENT storage: {e}")


def create_metadata_csv(
    conn: BlitzGateway,
    slurmClient: SlurmClient,
    target_path: str,
    job_id: str,
    wf_id: Optional[str] = None
) -> List[str]:
    """Create metadata.csv file for importer with workflow and job metadata.

    The metadata.csv file is created in the same directory as the image files
    to ensure the importer can find it during processing.

    Args:
        conn: OMERO BlitzGateway connection.
        slurmClient: BIOMERO SlurmClient instance.
        target_path: Base path where results were extracted.
        job_id: SLURM job ID.
        wf_id: Workflow ID. Defaults to None.

    Returns:
        List of metadata.csv file paths created.
    """
    metadata_rows = []

    # Add basic job metadata
    metadata_rows.append(['Job_ID', str(job_id)])
    metadata_rows.append(['Import_Type', 'SLURM_Results'])
    metadata_rows.append(['Import_User', conn.getUser().getName()])
    metadata_rows.append(
        ['Import_Group', conn.getGroupFromContext().getName()])

    if slurmClient.track_workflows and wf_id:
        try:
            wf = slurmClient.workflowTracker.repository.get(wf_id)

            # Extract version from description
            version_match = re.search(r'\d+\.\d+\.\d+', wf.description)
            workflow_version = version_match.group(
                0) if version_match else "Unknown"

            # Add workflow metadata
            metadata_rows.extend([
                ['Workflow_ID', str(wf_id)],
                ['Workflow_Name', wf.name],
                ['Workflow_Version', str(workflow_version)],
                ['Workflow_Created_On', wf._created_on.isoformat()],
                ['Workflow_Modified_On', wf._modified_on.isoformat()],
                ['Workflow_Task_IDs', ", ".join(
                    [str(tid) for tid in wf.tasks])]
            ])

            # Add task metadata
            for tid in wf.tasks:
                task = slurmClient.workflowTracker.repository.get(tid)
                task_prefix = f"Task_{task.task_name}_"

                metadata_rows.extend([
                    [f'{task_prefix}ID', str(task._id)],
                    [f'{task_prefix}Name', task.task_name],
                    [f'{task_prefix}Version', task.task_version],
                    [f'{task_prefix}Status', task.status],
                    [f'{task_prefix}Input_Data', task.input_data],
                    [f'{task_prefix}Job_IDs', ", ".join(
                        [str(jid) for jid in task.job_ids])]
                ])

                # Add task parameters
                if task.params:
                    for key, value in task.params.items():
                        metadata_rows.append(
                            [f'{task_prefix}Param_{key}', str(value)])

                # Add job-specific metadata
                for jid in task.job_ids:
                    job_prefix = f"{task_prefix}Job_{jid}_"
                    metadata_rows.append(
                        [f'{job_prefix}Result_Message', task.result_message or ''])

                    if task.results and len(task.results) > 0:
                        if "command" in task.results[0]:
                            metadata_rows.append(
                                [f'{job_prefix}Command', task.results[0]['command']])
                        if "env" in task.results[0]:
                            for env_key, env_value in task.results[0]['env'].items():
                                metadata_rows.append(
                                    [f'{job_prefix}Env_{env_key}', str(env_value)])
        except Exception as e:
            logger.error(f"Failed to extract detailed workflow metadata: {e}")
            # Add minimal workflow info
            metadata_rows.append(['Workflow_ID', str(wf_id)])

    # Use the same file discovery logic as upload orders to find where to put metadata.csv
    all_files = glob.glob(os.path.join(target_path, "**", "*"), recursive=True)
    image_files = [f for f in all_files if os.path.isfile(f)
                   and any(f.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS)]

    # Create metadata.csv in same directories as image files
    created_files = []
    if image_files:
        # Group image files by directory
        directories_with_images = {}
        for img_file in image_files:
            img_dir = os.path.dirname(img_file)
            if img_dir not in directories_with_images:
                directories_with_images[img_dir] = []
            directories_with_images[img_dir].append(img_file)

        # Create metadata.csv in each directory containing images
        for img_dir, files_in_dir in directories_with_images.items():
            metadata_file = os.path.join(img_dir, 'metadata.csv')

            with open(metadata_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                # Skip header - importer treats it as data
                writer.writerows(metadata_rows)

            created_files.append(metadata_file)
            logger.info(f"Created metadata CSV: {metadata_file}")
    else:
        # Fallback to base directory if no images found
        metadata_file = os.path.join(target_path, 'metadata.csv')
        logger.warning(
            f"No image files found, creating metadata.csv in base directory: {metadata_file}")

        with open(metadata_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Skip header - importer treats it as data
            writer.writerows(metadata_rows)

        created_files = [metadata_file]

    logger.info(
        f"Metadata CSV written successfully with {len(metadata_rows)} data rows to {len(created_files)} locations")
    return created_files


def initialize_importer_integration() -> bool:
    """Initialize biomero-importer integration if available.

    Returns:
        True if initialization successful.

    Raises:
        RuntimeError: If importer is not enabled, not available, or initialization fails.
    """
    if not IMPORTER_ENABLED:
        logger.error(
            "IMPORTER_ENABLED is false - cannot initialize importer integration")
        raise RuntimeError(
            "Importer is not enabled. Set IMPORTER_ENABLED=true to use dataset import functionality.")

    if not IMPORTER_AVAILABLE:
        logger.error("biomero-importer module is not available")
        raise RuntimeError(
            "biomero-importer not available. Please ensure it is properly installed.")

    # Check environment variables
    import os
    db_url = os.getenv("INGEST_TRACKING_DB_URL")
    logger.info(
        f"Environment check - INGEST_TRACKING_DB_URL present: {bool(db_url)}")
    if db_url:
        logger.info(f"Database URL: {_mask_url(db_url)}")

    try:
        # Use importer's own initialization
        from biomero_importer.utils.initialize import initialize_db

        init_result = initialize_db()

        if init_result:
            logger.info("IngestTracker initialized successfully")
            return True
        else:
            logger.error("Failed to initialize IngestTracker")
            raise RuntimeError("Failed to initialize importer database")
    except (ImportError, AttributeError) as import_error:
        # Fallback to manual initialization
        if not db_url:
            logger.error(
                "Environment variable 'INGEST_TRACKING_DB_URL' not set")
            raise RuntimeError(
                "INGEST_TRACKING_DB_URL not set and importer initialization failed")

        config = {"ingest_tracking_db": db_url}

        try:
            init_result = initialize_ingest_tracker(config)
            if init_result:
                logger.info("IngestTracker initialized successfully")
                return True
            else:
                logger.error("Failed to initialize IngestTracker")
                raise RuntimeError(
                    "Failed to initialize IngestTracker manually")
        except Exception as e:
            logger.error(
                f"Unexpected error during IngestTracker initialization: {e}", exc_info=True)
            raise RuntimeError(
                f"Unexpected error during IngestTracker initialization: {e}")


def create_upload_order(order_dict: Dict[str, Any]) -> None:
    """Create upload order in importer database.

    Args:
        order_dict: Upload order information containing UUID, files, and metadata.

    Raises:
        RuntimeError: If importer is not enabled/available or order creation fails.
    """
    if not IMPORTER_ENABLED:
        logger.error("Cannot create upload order: IMPORTER_ENABLED is false")
        raise RuntimeError(
            "Cannot create upload order: Importer is not enabled")

    if not IMPORTER_AVAILABLE:
        logger.error(
            "Cannot create upload order: biomero-importer not available")
        raise RuntimeError(
            "Cannot create upload order: biomero-importer not available")

    try:
        # Log the new order using importer's ingestion tracking
        log_ingestion_step(order_dict, STAGE_NEW_ORDER)
        logger.info(f"Created upload order: {order_dict['UUID']}")
    except Exception as e:
        logger.error(f"Failed to create upload order: {e}")
        raise


def create_upload_orders_for_results(
    group_name: str,
    username: str,
    destination_type: str,
    destination_id: int,
    results_path: str,
    wf_id: str
) -> List[Dict[str, Any]]:
    """Create upload orders for SLURM results (images only).

    Args:
        group_name: OMERO group name.
        username: Username creating the order.
        destination_type: Destination type ('Dataset', 'Screen', etc.).
        destination_id: Destination ID in OMERO.
        results_path: Path to results in importer storage.
        wf_id: Workflow UUID for tracking.

    Returns:
        List of created upload orders.
    """

    if not IMPORTER_AVAILABLE:
        logger.error(
            "Cannot create upload orders: biomero-importer not available")
        return []

    # Find only image files (CSV tables handled by zip workflow)
    # Fix: Use proper glob pattern to find files recursively
    all_files = glob.glob(os.path.join(
        results_path, "**", "*"), recursive=True)

    image_files = [f for f in all_files if os.path.isfile(f)
                   and any(f.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS)]

    if not image_files:
        logger.warning(
            f"No image files matched extensions {SUPPORTED_IMAGE_EXTENSIONS}")

    orders = []

    # Create order for image files if any exist
    if image_files:
        image_order = {
            "Group": group_name,
            "Username": username,
            "DestinationID": destination_id,
            "DestinationType": destination_type,
            "UUID": str(uuid.uuid4()),
            "Files": image_files,
            "wf_id": wf_id,
            "source": "SLURM_Results"
        }
        create_upload_order(image_order)
        orders.append(image_order)
        logger.info(f"Created image upload order for {len(image_files)} files")

    return orders


def rename_files_in_importer_storage(client: Any, results_path: str) -> str:
    """Rename files in importer storage according to rename pattern.

    Args:
        client: OMERO script client for accessing rename settings.
        results_path: Path to results in importer storage.

    Returns:
        Status message about rename operations.
    """

    # Check if renaming is enabled
    rename_enabled = unwrap(client.getInput(
        constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME))
    if not rename_enabled:
        return "\\nFile renaming skipped (disabled)"

    # Find all image files in results path
    all_files = []
    for root, dirs, files in os.walk(results_path):
        for file in files:
            file_path = os.path.join(root, file)
            if any(file_path.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS):
                all_files.append(file_path)

    renamed_count = 0
    message = ""

    for file_path in all_files:
        try:
            # Extract original filename from the processed filename
            original_filename = getOriginalFilename(file_path)

            # Generate new filename using rename pattern
            new_filename = rename_import_file(
                client, file_path, original_filename)

            # Create new file path
            file_dir = os.path.dirname(file_path)
            new_file_path = os.path.join(file_dir, new_filename)

            # Only rename if the names are actually different
            if os.path.basename(file_path) != new_filename:
                logger.info(
                    f"Renaming: {os.path.basename(file_path)} -> {new_filename}")

                # Ensure target doesn't already exist
                if os.path.exists(new_file_path):
                    logger.warning(
                        f"Target file already exists, skipping: {new_file_path}")
                    continue

                # Perform the rename
                os.rename(file_path, new_file_path)
                renamed_count += 1
                logger.info(f"Successfully renamed file: {new_file_path}")

        except Exception as e:
            logger.error(f"Failed to rename file {file_path}: {e}")
            message += f"\\nWarning: Failed to rename {os.path.basename(file_path)}: {e}"

    if renamed_count > 0:
        message += f"\\nRenamed {renamed_count} files in importer storage"
        logger.info(f"Completed renaming {renamed_count} files")
    else:
        message += "\\nNo files needed renaming"
        logger.info("No files required renaming")

    return message


def process_importer_workflow(
    client: Any,
    conn: BlitzGateway,
    slurmClient: SlurmClient,
    slurm_job_id: str,
    group_name: str,
    username: str,
    permanent_storage_path: str,
    wf_id: Optional[str],

) -> str:
    """Process dataset image imports via biomero-importer from comprehensive permanent storage.

    This function processes image imports using the biomero-importer while ALL workflow
    results (images, CSVs, metadata, etc.) are preserved in permanent storage for
    complete data archival and future analysis.

    Args:
        client: OMERO script client.
        conn: OMERO BlitzGateway connection.
        slurmClient: BIOMERO SlurmClient instance.
        slurm_job_id: SLURM job ID.
        group_name: OMERO group name.
        username: Username for upload orders.
        permanent_storage_path: Path to permanent storage location.
        wf_id: Workflow ID for metadata. Defaults to None.        

    Returns:
        Status message describing processing results.
    """
    message = ""
    logger.info(
        "Step 3: Processing comprehensive data preservation + biomero-importer")

    # Step 3c: Handle file renaming if enabled
    rename_enabled = unwrap(client.getInput(
        constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME))

    if rename_enabled:
        logger.info("Step 3c: Renaming files in importer storage...")
        rename_message = rename_files_in_importer_storage(client,
                                                          permanent_storage_path)
        message += rename_message
        logger.info("File renaming completed")
    else:
        logger.info("Step 3c: Skipping file renaming (not enabled)")

    # Get dataset info from existing parameters
    dataset_name = unwrap(client.getInput(
        constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME))
    logger.info(f"Step 3d: Setting up dataset '{dataset_name}'...")

    # Check if dataset exists or create new one
    create_new_dataset = unwrap(client.getInput(
        constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE))
    dataset_id = None

    if not create_new_dataset:
        try:
            existing_datasets_w_name = [d.id for d in conn.getObjects(
                'Dataset', attributes={"name": dataset_name})]
            if existing_datasets_w_name:
                dataset_id = existing_datasets_w_name[0]
                create_new_dataset = False
                logger.info(f"Using existing dataset ID: {dataset_id}")
            else:
                create_new_dataset = True
                logger.info("Dataset not found, will create new one")
        except Exception:
            create_new_dataset = True
            logger.info(
                "Error checking for existing dataset, will create new one")

    if create_new_dataset:
        logger.info("Step 3e: Creating new dataset...")
        dataset = omero.model.DatasetI()
        dataset.name = rstring(dataset_name)
        desc = f"Images from SLURM job {slurm_job_id}"
        if wf_id:
            desc += f" (Workflow {wf_id})"
        dataset.description = rstring(desc)
        update_service = conn.getUpdateService()
        dataset = update_service.saveAndReturnObject(dataset)
        dataset_id = dataset.id.val
        logger.info(f"Created new dataset ID: {dataset_id}")

    # Create upload order for images only
    logger.info("Step 3f: Creating upload orders for biomero-importer...")
    orders = create_upload_orders_for_results(
        group_name, username, "Dataset", dataset_id,
        permanent_storage_path, wf_id)

    if orders:
        message += f"\nCreated {len(orders)} upload orders for biomero-importer:"
        for order in orders:
            file_count = len(order.get('Files', []))
            message += f"\n  - Order {order['UUID']}: {file_count} files"
            logger.info(
                f"Created upload order {order['UUID']} with {file_count} files")
    else:
        message += "\nNo image files found for importer"
        logger.warning("No image files found for importer processing")

    return message


def process_slurm_tables(
    client: Any,
    conn: BlitzGateway,
    process_csv: bool,
    permanent_storage_path: str,
    wf_id: Optional[str]
) -> str:
    """Process SLURM results tables (CSV files) and create OMERO tables.

    Args:
        client: OMERO script client.
        conn: OMERO BlitzGateway connection.
        process_csv: Whether to process CSV files as OMERO.tables.
        permanent_storage_path: Path to permanent storage location.
        wf_id: Workflow ID for metadata. Defaults to None.

    Returns:
        Status message describing processing results.  
    """
    message = ""

    # Process CSV files as OMERO.tables if requested
    if process_csv:
        logger.info("Processing CSV files as OMERO.tables...")

        try:
            # This is EXACTLY the same as Get_Results - using the right parameters!
            if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_TABLE_DATASET)):
                data_type = 'Dataset'
                dataset_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_TABLE_DATASET_ID))
                logger.debug(dataset_ids)
                for d_id in dataset_ids:
                    object_id = d_id.split(":")[0]
                    msg = saveCSVToOmeroAsTable(
                        conn, permanent_storage_path, client,
                        data_type=data_type, object_id=object_id, wf_id=wf_id)
                    message += msg

            if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_TABLE_PLATE)):
                data_type = 'Plate'
                plate_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_TABLE_PLATE_ID))
                logger.debug(plate_ids)
                for p_id in plate_ids:
                    object_id = p_id.split(":")[0]
                    msg = saveCSVToOmeroAsTable(
                        conn, permanent_storage_path, client,
                        data_type=data_type, object_id=object_id, wf_id=wf_id)
                    message += msg

        except Exception as csv_e:
            message += f"\nFailed to process CSV files: {csv_e}"
            logger.error(f"CSV processing failed: {csv_e}", exc_info=True)
    else:
        logger.info("Skipping CSV processing (not requested)")

    return message


def process_zip_attachments(
    client: Any,
    conn: BlitzGateway,
    permanent_storage_path: str,
    filename: str,
    projects: List[Any],
    slurm_job_id: str,
    metadata_files: List[str],
    wf_id: Optional[str]
) -> str:
    """Process project/plate zip file attachments.

    Args:
        client: OMERO script client.
        conn: OMERO BlitzGateway connection.
        permanent_storage_path: Path to permanent storage location.
        filename: Name of the zip file to be attached.
        projects: List of OMERO projects/plates for attachments.
        slurm_job_id: SLURM job ID for metadata.
        metadata_files: List of metadata CSV file paths to attach.
        wf_id: Workflow ID for metadata.

    Returns:
        Status message describing attachment results.
    """
    message = ""

    if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_OG_IMAGES)):
        # upload and link individual images
        msg = saveImagesToOmeroAsAttachments(conn=conn,
                                             folder=permanent_storage_path,
                                             client=client,
                                             metadata_files=metadata_files,
                                             wf_id=wf_id)
        message += msg

    # Process project and plate zip attachments - EXACTLY like Get_Results
    if (unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PROJECT)) or
            unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE))):
        zip_path = os.path.join(permanent_storage_path, filename)
        message = upload_zip_to_omero(
            client, conn, message, slurm_job_id, projects, zip_path, wf_id)

        # Create and attach metadata CSV for ZIP attachments
        if projects and metadata_files:
            message = upload_metadata_csv_to_omero(
                client, conn, message, slurm_job_id, projects, metadata_files, wf_id)

    return message


def cleanup_resources(
    client: Any,
    slurmClient: SlurmClient,
    slurm_job_id: str,
    data_location: Optional[str],
    folder: Optional[str],
    log_file: Optional[str]
) -> str:
    """Handle cleanup of SLURM and local resources.

    IMPORTANT: This function only cleans up temporary files. The permanent storage
    (.analyzed directories) is NEVER touched by cleanup operations.

    What gets cleaned when cleanup is enabled:
    - SLURM server: Original job results directory, temporary zip files, job logs  
    - Local: Zip workflow temporary files (folder, log_file) if they exist

    What is NEVER cleaned (preserved permanently):
    - Permanent storage: .analyzed/uuid/timestamp/ directories and ALL contents
    - This includes complete workflow results: images, CSVs, metadata, logs, etc.
    - Enables both data preservation and biomero-importer processing

    Args:
        client: OMERO script client.
        slurmClient: BIOMERO SlurmClient instance.
        slurm_job_id: SLURM job ID.
        data_location: Path to SLURM data location.
        folder: Path to local folder to clean.
        log_file: Path to local log file to clean.

    Returns:
        Status message describing cleanup results.
    """
    message = ""

    # Cleanup SLURM files if requested
    if unwrap(client.getInput("Cleanup?")):
        logger.info(
            "=== CLEANUP: Removing temporary files (PRESERVING importer storage) ===")

        # SLURM server cleanup: removes original data + temp zip, preserves importer storage
        try:
            if data_location:
                filename = f"{slurm_job_id}_out"
                logger.info(
                    f"Cleaning SLURM temporary files: {data_location}, {filename}.zip, job logs")
                clean_result = slurmClient.cleanup_tmp_files(
                    slurm_job_id, filename, data_location)
                message += f"\nCleaned SLURM temporary files: {data_location} + logs"
                logger.info("SLURM temporary files cleaned successfully")
            else:
                logger.warning("No SLURM data location available for cleanup")
                message += "\nNo SLURM data location found for cleanup"
        except Exception as cleanup_e:
            logger.error(f"SLURM cleanup failed: {cleanup_e}")
            message += f"\nSLURM cleanup failed: {cleanup_e}"

        # Local files cleanup: removes zip workflow temp files only
        if folder and log_file:
            logger.info(
                f"Cleaning zip workflow temporary files: {folder}, {log_file}")
            message = cleanup_tmp_files_locally(message, folder, log_file)
            message += "\nTemporary files cleaned"
            logger.info("Temporary files cleaned")
        else:
            logger.info("No temporary files to clean")

        # Explicit confirmation of what's preserved
        message += "\nPermanent storage (.analyzed directories) preserved with ALL workflow data"
        logger.info(
            "CLEANUP COMPLETE: Comprehensive permanent storage preserved, temporary files removed")

    else:
        message += "\nCleanup disabled: All files preserved"
        logger.info("Cleanup disabled: All SLURM and local files preserved")
        if folder:
            message += f"\nFiles preserved at: {folder}"
        if data_location:
            message += f"\nSLURM data preserved at: {data_location}"
        message += "\n Permanent storage with complete workflow data naturally preserved"

    return message


def resolve_workflow_id(
    script_wf_id: Optional[str],
    slurmClient: SlurmClient,
    slurm_job_id: str
) -> str:
    """Validate and resolve workflow ID from available sources with consistency checking.

    Validation steps (fail-fast):
    1. Validate script parameter (if provided) - exception if invalid
    2. Extract from SLURM log - exception if fails
    3. Get from job tracker - exception if fails  
    4. Verify all sources match - exception if inconsistent
    5. Generate fallback only if no sources available

    Args:
        script_wf_id: Workflow ID from script parameter (can be None).
        slurmClient: BIOMERO SlurmClient instance.
        slurm_job_id: SLURM job ID.

    Returns:
        Validated workflow ID to use throughout the script.

    Raises:
        ValueError: If script parameter is invalid UUID.
        RuntimeError: If extraction fails or sources are inconsistent.
    """
    validated_sources = {}

    # Step 1: Validate script parameter (fail immediately if invalid)
    if script_wf_id:
        if not re.match(UUID_PATTERN, script_wf_id, re.IGNORECASE):
            raise ValueError(
                f"Script parameter validation failed: '{script_wf_id[:50]}...' is not a valid UUID")
        validated_sources['script_parameter'] = script_wf_id
        logger.info(f"Script parameter workflow ID validated: {script_wf_id}")

    # Step 2: Extract from SLURM log
    try:
        slurm_wf_id = extract_workflow_uuid_from_log_file(
            slurmClient, slurm_job_id)
        if slurm_wf_id:
            validated_sources['slurm_log'] = slurm_wf_id
            logger.info(f"SLURM log workflow ID extracted: {slurm_wf_id}")
    except Exception as e:
        logger.warning(f"SLURM log extraction failed: {e}")

    # Step 3: Get from job tracker
    if slurmClient.track_workflows:
        try:
            task_id = slurmClient.jobAccounting.get_task_id(slurm_job_id)
            task = slurmClient.workflowTracker.repository.get(task_id)
            tracker_wf_id = str(task.workflow_id)
            validated_sources['job_tracker'] = tracker_wf_id
            logger.info(f"Job tracker workflow ID found: {tracker_wf_id}")
        except Exception as e:
            logger.warning(f"Job tracker lookup failed: {e}")

    # Step 4: Consistency validation (fail immediately if inconsistent)
    if validated_sources:
        unique_ids = set(validated_sources.values())
        if len(unique_ids) > 1:
            raise RuntimeError(
                f"Workflow ID inconsistency: {dict(validated_sources)}")

        wf_id = list(validated_sources.values())[0]
        sources = list(validated_sources.keys())
        logger.info(f"Workflow ID validated across {sources}: {wf_id}")
        return wf_id

    # Step 5: Generate fallback (only if no sources were available)
    wf_id = str(uuid.uuid4())
    logger.warning(
        f"No workflow ID sources available - generated fallback: {wf_id}")
    return wf_id


def extract_workflow_uuid_from_log_file(
    slurmClient: SlurmClient,
    slurm_job_id: str
) -> Optional[str]:
    """Extract workflow UUID from SLURM job logfile using SlurmClient.

    Uses SlurmClient's existing data location extraction, then extracts the UUID from
    the directory name. UUIDs in the data path typically appear as suffixes.

    Args:
        slurmClient: BIOMERO SlurmClient instance.
        slurm_job_id: SLURM job ID.

    Returns:
        Workflow UUID if found and valid, None otherwise.

    Example:
        >>> extract_workflow_uuid_from_log_file(client, "151")
        "82f53736-278b-4d9a-a6ef-485fc0758993"

    Note:
        Example path: /data/my-scratch/data/151_mask_test_82f53736-278b-4d9a-a6ef-485fc0758993
        Extracts: 82f53736-278b-4d9a-a6ef-485fc0758993
    """

    try:
        # Use existing SlurmClient method to get data location
        data_location = slurmClient.extract_data_location_from_log(
            slurm_job_id)
        if not data_location:
            logger.warning(
                f"No data location found in logfile for job {slurm_job_id}")
            return None

        # Extract directory name from the path
        dir_name = os.path.basename(data_location.rstrip('/'))

        # Look for UUID in the directory name using a more flexible approach
        # UUIDs are 32 hex chars with 4 hyphens in specific positions: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        uuid_regex = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        potential_uuids = re.findall(uuid_regex, dir_name, re.IGNORECASE)
        
        for potential_uuid in potential_uuids:
            try:
                # Validate using standard library - more reliable than regex
                validated_uuid = uuid.UUID(potential_uuid)
                extracted_uuid = str(validated_uuid).lower()
                logger.info(f"Extracted workflow UUID from directory '{dir_name}': {extracted_uuid}")
                return extracted_uuid
            except ValueError:
                # Not a valid UUID, continue searching
                continue
        
        logger.warning(f"No valid UUID found in directory name: {dir_name}")
        return None

    except Exception as e:
        logger.error(
            f"Failed to extract workflow UUID from SLURM job {slurm_job_id}: {e}")
        return None


def runScript() -> None:
    """Main entry point for SLURM results import with comprehensive data handling.

    Orchestrates the complete import workflow including data extraction, permanent
    storage setup, and execution of requested import operations (attachments,
    dataset imports, tables, etc.).

    The workflow handles both standard OMERO import workflows and importer-enabled
    dataset imports based on configuration.
    """
    try:
        with SlurmClient.from_config() as slurmClient:

        _oldjobs = slurmClient.list_completed_jobs()
        _projects = getUserProjects()
        _plates = getUserPlates()
        _datasets = getUserDatasets()

        client = scripts.client(
            'BIOMERO.SLURM_Import_Results',
            '''Import workflow results from SLURM via biomero-importer.

            Process completed SLURM job results using biomero-importer for scalable remote processing.
            ''',
            scripts.Bool(constants.results.OUTPUT_COMPLETED_JOB,
                         optional=False, grouping="01",
                         default=True),
            scripts.String(constants.results.OUTPUT_SLURM_JOB_ID,
                           optional=False, grouping="01.1",
                           values=_oldjobs),
            scripts.String("workflow_uuid",
                           optional=True, grouping="01.2",
                           description="UUID of the workflow that generated these results (auto-extracted from SLURM log if not provided)"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_PROJECT,
                         optional=False,
                         grouping="03",
                         description="Attach all results in zip to a project",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_PROJECT_ID,
                         optional=True, grouping="03.1",
                         description="Project to attach workflow results to",
                         values=_projects),
            scripts.Bool(constants.results.OUTPUT_ATTACH_OG_IMAGES,
                         optional=False,
                         grouping="05",
                         description="Attach all results to original images as attachments",
                         default=False),
            scripts.Bool(constants.results.OUTPUT_ATTACH_PLATE,
                         optional=False,
                         grouping="04",
                         description="Attach all results in zip to a plate",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_PLATE_ID,
                         optional=True, grouping="04.1",
                         description="Plate to attach workflow results to",
                         values=_plates),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET,
                         optional=False,
                         grouping="06",
                         description="Import all result as a new dataset via biomero-importer",
                         default=True),
            scripts.String(constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME,
                           optional=True,
                           grouping="06.1",
                           description="Name for the new dataset w/ results",
                           default="Imported_Results"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE,
                         optional=True,
                         grouping="06.2",
                         description="If there is already a dataset with this name, still create new one? (True) or add to it? (False) ",
                         default=False),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME,
                         optional=True,
                         grouping="06.3",
                         description="Rename all imported files as below. You can use variables {original_file}, {original_ext}, {file}, and {ext}. E.g. {original_file}_IMPORTED.{ext}",
                         default=True),
            scripts.String(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME_NAME,
                           optional=True,
                           grouping="06.4",
                           description="A new name for the imported images.",
                           default="{original_file}_IMPORTED.{ext}"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_TABLE,
                         optional=False,
                         grouping="07",
                         description="Add all csv files as OMERO.tables to the chosen dataset",
                         default=True),
            scripts.Bool(constants.results.OUTPUT_ATTACH_TABLE_DATASET,
                         optional=True,
                         grouping="07.1",
                         description="Attach to the dataset chosen below",
                         default=True),
            scripts.List(constants.results.OUTPUT_ATTACH_TABLE_DATASET_ID,
                         optional=True,
                         grouping="07.2",
                         description="Dataset to attach workflow results to",
                         values=_datasets),
            scripts.Bool(constants.results.OUTPUT_ATTACH_TABLE_PLATE,
                         optional=True,
                         grouping="07.3",
                         description="Attach to the plate chosen below",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_TABLE_PLATE_ID,
                         optional=True,
                         grouping="07.4",
                         description="Plate to attach workflow results to",
                         values=_plates),
            scripts.Bool("Cleanup?",
                         optional=True,
                         grouping="08",
                         description="Cleanup temporary files after completion (default: True). Turn off for debugging.",
                         default=True),


            namespaces=[omero.constants.namespaces.NSDYNAMIC],
            version=VERSION,
            authors=["Torec Luik"],
            institutions=["Amsterdam UMC"],
            contact='cellularimaging@amsterdamumc.nl',
            authorsInstitutions=[[1]]
        )

        try:
            scriptParams = client.getInputs(unwrap=True)
            conn = BlitzGateway(client_obj=client)

            message = ""
            logger.info(f"Import Results: {scriptParams}\n")

            # Job id
            slurm_job_id = unwrap(client.getInput(
                constants.results.OUTPUT_SLURM_JOB_ID)).strip()

            # Get workflow ID from script parameter
            script_wf_id = unwrap(client.getInput("workflow_uuid"))

            # Resolve workflow ID from all available sources with validation
            wf_id = resolve_workflow_id(
                script_wf_id, slurmClient, slurm_job_id)

            # Get group and user info
            group_name = conn.getGroupFromContext().getName()
            username = conn.getUser().getName()

            # Determine which operations are requested
            use_importer_for_datasets = unwrap(client.getInput(
                constants.results.OUTPUT_ATTACH_NEW_DATASET))
            process_csv_tables = unwrap(client.getInput(
                constants.results.OUTPUT_ATTACH_TABLE))
            process_attachments = (unwrap(client.getInput(constants.results.OUTPUT_ATTACH_OG_IMAGES)) or
                                   unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PROJECT)) or
                                   unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)))

            # Debug the workflow decisions
            logger.info(
                f"Workflow decisions: use_importer_for_datasets={use_importer_for_datasets}, process_csv_tables={process_csv_tables}, process_attachments={process_attachments}")

            # Initialize importer only if needed for dataset imports
            importer_initialized = False
            if use_importer_for_datasets:
                importer_initialized = initialize_importer()

            # Ask job State
            if unwrap(client.getInput(constants.results.OUTPUT_COMPLETED_JOB)):
                _, result = slurmClient.check_job_status([slurm_job_id])
                message += f"\n{result.stdout}"

            # Pull project from OMERO for import operations
            projects = []  # note, can also be plate now
            if unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PROJECT)):
                project_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PROJECT_ID))
                projects = [conn.getObject("Project", p.split(":")[0])
                            for p in project_ids]
            if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)):
                plate_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PLATE_ID))
                projects = [conn.getObject("Plate", p.split(":")[0])
                            for p in plate_ids]

            # MAIN WORKFLOW EXECUTION - runs regardless of project/plate attachment settings
            folder = None
            log_file = None

            # Extract project/plate info
            project_info = [
                f"{p.getId()}:{p.getName()}" for p in projects] if projects else []

            # Collect all workflow configuration info
            workflow_config = {
                "ID": wf_id,
                "SLURM_Job": slurm_job_id,
                "User": username,
                "Group": group_name,
                "Importer_initialized": importer_initialized,
                "Projects/Plates": project_info,
                "Use_importer_for_datasets": use_importer_for_datasets,
                "Process_CSV_tables": process_csv_tables,
                "Process_attachments": process_attachments
            }

            logger.info(f"Starting import: {workflow_config}")

            # Step 1: Copy logfile to server
            logger.info("Step 1: Copying logfile from SLURM...")
            tup = slurmClient.get_logfile_from_slurm(slurm_job_id)
            (local_tmp_storage, log_file, get_result) = tup
            # Ensure local_tmp_storage has trailing slash (required for copy operations)
            if not local_tmp_storage.endswith('/'):
                local_tmp_storage += '/'
            message += "\nSuccessfully copied logfile."
            logger.info(f"Logfile copied successfully: {log_file}")

            # Step 2: Upload logfile to OMERO as Original File
            logger.info("Step 2: Uploading logfile to OMERO...")
            message = upload_log_to_omero(
                client, conn, message, slurm_job_id,
                projects, log_file, wf_id=wf_id)

            # Step 3: Extract SLURM zip once
            logger.info(
                "Step 3: Extracting SLURM results (shared by multiple workflows)...")
            slurm_data_path, permanent_storage_path, temporary_zip_file_path, filename = (
                None, None, None, None)
            try:
                slurm_data_path, permanent_storage_path, temporary_zip_file_path, filename, message = extract_slurm_results_zip(
                    slurmClient, slurm_job_id, local_tmp_storage, group_name, wf_id, message)
                message += f"\nSuccessfully extracted SLURM results: {permanent_storage_path}"
                logger.info(
                    f"SLURM results extracted and available: {permanent_storage_path}")
            except Exception as e:
                logger.error(f"Failed to extract SLURM results: {e}")
                message += f"\nFailed to extract SLURM results: {e}"
                raise e

            # Create metadata CSV file
            logger.info("Step 3b: Creating metadata CSV...")
            metadata_files = create_metadata_csv(
                conn, slurmClient, permanent_storage_path, slurm_job_id, wf_id)
            message += f"\nCreated metadata files: {metadata_files}"
            logger.info(f"Metadata files created: {metadata_files}")

            # Step 4: IMPORTER WORKFLOW - Dataset image imports
            if use_importer_for_datasets:
                importer_message = process_importer_workflow(
                    client, conn, slurmClient, slurm_job_id,
                    group_name, username,
                    permanent_storage_path, wf_id)
                message += importer_message

            # Step 5: SLURM TABLES PROCESSING
            if process_csv_tables:
                zip_message = process_slurm_tables(
                    client, conn, process_csv_tables,
                    permanent_storage_path, wf_id)
                message += zip_message

            # Step 6: ZIP ATTACHMENT OPERATIONS
            if process_attachments:
                attachment_message = process_zip_attachments(
                    client, conn, permanent_storage_path,
                    filename, projects, slurm_job_id,
                    metadata_files, wf_id)
                message += attachment_message

            logger.info("Results imported successfully")
            message += "\nWorkflow execution completed successfully."

        except Exception as e:
            logger.error(f"Script execution failed: {e}", exc_info=True)
            message += f"\nScript execution failed: {e}"

            # Set failure output and re-raise to show red X in OMERO
            try:
                client.setOutput("Message", rstring(f"FAILED: {message}"))
            except Exception:
                pass

            # Critical errors should cause script failure
            raise e
        finally:
            # Step 6: Cleanup resources - always runs regardless of success/failure
            # Be defensive - don't let cleanup errors mask the original failure
            try:
                cleanup_message = cleanup_resources(
                    client, slurmClient, slurm_job_id,
                    slurm_data_path, folder, log_file)
                message += cleanup_message
            except Exception as cleanup_error:
                logger.error(f"Cleanup failed: {cleanup_error}", exc_info=True)
                # Don't add cleanup errors to message if script already failed

            # Always try to set outputs - but don't overwrite failure messages
            try:
                client.setOutput("Workflow_UUID", rstring(wf_id))
                # Only set success message if we haven't already set a failure message
                if not message.startswith("FAILED:"):
                    client.setOutput("Message", rstring(str(message)))
            except Exception as output_error:
                logger.error(f"Failed to set output: {output_error}")

            try:
                logger.info(
                    f"Completed biomero-importer workflow for job {slurm_job_id} with workflow ID {wf_id}")
            except Exception:
                logger.info("Completed biomero-importer workflow execution")

            try:
                client.closeSession()
            except Exception as close_error:
                logger.error(f"Failed to close session: {close_error}")
    
    except Exception as e:
        logger.exception(f"Script failed with error: {e}")
        # Try to report error to client if available
        try:
            if 'client' in locals():
                client.setOutput("Message", wrap(f"Error: {str(e)}"))
        except:
            pass  # Client might not be available
        raise  # Re-raise to ensure proper exit code


def initialize_importer():
    importer_initialized = False
    error_msg = "Failed to initialize biomero-importer"
    try:
        if initialize_importer_integration():
            importer_initialized = True
            logger.info(
                "Successfully initialized biomero-importer for dataset imports")
        else:
            logger.error(error_msg)
            raise RuntimeError(error_msg)
    except Exception as e:
        logger.error(f"{error_msg}: {str(e)}")
        raise RuntimeError(f"{error_msg}: {str(e)}")
    return importer_initialized


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
    logging.getLogger('omero.gateway').setLevel(
        logging.WARNING)  # Silences proxy creation spam
    logging.getLogger('omero.client').setLevel(logging.WARNING)
    logging.getLogger('paramiko.transport').setLevel(logging.WARNING)
    logging.getLogger('paramiko.sftp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('requests_cache').setLevel(logging.WARNING)  # Cache logs
    logging.getLogger('requests-cache').setLevel(logging.WARNING)  # Alt naming
    logging.getLogger('requests_cache.core').setLevel(
        logging.WARNING)  # Core module
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
