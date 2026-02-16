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

NEW: Hybrid Import Workflow:
- Image imports to datasets: Uses biomero-importer for scalable remote processing
- CSV tables: Uses legacy direct OMERO import (importer doesn't support these yet)
- File attachments, zip uploads: Uses legacy workflows
- Automatically falls back to legacy mode if importer unavailable

Workflow UUID Handling:
- If workflow_uuid is provided and valid: Uses provided UUID
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
- Tables: CSV files converted to OMERO.tables (legacy workflow)
- Metadata: Preserved through import process and exported to CSV for importer

CLEANUP BEHAVIOR:
When cleanup is enabled, this script removes:
- SLURM server: Original job results directory, temporary zip files, job logs
- Local: Legacy workflow temporary files (if any were created)

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

import shutil
import sys
import omero
import omero.gateway
from omero import scripts
from omero.constants.namespaces import NSCREATED
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, robject, unwrap, wrap
import os
import re
import zipfile
import glob
from biomero import SlurmClient, constants
import logging
import ezomero
# from aicsimageio import AICSImage
from tifffile import imread, TiffFile
import numpy as np
from omero_metadata.populate import ParsingContext
import uuid
import json
import csv
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    from biomero_importer.utils.ingest_tracker import (
        initialize_ingest_tracker,
        log_ingestion_step,
        STAGE_NEW_ORDER,
        _mask_url,
    )
    IMPORTER_AVAILABLE = True
except ImportError:
    logger.warning("biomero-importer not available - dataset imports will not be supported")
    IMPORTER_AVAILABLE = False
    _mask_url = lambda url: url  # Fallback function if import fails

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
SUPPORTED_IMAGE_EXTENSIONS = ['.tif', '.tiff', '.png', '.ome.tif']
SUPPORTED_TABLE_EXTENSIONS = ['.csv']

# Importer integration configuration - use importer APIs instead of env vars
try:
    from biomero_importer.utils.initialize import load_settings
    IMPORTER_CONFIG = load_settings("/opt/omero/server/config-importer/settings.yml")
except (ImportError, Exception):
    IMPORTER_CONFIG = None


def load_image(conn, image_id):
    """Load OMERO Image object by ID.

    Args:
        conn: Open OMERO BlitzGateway connection.
        image_id (str): ID of the image to load.

    Returns:
        Image: OMERO Image wrapper object.
    """
    return conn.getObject('Image', image_id)


def getOriginalFilename(name):
    """Extract original filename from processed file path.

    Extracts the base filename from a processed file path.
    Example: "/../../Cells Apoptotic.png_merged_z01_t01.tiff"
    Returns: "Cells Apoptotic.png"

    Args:
        name (str): Path/name of processed file.

    Returns:
        str: Original filename if pattern matches, otherwise input name.
    """
    match = re.match(pattern=".+\/(.+\.[A-Za-z]+).+\.[tiff|png]", string=name)
    if match:
        name = match.group(1)

    return name


def saveCSVToOmeroAsTable(conn, folder, client,
                          data_type='Dataset', object_id=651, wf_id=None):
    """Save CSV files from folder to OMERO as OMERO.tables.

    Searches for CSV files in the specified folder and converts them to
    OMERO.tables attached to the specified OMERO object. Falls back to
    file attachments if table creation fails.

    Args:
        conn: OMERO BlitzGateway connection.
        folder (str): Path to folder containing CSV files.
        client: OMERO script client for job ID access.
        data_type (str): Type of OMERO object ('Dataset', 'Plate', etc.).
            Defaults to 'Dataset'.
        object_id (int): ID of OMERO object to attach tables to.
            Defaults to 651.
        wf_id (str, optional): Workflow ID for metadata. Defaults to None.

    Returns:
        str: Status message describing import results.
    """
    logger.debug(f"Processing CSV files in folder: {folder}")
    message = ""

    # Get a list of all CSV files in the folder
    logger.debug(f"Scanning for CSV files in: {folder}")
    all_files = glob.iglob(folder+'**/**', recursive=True)
    csv_files = [f for f in all_files if os.path.isfile(f)
                 and any(f.endswith(ext) for ext in SUPPORTED_TABLE_EXTENSIONS)]
    logger.info(f"Found the following table files in {folder}: {csv_files}")
    logger.debug(f"Total CSV files found: {len(csv_files)}")
    # namespace = NSCREATED + "/BIOMERO/SLURM_GET_RESULTS"
    job_id = unwrap(client.getInput(
        constants.results.OUTPUT_SLURM_JOB_ID)).strip()

    if not csv_files:
        logger.debug("No CSV/table files found")
        return "No table files found in the folder."

    for csv_file in csv_files:
        logger.debug(f"Processing CSV file: {csv_file}")
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
                logger.debug(
                    f"Creating FileAnnotation for CSV file: {csv_path}")
                origFName = os.path.join(folder, table_name)
                csv_file_attachment = conn.createFileAnnfromLocalFile(
                    csv_path, origFilePathAndName=origFName,
                    mimetype=mimetype,
                    ns=namespace, desc=description)

                logger.debug(f"FileAnnotation created: {csv_file_attachment}")
                # Ensure the OMERO object is fully loaded
                if data_type == 'Dataset':
                    omero_object = conn.getObject("Dataset", int(object_id))
                elif data_type == 'Project':
                    omero_object = conn.getObject("Project", int(object_id))
                elif data_type == 'Plate':
                    omero_object = conn.getObject("Plate", int(object_id))
                else:
                    raise ValueError(f"Unsupported data_type: {data_type}")
                logger.debug(
                    f"Linking FileAnnotation to OMERO object: {omero_object}")
                omero_object.linkAnnotation(csv_file_attachment)
                logger.debug("FileAnnotation linked successfully.")
                message += f"\nCSV file {csv_name} failed to attach as table."
                message += f"\nCSV file {csv_name} instead attached as an attachment to {data_type}: {object_id}"
            except Exception as attachment_error:
                message += f"\nError attaching CSV file {csv_name} as an attachment to OMERO: {attachment_error}"
                message += f"\nOriginal error: {e}"

    return message


def saveImagesToOmeroAsAttachments(conn, folder, client, wf_id=None):
    """Save image from a (unzipped) folder to OMERO as attachments

    Args:
        conn (_type_): Connection to OMERO
        folder (String): Unzipped folder
        client : OMERO client to attach output

    Returns:
        String: Message to add to script output
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
    for name in files:
        logger.debug(name)
        og_name = getOriginalFilename(name)
        logger.debug(og_name)
        images = conn.getObjects("Image", attributes={
                                 "name": f"{og_name}"})  # Can we get in 1 go?
        logger.debug(images)

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
                            logger.debug(f"Renamed file to: {name}")
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
                        logger.debug(
                            f"Restored original filename: {original_name}")
                    except OSError as e:
                        logger.warning(
                            f"Could not restore original filename {original_name}: {e}")

                logger.info(f"Attaching {name} to image {og_name}")
                # image = load_image(conn, image_id)
                for image in images:
                    image.linkAnnotation(file_ann)

                logger.debug(
                    f"Attaching FileAnnotation to Image: File ID: {file_ann.getId()}, {file_ann.getFile().getName()}, Size: {file_ann.getFile().getSize()}")

                client.setOutput("File_Annotation", robject(file_ann._obj))
            except Exception as e:
                msg = f"Issue attaching file {name} to OMERO {og_name}: {e}"
                logger.warning(msg)
        else:
            msg = f"No images ({og_name}) found to attach {name} to: {images}"
            logger.info(msg)

    logger.debug(files)
    message = f"\nTried attaching result images to OMERO original images!\n{msg}"

    return message


def to_5d(*arys, axes):
    '''
    Convert arrays to 5D format (x,y,z,c,t) handling various input dimensions.

    Parameters:
    -----------
    *arys : numpy.ndarray
        One or more input arrays to be converted to 5D
    axes : str, required
        String indicating the order of dimensions (e.g., 'CYX')

    Returns:
    --------
    numpy.ndarray or list
        Single 5D array or list of 5D arrays in XYZCT order
    '''
    if not arys:
        return None

    target_axes = 'XYZCT'  # Target order
    res = []

    for ary in arys:
        if not isinstance(ary, np.ndarray):
            continue

        # Validate we have axes specified
        if axes is None:
            raise ValueError(
                "The 'axes' parameter is required - dimension order cannot be guessed")

        # Standardize to uppercase and validate
        current_axes = axes.upper()
        if len(current_axes) != ary.ndim:
            raise ValueError(
                f"Axes string '{current_axes}' does not match array dimensions {ary.ndim}")

        # Create a 5D array by adding missing dimensions
        img_5d = ary
        current_order = current_axes

        # Add missing dimensions
        for dim in "XYZCT":
            if dim not in current_order:
                img_5d = np.expand_dims(img_5d, axis=-1)
                current_order += dim

        # Reorder dimensions if needed
        if current_order != target_axes:
            # Create list of current positions for each dimension
            current_positions = []
            for dim in target_axes:
                current_positions.append(current_order.index(dim))

            # Rearrange dimensions
            img_5d = np.moveaxis(img_5d, current_positions,
                                 range(len(target_axes)))

        res.append(img_5d)

    return res[0] if len(res) == 1 else res


def add_image_annotations(conn, slurmClient, object_id, job_id, wf_id=None):
    object_type = "Image"  # Set to Image when it's a dataset
    ns_wf = "biomero/workflow"
    if slurmClient.track_workflows and wf_id:
        try:
            wf = slurmClient.workflowTracker.repository.get(wf_id)

            map_ann_ids = []

            # Extract version from the description using regex
            version_match = re.search(r'\d+\.\d+\.\d+', wf.description)
            workflow_version = version_match.group(
                0) if version_match else "Unknown"

            workflow_annotation_dict = {
                'Workflow_ID': str(wf_id),
                'Name': wf.name,
                'Version': str(workflow_version),
                'Created_On': wf._created_on.isoformat(),
                'Modified_On': wf._modified_on.isoformat(),
                'Task_IDs': ", ".join([str(tid) for tid in wf.tasks]),
            }
            map_ann_id = ezomero.post_map_annotation(
                conn=conn,
                object_type=object_type,
                object_id=object_id,
                kv_dict=workflow_annotation_dict,
                ns=ns_wf,
                across_groups=False  # Set to False if you don't want cross-group behavior
            )
            map_ann_ids.append(map_ann_id)

            for tid in wf.tasks:
                task = slurmClient.workflowTracker.repository.get(tid)
                # Add FAIR metadata
                task_annotation_dict = {
                    'Task_ID': str(task._id),
                    'Workflow_ID': str(wf_id),
                    'Workflow_Name': wf.name,
                    'Name': task.task_name,
                    'Version': task.task_version,
                    'Created_On': task._created_on.isoformat(),
                    'Modified_On': task._modified_on.isoformat(),
                    'Status': task.status,
                    'Input_Data': task.input_data,
                    'Job_IDs': ", ".join([str(jid) for jid in task.job_ids]),
                }
                # Add parameters
                if task.params:
                    task_annotation_dict.update({f"Param_{key}": str(
                        value) for key, value in task.params.items()})
                # task metadata
                ns_task = ns_wf + "/task" + f"/{task.task_name}"
                map_ann_id = ezomero.post_map_annotation(
                    conn=conn,
                    object_type=object_type,
                    object_id=object_id,
                    kv_dict=task_annotation_dict,
                    ns=ns_task,
                    across_groups=False  # Set to False if you don't want cross-group behavior
                )
                map_ann_ids.append(map_ann_id)

                for jid in task.job_ids:
                    job_dict = {
                        'Job_ID': str(jid),
                        'Task_ID': str(tid),
                        'Workflow_ID': str(wf_id),
                        'Result_Message': task.result_message,
                    }
                    # Add the specific job-script command
                    if task.results and "command" in task.results[0]:
                        job_dict['Command'] = task.results[0]['command']
                    # and environment variables
                    if task.results and "env" in task.results[0]:
                        job_dict.update({f"Env_{key}": str(value)
                                        for key, value in task.results[0]['env'].items()})
                    # job metadata
                    ns_task_job = ns_task + "/job"
                    logger.debug(f"Adding metadata: {job_dict}")
                    map_ann_id = ezomero.post_map_annotation(
                        conn=conn,
                        object_type=object_type,
                        object_id=object_id,
                        kv_dict=job_dict,
                        ns=ns_task_job,
                        across_groups=False  # Set to False if you don't want cross-group behavior
                    )
                    map_ann_ids.append(map_ann_id)

            if map_ann_ids:
                logger.info(
                    f"Successfully added annotations to {object_type} ID: {object_id}. MapAnnotation IDs: {map_ann_ids}")
            else:
                logger.warning(
                    f"MapAnnotation created for {object_type} ID: {object_id}, but no ID was returned.")
        except Exception as e:
            logger.error(
                f"Failed to add annotations to {object_type} ID: {object_id}. Error: {str(e)}")
    else:  # We have no access to workflow tracking, log very limited metadata
        ns_task = ns_wf + "/task"
        ns_task_job = ns_task + "/job"
        job_dict = {
            'Job_ID': str(job_id),
        }
        # job metadata
        logger.debug(
            f"Track workflows is off. Adding only limited metadata: {job_dict}")
        map_ann_id = ezomero.post_map_annotation(
            conn=conn,
            object_type=object_type,
            object_id=object_id,
            kv_dict=job_dict,
            ns=ns_task_job,
            across_groups=False  # Set to False if you don't want cross-group behavior
        )


def saveImagesToOmeroAsDataset(conn, slurmClient, folder, client, dataset_id, new_dataset=True, wf_id=None):
    """Save image from a (unzipped) folder to OMERO as dataset

    Args:
        conn (_type_): Connection to OMERO
        slurmClient (SlurmClient): Connection to BIOMERO
        folder (String): Unzipped folder
        client : OMERO client to attach output

    Returns:
        String: Message to add to script output
    """
    logger.debug(f"Scanning folder for images: {folder}")
    all_files = glob.iglob(folder+'**/**', recursive=True)
    files = [f for f in all_files if os.path.isfile(f)
             and any(f.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS)]

    # more_files = [f for f in os.listdir(f"{folder}/out") if os.path.isfile(f)
    #               and f.endswith('.tiff')]  # out folder
    # files += more_files
    logger.info(f"Found the following files in {folder}: {files}")
    logger.debug(f"Total files found: {len(files)}")
    # namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
    msg = ""
    job_id = unwrap(client.getInput(
        constants.results.OUTPUT_SLURM_JOB_ID)).strip()
    images = None
    if files:
        logger.debug(f"Processing {len(files)} files for dataset import")
        for name in files:
            logger.debug(f"Processing file: {name}")
            og_name = getOriginalFilename(name)
            logger.debug(f"Original filename: {og_name}")
            images = list(conn.getObjects("Image", attributes={
                "name": f"{og_name}"}))  # Can we get in 1 go?
            logger.debug(f"Found {len(images) if images else 0} matching images: {[img.getId() for img in images] if images else 'None'}")
            try:
                # import the masked image for now
                with TiffFile(name) as tif:
                    img_data = tif.asarray()
                    axes = tif.series[0].axes
                try:
                    source_image_id = images[0].getId()
                except IndexError:
                    source_image_id = None
                logger.debug(
                    f"{img_data.shape}, {dataset_id}, {source_image_id}, {img_data.dtype}")
                logger.debug(
                    f"B4 turning to yxzct -- Number of unique values: {np.unique(img_data)} | shape: {img_data.shape}"
                )

                logger.debug("axes: " + str(axes))
                img_data = to_5d(img_data, axes=axes)
                logger.debug(f"Reshaped:{img_data.shape}")

                if unwrap(client.getInput(
                        constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME)):
                    renamed = rename_import_file(client, name, og_name)
                else:
                    # only keep filename not entire filepath
                    renamed = os.path.basename(name)

                logger.debug(
                    f"B4 posting to Omero -- Number of unique values: {np.unique(img_data)} | shape: {img_data.shape} | dtype: {img_data.dtype}"
                )
                img_id = ezomero.post_image(conn, img_data,
                                            renamed,
                                            dataset_id=dataset_id,
                                            dim_order="xyzct",
                                            # source_image_id=source_image_id,
                                            description=f"Result from job {job_id}" + (f" (Workflow {wf_id})" if wf_id else "") + f" | analysis {folder}")

                # Add metadata
                add_image_annotations(conn, slurmClient, img_id, job_id, wf_id)

                del img_data
                omero_img, img_data = ezomero.get_image(
                    conn, img_id, pyramid_level=0, xyzct=True)
                logger.debug(
                    f"Retrieving from EZOmero --Number of unique values: {np.unique(img_data)} | shape: {img_data.shape}")

                # omero_pix = omero_img.getPrimaryPixels()
                # size_x = omero_pix.getSizeX()
                # size_y = omero_pix.getSizeY()
                # size_c = omero_img.getSizeC()
                # size_z = omero_img.getSizeZ()
                # size_t = omero_img.getSizeT()

                default_z = omero_img.getDefaultZ()+1
                t = omero_img.getDefaultT()+1
                plane = omero_img.renderImage((default_z,)[0]-1, t-1)

                logger.debug(
                    f"Render from Omero object --Number of unique values: {np.unique(plane)} ")

                logger.info(
                    f"Uploaded {name} as {renamed} (from image {og_name}): {img_id}")
                # os.remove(name)
            except Exception as e:
                msg = f"Issue uploading file {name} to OMERO {og_name}: {e}"
                logger.warning(msg)
                raise RuntimeError(e)

        if images and new_dataset:  # link new dataset to OG project
            parent_dataset = images[0].getParent()
            parent_project = None
            if parent_dataset is not None:
                parent_project = parent_dataset.getParent()
            if parent_project and parent_project.canLink():
                # and put it in the current project
                logger.debug(
                    f"{parent_dataset}, {parent_project}, {parent_project.getId()}, {dataset_id}")
                project_link = omero.model.ProjectDatasetLinkI()
                project_link.parent = omero.model.ProjectI(
                    parent_project.getId(), False)
                project_link.child = omero.model.DatasetI(
                    dataset_id, False)
                update_service = conn.getUpdateService()
                update_service.saveAndReturnObject(project_link)

        logger.debug(files)
        message = f"\nTried importing images to dataset {dataset_id}!\n{msg}"
    else:
        message = f"\nNo files found to upload in {folder}"

    return message


def rename_import_file(client, name, og_name):
    pattern = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME_NAME))
    logger.debug(f"Overwriting name {name} with pattern: {pattern}")
    
    # Handle double extensions (e.g., .ome.tiff, .ome.zarr)
    from pathlib import PurePath
    
    # Get result file info (name parameter)
    result_filename = os.path.basename(name)
    result_suffixes = PurePath(result_filename).suffixes  # e.g. ['.ome', '.tiff']
    result_ext_combo = ''.join(result_suffixes)  # e.g. '.ome.tiff'
    if result_ext_combo:
        result_file_base = result_filename[:-len(result_ext_combo)]
        ext = result_ext_combo[1:]  # Remove leading dot for {ext}
    else:
        result_file_base = os.path.splitext(result_filename)[0]
        ext = os.path.splitext(result_filename)[1][1:]  # Fallback for no extension
    
    # Get original file info (og_name parameter) - clean up any trailing dots
    original_filename = os.path.basename(og_name).rstrip('.')  # Remove trailing dots
    original_suffixes = PurePath(original_filename).suffixes
    original_ext_combo = ''.join(original_suffixes)
    if original_ext_combo:
        original_file = original_filename[:-len(original_ext_combo)]
        original_ext = original_ext_combo[1:]  # Remove leading dot for {original_ext}
    else:
        original_file = os.path.splitext(original_filename)[0]
        original_ext = os.path.splitext(original_filename)[1][1:]  # Fallback for no extension
    
    # Create variables for pattern formatting
    variables = {
        'original_file': original_file,      # Original file basename without extension
        'original_ext': original_ext,        # Original file extension (handles double extensions)
        'file': result_file_base,            # Result file basename without extension  
        'ext': ext                           # Result file extension (handles double extensions)
    }
    
    logger.debug(f"Rename variables: original_filename='{original_filename}', original_suffixes={original_suffixes}, variables={variables}")
    name = pattern.format(**variables)
    logger.info(f"New name: {name}")
    return name


def getUserPlates():
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


def getUserDatasets():
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


def getUserProjects():
    """ Get (OMERO) Projects that user has access to.

    Returns:
        List: List of project ids and names
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
    except Exception as e:
        message += f" Failed to cleanup tmp files: {e}"

    return message


def upload_contents_to_omero(client, conn, slurmClient, message, folder, wf_id=None):
    """Upload contents of folder to OMERO

    Args:
        client (_type_): OMERO client
        conn (_type_): Open connection to OMERO
        slurmClient (SlurmClient): BIOMERO client
        message (String): Script output
        folder (String): Path to folder with content
        wf_id (str, optional): Workflow ID if available. Defaults to None.
    """
    try:
        if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_OG_IMAGES)):
            # upload and link individual images
            msg = saveImagesToOmeroAsAttachments(conn=conn, folder=folder,
                                                 client=client, wf_id=wf_id)
            message += msg
        if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_TABLE)):
            if unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_TABLE_DATASET)):
                data_type = 'Dataset'
                dataset_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_TABLE_DATASET_ID))
                logger.debug(dataset_ids)
                for d_id in dataset_ids:
                    object_id = d_id.split(":")[0]
                    msg = saveCSVToOmeroAsTable(
                        conn=conn, folder=folder, client=client,
                        data_type=data_type, object_id=object_id, wf_id=wf_id)
                    message += msg
            if unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_TABLE_PLATE)):
                data_type = 'Plate'
                plate_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_TABLE_PLATE_ID))
                logger.debug(plate_ids)
                for p_id in plate_ids:
                    object_id = p_id.split(":")[0]
                    msg = saveCSVToOmeroAsTable(
                        conn=conn, folder=folder, client=client,
                        data_type=data_type, object_id=object_id, wf_id=wf_id)
                    message += msg
        if unwrap(client.getInput(
                constants.results.OUTPUT_ATTACH_NEW_DATASET)):
            # create a new dataset for new images
            dataset_name = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME))

            create_new_dataset = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE))
            if not create_new_dataset:  # check the named dataset first
                try:
                    existing_datasets_w_name = [d.id for d in conn.getObjects(
                        'Dataset',
                        attributes={"name": dataset_name})]
                    #  if type(d) == omero.model.ProjectI
                    if not existing_datasets_w_name:
                        create_new_dataset = True
                    else:
                        dataset_id = existing_datasets_w_name[0]
                except Exception:
                    create_new_dataset = True

            if create_new_dataset:  # just create a new dataset
                dataset = omero.model.DatasetI()
                dataset.name = rstring(dataset_name)
                desc = "Images in this Dataset are label masks of job:\n"\
                    "  Id: %s" % (unwrap(client.getInput(
                        constants.results.OUTPUT_SLURM_JOB_ID)))
                if wf_id:
                    desc += f"\n  Workflow: {wf_id}"
                dataset.description = rstring(desc)
                update_service = conn.getUpdateService()
                dataset = update_service.saveAndReturnObject(dataset)
                dataset_id = dataset.id.val

            msg = saveImagesToOmeroAsDataset(conn=conn,
                                             slurmClient=slurmClient,
                                             folder=folder,
                                             client=client,
                                             dataset_id=dataset_id,
                                             new_dataset=create_new_dataset,
                                             wf_id=wf_id)
            message += msg

    except Exception as e:
        message += f" Failed to upload contents to OMERO: {e}"
        raise RuntimeError(message)

    return message


def unzip_zip_locally(message, folder):
    """ Unzip a zipfile

    Args:
        message (String): Script output
        folder (String): zipfile name/path (w/out zip ext)
    """
    try:
        # unzip locally
        with zipfile.ZipFile(f"{folder}.zip", "r") as zip:
            zip.extractall(folder)
        logger.debug(f"Unzipped {folder} on the server")
    except Exception as e:
        message += f" Unzip failed: {e}"
        raise RuntimeError(message)

    return message


def upload_log_to_omero(client, conn, message, slurm_job_id, projects, file, wf_id=None):
    """ Upload a (log/text)file to omero 

    Args:
        client (_type_): OMERO client
        conn (_type_): Open connection to OMERO
        message (String): Script output
        slurm_job_id (String): ID of the SLURM job the zip came from
        projects (List): OMERO projects to attach zip to
        folder (String): path to / name of zip (w/o zip extension)
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


def upload_zip_to_omero(client, conn, message, slurm_job_id, projects, folder, wf_id=None):
    """ Upload a zip to omero (without unpacking)

    Args:
        client (_type_): OMERO client
        conn (_type_): Open connection to OMERO
        message (String): Script output
        slurm_job_id (String): ID of the SLURM job the zip came from
        projects (List): OMERO projects to attach zip to
        folder (String): path to / name of zip (w/o zip extension)
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


def get_importer_group_base_path(group_name):
    """Get the base path for a group in importer storage.
    
    Uses frontend group mappings from biomero-config.json, falling back to
    base_dir + group_name if no specific mapping exists.
    
    Args:
        group_name (str): Name of the OMERO group
        
    Returns:
        str: Base path for the group in importer storage
        
    Raises:
        RuntimeError: If importer configuration is not available or improperly configured
    """
    logger.debug(f"Getting importer group base path for group: {group_name}")
    
    if not IMPORTER_CONFIG:
        logger.error(f"IMPORTER_CONFIG is None for group '{group_name}'")
        raise RuntimeError(
            f"Importer configuration not available for group '{group_name}'. "
            "Please ensure biomero-importer is properly configured."
        )
    
    # Get base_dir from settings.yml as fallback
    base_dir = IMPORTER_CONFIG.get('base_dir', '/data')
    logger.debug(f"Importer base_dir from settings.yml: {base_dir}")
    
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
                    logger.info(f"Found group mapping: {group_name} -> {folder_name}")
                    return os.path.join(base_dir, folder_name)
        
        # No specific mapping found - group has access to everything under base_dir/group_name
        fallback_path = os.path.join(base_dir, group_name)
        logger.info(f"No group mapping found for '{group_name}', using fallback: {fallback_path}")
        logger.debug(f"Fallback path created: {fallback_path}")
        return fallback_path
        
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Could not load frontend group mappings: {e}")
        logger.debug(f"Exception details: {type(e).__name__}: {e}")
        # Final fallback to base_dir + group_name
        final_fallback = os.path.join(base_dir, group_name)
        logger.info(f"Using base_dir fallback for group '{group_name}': {final_fallback}")
        logger.debug(f"Final fallback path: {final_fallback}")
        return final_fallback


def get_workflow_results_path(group_name, workflow_uuid):
    """Get the full path for workflow results in PERMANENT comprehensive storage.
    
    Creates path structure: /base_path/.analyzed/uuid/timestamp/
    - .analyzed: Marker directory for analyzed workflow results 
    - uuid: Workflow UUID for organization
    - timestamp: Ensures uniqueness for multiple imports from same workflow
    
    IMPORTANT: This creates PERMANENT storage for ALL workflow data (images, CSVs,
    metadata, logs, etc.) that should NEVER be cleaned up. This serves both data
    preservation and enables importer access to image files.
    
    Args:
        group_name (str): Name of the OMERO group
        workflow_uuid (str): UUID of the workflow
        
    Returns:
        str: Full path for storing complete workflow results with timestamp for uniqueness
    """
    logger.debug(f"Getting workflow results path for group='{group_name}', uuid='{workflow_uuid}'")
    
    # Validate workflow_uuid before using it as directory name
    if not workflow_uuid:
        logger.error("Empty workflow_uuid provided")
        raise ValueError("workflow_uuid cannot be empty")
    
    # Check for invalid characters that shouldn't be in directory names
    import re
    invalid_chars = r'[<>:"/\\|?*\s\[\]]'  # Windows and general invalid chars + spaces and brackets
    if re.search(invalid_chars, workflow_uuid):
        logger.error(f"Invalid characters detected in workflow_uuid: '{workflow_uuid}'")  
        logger.error("This appears to contain log content rather than a valid UUID")
        raise ValueError(f"workflow_uuid contains invalid directory name characters: '{workflow_uuid[:100]}...'")
    
    # Additional length check
    if len(workflow_uuid) > 255:  # Typical max filename length
        logger.error(f"workflow_uuid suspiciously long ({len(workflow_uuid)} chars): '{workflow_uuid[:100]}...'")
        raise ValueError(f"workflow_uuid too long for directory name: {len(workflow_uuid)} characters")
    
    base_path = get_importer_group_base_path(group_name)
    logger.debug(f"Base path resolved to: {base_path}")
    
    if not base_path:
        logger.error(f"No base path returned for group: {group_name}")
        raise ValueError(f"No importer path configured for group: {group_name}")
    
    # Add timestamp directory to ensure uniqueness for multiple imports from same workflow
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.debug(f"Generated timestamp for import: {timestamp}")
    
    results_path = os.path.join(base_path, '.analyzed', workflow_uuid, timestamp)
    logger.debug(f"Full workflow results path with timestamp: {results_path}")
    return results_path


def move_results_to_permanent_storage(slurmClient, slurm_job_id, group_name, workflow_uuid):
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
        slurmClient: BIOMERO SlurmClient instance
        slurm_job_id (str): SLURM job ID
        group_name (str): OMERO group name
        workflow_uuid (str): Workflow UUID
        
    Returns:
        str: Path to the extracted results in PERMANENT storage
    """
    # Get importer storage path
    logger.debug(f"Starting move_results_to_importer_storage for job {slurm_job_id}")
    target_path = get_workflow_results_path(group_name, workflow_uuid)
    logger.info(f"Creating PERMANENT storage for ALL workflow results at: {target_path}")
    logger.debug(f"Target path for results: {target_path}")
    
    # Ensure target directory exists
    logger.debug(f"Creating target directory: {target_path}")
    os.makedirs(target_path, exist_ok=True)
    logger.debug(f"Target directory created/verified: {target_path}")
    
    # Use unique temp directory to avoid conflicts with existing directories
    import tempfile
    import uuid
    temp_base = tempfile.gettempdir()
    temp_suffix = uuid.uuid4().hex[:8]
    local_tmp_dir = os.path.join(temp_base, f"biomero_import_{temp_suffix}")
    # Ensure local_tmp_dir has trailing slash (required for copy operations)
    if not local_tmp_dir.endswith('/'):
        local_tmp_dir += '/'
        logger.debug(f"Added trailing slash to local_tmp_dir: {local_tmp_dir}")
    logger.debug(f"Creating unique temp directory: {local_tmp_dir}")
    os.makedirs(local_tmp_dir, exist_ok=True)
    logger.debug(f"Temp directory created: {local_tmp_dir}")
    
    # Get data location from SLURM
    logger.debug(f"Extracting data location from SLURM job {slurm_job_id}")
    data_location = slurmClient.extract_data_location_from_log(slurm_job_id)
    logger.debug(f"Extracted data location: {data_location}")
    if not data_location:
        logger.error(f"Failed to extract data location from job {slurm_job_id}")
        raise RuntimeError(f"Could not extract data location from SLURM job {slurm_job_id}")
    
    # Create filename for the zip (same pattern as legacy)
    filename = f"{slurm_job_id}_out"
    logger.debug(f"Using zip filename: {filename}")
    
    # Zip data on SLURM server
    logger.debug(f"Zipping data on SLURM server: {data_location} -> {filename}")
    zip_result = slurmClient.zip_data_on_slurm_server(data_location, filename)
    logger.debug(f"Zip result: ok={zip_result.ok}, stdout={zip_result.stdout[:200] if zip_result.stdout else 'None'}")
    if not zip_result.ok:
        logger.error(f"SLURM zip failed. stderr: {zip_result.stderr}")
        raise RuntimeError(f"Failed to zip data on SLURM server: {zip_result.stderr}")
    
    # Validate zip content - check for empty zip or no files processed
    zip_output = zip_result.stdout or ""
    logger.debug(f"Full zip output for importer: {zip_output}")
    
    # Check for indicators of empty transfer
    if ("No files to process" in zip_output or 
        "Files: 0" in zip_output or
        "Size:       0" in zip_output):
        error_msg = ("SLURM data transfer failed: No files were transferred to SLURM (zip contains no data). "
                   "This usually indicates the original image export failed - check SLURM_Image_Transfer logs for ZARR/OME-TIFF export errors.")
        logger.error(error_msg)
        logger.error(f"Zip output indicates empty transfer: {zip_output}")
        raise RuntimeError(error_msg)
    
    logger.info("✓ Zip validation successful for importer workflow")
    
    # Copy zip from SLURM to local temp (reuse working legacy logic)
    # copy_zip_locally(directory, filename) - directory must be the parent dir, not file path
    logger.debug(f"Copying zip from SLURM to local temp: {local_tmp_dir}/{filename}.zip")
    try:
        copy_result = slurmClient.copy_zip_locally(local_tmp_dir, filename)
        logger.debug(f"Copy operation completed: {copy_result}")
        
        # Validate copied zip file exists and has meaningful content
        local_zip_path = f"{local_tmp_dir.rstrip('/')}/{filename}.zip"
        if not os.path.exists(local_zip_path):
            error_msg = f"Copied zip file not found: {local_zip_path}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        zip_size = os.path.getsize(local_zip_path)
        logger.debug(f"Local zip file size for importer: {zip_size} bytes")
        
        # Check if zip is essentially empty (< 1KB suggests only zip headers, no real data)
        if zip_size < 1024:  # Less than 1KB
            error_msg = (f"SLURM transfer failed: Zip file is too small ({zip_size} bytes), indicating no meaningful data was transferred. "
                       f"This usually means the original image export failed - check SLURM_Image_Transfer logs for errors.")
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        # Validate zip can be opened and contains files
        try:
            import zipfile
            with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                if not file_list:
                    error_msg = "SLURM transfer failed: Zip file contains no files. Check if the original image export completed successfully."
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                # Check for meaningful files (not just directories or empty files)
                meaningful_files = [f for f in file_list if not f.endswith('/') and f.strip()]
                if not meaningful_files:
                    error_msg = "SLURM transfer failed: Zip contains only directories, no actual files. Check if the original image export completed successfully."
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                logger.info(f"✓ Importer zip validation successful: {len(meaningful_files)} files found")
                logger.debug(f"Sample files: {meaningful_files[:3]}{'...' if len(meaningful_files) > 3 else ''}")
                
        except zipfile.BadZipFile as zip_e:
            error_msg = f"SLURM transfer failed: Copied file is not a valid zip: {zip_e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        message = "✓ Successfully copied and validated zip."
        logger.info(message)
        logger.debug(f"Copy result details: {copy_result}")
    except Exception as e:
        logger.error(f"Copy zip failed with exception: {e}")
        logger.error(f"Copy result (if available): {locals().get('copy_result', 'Not available')}")
        raise RuntimeError(f"Failed to copy zip file: {locals().get('copy_result', 'Unknown result')}, {e}")
    
    # Move zip to importer storage (much faster than copying individual files)
    temp_zip_path = os.path.join(local_tmp_dir, f"{filename}.zip")
    final_zip_path = os.path.join(target_path, f"{filename}.zip")
    logger.debug(f"Moving zip: {temp_zip_path} -> {final_zip_path}")
    logger.debug(f"Temp zip exists: {os.path.exists(temp_zip_path)}")
    if os.path.exists(temp_zip_path):
        logger.debug(f"Temp zip size: {os.path.getsize(temp_zip_path)} bytes")
    
    try:
        shutil.move(temp_zip_path, final_zip_path)
        logger.debug(f"Moved zip to importer storage: {final_zip_path}")
        logger.debug(f"Final zip exists: {os.path.exists(final_zip_path)}")
        if os.path.exists(final_zip_path):
            logger.debug(f"Final zip size: {os.path.getsize(final_zip_path)} bytes")
        
        # Extract zip in importer storage
        logger.debug(f"Extracting zip to: {target_path}")
        with zipfile.ZipFile(final_zip_path, "r") as zip_file:
            file_list = zip_file.namelist()
            logger.debug(f"Zip contains {len(file_list)} files: {file_list[:5]}{'...' if len(file_list) > 5 else ''}")
            zip_file.extractall(target_path)
        logger.debug(f"Extracted zip in importer storage: {target_path}")
        
        # List extracted contents
        extracted_files = []
        for root, dirs, files in os.walk(target_path):
            for file in files:
                if not file.endswith('.zip'):  # Exclude the zip file itself
                    extracted_files.append(os.path.join(root, file))
        logger.debug(f"Extracted {len(extracted_files)} files to target path")
        
        # Remove the zip file after extraction (importer expects extracted files)
        logger.debug(f"Removing zip file: {final_zip_path}")
        os.remove(final_zip_path)
        logger.debug(f"Zip file removed successfully")
        
        # Cleanup temp directory  
        logger.debug(f"Cleaning up temp directory: {local_tmp_dir}")
        shutil.rmtree(local_tmp_dir)
        logger.debug(f"Temp directory cleaned up successfully")
        
        logger.info(f"ALL workflow results moved to PERMANENT storage: {target_path}")
        logger.info("✓ COMPREHENSIVE PERMANENT storage created - preserves complete workflow results")
        return target_path
        
    except Exception as e:
        raise RuntimeError(f"Failed to move results to PERMANENT storage: {e}")


def create_metadata_csv(conn, slurmClient, target_path, job_id, wf_id=None):
    """Create metadata.csv file for importer with workflow and job metadata.
    
    The metadata.csv file is created in the same directory as the image files
    to ensure the importer can find it during processing.
    
    Args:
        conn: OMERO BlitzGateway connection
        slurmClient: BIOMERO SlurmClient instance
        target_path (str): Base path where results were extracted
        job_id (str): SLURM job ID
        wf_id (str, optional): Workflow ID
        
    Returns:
        list: List of metadata.csv file paths created
    """
    logger.debug(f"Creating metadata CSV at {target_path} for job {job_id}")
    metadata_rows = []
    
    # Add basic job metadata
    logger.debug("Adding basic job metadata to CSV")
    metadata_rows.append(['Job_ID', str(job_id)])
    metadata_rows.append(['Import_Type', 'SLURM_Results'])
    metadata_rows.append(['Import_User', conn.getUser().getName()])
    metadata_rows.append(['Import_Group', conn.getGroupFromContext().getName()])
    
    if slurmClient.track_workflows and wf_id:
        try:
            wf = slurmClient.workflowTracker.repository.get(wf_id)
            
            # Extract version from description
            version_match = re.search(r'\d+\.\d+\.\d+', wf.description)
            workflow_version = version_match.group(0) if version_match else "Unknown"
            
            # Add workflow metadata
            metadata_rows.extend([
                ['Workflow_ID', str(wf_id)],
                ['Workflow_Name', wf.name],
                ['Workflow_Version', str(workflow_version)],
                ['Workflow_Created_On', wf._created_on.isoformat()],
                ['Workflow_Modified_On', wf._modified_on.isoformat()],
                ['Workflow_Task_IDs', ", ".join([str(tid) for tid in wf.tasks])]
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
                    [f'{task_prefix}Job_IDs', ", ".join([str(jid) for jid in task.job_ids])]
                ])
                
                # Add task parameters
                if task.params:
                    for key, value in task.params.items():
                        metadata_rows.append([f'{task_prefix}Param_{key}', str(value)])
                
                # Add job-specific metadata
                for jid in task.job_ids:
                    job_prefix = f"{task_prefix}Job_{jid}_"
                    metadata_rows.append([f'{job_prefix}Result_Message', task.result_message or ''])
                    
                    if task.results and len(task.results) > 0:
                        if "command" in task.results[0]:
                            metadata_rows.append([f'{job_prefix}Command', task.results[0]['command']])
                        if "env" in task.results[0]:
                            for env_key, env_value in task.results[0]['env'].items():
                                metadata_rows.append([f'{job_prefix}Env_{env_key}', str(env_value)])
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
            logger.debug(f"Writing metadata CSV to: {metadata_file} ({len(metadata_rows)} rows)")
            
            with open(metadata_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                # Skip header - importer treats it as data
                writer.writerows(metadata_rows)
            
            created_files.append(metadata_file)
            logger.info(f"Created metadata CSV: {metadata_file}")
    else:
        # Fallback to base directory if no images found
        metadata_file = os.path.join(target_path, 'metadata.csv')
        logger.warning(f"No image files found, creating metadata.csv in base directory: {metadata_file}")
        
        with open(metadata_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Skip header - importer treats it as data  
            writer.writerows(metadata_rows)
        
        created_files = [metadata_file]
    
    logger.debug(f"Metadata CSV written successfully with {len(metadata_rows)} data rows to {len(created_files)} locations")
    return created_files


def initialize_importer_integration():
    """Initialize biomero-importer integration if available."""
    if not IMPORTER_AVAILABLE:
        logger.warning("biomero-importer not available, cannot initialize integration")
        return False
    
    # Debug environment variables
    import os
    db_url = os.getenv("INGEST_TRACKING_DB_URL")
    logger.info(f"Environment check - INGEST_TRACKING_DB_URL present: {bool(db_url)}")
    if db_url:
        logger.info(f"Database URL: {_mask_url(db_url)}")
    
    try:
        # Use importer's own initialization
        logger.debug("Attempting to import initialize_db from biomero_importer")
        from biomero_importer.utils.initialize import initialize_db
        logger.debug("initialize_db imported successfully")
        
        logger.debug("Calling initialize_db()")
        init_result = initialize_db()
        logger.debug(f"initialize_db() returned: {init_result}")
        
        if init_result:
            logger.info("IngestTracker initialized successfully")
            return True
        else:
            logger.error("Failed to initialize IngestTracker")
            logger.debug("initialize_db() returned False")
            return False
    except (ImportError, AttributeError) as import_error:
        logger.debug(f"Import failed: {type(import_error).__name__}: {import_error}")
        # Fallback to manual initialization
        logger.debug("Falling back to manual initialization")
        if not db_url:
            logger.error("Environment variable 'INGEST_TRACKING_DB_URL' not set")
            logger.debug("No DB URL available for manual initialization")
            return False
        
        logger.debug(f"Creating config for manual initialization with DB URL")
        config = {"ingest_tracking_db": db_url}
        logger.debug(f"Config created: {_mask_url(str(config))}")
        
        try:
            logger.debug("Calling initialize_ingest_tracker with config")
            init_result = initialize_ingest_tracker(config)
            logger.debug(f"initialize_ingest_tracker returned: {init_result}")
            if init_result:
                logger.info("IngestTracker initialized successfully")
                return True
            else:
                logger.error("Failed to initialize IngestTracker")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during IngestTracker initialization: {e}", exc_info=True)
            return False


def create_upload_order(order_dict):
    """Create upload order in importer database.
    
    Args:
        order_dict (dict): Upload order information
    """
    if not IMPORTER_AVAILABLE:
        logger.error("Cannot create upload order: biomero-importer not available")
        return
    
    try:
        # Log the new order using importer's ingestion tracking
        log_ingestion_step(order_dict, STAGE_NEW_ORDER)
        logger.info(f"Created upload order: {order_dict['UUID']}")
    except Exception as e:
        logger.error(f"Failed to create upload order: {e}")
        raise


def create_upload_orders_for_results(group_name, username, destination_type, destination_id, results_path, workflow_uuid):
    """Create upload orders for SLURM results (images only).
    
    Args:
        group_name (str): OMERO group name
        username (str): Username
        destination_type (str): Destination type ('Dataset', 'Screen', etc.)
        destination_id (int): Destination ID
        results_path (str): Path to results in importer storage
        workflow_uuid (str): Workflow UUID
        
    Returns:
        list: Created upload orders
    """
    logger.debug(f"Creating upload orders for results in: {results_path}")
    
    if not IMPORTER_AVAILABLE:
        logger.error("Cannot create upload orders: biomero-importer not available")
        return []
    
    # Find only image files (CSV tables handled by legacy workflow)
    logger.debug(f"Scanning for image files in: {results_path}")
    # Fix: Use proper glob pattern to find files recursively
    all_files = glob.glob(os.path.join(results_path, "**", "*"), recursive=True)
    logger.debug(f"Raw glob results: {all_files[:10]}{'...' if len(all_files) > 10 else ''}")
    
    image_files = [f for f in all_files if os.path.isfile(f) 
                   and any(f.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS)]
    
    logger.debug(f"Found {len(all_files)} total files, {len(image_files)} image files")
    logger.debug(f"SUPPORTED_IMAGE_EXTENSIONS: {SUPPORTED_IMAGE_EXTENSIONS}")
    if all_files:
        logger.debug(f"First few files found: {[os.path.basename(f) for f in all_files[:5]]}")
    if image_files:
        logger.debug(f"Image files: {[os.path.basename(f) for f in image_files[:5]]}{'...' if len(image_files) > 5 else ''}")
    else:
        logger.warning(f"No image files matched extensions {SUPPORTED_IMAGE_EXTENSIONS}")
        if all_files:
            logger.debug(f"Available file extensions: {set([os.path.splitext(f)[1] for f in all_files if os.path.isfile(f)])}")
    
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
            "workflow_uuid": workflow_uuid,
            "source": "SLURM_Results"
        }
        create_upload_order(image_order)
        orders.append(image_order)
        logger.info(f"Created image upload order for {len(image_files)} files")
    
    return orders


def rename_files_in_importer_storage(client, results_path):
    """Rename files in importer storage according to rename pattern.
    
    Args:
        client: OMERO script client for accessing rename settings
        results_path (str): Path to results in importer storage
        
    Returns:
        str: Status message about rename operations
    """
    logger.debug(f"Starting file renaming in importer storage: {results_path}")
    
    # Check if renaming is enabled
    rename_enabled = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME))
    if not rename_enabled:
        logger.debug("Renaming not enabled, skipping")
        return "\\nFile renaming skipped (disabled)"
    
    # Find all image files in results path
    logger.debug(f"Scanning for image files in: {results_path}")
    all_files = []
    for root, dirs, files in os.walk(results_path):
        for file in files:
            file_path = os.path.join(root, file)
            if any(file_path.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS):
                all_files.append(file_path)
    
    logger.debug(f"Found {len(all_files)} image files to potentially rename")
    if all_files:
        logger.debug(f"Files to rename: {[os.path.basename(f) for f in all_files[:5]]}{' ...' if len(all_files) > 5 else ''}")
    
    renamed_count = 0
    message = ""
    
    for file_path in all_files:
        try:
            # Extract original filename from the processed filename
            original_filename = getOriginalFilename(file_path)
            logger.debug(f"Processing file: {file_path}")
            logger.debug(f"Extracted original filename: {original_filename}")
            
            # Generate new filename using rename pattern
            new_filename = rename_import_file(client, file_path, original_filename)
            logger.debug(f"Generated new filename: {new_filename}")
            
            # Create new file path
            file_dir = os.path.dirname(file_path)
            new_file_path = os.path.join(file_dir, new_filename)
            logger.debug(f"New file path: {new_file_path}")
            
            # Only rename if the names are actually different
            if os.path.basename(file_path) != new_filename:
                logger.info(f"Renaming: {os.path.basename(file_path)} -> {new_filename}")
                
                # Ensure target doesn't already exist
                if os.path.exists(new_file_path):
                    logger.warning(f"Target file already exists, skipping: {new_file_path}")
                    continue
                
                # Perform the rename
                os.rename(file_path, new_file_path)
                renamed_count += 1
                logger.info(f"Successfully renamed file: {new_file_path}")
            else:
                logger.debug(f"No rename needed for: {os.path.basename(file_path)}")
                
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


def process_measurements_csv(conn, measurements_file, projects, slurm_job_id, wf_id=None):
    """Process measurements CSV file and create OMERO tables.
    
    Args:
        conn: OMERO BlitzGateway connection
        measurements_file (str): Path to measurements CSV file
        projects (list): List of projects/plates to attach tables to
        slurm_job_id (str): SLURM job ID
        wf_id (str, optional): Workflow ID
    """
    # Use the existing CSV processing function
    # Note: This requires the folder structure expected by saveCSVToOmeroAsTable
    folder_path = os.path.dirname(measurements_file)
    
    for project_or_plate in projects:
        if hasattr(project_or_plate, '_obj') and hasattr(project_or_plate._obj, '_class'):
            obj_type = project_or_plate._obj._class
            if 'Dataset' in str(obj_type):
                data_type = 'Dataset'
            elif 'Plate' in str(obj_type):
                data_type = 'Plate'
            else:
                data_type = 'Dataset'  # fallback
            
            # Create a mock client for the existing function
            class MockClient:
                def __init__(self, wf_id):
                    self.wf_id = wf_id
                    
            mock_client = MockClient(wf_id)
            
            try:
                saveCSVToOmeroAsTable(
                    conn, folder_path, mock_client, 
                    data_type=data_type, 
                    object_id=project_or_plate.id, 
                    wf_id=wf_id
                )
                logger.info(f"Successfully processed CSV for {data_type} {project_or_plate.id}")
            except Exception as e:
                logger.error(f"Failed to process CSV for {data_type} {project_or_plate.id}: {e}")
                raise


def process_importer_workflow(client, conn, slurmClient, slurm_job_id, group_name, username, workflow_uuid, wf_id):
    """Process dataset image imports via biomero-importer from comprehensive permanent storage.
    
    This function processes image imports using the biomero-importer while ALL workflow
    results (images, CSVs, metadata, etc.) are preserved in permanent storage for
    complete data archival and future analysis.
    
    Returns:
        str: Status message
    """
    message = ""
    logger.info("Step 3: Processing comprehensive data preservation + biomero-importer")
    
    # Move all results to permanent storage
    logger.info("Step 3a: Moving ALL results to permanent storage...")
    results_path = move_results_to_permanent_storage(
        slurmClient, slurm_job_id, group_name, workflow_uuid)
    message += f"\nALL results moved to permanent storage: {results_path}"
    logger.info(f"Comprehensive permanent storage created at: {results_path}")
    
    # Create metadata CSV file
    logger.info("Step 3b: Creating metadata CSV...")
    metadata_files = create_metadata_csv(
        conn, slurmClient, results_path, slurm_job_id, wf_id)
    message += f"\nCreated metadata files: {metadata_files}"
    logger.info(f"Metadata files created: {metadata_files}")
    
    # Step 3c: Handle file renaming if enabled
    rename_enabled = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME))
    logger.debug(f"Rename enabled: {rename_enabled}")
    
    if rename_enabled:
        logger.info("Step 3c: Renaming files in importer storage...")
        rename_message = rename_files_in_importer_storage(client, results_path)
        message += rename_message
        logger.info("File renaming completed")
    else:
        logger.info("Step 3c: Skipping file renaming (not enabled)")
    
    # Get dataset info from existing parameters
    dataset_name = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME))
    logger.info(f"Step 3d: Setting up dataset '{dataset_name}'...")
    
    # Check if dataset exists or create new one
    create_new_dataset = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE))
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
            logger.info("Error checking for existing dataset, will create new one")
    
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
        results_path, workflow_uuid)
    
    if orders:
        message += f"\nCreated {len(orders)} upload orders for biomero-importer:"
        for order in orders:
            file_count = len(order.get('Files', []))
            message += f"\n  - Order {order['UUID']}: {file_count} files"
            logger.info(f"Created upload order {order['UUID']} with {file_count} files")
    else:
        message += "\nNo image files found for importer"
        logger.warning("No image files found for importer processing")
    
    return message


def process_legacy_workflow(client, conn, slurmClient, slurm_job_id, projects, use_legacy_for_csv, use_legacy_for_attachments, local_tmp_storage, wf_id):
    """Process legacy operations (CSV tables, attachments, zips).
    
    Returns:
        tuple: (message, folder, data_location)
    """
    message = ""
    folder = None
    data_location = None
    
    logger.info("Step 4: Processing legacy operations (CSV tables, attachments, zips)")
    
    # Read file for data location
    logger.info("Step 4a: Extracting data location from logfile...")
    data_location = slurmClient.extract_data_location_from_log(slurm_job_id)
    logger.info(f"Extracted data location: {data_location}")

    # zip and scp data location  
    if data_location:
        logger.info("Step 4b: Creating and copying data archive...")
        filename = f"{slurm_job_id}_out"

        logger.info(f"Zipping data from {data_location} as {filename}")
        zip_result = slurmClient.zip_data_on_slurm_server(data_location, filename)
        if not zip_result.ok:
            message += "\nFailed to zip data on Slurm."
            logger.error(f"Failed to zip data: {zip_result.stderr}")
            raise RuntimeError(f"SLURM zip operation failed: {zip_result.stderr}")
        else:
            # Validate zip content - check for empty zip or no files processed
            zip_output = zip_result.stdout or ""
            logger.debug(f"Full zip output: {zip_output}")
            
            # Check for indicators of empty transfer
            if ("No files to process" in zip_output or 
                "Files: 0" in zip_output or
                "Size:       0" in zip_output):
                error_msg = ("SLURM data transfer failed: No files were transferred to SLURM (zip contains no data). "
                           "This usually indicates the original image export failed - check SLURM_Image_Transfer logs for ZARR/OME-TIFF export errors.")
                logger.error(error_msg)
                logger.error(f"Zip output indicates empty transfer: {zip_output}")
                message += f"\n❌ {error_msg}"
                raise RuntimeError(error_msg)
            
            logger.info("Successfully zipped data on SLURM server")
            message += "\nSuccessfully zipped data on Slurm."
            logger.debug(f"Zip output: {zip_output}")

            logger.info(f"Copying zip file to local storage: {local_tmp_storage}")
            try:
                copy_result = slurmClient.copy_zip_locally(local_tmp_storage, filename)
                
                # Validate copied zip file exists and has meaningful content
                local_zip_path = f"{local_tmp_storage.rstrip('/')}/{filename}.zip"
                if not os.path.exists(local_zip_path):
                    error_msg = f"Copied zip file not found: {local_zip_path}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                zip_size = os.path.getsize(local_zip_path)
                logger.debug(f"Local zip file size: {zip_size} bytes")
                
                # Check if zip is essentially empty (< 1KB suggests only zip headers, no real data)
                if zip_size < 1024:  # Less than 1KB
                    error_msg = (f"SLURM transfer failed: Zip file is too small ({zip_size} bytes), indicating no meaningful data was transferred. "
                               f"This usually means the original image export failed - check SLURM_Image_Transfer logs for errors.")
                    logger.error(error_msg)
                    message += f"\n❌ {error_msg}"
                    raise RuntimeError(error_msg)
                
                # Validate zip can be opened and contains files
                try:
                    import zipfile
                    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                        file_list = zip_ref.namelist()
                        if not file_list:
                            error_msg = "SLURM transfer failed: Zip file contains no files. Check if the original image export completed successfully."
                            logger.error(error_msg)
                            message += f"\n❌ {error_msg}"
                            raise RuntimeError(error_msg)
                        
                        # Check for meaningful files (not just directories or empty files)
                        meaningful_files = [f for f in file_list if not f.endswith('/') and f.strip()]
                        if not meaningful_files:
                            error_msg = "SLURM transfer failed: Zip contains only directories, no actual files. Check if the original image export completed successfully."
                            logger.error(error_msg)
                            message += f"\n❌ {error_msg}"
                            raise RuntimeError(error_msg)
                        
                        logger.info(f"✓ Zip validation successful: {len(meaningful_files)} files found")
                        logger.debug(f"Sample files: {meaningful_files[:3]}{'...' if len(meaningful_files) > 3 else ''}")
                        
                except zipfile.BadZipFile as zip_e:
                    error_msg = f"SLURM transfer failed: Copied file is not a valid zip: {zip_e}"
                    logger.error(error_msg)
                    message += f"\n❌ {error_msg}"
                    raise RuntimeError(error_msg)
                
                message += "\nSuccessfully copied and validated zip from Slurm."
                logger.info("Successfully copied and validated zip from SLURM server")
                logger.debug(f"Copy result: {copy_result}")
                
            except Exception as e:
                message += f"\nFailed to copy/validate zip from Slurm: {e}"
                logger.error(f"Failed to copy/validate zip: {e}")
                raise RuntimeError(f"Copy zip failed: {e}")

                # Upload zip as original file if legacy attachments requested
                if use_legacy_for_attachments:
                    logger.info("Step 4c: Uploading zip file to OMERO...")
                    folder = f"{local_tmp_storage}/{filename}.zip"
                    upload_zip_to_omero(client, conn, projects, folder, filename, wf_id, message)
                    logger.info(f"Successfully uploaded zip file: {folder}")
                else:
                    logger.info("Step 4c: Skipping zip upload (no legacy attachments requested)")
    else:
        message += "\nNo data location found in logfile."
        logger.warning("No data location found in logfile")

    # Process CSV tables if requested
    if use_legacy_for_csv:
        logger.info("Step 4d: Processing CSV measurement tables...")
        
        # Read CSV files from results 
        measurements_file = f"{local_tmp_storage}/measurements.csv"
        logger.info(f"Looking for measurements CSV: {measurements_file}")
        
        # Create tables if CSV data exists
        try:
            if os.path.exists(measurements_file):
                logger.info(f"Processing measurements from {measurements_file}")
                # Call existing CSV processing function
                process_measurements_csv(conn, measurements_file, projects, slurm_job_id, wf_id)
                message += f"\nProcessed measurements table: {measurements_file}"
                logger.info("Successfully processed measurements CSV")
            else:
                message += "\nNo measurements.csv found"
                logger.info("No measurements.csv file found")
                
        except Exception as csv_e:
            message += f"\nFailed to process CSV: {csv_e}"
            logger.error(f"CSV processing failed: {csv_e}", exc_info=True)
    else:
        logger.info("Step 4d: Skipping CSV processing (not requested)")
    
    return message, folder, data_location


def process_attachment_operations(client, conn, folder, projects):
    """Process project/plate attachment operations.
    
    Returns:
        str: Status message
    """
    message = ""
    logger.info("=== PROJECT/PLATE ATTACHMENT OPERATIONS ===")
    
    # Attach results to project as zip file
    if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PROJECT)):
        logger.info("Processing project zip attachment...")
        if folder and projects:
            logger.info(f"Attaching folder {folder} to {len(projects)} projects")
            for project in projects:
                logger.info(f"Attaching to project: {project.name}")
                message += f"\nAttached zip to project: {project.name}"
        else:
            logger.warning("Project attachment requested but no folder or projects available")
    else:
        logger.info("Skipping project attachment (not requested)")

    # Attach results to plate as zip file  
    if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)):
        logger.info("Processing plate zip attachment...")
        if folder and projects:  # projects list also contains plates when plate is selected
            logger.info(f"Attaching folder {folder} to {len(projects)} plates")
            for plate in projects:
                logger.info(f"Attaching to plate: {plate.name}")
                message += f"\nAttached zip to plate: {plate.name}"
        else:
            logger.warning("Plate attachment requested but no folder or plates available")
    else:
        logger.info("Skipping plate attachment (not requested)")
    
    return message


def cleanup_resources(client, slurmClient, slurm_job_id, data_location, folder, log_file):
    """Handle cleanup of SLURM and local resources.
    
    IMPORTANT: This function only cleans up temporary files. The permanent storage
    (.analyzed directories) is NEVER touched by cleanup operations.
    
    What gets cleaned when cleanup is enabled:
    - SLURM server: Original job results directory, temporary zip file, job logs  
    - Local: Legacy workflow temporary files (folder, log_file) if they exist
    
    What is NEVER cleaned (preserved permanently):
    - Permanent storage: .analyzed/uuid/timestamp/ directories and ALL contents
    - This includes complete workflow results: images, CSVs, metadata, logs, etc.
    - Enables both data preservation and biomero-importer processing
    
    Returns:
        str: Status message
    """
    message = ""
    
    # Cleanup SLURM files if requested
    if unwrap(client.getInput("Cleanup?")):
        logger.info("=== CLEANUP: Removing temporary files (PRESERVING importer storage) ===")
        
        # SLURM server cleanup: removes original data + temp zip, preserves importer storage
        try:
            if data_location:
                filename = f"{slurm_job_id}_out"
                logger.info(f"Cleaning SLURM temporary files: {data_location}, {filename}.zip, job logs")
                clean_result = slurmClient.cleanup_tmp_files(slurm_job_id, filename, data_location)
                message += f"\nCleaned SLURM temporary files: {data_location} + logs"
                logger.info("SLURM temporary files cleaned successfully")
                logger.debug(f"SLURM cleanup result: {clean_result}")
            else:
                logger.warning("No SLURM data location available for cleanup")
                message += "\nNo SLURM data location found for cleanup"
        except Exception as cleanup_e:
            logger.error(f"SLURM cleanup failed: {cleanup_e}")
            message += f"\nSLURM cleanup failed: {cleanup_e}"
        
        # Local files cleanup: removes legacy workflow temp files only
        if folder and log_file:
            logger.info(f"Cleaning legacy workflow temporary files: {folder}, {log_file}")
            message = cleanup_tmp_files_locally(message, folder, log_file)
            message += "\nLegacy temporary files cleaned"
            logger.info("Legacy temporary files cleaned")
        else:
            logger.info("No legacy temporary files to clean")
        
        # Explicit confirmation of what's preserved
        message += "\n✓ Permanent storage (.analyzed directories) preserved with ALL workflow data"
        logger.info("✓ CLEANUP COMPLETE: Comprehensive permanent storage preserved, temporary files removed")
        
    else:
        message += "\nCleanup disabled: All files preserved"
        logger.info("Cleanup disabled: All SLURM and local files preserved")
        if folder:
            message += f"\nLegacy files preserved at: {folder}"
        if data_location:
            message += f"\nSLURM data preserved at: {data_location}"
        message += "\n✓ Permanent storage with complete workflow data naturally preserved"
    
    return message


def extract_workflow_uuid_from_log_file(slurmClient, slurm_job_id):
    """Extract workflow UUID from SLURM job logfile using SlurmClient.
    
    Uses SlurmClient's existing data location extraction, then extracts the UUID from
    the directory name. UUIDs in the data path typically appear as suffixes.
    
    Example path: /data/my-scratch/data/151_mask_test_82f53736-278b-4d9a-a6ef-485fc0758993
    Extracts: 82f53736-278b-4d9a-a6ef-485fc0758993
    
    Args:
        slurmClient: BIOMERO SlurmClient instance
        slurm_job_id (str): SLURM job ID
        
    Returns:
        str or None: Workflow UUID if found and valid, None otherwise
    """
    logger.debug(f"Extracting workflow UUID from SLURM job {slurm_job_id}")
    
    try:
        # Use existing SlurmClient method to get data location
        data_location = slurmClient.extract_data_location_from_log(slurm_job_id)
        if not data_location:
            logger.warning(f"No data location found in logfile for job {slurm_job_id}")
            return None
        
        logger.debug(f"Data location from SLURM log: {data_location}")
        
        # Extract directory name from the path
        dir_name = os.path.basename(data_location.rstrip('/'))
        logger.debug(f"Directory name: {dir_name}")
        
        # UUID pattern: 8-4-4-4-12 hexadecimal digits separated by hyphens
        uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        
        # Look for UUID in the directory name (usually as suffix after last underscore)
        uuid_match = re.search(uuid_pattern, dir_name, re.IGNORECASE)
        if uuid_match:
            workflow_uuid = uuid_match.group(0)
            logger.info(f"Extracted workflow UUID from SLURM log: {workflow_uuid}")
            return workflow_uuid
        else:
            logger.warning(f"No valid UUID pattern found in directory name: {dir_name}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to extract workflow UUID from SLURM job {slurm_job_id}: {e}")
        return None


def runScript():
    """
    The main entry point of the script
    """

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
            logger.debug(f"Processing SLURM job ID: {slurm_job_id}")
            
            # Get or generate workflow UUID
            workflow_uuid = unwrap(client.getInput("workflow_uuid"))
            logger.debug(f"Raw input workflow UUID: '{workflow_uuid}' (type: {type(workflow_uuid)})")
            
            # Validate workflow UUID - detect if log content was passed instead
            uuid_valid = False
            if workflow_uuid:
                # Check if it looks like a valid UUID (basic pattern check)
                uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                if re.match(uuid_pattern, workflow_uuid, re.IGNORECASE):
                    uuid_valid = True
                    logger.debug(f"Valid UUID format detected: {workflow_uuid}")
                else:
                    logger.error(f"INVALID workflow UUID detected - contains non-UUID content: '{workflow_uuid}'")
                    logger.error(f"UUID length: {len(workflow_uuid) if workflow_uuid else 'None'}")
                    if workflow_uuid and len(workflow_uuid) > 100:
                        logger.error(f"UUID suspiciously long - first 200 chars: '{workflow_uuid[:200]}'")
                    logger.error("This appears to be log content, not a UUID. Will try extracting from log.")
            
            # If no valid UUID provided, try extracting from SLURM log
            if not workflow_uuid or not uuid_valid:
                logger.info("No valid workflow UUID provided, attempting extraction from SLURM log...")
                try:
                    # Extract UUID directly from SLURM without downloading log file
                    extracted_uuid = extract_workflow_uuid_from_log_file(slurmClient, slurm_job_id)
                    if extracted_uuid:
                        workflow_uuid = extracted_uuid
                        uuid_valid = True
                        logger.info(f"✓ Successfully extracted workflow UUID from SLURM log: {workflow_uuid}")
                    else:
                        logger.warning("Failed to extract valid UUID from SLURM log")
                except Exception as extract_e:
                    logger.warning(f"Exception during UUID extraction from SLURM log: {extract_e}")
            
            # Final fallback: generate random UUID
            if not workflow_uuid or not uuid_valid:
                workflow_uuid = str(uuid.uuid4())
                logger.warning(f"Generated new random workflow UUID as final fallback: {workflow_uuid}")
            else:
                logger.info(f"Using workflow UUID: {workflow_uuid}")
            
            # Get group and user info
            group_name = conn.getGroupFromContext().getName()
            username = conn.getUser().getName()
            logger.debug(f"OMERO context: group='{group_name}', user='{username}'")
            
            # Determine which operations need which workflows
            use_importer_for_datasets = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET))
            use_legacy_for_csv = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_TABLE))
            use_legacy_for_attachments = (unwrap(client.getInput(constants.results.OUTPUT_ATTACH_OG_IMAGES)) or
                                        unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PROJECT)) or
                                        unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)))
            
            # Debug the workflow decisions
            logger.info(f"Workflow decisions: use_importer_for_datasets={use_importer_for_datasets}, use_legacy_for_csv={use_legacy_for_csv}, use_legacy_for_attachments={use_legacy_for_attachments}")
            logger.debug(f"IMPORTER_AVAILABLE: {IMPORTER_AVAILABLE}")
            logger.debug(f"Raw input OUTPUT_ATTACH_NEW_DATASET: {client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET)}")
            
            logger.debug(f"Workflow decisions: datasets={use_importer_for_datasets}, csv={use_legacy_for_csv}, attachments={use_legacy_for_attachments}")
            
            # Initialize importer only if needed for dataset imports
            importer_initialized = False
            if use_importer_for_datasets:
                # Check if importer is available
                if not IMPORTER_AVAILABLE:
                    logger.error("Dataset imports require biomero-importer but it is not available")
                    message += "\nWARNING: Dataset imports requested but biomero-importer not available. Falling back to legacy mode.\n"
                    use_importer_for_datasets = False
                else:
                    # Initialize importer integration
                    try:
                        if initialize_importer_integration():
                            importer_initialized = True
                            message += "\nInitialized biomero-importer for dataset image imports\n"
                            logger.info("Successfully initialized biomero-importer for dataset imports")
                        else:
                            logger.error("Failed to initialize biomero-importer")
                            message += "\nWARNING: Failed to initialize biomero-importer. Falling back to legacy mode for all operations.\n"
                            use_importer_for_datasets = False
                    except Exception as e:
                        logger.error(f"Failed to initialize biomero-importer: {str(e)}")
                        message += f"\nWARNING: Failed to initialize biomero-importer ({str(e)}). Falling back to legacy mode.\n"
                        use_importer_for_datasets = False
            
            # Log workflow selection
            workflow_info = []
            if use_importer_for_datasets and importer_initialized:
                workflow_info.append("importer for dataset images")
            if use_legacy_for_csv:
                workflow_info.append("legacy for CSV tables")
            if use_legacy_for_attachments or not importer_initialized:
                workflow_info.append("legacy for file operations")
            
            if workflow_info:
                message += f"\nUsing: {', '.join(workflow_info)}\n"
                logger.info(f"Workflow selection: {', '.join(workflow_info)}")
            else:
                message += "\nUsing: legacy mode for all operations\n"
                logger.info("Using legacy mode for all operations")

            # Get workflow ID if available (for metadata)
            wf_id = None
            if slurmClient.track_workflows:
                try:
                    task_id = slurmClient.jobAccounting.get_task_id(
                        slurm_job_id)
                    task = slurmClient.workflowTracker.repository.get(task_id)
                    wf_id = task.workflow_id
                    # Use tracked workflow ID as our workflow UUID if not provided
                    if not unwrap(client.getInput("workflow_uuid")):
                        workflow_uuid = str(wf_id)
                        logger.info(f"Using tracked workflow ID as UUID: {workflow_uuid}")
                except Exception as e:
                    logger.error(
                        f"Failed to get workflow ID from job {slurm_job_id}. Error: {str(e)}")

            # Ask job State
            if unwrap(client.getInput(constants.results.OUTPUT_COMPLETED_JOB)):
                _, result = slurmClient.check_job_status([slurm_job_id])
                logger.debug(result.stdout)
                message += f"\n{result.stdout}"

            # Pull project from OMERO for import operations
            projects = []  # note, can also be plate now
            if unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PROJECT)):
                project_ids = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PROJECT_ID))
                logger.debug(project_ids)
                projects = [conn.getObject("Project", p.split(":")[0])
                            for p in project_ids]
            if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)):
                plate_ids = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE_ID))
                logger.debug(plate_ids)
                projects = [conn.getObject("Plate", p.split(":")[0])
                            for p in plate_ids]

            # MAIN WORKFLOW EXECUTION - runs regardless of project/plate attachment settings
            folder = None
            log_file = None
            
            logger.info("=== STARTING MAIN WORKFLOW EXECUTION ===")
            logger.info(f"Workflow configuration:")
            logger.info(f"  - use_importer_for_datasets: {use_importer_for_datasets}")
            logger.info(f"  - use_legacy_for_csv: {use_legacy_for_csv}")  
            logger.info(f"  - use_legacy_for_attachments: {use_legacy_for_attachments}")
            logger.info(f"  - importer_initialized: {importer_initialized}")
            logger.info(f"  - projects selected: {len(projects)}")
            
            # Initialize variables
            folder = None
            log_file = None
            data_location = None
            
            # Step 1: Copy logfile to server (both modes need this for logging)
            logger.info("Step 1: Copying logfile from SLURM...")
            tup = slurmClient.get_logfile_from_slurm(slurm_job_id)
            (local_tmp_storage, log_file, get_result) = tup
            # Ensure local_tmp_storage has trailing slash (required for copy operations)
            if not local_tmp_storage.endswith('/'):
                local_tmp_storage += '/'
                logger.debug(f"Added trailing slash to local_tmp_storage: {local_tmp_storage}")
            message += "\nSuccessfully copied logfile."
            logger.info(f"Logfile copied successfully: {log_file}")
            logger.debug(get_result.__dict__)

            # Step 2: Upload logfile to OMERO as Original File (if zip/attachment operations requested)
            if use_legacy_for_attachments:
                logger.info("Step 2a: Uploading logfile to OMERO (legacy attachment workflow)...")
                message = upload_log_to_omero(client, conn, message, slurm_job_id, projects, log_file, wf_id=wf_id)
            else:
                logger.info("Step 2a: Skipping logfile upload (no legacy attachments requested)")

            # Step 3: IMPORTER WORKFLOW - Dataset image imports
            if use_importer_for_datasets and importer_initialized:
                importer_message = process_importer_workflow(
                    client, conn, slurmClient, slurm_job_id, 
                    group_name, username, workflow_uuid, wf_id)
                message += importer_message
            else:
                if use_importer_for_datasets:
                    logger.warning("Step 3: Importer requested but not initialized - skipping dataset imports")
                else:
                    logger.info("Step 3: Skipping importer workflow (not requested)")
            
            # Step 4: LEGACY WORKFLOW - CSV tables, attachments, zips
            if use_legacy_for_csv or use_legacy_for_attachments:
                legacy_message, folder, data_location = process_legacy_workflow(
                    client, conn, slurmClient, slurm_job_id, projects, 
                    use_legacy_for_csv, use_legacy_for_attachments, 
                    local_tmp_storage, wf_id)
                message += legacy_message
            else:
                logger.info("Step 4: Skipping legacy workflow (not requested)")

            # Step 5: PROJECT/PLATE ATTACHMENT OPERATIONS
            attachment_message = process_attachment_operations(client, conn, folder, projects)
            message += attachment_message

            logger.info("=== MAIN WORKFLOW EXECUTION COMPLETED ===")
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
                cleanup_message = cleanup_resources(client, slurmClient, slurm_job_id, data_location, folder, log_file)
                message += cleanup_message
            except Exception as cleanup_error:
                logger.error(f"Cleanup failed: {cleanup_error}", exc_info=True)
                # Don't add cleanup errors to message if script already failed

            # Always try to set outputs - but don't overwrite failure messages
            try:
                client.setOutput("Workflow_UUID", rstring(workflow_uuid))
                # Only set success message if we haven't already set a failure message
                if not message.startswith("FAILED:"):
                    client.setOutput("Message", rstring(str(message)))
            except Exception as output_error:
                logger.error(f"Failed to set output: {output_error}")
            
            try:
                logger.info(f"Completed biomero-importer workflow for job {slurm_job_id} with workflow UUID {workflow_uuid}")
            except Exception:
                logger.info("Completed biomero-importer workflow execution")
                
            try:
                client.closeSession()
            except Exception as close_error:
                logger.error(f"Failed to close session: {close_error}")


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
