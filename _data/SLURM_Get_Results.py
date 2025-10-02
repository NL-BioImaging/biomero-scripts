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

Import Options:
- Attach results to original images as attachments
- Create new datasets with custom naming
- Import into parent dataset/plate structure
- Convert CSV files to OMERO tables
- Rename imported images with custom patterns

File Support:
- Images: TIFF, OME-TIFF, PNG formats
- Tables: CSV files converted to OMERO.tables
- Metadata: Preserved through import process

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

# Version constant for easy version management
VERSION = "2.0.0-alpha.8"

OBJECT_TYPES = (
    'Plate',
    'Screen',
    'Dataset',
    'Project',
    'Image',
)

logger = logging.getLogger(__name__)

_LOGFILE_PATH_PATTERN_GROUP = "DATA_PATH"
_LOGFILE_PATH_PATTERN = "Running [\w-]+? Job w\/ .+? \| .+? \| (?P<DATA_PATH>.+?) \|.*"
SUPPORTED_IMAGE_EXTENSIONS = ['.tif', '.tiff', '.png', '.ome.tif']
SUPPORTED_TABLE_EXTENSIONS = ['.csv']


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
                            logger.warning(f"Could not rename file {name}: {e}")

                # Create annotation with renamed file
                ext = os.path.splitext(name)[1][1:]
                file_ann = conn.createFileAnnfromLocalFile(
                    name, mimetype=f"image/{ext}",
                    ns=namespace, desc=f"Result from job {job_id}" + (f" (Workflow {wf_id})" if wf_id else "") + f" | analysis {folder}")
                
                # Restore original filename after annotation is created
                if name != original_name:
                    try:
                        os.rename(name, original_name)
                        logger.debug(f"Restored original filename: {original_name}")
                    except OSError as e:
                        logger.warning(f"Could not restore original filename {original_name}: {e}")
                
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
            logger.debug(f"Adding metadata: {workflow_annotation_dict}")
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
                logger.debug(f"Adding metadata: {task_annotation_dict}")
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
    all_files = glob.iglob(folder+'**/**', recursive=True)
    files = [f for f in all_files if os.path.isfile(f)
             and any(f.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS)]

    # more_files = [f for f in os.listdir(f"{folder}/out") if os.path.isfile(f)
    #               and f.endswith('.tiff')]  # out folder
    # files += more_files
    logger.info(f"Found the following files in {folder}: {files}")
    # namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
    msg = ""
    job_id = unwrap(client.getInput(
        constants.results.OUTPUT_SLURM_JOB_ID)).strip()
    images = None
    if files:
        for name in files:
            logger.debug(name)
            og_name = getOriginalFilename(name)
            logger.debug(og_name)
            images = list(conn.getObjects("Image", attributes={
                "name": f"{og_name}"}))  # Can we get in 1 go?
            logger.debug(images)
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
    pattern = unwrap(client.getInput("Rename"))
    logger.debug(f"Overwriting name {name} with pattern: {pattern}")
    ext = os.path.splitext(name)[1][1:]  # new extension
    original_file = os.path.splitext(og_name)[0]  # original base
    name = pattern.format(original_file=original_file, ext=ext)
    logger.info(f"New name: {name} ({original_file}, {ext})")
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
            dataset_name = unwrap(client.getInput("New Dataset"))

            create_new_dataset = unwrap(client.getInput("Allow duplicate?"))
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


def extract_data_location_from_log(export_file):
    """Read SLURM job logfile to find location of the data

    Args:
        export_file (String): Path to the logfile

    Returns:
        String: Data location according to the log
    """
    # TODO move to SlurmClient? makes more sense to read this remotely? Can we?
    with open(export_file, 'r', encoding='utf-8') as log:
        data_location = None
        for line in log:
            try:
                logger.debug(f"logline: {line}")
            except UnicodeEncodeError as e:
                logger.error(f"Unicode error: {e}")
                line = line.encode(
                    'ascii', 'ignore').decode('ascii')
                logger.debug(f"logline: {line}")
            match = re.match(pattern=_LOGFILE_PATH_PATTERN, string=line)
            if match:
                data_location = match.group(_LOGFILE_PATH_PATTERN_GROUP)
                break
    return data_location


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
            'Slurm Get Results',
            '''Retrieve the results from your completed SLURM job.

            Attach files to provided project.
            ''',
            scripts.Bool(constants.results.OUTPUT_COMPLETED_JOB,
                         optional=False, grouping="01",
                         default=True),
            scripts.String(constants.results.OUTPUT_SLURM_JOB_ID,
                           optional=False, grouping="01.1",
                           values=_oldjobs),
            scripts.Bool(constants.results.OUTPUT_ATTACH_PROJECT,
                         optional=False,
                         grouping="03",
                         description="Attach all results in zip to a project",
                         default=True),
            scripts.List(constants.results.OUTPUT_ATTACH_PROJECT_ID,
                         optional=True, grouping="03.1",
                         description="Project to attach workflow results to",
                         values=_projects),
            scripts.Bool(constants.results.OUTPUT_ATTACH_OG_IMAGES,
                         optional=False,
                         grouping="05",
                         description="Attach all results to original images as attachments",
                         default=True),
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
                         description="Import all result as a new dataset",
                         default=False),
            scripts.String(constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME,
                           optional=True,
                           grouping="06.1",
                           description="Name for the new dataset w/ results",
                           default="My_Results"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE,
                         optional=True,
                         grouping="06.2",
                         description="If there is already a dataset with this name, still create new one? (True) or add to it? (False) ",
                         default=True),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME,
                         optional=True,
                         grouping="06.3",
                         description="Rename all imported files as below. You can use variables {original_file} and {ext}. E.g. {original_file}NucleiLabels.{ext}",
                         default=False),
            scripts.String(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME_NAME,
                           optional=True,
                           grouping="06.4",
                           description="A new name for the imported images.",
                           default="{original_file}NucleiLabels.{ext}"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_TABLE,
                         optional=False,
                         grouping="07",
                         description="Add all csv files as OMERO.tables to the chosen dataset",
                         default=False),
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
            logger.info(f"Get Results: {scriptParams}\n")

            # Job id
            slurm_job_id = unwrap(client.getInput(
                constants.results.OUTPUT_SLURM_JOB_ID)).strip()

            # Get workflow ID if available
            wf_id = None
            if slurmClient.track_workflows:
                try:
                    task_id = slurmClient.jobAccounting.get_task_id(
                        slurm_job_id)
                    task = slurmClient.workflowTracker.repository.get(task_id)
                    wf_id = task.workflow_id
                except Exception as e:
                    logger.error(
                        f"Failed to get workflow ID from job {slurm_job_id}. Error: {str(e)}")

            # Ask job State
            if unwrap(client.getInput(constants.results.OUTPUT_COMPLETED_JOB)):
                _, result = slurmClient.check_job_status([slurm_job_id])
                logger.debug(result.stdout)
                message += f"\n{result.stdout}"

            # Pull project from Omero
            projects = []  # note, can also be plate now
            if unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PROJECT)):
                project_ids = unwrap(client.getInput("Project"))
                logger.debug(project_ids)
                projects = [conn.getObject("Project", p.split(":")[0])
                            for p in project_ids]
            if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)):
                plate_ids = unwrap(client.getInput("Plate"))
                logger.debug(plate_ids)
                projects = [conn.getObject("Plate", p.split(":")[0])
                            for p in plate_ids]

            # Job log
            if unwrap(client.getInput(constants.results.OUTPUT_COMPLETED_JOB)):
                folder = None
                log_file = None
                try:
                    # Copy file to server
                    tup = slurmClient.get_logfile_from_slurm(
                        slurm_job_id)
                    (local_tmp_storage, log_file, get_result) = tup
                    message += "\nSuccesfully copied logfile."
                    logger.info(message)
                    logger.debug(get_result.__dict__)

                    # Upload logfile to Omero as Original File
                    message = upload_log_to_omero(
                        client, conn, message,
                        slurm_job_id, projects, log_file, wf_id=wf_id)

                    # Read file for data location
                    data_location = slurmClient.extract_data_location_from_log(
                        slurm_job_id)
                    logger.debug(f"Extracted {data_location}")

                    # zip and scp data location
                    if data_location:
                        filename = f"{slurm_job_id}_out"

                        zip_result = slurmClient.zip_data_on_slurm_server(
                            data_location, filename)
                        if not zip_result.ok:
                            message += "\nFailed to zip data on Slurm."
                            logger.warning(f"{message}, {zip_result.stderr}")
                        else:
                            message += "\nSuccesfully zipped data on Slurm."
                            logger.info(f"{message}")
                            logger.debug(f"{zip_result.stdout}")

                            copy_result = slurmClient.copy_zip_locally(
                                local_tmp_storage, filename)

                            message += "\nSuccesfully copied zip."
                            logger.info(f"{message}")
                            logger.debug(f"{copy_result}")

                            folder = f"{local_tmp_storage}/{filename}"

                            if (unwrap(client.getInput(
                                constants.results.OUTPUT_ATTACH_PROJECT)) or
                                    unwrap(client.getInput(
                                        constants.results.OUTPUT_ATTACH_PLATE))):
                                message = upload_zip_to_omero(
                                    client, conn, message,
                                    slurm_job_id, projects, folder, wf_id=wf_id)

                            message = unzip_zip_locally(message, folder)

                            message = upload_contents_to_omero(
                                client, conn, slurmClient, message, folder, wf_id=wf_id)

                            # clean_result = slurmClient.cleanup_tmp_files(
                            #     slurm_job_id,
                            #     filename,
                            #     data_location)
                            message += "\nSuccesfully not cleaned up tmp files"
                            logger.info(message)
                            # logger.debug(clean_result)
                except Exception as e:
                    message += f"\nEncountered error: {e}"
                finally:
                    if folder or log_file:
                        # cleanup_tmp_files_locally(message, folder, log_file)
                        print("woops, cleanup not implemented yet")

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
