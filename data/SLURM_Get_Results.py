#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Original work Copyright (C) 2014 University of Dundee
#                                   & Open Microscopy Environment.
#                    All Rights Reserved.
# Modified work Copyright 2022 Torec Luik, Amsterdam UMC
# Use is subject to license terms supplied in LICENSE.txt
#
# Example OMERO.script to get results from a Slurm job.

import shutil
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
from omero_slurm_client import SlurmClient
import logging
import ezomero
# from aicsimageio import AICSImage
from tifffile import imread
import numpy as np

logger = logging.getLogger(__name__)

_SLURM_JOB_ID = "SLURM Job Id"
_COMPLETED_JOB = "Completed Job"
_OUTPUT_ATTACH_PROJECT = "Output - Attach as zip to project?"
_OUTPUT_ATTACH_PLATE = "Output - Attach as zip to plate?"
_OUTPUT_ATTACH_OG_IMAGES = "Output - Add as attachment to original images"
_OUTPUT_ATTACH_NEW_DATASET = "Output - Add as new images in NEW dataset"
_LOGFILE_PATH_PATTERN_GROUP = "DATA_PATH"
_LOGFILE_PATH_PATTERN = "Running [\w-]+? Job w\/ .+? \| .+? \| (?P<DATA_PATH>.+?) \|.*"
_OUTPUT_RENAME = "Rename imported files?"


def load_image(conn, image_id):
    """Load the Image object.

    Args:
        conn (_type_): Open OMERO connection
        image_id (String): ID of the image

    Returns:
        _type_: OMERO Image object
    """
    return conn.getObject('Image', image_id)


def getOriginalFilename(name):
    """Attempt to retrieve original filename.

    Assuming /../../Cells Apoptotic.png_merged_z01_t01.tiff,
    we want 'Cells Apoptotic.png' to be returned.

    Args:
        name (String): name of processed file
    """
    match = re.match(pattern=".+\/(.+\.[A-Za-z]+).+\.[tiff|png]", string=name)
    if match:
        name = match.group(1)

    return name


def saveImagesToOmeroAsAttachments(conn, folder, client):
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
             and (f.endswith('.tiff') or f.endswith('.png'))]
    # more_files = [f for f in os.listdir(f"{folder}/out") if os.path.isfile(f)
    #               and f.endswith('.tiff')]  # out folder
    # files += more_files
    print(f"Found the following files in {folder}: {all_files} && {files}")
    namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
    job_id = unwrap(client.getInput(_SLURM_JOB_ID)).strip()
    msg = ""
    for name in files:
        print(name)
        og_name = getOriginalFilename(name)
        print(og_name)
        images = conn.getObjects("Image", attributes={
                                 "name": f"{og_name}"})  # Can we get in 1 go?
        print(images)

        if images:
            try:
                # attach the masked image to the original image
                ext = os.path.splitext(name)[1][1:]
                
                # if unwrap(client.getInput(_OUTPUT_RENAME)):                
                #     renamed = rename_import_file(client, name, og_name)
                # TODO: API doesn't allow changing filename when uploading.
                # Maybe afterward? Update the originalFile name?
                
                file_ann = conn.createFileAnnfromLocalFile(
                    name, mimetype=f"image/{ext}",
                    ns=namespace, desc=f"Result from job {job_id} | analysis {folder}")
                print(f"Attaching {name} to image {og_name}")
                # image = load_image(conn, image_id)
                for image in images:
                    image.linkAnnotation(file_ann)

                print("Attaching FileAnnotation to Image: ", "File ID:",
                      file_ann.getId(), ",",
                      file_ann.getFile().getName(), "Size:",
                      file_ann.getFile().getSize())

                client.setOutput("File_Annotation", robject(file_ann._obj))
            except Exception as e:
                msg = f"Issue attaching file {name} to OMERO {og_name}: {e}"
                print(msg)
        else:
            msg = f"No images ({og_name}) found to attach {name} to: {images}"
            print(msg)

    print(files)
    message = f"\nTried attaching result images to OMERO original images!\n{msg}"

    return message


def to_5d(*arys):
    '''
    Implementation extended from Numpy `atleast_3d`.
    '''
    res = []
    for ary in arys:
        ary = np.asanyarray(ary)
        if ary.ndim == 0:
            result = ary.reshape(1, 1, 1, 1, 1)
        elif ary.ndim == 1:
            result = ary[np.newaxis, :, np.newaxis, np.newaxis, np.newaxis]
        elif ary.ndim == 2:
            result = ary[:, :, np.newaxis, np.newaxis, np.newaxis]
        elif ary.ndim == 3:
            result = ary[:, :, :, np.newaxis, np.newaxis]
        elif ary.ndim == 4:
            result = ary[:, :, :, :, np.newaxis]
        elif ary.ndim == 5:
            result = ary
        else:
            logger.warn("Randomly reducing import down to first 5d")
            result = np.resize(ary, (ary.shape[0:5]))
        res.append(result)
    if len(res) == 1:
        return res[0]
    else:
        return res


def saveImagesToOmeroAsDataset(conn, folder, client, dataset):
    """Save image from a (unzipped) folder to OMERO as dataset

    Args:
        conn (_type_): Connection to OMERO
        folder (String): Unzipped folder
        client : OMERO client to attach output

    Returns:
        String: Message to add to script output
    """
    all_files = glob.iglob(folder+'**/**', recursive=True)
    files = [f for f in all_files if os.path.isfile(f)
             and (f.endswith('.tiff') or f.endswith('.png'))]
    # more_files = [f for f in os.listdir(f"{folder}/out") if os.path.isfile(f)
    #               and f.endswith('.tiff')]  # out folder
    # files += more_files
    print(f"Found the following files in {folder}: {all_files} && {files}")
    # namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
    msg = ""
    job_id = unwrap(client.getInput(_SLURM_JOB_ID)).strip()
    images = None
    if files:
        for name in files:
            print(name)
            og_name = getOriginalFilename(name)
            print(og_name)
            images = list(conn.getObjects("Image", attributes={
                "name": f"{og_name}"}))  # Can we get in 1 go?
            print(images)
            try:
                # import the masked image for now
                img_data = imread(name)
                try:
                    source_image_id = images[0].getId()
                except IndexError:
                    source_image_id = None
                print(img_data.shape, dataset.id.val, source_image_id)
                
                img_data = to_5d(img_data)
                
                print("Reshaped:", img_data.shape)
                
                if unwrap(client.getInput(_OUTPUT_RENAME)):            
                    renamed = rename_import_file(client, name, og_name)
                else:
                    renamed = name
                img_id = ezomero.post_image(conn, img_data,
                                            renamed, 
                                            dataset_id=dataset.id.val,
                                            dim_order="xyzct",
                                            source_image_id=source_image_id,
                                            description=f"Result from job {job_id} | analysis {folder}")
                del img_data
                print(f"Uploaded {name} as {renamed} (from image {og_name}): {img_id}")
                # os.remove(name)
            except Exception as e:
                msg = f"Issue uploading file {name} to OMERO {og_name}: {e}"
                print(msg)

        if images:  # link dataset to OG project
            parent_dataset = images[0].getParent()
            parent_project = None
            if parent_dataset is not None:
                parent_project = parent_dataset.getParent()
            if parent_project and parent_project.canLink():
                # and put it in the current project
                print(parent_dataset, parent_project, parent_project.getId(), dataset.id.val)
                project_link = omero.model.ProjectDatasetLinkI()
                project_link.parent = omero.model.ProjectI(
                    parent_project.getId(), False)
                project_link.child = omero.model.DatasetI(
                    dataset.id.val, False)
                update_service = conn.getUpdateService()
                update_service.saveAndReturnObject(project_link)

        print(files)
        message = f"\nTried importing images to {dataset.id.val} {dataset.name.val}!\n{msg}"
    else:
        message = f"\nNo files found to upload in {folder}"

    return message


def rename_import_file(client, name, og_name):
    pattern = unwrap(client.getInput("Rename"))
    print(f"Overwriting name {name} with pattern: {pattern}")
    ext = os.path.splitext(name)[1][1:]  # new extension
    original_file = os.path.splitext(og_name)[0]  # original base 
    name = pattern.format(original_file=original_file, ext=ext)
    print(f"New name: {name} ({original_file}, {ext})")
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


def upload_contents_to_omero(client, conn, message, folder):
    """Upload contents of folder to OMERO

    Args:
        client (_type_): OMERO client
        conn (_type_): Open connection to OMERO
        message (String): Script output
        folder (String): Path to folder with content
    """
    try:
        if unwrap(client.getInput(_OUTPUT_ATTACH_OG_IMAGES)):
            # upload and link individual images
            msg = saveImagesToOmeroAsAttachments(conn=conn, folder=folder,
                                                 client=client)
            message += msg
        if unwrap(client.getInput(_OUTPUT_ATTACH_NEW_DATASET)):
            # create a new dataset for new images
            dataset_name = unwrap(client.getInput("New Dataset"))
            dataset = omero.model.DatasetI()
            dataset.name = rstring(dataset_name)
            desc = "Images in this Dataset are label masks of job:\n"\
                "  Id: %s" % (unwrap(client.getInput(_SLURM_JOB_ID)))
            dataset.description = rstring(desc)
            update_service = conn.getUpdateService()
            dataset = update_service.saveAndReturnObject(dataset)

            msg = saveImagesToOmeroAsDataset(conn=conn, 
                                             folder=folder, 
                                             client=client,
                                             dataset=dataset)
            message += msg

    except Exception as e:
        message += f" Failed to upload images to OMERO: {e}"

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
        print(f"Unzipped {folder} on the server")
    except Exception as e:
        message += f" Unzip failed: {e}"

    return message


def upload_log_to_omero(client, conn, message, slurm_job_id, projects, file):
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
        print(f"Uploading {file} and attaching to {projects}")
        mimetype = "text/plain"
        namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
        description = f"Log from SLURM job {slurm_job_id}"
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
        print(message)

    return message


def upload_zip_to_omero(client, conn, message, slurm_job_id, projects, folder):
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
        print(f"Uploading {folder}.zip and attaching to {projects}")
        mimetype = "application/zip"
        namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
        description = f"Results from SLURM job {slurm_job_id}"
        zip_annotation = conn.createFileAnnfromLocalFile(
            f"{folder}.zip", mimetype=mimetype,
            ns=namespace, desc=description)

        client.setOutput("File_Annotation", robject(zip_annotation._obj))

        for project in projects:
            project.linkAnnotation(zip_annotation)  # link it to project.
        message += f"Attached zip {folder} to {projects}"
    except Exception as e:
        message += f" Uploading zip failed: {e}"
        print(message)

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
                print(f"logline: {line}")
            except UnicodeEncodeError as e:
                logger.error(f"Unicode error: {e}")
                line = line.encode(
                    'ascii', 'ignore').decode('ascii')
                print(f"logline: {line}")
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
     
        client = scripts.client(
            'Slurm Get Results',
            '''Retrieve the results from your completed SLURM job.

            Attach files to provided project.
            ''',
            scripts.Bool(_COMPLETED_JOB, optional=False, grouping="01",
                         default=True),
            scripts.String(_SLURM_JOB_ID, optional=False, grouping="01.1",
                           values=_oldjobs),
            scripts.Bool(_OUTPUT_ATTACH_PROJECT,
                         optional=False,
                         grouping="03",
                         description="Attach all results in zip to a project",
                         default=True),
            scripts.List("Project", optional=True, grouping="03.1",
                         description="Project to attach workflow results to",
                         values=_projects),
            scripts.Bool(_OUTPUT_ATTACH_OG_IMAGES,
                         optional=False,
                         grouping="05",
                         description="Attach all results to original images as attachments",
                         default=True),
            scripts.Bool(_OUTPUT_ATTACH_PLATE,
                         optional=False,
                         grouping="04",
                         description="Attach all results in zip to a plate",
                         default=False),
            scripts.List("Plate", optional=True, grouping="04.1",
                         description="Plate to attach workflow results to",
                         values=_plates),
            scripts.Bool(_OUTPUT_ATTACH_NEW_DATASET,
                         optional=False,
                         grouping="06",
                         description="Import all result as a new dataset",
                         default=False),
            scripts.String("New Dataset", optional=True,
                           grouping="06.1",
                           description="Name for the new dataset w/ results",
                           default="My_Results"),
            scripts.Bool(_OUTPUT_RENAME,
                         optional=True,
                         grouping="06.2",
                         description="Rename all imported files as below. You can use variables {original_file} and {ext}. E.g. {original_file}NucleiLabels.{ext}",
                         default=False),
            scripts.String("Rename", optional=True,
                           grouping="06.3",
                           description="A new name for the imported images.",
                           default="{original_file}NucleiLabels.{ext}"),
            # scripts.Bool("Output - Add as new images in same dataset",
            #  optional=False,
            #  grouping="07",
            #  description="Add all images to the original dataset",
            #  default=False),

            namespaces=[omero.constants.namespaces.NSDYNAMIC],
        )

        try:
            scriptParams = client.getInputs(unwrap=True)
            conn = BlitzGateway(client_obj=client)

            message = ""
            print(f"Request: {scriptParams}\n")

            # Job id
            slurm_job_id = unwrap(client.getInput(_SLURM_JOB_ID)).strip()

            # Ask job State
            if unwrap(client.getInput(_COMPLETED_JOB)):
                _, result = slurmClient.check_job_status([slurm_job_id])
                print(result.stdout)
                message += f"\n{result.stdout}"

            # Pull project from Omero
            projects = []  # note, can also be plate now
            if unwrap(client.getInput(_OUTPUT_ATTACH_PROJECT)):
                project_ids = unwrap(client.getInput("Project"))
                print(project_ids)
                projects = [conn.getObject("Project", p.split(":")[0])
                            for p in project_ids]
            if unwrap(client.getInput(_OUTPUT_ATTACH_PLATE)):
                plate_ids = unwrap(client.getInput("Plate"))
                print(plate_ids)
                projects = [conn.getObject("Plate", p.split(":")[0])
                            for p in plate_ids]

            # Job log
            if unwrap(client.getInput(_COMPLETED_JOB)):
                
                try:
                    # Copy file to server
                    tup = slurmClient.get_logfile_from_slurm(
                        slurm_job_id)
                    (local_tmp_storage, log_file, get_result) = tup
                    message += "\nSuccesfully copied logfile."
                    print(message)
                    print(get_result.__dict__)

                    # Upload logfile to Omero as Original File
                    message = upload_log_to_omero(
                        client, conn, message,
                        slurm_job_id, projects, log_file)

                    # Read file for data location
                    data_location = slurmClient.extract_data_location_from_log(
                        slurm_job_id)
                    print(f"Extracted {data_location}")

                    # zip and scp data location
                    if data_location:
                        filename = f"{slurm_job_id}_out"

                        zip_result = slurmClient.zip_data_on_slurm_server(
                            data_location, filename)
                        if not zip_result.ok:
                            message += "\nFailed to zip data on Slurm."
                            print(message, zip_result.stderr)
                        else:
                            message += "\nSuccesfully zipped data on Slurm."
                            print(message, zip_result.stdout)

                            copy_result = slurmClient.copy_zip_locally(
                                local_tmp_storage, filename)

                            message += "\nSuccesfully copied zip."
                            print(message, copy_result)

                            folder = f"{local_tmp_storage}/{filename}"

                            if (unwrap(client.getInput(_OUTPUT_ATTACH_PROJECT)) or
                                    unwrap(client.getInput(_OUTPUT_ATTACH_PLATE))):
                                message = upload_zip_to_omero(
                                    client, conn, message,
                                    slurm_job_id, projects, folder)

                            message = unzip_zip_locally(message, folder)

                            message = upload_contents_to_omero(
                                client, conn, message, folder)

                            message = cleanup_tmp_files_locally(
                                message, folder, log_file)

                            clean_result = slurmClient.cleanup_tmp_files(
                                slurm_job_id,
                                filename,
                                data_location)
                            message += "\nSuccesfully cleaned up tmp files"
                            print(message, clean_result)
                except Exception as e:
                    message += f"\nEncountered error: {e}"

            client.setOutput("Message", rstring(str(message)))
        finally:
            client.closeSession()


if __name__ == '__main__':
    runScript()
