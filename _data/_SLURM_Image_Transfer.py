#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 T T Luik
# Copyright (C) 2006-2014 University of Dundee. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
BIOMERO SLURM Image Transfer Script

This script provides comprehensive data export from OMERO to SLURM clusters
with support for multiple data formats and automatic cleanup of temporary
artifacts.

Key Features:
- Multi-format export: TIFF, OME-TIFF, ZARR
- Support for Images, Datasets, and Plates
- Automatic data transfer to SLURM cluster
- Intelligent compression and packaging
- Temporary file annotation cleanup after successful transfer
- Configurable rendering options (channels, Z-projection, time points)
- Robust error handling and logging

Data Export Process:
1. Render and save image data in selected format
2. Package data into zip archive (except single OME-TIFF files)
3. Transfer data to SLURM cluster via SSH
4. Unpack data on SLURM for processing
5. Create temporary file annotation in OMERO
6. Clean up annotation after successful transfer

Supported Formats:
- TIFF: Rendered image planes with configurable options
- OME-TIFF: Original pixel data preservation
- ZARR: Native OME-ZARR format using omero-cli-zarr

Authors: Torec Luik, William Moore, OME Team
Institutions: Amsterdam UMC, University of Dundee
License: GPL v2+ (see LICENSE.txt)
"""

import shutil
import subprocess
import shlex
import omero.scripts as scripts
from omero.gateway import BlitzGateway
import omero.util.script_utils as script_utils
import omero
from omero.rtypes import rstring, rlong, robject
from omero.constants.namespaces import NSCREATED, NSOMETIFF
import os
from pathlib import Path
import glob
import zipfile
from datetime import datetime
try:
    from PIL import Image  # see ticket:2597
except ImportError:
    import Image
from biomero import SlurmClient, constants
import logging
import sys

logger = logging.getLogger(__name__)

# Version constant for easy version management
VERSION = "2.3.0"

# keep track of log strings.
log_strings = []


def log(text):
    """Add text to log strings list for later compilation to file.
    
    Args:
        text: Text to add to log, handles unicode encoding.
    """
    # Handle unicode
    try:
        text = text.encode('utf8')
    except UnicodeEncodeError:
        pass
    log_strings.append(str(text))
    logger.debug(str(text))


def compress(target, base):
    """Create a ZIP archive recursively from a given base directory.
    
    Args:
        target (str): Name of the zip file to write (e.g., "folder.zip").
        base (str): Name of folder to zip up (e.g., "folder").
    """
    base_name, ext = target.split(".")
    shutil.make_archive(base_name, ext, base)


def save_plane(image, format, c_name, z_range, project_z, t=0,
               channel=None,
               greyscale=False, zoom_percent=None, folder_name=None):
    """Render and save an image plane to disk.
    
    Args:
        image: OMERO image wrapper to render.
        format (str): Image format to save as (PNG, TIFF, or JPG).
        c_name (str): Channel name for filename.
        z_range (tuple): Either (zIndex,) or (zStart, zStop) for projection.
        project_z (bool): Whether to use Z projection.
        t (int): Time point index. Defaults to 0.
        channel (int, optional): Active channel index. If None, uses current
            rendering settings.
        greyscale (bool): If True, render all visible channels as greyscale.
            Defaults to False.
        zoom_percent (int, optional): Resize image by this percentage.
        folder_name (str, optional): Directory to save the plane in.
    """

    original_name = image.getName()
    log("")
    log("save_plane..")
    log("channel: %s" % c_name)
    log("z: %s" % z_range)
    log("t: %s" % t)

    # if channel == None: use current rendering settings
    if channel is not None:
        image.setActiveChannels([channel+1])    # use 1-based Channel indices
        if greyscale:
            image.setGreyscaleRenderingModel()
        else:
            image.setColorRenderingModel()
    if project_z:
        # imageWrapper only supports projection of full Z range (can't
        # specify)
        image.setProjection('intmax')

    # All Z and T indices in this script are 1-based, but this method uses
    # 0-based.
    plane = image.renderImage(z_range[0]-1, t-1)
    if zoom_percent:
        w, h = plane.size
        fraction = (float(zoom_percent) / 100)
        plane = plane.resize((int(w * fraction), int(h * fraction)),
                             Image.ANTIALIAS)

    if format == "PNG":
        img_name = make_image_name(
            original_name, c_name, z_range, t, "png", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name, "PNG")
    elif format == constants.transfer.FORMAT_TIFF:
        img_name = make_image_name(
            original_name, c_name, z_range, t, "tiff", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name, constants.transfer.FORMAT_TIFF)
    else:
        img_name = make_image_name(
            original_name, c_name, z_range, t, "jpg", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name)


def make_image_name(original_name, c_name, z_range, t, extension, folder_name):
    """Generate filename for saved image with standardized naming convention.
    
    Creates descriptive filenames incorporating image metadata.
    Example: "imported/myImage.dv" → "myImage_DAPI_z13_t01.png"
    
    Args:
        original_name (str): Original image name from OMERO.
        c_name (str): Channel name.
        z_range (tuple): Z-slice range (single index or start-stop range).
        t (int): Time point index.
        extension (str): File extension for output format.
        folder_name (str, optional): Target folder path.
    
    Returns:
        str: Generated filename with full path if folder specified.
    """
    name = os.path.basename(original_name)
    # name = name.rsplit(".",1)[0]  # remove extension
    if len(z_range) == 2:
        z = "%02d-%02d" % (z_range[0], z_range[1])
    else:
        z = "%02d" % z_range[0]
    img_name = "%s_%s_z%s_t%02d.%s" % (name, c_name, z, t, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    name = img_name[:-(len(extension)+1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (name, i, extension)
        i += 1
    return img_name


def save_as_ome_tiff(conn, image, folder_name=None):
    """Save image as OME-TIFF preserving original pixel data.
    
    Args:
        conn: OMERO BlitzGateway connection.
        image: OMERO image wrapper to export.
        folder_name (str, optional): Target folder for the file.
    """

    extension = "ome.tif"
    name = os.path.basename(image.getName())
    img_name = "%s.%s" % (name, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    path_name = img_name[:-(len(extension)+1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (path_name, i, extension)
        i += 1

    log("  Saving file as: %s" % img_name)
    file_size, block_gen = image.exportOmeTiff(bufsize=65536)
    with open(str(img_name), "wb") as f:
        for piece in block_gen:
            f.write(piece)


def save_plate_as_zarr(conn, suuid, plate, folder_name=None, client=None):
    """Export plate as ZARR format using omero-cli-zarr.
    
    Args:
        conn: OMERO BlitzGateway connection.
        suuid: Session UUID for authentication.
        plate: OMERO plate wrapper to export.
        folder_name (str, optional): Target folder for export.
        client: OMERO client (unused, for compatibility).
    """
    # TODO use raw converter directly
    # (1) find out the plate's file
    # (2) (a) if not zarr: subprocess raw on that file
    # (2) (b) if zarr: copy/scp directly
    save_as_zarr(conn, suuid, plate, folder_name,
                 constants.transfer.DATA_TYPE_PLATE)


def save_image_as_zarr(conn, suuid, image, folder_name=None):
    """Export image as ZARR format using omero-cli-zarr.
    
    Args:
        conn: OMERO BlitzGateway connection.
        suuid: Session UUID for authentication.
        image: OMERO image wrapper to export.
        folder_name (str, optional): Target folder for export.
    """
    save_as_zarr(conn, suuid, image, folder_name,
                 constants.transfer.DATA_TYPE_IMAGE)
    

def save_as_zarr(conn, suuid, object, folder_name=None, data_type=None):
    """Export OMERO object as ZARR using subprocess call to omero-cli-zarr.
    
    Args:
        conn: OMERO BlitzGateway connection.
        suuid: Session UUID for OMERO authentication.
        object: OMERO object wrapper (Image or Plate) to export.
        folder_name (str, optional): Target folder for export.
        data_type (str, optional): Type of OMERO object for appropriate export.
    
    Raises:
        ValueError: If unsupported data_type is provided.
    """
    extension = "zarr"
    name = os.path.basename(object.getName())
    img_name = "%s.%s" % (name, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    path_name = img_name[:-(len(extension)+1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (path_name, i, extension)
        i += 1

    log("  Saving file as: %s" % img_name)

    curr_dir = os.getcwd()
    exp_dir = os.path.join(curr_dir, folder_name)

    # TODO: Check if import omero-cli-zarr and #export(...) works better than subprocess?
    # https://github.com/ome/omero-cli-zarr/blob/a35ade1d8177585b3e21ef860fd645a8d6eb5aea/src/omero_zarr/cli.py#L327C9-L327C15

    # command = f'omero zarr -s "$CONFIG_omero_master_host" -k "{suuid}" export --bf Image:{image.getId()}'
    cmd1 = 'export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")'
    if data_type == constants.transfer.DATA_TYPE_PLATE:
        command = f'omero zarr -s "{conn.host}" -k "{suuid}" --output "{exp_dir}" export Plate:{object.getId()}'
    elif data_type == constants.transfer.DATA_TYPE_IMAGE:
        command = f'omero zarr -s "{conn.host}" -k "{suuid}" --output "{exp_dir}" export Image:{object.getId()}'
    else:
        raise ValueError(f"No OMERO ZARR command known for data_type: {data_type}")
    log(f"OMERO ZARR command for {data_type}: {command}")
    cmd = cmd1 + " && " + command
    logger.debug(cmd)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True
    )
    stdout, stderr = process.communicate()
    if stderr:
        logger.warning(stderr.decode("utf-8"))
    if process.returncode == 0:
        log(f"OME ZARR CLI: {stdout}")
        logger.debug(img_name)
        
        # Check for both .ome.zarr and .zarr extensions for compatibility
        source_ome_zarr = f"{exp_dir}/{object.getId()}.ome.zarr"
        source_zarr = f"{exp_dir}/{object.getId()}.zarr"

        if os.path.exists(source_ome_zarr):
            os.rename(source_ome_zarr, img_name)
            log(f"Renamed .ome.zarr file: {source_ome_zarr} -> {img_name}")
        elif os.path.exists(source_zarr):
            os.rename(source_zarr, img_name)
            log(f"Renamed .zarr file: {source_zarr} -> {img_name}")
        else:
            error_msg = (f"Neither {source_ome_zarr} nor {source_zarr} "
                         f"found after ZARR export")
            raise FileNotFoundError(error_msg)
    else:
        error_msg = f"ZARR export failed with return code {process.returncode}: {stderr.decode('utf-8') if stderr else 'Unknown error'}"
        logger.error(f"Critical error: {error_msg}")
        raise Exception(error_msg)
    return  # shortcut


def save_planes_for_image(suuid, image, size_c, split_cs, merged_cs,
                          channel_names=None, z_range=None, t_range=None,
                          greyscale=False, zoom_percent=None, project_z=False,
                          format="PNG", folder_name=None):
    """
    Saves all the required planes for a single image, either as individual
    planes or projection.

    @param renderingEngine:     Rendering Engine, NOT initialised.
    @param queryService:        OMERO query service
    @param imageId:             Image ID
    @param zRange:              Tuple: (zStart, zStop). If None, use default
                                Zindex
    @param tRange:              Tuple: (tStart, tStop). If None, use default
                                Tindex
    @param greyscale:           If true, all visible channels will be
                                greyscale
    @param zoomPercent:         Resize image by this percent if specified.
    @param projectZ:            If true, project over Z range.
    """

    channels = []
    if merged_cs:
        # render merged first with current rendering settings
        channels.append(None)
    if split_cs:
        for i in range(size_c):
            channels.append(i)

    # set up rendering engine with the pixels
    """
    renderingEngine.lookupPixels(pixelsId)
    if not renderingEngine.lookupRenderingDef(pixelsId):
        renderingEngine.resetDefaults()
    if not renderingEngine.lookupRenderingDef(pixelsId):
        raise "Failed to lookup Rendering Def"
    renderingEngine.load()
    """

    if t_range is None:
        # use 1-based indices throughout script
        t_indexes = [image.getDefaultT()+1]
    else:
        if len(t_range) > 1:
            t_indexes = range(t_range[0], t_range[1])
        else:
            t_indexes = [t_range[0]]

    c_name = 'merged'
    for c in channels:
        if c is not None:
            g_scale = greyscale
            if c < len(channel_names):
                c_name = channel_names[c].replace(" ", "_")
            else:
                c_name = "c%02d" % c
        else:
            # if we're rendering 'merged' image - don't want grey!
            g_scale = False
        for t in t_indexes:
            if z_range is None:
                default_z = image.getDefaultZ()+1
                save_plane(image, format, c_name, (default_z,), project_z, t,
                           c, g_scale, zoom_percent, folder_name)
            elif project_z:
                save_plane(image, format, c_name, z_range, project_z, t, c,
                           g_scale, zoom_percent, folder_name)
            else:
                if len(z_range) > 1:
                    for z in range(z_range[0], z_range[1]):
                        save_plane(image, format, c_name, (z,), project_z, t,
                                   c, g_scale, zoom_percent, folder_name)
                else:
                    save_plane(image, format, c_name, z_range, project_z, t,
                               c, g_scale, zoom_percent, folder_name)


def batch_image_export(conn, script_params, slurmClient: SlurmClient,
                       suuid: str, client):
    """
    Export selected OMERO data to SLURM cluster with automatic cleanup.
    
    This function handles the complete export pipeline:
    1. Processes selected images, datasets, or plates from OMERO
    2. Renders and saves data in specified format (TIFF/OME-TIFF/ZARR)
    3. Packages data for transfer (zip compression when needed)
    4. Transfers data to SLURM cluster via SSH
    5. Unpacks data on SLURM for processing
    6. Creates temporary file annotation in OMERO
    7. Automatically cleans up annotation after successful transfer
    
    Args:
        conn: OMERO BlitzGateway connection
        script_params: Dictionary of script parameters from user input
        slurmClient: Active SLURM client for data transfer
        suuid: OMERO session UUID for authentication
        client: OMERO script client for creating annotations
        
    Returns:
        tuple: (file_annotation, message) where file_annotation is None
               if successfully cleaned up, or the annotation object if
               cleanup failed or transfer was unsuccessful
               
    Raises:
        Exception: Various exceptions during export process, all logged
    """
    # for params with default values, we can get the value directly
    split_cs = script_params[constants.transfer.CHANNELS]
    merged_cs = script_params[constants.transfer.MERGED]
    greyscale = script_params[constants.transfer.CHANNELS_GREY]
    data_type = script_params[constants.transfer.DATA_TYPE]
    folder_name = script_params[constants.transfer.FOLDER]
    folder_name = os.path.basename(folder_name)
    format = script_params[constants.transfer.FORMAT]
    project_z = constants.transfer.Z in script_params and \
        script_params[constants.transfer.Z] == constants.transfer.Z_MAXPROJ

    if (not split_cs) and (not merged_cs):
        log("Not chosen to save Individual Channels OR Merged Image")
        return

    # check if we have these params
    channel_names = []
    if constants.transfer.CHANNELS_NAMES in script_params:
        channel_names = script_params[constants.transfer.CHANNELS_NAMES]
    zoom_percent = None
    if constants.transfer.ZOOM in script_params and script_params[constants.transfer.ZOOM] != constants.transfer.ZOOM_100:
        zoom_percent = int(script_params[constants.transfer.ZOOM][:-1])

    # functions used below for each imaage.
    def get_z_range(size_z, script_params):
        z_range = None
        if constants.transfer.Z in script_params:
            z_choice = script_params[constants.transfer.Z]
            # NB: all Z indices in this script are 1-based
            if z_choice == constants.transfer.Z_ALL:
                z_range = (1, size_z+1)
            elif constants.transfer.Z_IDX in script_params:
                z_index = script_params[constants.transfer.Z_IDX]
                z_index = min(z_index, size_z)
                z_range = (z_index,)
            elif constants.transfer.Z_IDX_START in script_params and \
                    constants.transfer.Z_IDX_END in script_params:
                start = script_params[constants.transfer.Z_IDX_START]
                start = min(start, size_z)
                end = script_params[constants.transfer.Z_IDX_END]
                end = min(end, size_z)
                # in case user got z_start and z_end mixed up
                z_start = min(start, end)
                z_end = max(start, end)
                if z_start == z_end:
                    z_range = (z_start,)
                else:
                    z_range = (z_start, z_end+1)
        return z_range

    def get_t_range(size_t, script_params):
        t_range = None
        if constants.transfer.T in script_params:
            t_choice = script_params[constants.transfer.T]
            # NB: all T indices in this script are 1-based
            if t_choice == constants.transfer.T_ALL:
                t_range = (1, size_t+1)
            elif constants.transfer.T_IDX in script_params:
                t_index = script_params[constants.transfer.T_IDX]
                t_index = min(t_index, size_t)
                t_range = (t_index,)
            elif constants.transfer.T_IDX_START in script_params and \
                    constants.transfer.T_IDX_END in script_params:
                start = script_params[constants.transfer.T_IDX_START]
                start = min(start, size_t)
                end = script_params[constants.transfer.T_IDX_END]
                end = min(end, size_t)
                # in case user got t_start and t_end mixed up
                t_start = min(start, end)
                t_end = max(start, end)
                if t_start == t_end:
                    t_range = (t_start,)
                else:
                    t_range = (t_start, t_end+1)
        return t_range

    # Get the images or datasets
    message = ""
    objects, log_message = script_utils.get_objects(conn, script_params)
    message += log_message
    if not objects:
        return None, message

    # Attach figure to the first image
    parent = objects[0]

    if data_type == constants.transfer.DATA_TYPE_DATASET:
        images = []
        for ds in objects:
            images.extend(list(ds.listChildren()))
        if not images:
            message += "No image found in dataset(s)"
            return None, message
    elif data_type == constants.transfer.DATA_TYPE_PLATE:
        if format == constants.transfer.FORMAT_ZARR:
            log("Processing %s Plates to ZARR, not individual images." % len(objects))         
            images = []  # skip the rest of the processing below
            wells = []
        else:
            images = []
            wells = []
            for plate in objects:
                wells.extend(list(plate.listChildren()))
            for well in wells:
                nr_samples = well.countWellSample()
                for index in range(0, nr_samples):
                    image = well.getImage(index)
                    images.append(image)
            if not images:
                message += "No image found in plate(s)"
                return None, message
    else:
        images = objects

    log("Processing %s images" % len(images))

    # somewhere to put images
    curr_dir = os.getcwd()
    exp_dir = os.path.join(curr_dir, folder_name)
    try:
        os.mkdir(exp_dir)
    except OSError:
        pass
    # max size (default 12kx12k)
    size = conn.getDownloadAsMaxSizeSetting()
    size = int(size)

    ids = []
    # do the saving to disk
    
    if format == constants.transfer.FORMAT_ZARR and data_type == constants.transfer.DATA_TYPE_PLATE:
        for plate in objects:
            log("Processing plate: ID %s: %s" % (plate.id, plate.getName()))
            save_plate_as_zarr(conn, suuid, plate, folder_name, client)
            write_logfile(exp_dir)
            
    for img in images:
        log("Processing image: ID %s: %s" % (img.id, img.getName()))
        pixels = img.getPrimaryPixels()
        if (pixels.getId() in ids):
            continue
        ids.append(pixels.getId())

        if format == constants.transfer.FORMAT_OMETIFF:
            if img._prepareRE().requiresPixelsPyramid():
                log("  ** Can't export a 'Big' image to OME-TIFF. **")
                if len(images) == 1:
                    return None, "Can't export a 'Big' image to %s." % format
                continue
            else:
                save_as_ome_tiff(conn, img, folder_name)
        elif format == constants.transfer.FORMAT_ZARR:
            save_image_as_zarr(conn, suuid, img, folder_name)
        else:
            size_x = pixels.getSizeX()
            size_y = pixels.getSizeY()
            if size_x*size_y > size:
                msg = "Can't export image over %s pixels. " \
                      "See 'omero.client.download_as.max_size'" % size
                log("  ** %s. **" % msg)
                if len(images) == 1:
                    return None, msg
                continue
            else:
                log("Exporting image as %s: %s" % (format, img.getName()))

            log("\n----------- Saving planes from image: '%s' ------------"
                % img.getName())
            size_c = img.getSizeC()
            size_z = img.getSizeZ()
            size_t = img.getSizeT()
            z_range = get_z_range(size_z, script_params)
            t_range = get_t_range(size_t, script_params)
            log("Using:")
            if z_range is None:
                log("  Z-index: Last-viewed")
            elif len(z_range) == 1:
                log("  Z-index: %d" % z_range[0])
            else:
                log("  Z-range: %s-%s" % (z_range[0], z_range[1]-1))
            if project_z:
                log("  Z-projection: ON")
            if t_range is None:
                log("  T-index: Last-viewed")
            elif len(t_range) == 1:
                log("  T-index: %d" % t_range[0])
            else:
                log("  T-range: %s-%s" % (t_range[0], t_range[1]-1))
            log("  Format: %s" % format)
            if zoom_percent is None:
                log("  Image Zoom: 100%")
            else:
                log("  Image Zoom: %s" % zoom_percent)
            log("  Greyscale: %s" % greyscale)
            log("Channel Rendering Settings:")
            for ch in img.getChannels():
                log("  %s: %d-%d"
                    % (ch.getLabel(), ch.getWindowStart(), ch.getWindowEnd()))

            try:
                save_planes_for_image(suuid, img, size_c, split_cs, merged_cs,
                                      channel_names, z_range, t_range,
                                      greyscale, zoom_percent,
                                      project_z=project_z, format=format,
                                      folder_name=folder_name)
            finally:
                # Make sure we close Rendering Engine
                img._re.close()

        # write log for exported images (not needed for ome-tiff)
        write_logfile(exp_dir)

    if len(os.listdir(exp_dir)) == 0:
        error_msg = "No files exported. Check export settings and data availability."
        logger.error(f"Critical error: {error_msg}")
        raise Exception(error_msg)
    # zip everything up (unless we've only got a single ome-tiff)
    if format == constants.transfer.FORMAT_OMETIFF and len(os.listdir(exp_dir)) == 1:
        ometiff_ids = [t.id for t in parent.listAnnotations(ns=NSOMETIFF)]
        conn.deleteObjects("Annotation", ometiff_ids)
        export_file = os.path.join(folder_name, os.listdir(exp_dir)[0])
        namespace = NSOMETIFF
        output_display_name = "OME-TIFF"
        mimetype = 'image/tiff'
    else:
        export_file = "%s.zip" % folder_name
        compress(export_file, folder_name)
        mimetype = 'application/zip'
        output_display_name = f"Batch export zip '{folder_name}'"
        namespace = NSCREATED + "/omero/export_scripts/Batch_Image_Export"

    # Copy to SLURM
    transfer_successful = False
    try:
        r = slurmClient.transfer_data(Path(export_file))
        logger.debug(r)
        message += f"'{folder_name}' succesfully copied to SLURM!\n"
        transfer_successful = True
    except Exception as e:
        logger.error(f"Critical error: Copying to SLURM failed: {e}")
        raise Exception(f"Data transfer to SLURM failed: {e}") from e
        
    # Unpack on SLURM
    unpack_successful = False
    if transfer_successful:
        try:
            unpack_result = slurmClient.unpack_data(folder_name)
            logger.debug(unpack_result.stdout)
            if not unpack_result.ok:
                error_msg = f"Error unpacking data on SLURM: {unpack_result.stderr}"
                logger.error(error_msg)
                raise Exception(error_msg)
            else:
                unpack_successful = True
        except Exception as e:
            logger.error(f"Critical error: Unzipping on SLURM failed: {e}")
            raise Exception(f"Data unpacking on SLURM failed: {e}") from e
    
    file_annotation, ann_message = script_utils.create_link_file_annotation(
        conn, export_file, parent, output=output_display_name,
        namespace=namespace, mimetype=mimetype)
    message += ann_message
    
    # Clean up file annotation if transfer and unpack were successful
    if transfer_successful and unpack_successful and file_annotation:
        try:
            conn.deleteObjects("FileAnnotation", [file_annotation.id],
                               deleteAnns=True, deleteChildren=True, wait=True)
            message += ("Temporary file annotation cleaned up after "
                        "successful transfer.\n")
            logger.info(f"Cleaned up file annotation {file_annotation.id}")
            # Return None to indicate cleanup was done
            file_annotation = None
        except Exception as cleanup_error:
            # Cleanup failure is non-critical - log warning but don't fail script
            logger.warning(f"Failed to cleanup file annotation: "
                           f"{cleanup_error}")
            message += (f"Warning: Could not cleanup temporary file "
                        f"annotation: {cleanup_error}\n")

    return file_annotation, message


def write_logfile(exp_dir):
    """Write accumulated log strings to a batch export log file.
    
    Args:
        exp_dir (str): Export directory path where log file will be created.
    """
    name = 'Batch_Image_Export.txt'
    with open(os.path.join(exp_dir, name), 'w') as log_file:
        for s in log_strings:
            log_file.write(s)
            log_file.write("\n")


def run_script():
    """Main entry point for SLURM image transfer script.
    
    Called by OMERO scripting service to handle data export from OMERO
    to SLURM clusters. Configures script parameters, processes user inputs,
    and delegates to batch_image_export for the actual transfer work.
    """

    with SlurmClient.from_config() as slurmClient:

        data_types = [rstring(constants.transfer.DATA_TYPE_DATASET),
                      rstring(constants.transfer.DATA_TYPE_IMAGE),
                      rstring(constants.transfer.DATA_TYPE_PLATE)]
        formats = [rstring(constants.transfer.FORMAT_TIFF),
                   rstring(constants.transfer.FORMAT_OMETIFF),
                   rstring(constants.transfer.FORMAT_ZARR)]
        default_z_option = constants.transfer.Z_DEFAULT
        z_choices = [rstring(default_z_option),
                     rstring(constants.transfer.Z_ALL),
                     # currently ImageWrapper only allows full Z-stack
                     # projection
                     rstring(constants.transfer.Z_MAXPROJ),
                     rstring(constants.transfer.Z_OTHER)]
        default_t_option = constants.transfer.T_DEFAULT
        t_choices = [rstring(default_t_option),
                     rstring(constants.transfer.T_ALL),
                     rstring(constants.transfer.T_OTHER)]
        zoom_percents = omero.rtypes.wrap([constants.transfer.ZOOM_25,
                                           constants.transfer.ZOOM_50,
                                           constants.transfer.ZOOM_100,
                                           constants.transfer.ZOOM_200,
                                           constants.transfer.ZOOM_300,
                                           constants.transfer.ZOOM_400])

        client = scripts.client(
            '_SLURM_Image_Transfer',
            f"""Save multiple images as TIFF or ZARR
            in a zip file and export them to SLURM.
            
            Note that TIFF will be a rendered version of your image
            as shown in OMERO.web currently: not the original pixel
            values. This matters for e.g. Mask images, where each
            ROI / mask should be a very specific pixel value.
            
            ZARR will use the specific pixel values of the original 
            file, but you will need to convert it to a format that
            the workflows can read (which is TIFF).
            
            Please use SLURM_Run_Workflow directly instead if you 
            don't know how to convert ZARR to TIFF on Slurm!
            Otherwise, use the conversion job on Slurm to convert.

            This runs a script remotely on your SLURM cluster.
            Connection ready? {slurmClient.validate()}""",

            scripts.String(
                constants.transfer.DATA_TYPE, optional=False, grouping="1",
                description="The data you want to work with.",
                values=data_types,
                default=constants.transfer.DATA_TYPE_IMAGE),

            scripts.List(
                constants.transfer.IDS, optional=False, grouping="2",
                description="List of Dataset IDs or Image IDs").ofType(
                    rlong(0)),

            scripts.Bool(
                constants.transfer.SETTINGS, grouping="5",
                description="Select how to export your images",
                optional=False,
                default=True
            ),

            scripts.Bool(
                constants.transfer.CHANNELS, grouping="5.6",
                description="Save individual channels as separate images",
                default=False),

            scripts.Bool(
                constants.transfer.CHANNELS_GREY, grouping="5.6.1",
                description="If true, all individual channel images will be"
                " grayscale", default=False),

            scripts.List(
                constants.transfer.CHANNELS_NAMES, grouping="5.6.2",
                description="Names for saving individual channel images"),

            scripts.Bool(
                constants.transfer.MERGED, grouping="5.5",
                description="Save merged image, using current \
                    rendering settings",
                default=True),

            scripts.String(
                constants.transfer.Z, grouping="5.7",
                description="Default Z is last viewed Z for each image\
                    , OR choose"
                " Z below.", values=z_choices, default=default_z_option),

            scripts.Int(
                constants.transfer.Z_IDX, grouping="5.7.1",
                description="Choose a specific Z-index to export", min=1),

            scripts.Int(
                constants.transfer.Z_IDX_START, grouping="5.7.2",
                description="Choose a specific Z-index to export", min=1),

            scripts.Int(
                constants.transfer.Z_IDX_END, grouping="5.7.3",
                description="Choose a specific Z-index to export", min=1),

            scripts.String(
                constants.transfer.T, grouping="5.8",
                description="Default T is last viewed T for each image"
                ", OR choose T below.", values=t_choices,
                default=default_t_option),

            scripts.Int(
                constants.transfer.T_IDX, grouping="5.8.1",
                description="Choose a specific T-index to export", min=1),

            scripts.Int(
                constants.transfer.T_IDX_START, grouping="5.8.2",
                description="Choose a specific T-index to export", min=1),

            scripts.Int(
                constants.transfer.T_IDX_END, grouping="5.8.3",
                description="Choose a specific T-index to export", min=1),

            scripts.String(
                constants.transfer.ZOOM, grouping="5.9", values=zoom_percents,
                description="Zoom (jpeg, png or tiff) before saving with"
                " ANTIALIAS interpolation",
                default=constants.transfer.ZOOM_100),

            scripts.String(
                constants.transfer.FORMAT, grouping="5.1",
                description="Format to save image", values=formats,
                default=constants.transfer.FORMAT_ZARR),

            scripts.String(
                constants.transfer.FOLDER, grouping="3",
                description="Name of folder (and zip file) to store images. Don't use spaces!",
                default=constants.transfer.FOLDER_DEFAULT+str(int(datetime.now().timestamp()))),

            version=VERSION,
            authors=["Torec Luik", "William Moore", "OME Team"],
            institutions=["Amsterdam UMC", "University of Dundee"],
            contact='cellularimaging@amsterdamumc.nl',
            authorsInstitutions=[[1], [2]],
            namespaces=[omero.constants.namespaces.NSDYNAMIC],
        )

        try:
            start_time = datetime.now()
            script_params = {}

            conn = BlitzGateway(client_obj=client)

            suuid = client.getSessionId()

            script_params = client.getInputs(unwrap=True)
            for key, value in script_params.items():
                log("%s:%s" % (key, value))

            # call the main script - returns a file annotation wrapper
            file_annotation, message = batch_image_export(
                conn, script_params, slurmClient, suuid, client)

            stop_time = datetime.now()
            log("Duration: %s" % str(stop_time-start_time))

            # return this fileAnnotation to the client.
            client.setOutput("Message", rstring(message))
            if file_annotation is not None:
                client.setOutput("File_Annotation",
                                 robject(file_annotation._obj))

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
