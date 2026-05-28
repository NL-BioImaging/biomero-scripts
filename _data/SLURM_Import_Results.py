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
import difflib
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
from uuid import UUID

import omero
import omero.gateway
from omero import scripts
from omero.constants.namespaces import NSCREATED
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, robject, unwrap, wrap
from omero_metadata.populate import ParsingContext
import ezomero

from biomero import SlurmClient, constants

try:
    from omero_upload import upload_ln_s
    INPLACE_ATTACHMENTS_AVAILABLE = True
except ImportError:
    INPLACE_ATTACHMENTS_AVAILABLE = False

USE_INPLACE_ATTACHMENTS = os.getenv("USE_INPLACE_ATTACHMENTS", "true").lower() == "true"

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
        IngestionTracking,
        get_ingest_tracker,
        STAGE_IMPORTED,
        STAGE_INGEST_FAILED,
        STAGE_INGEST_STARTED
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
VERSION = "2.7.0"

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


def find_supported_image_paths(base_path: str, recursive: bool = True) -> List[str]:
    """Find all supported image files and directories (e.g., ZARR) in a path.
    
    Some image formats like ZARR are stored as directories, while others are files.
    This function handles both cases for any supported extension.
    
    Args:
        base_path: Base directory to search.
        recursive: Whether to search recursively. Defaults to True.
        
    Returns:
        List of paths to supported image files and directories.
    """
    if recursive:
        all_paths = glob.glob(os.path.join(base_path, "**", "*"), recursive=True)
    else:
        all_paths = glob.glob(os.path.join(base_path, "*"))
    
    image_paths = []
    logger.debug(f"Scanning for image files in: {base_path}")
    logger.debug(f"Found {len(all_paths)} total paths to check")
    
    for path in all_paths:
        for ext in SUPPORTED_IMAGE_EXTENSIONS:
            if path.endswith(ext):
                # Check if it's either a file or directory with supported extension
                if os.path.isfile(path) or os.path.isdir(path):
                    logger.debug(f"Found supported image: {path}")
                    image_paths.append(path)
                    break  # Don't check other extensions for this path
    
    logger.info(f"Found {len(image_paths)} supported image files/directories in {base_path}")            
    return image_paths


def find_label_zarr_paths(base_path: str) -> List[str]:
    """Find label zarr directories within zarr files.
    
    Searches for subdirectories matching the pattern: *.zarr/labels/*/
    where the subdirectory itself is a valid zarr array (contains .zattrs, .zgroup, etc.)
    
    Args:
        base_path: Base directory to search for zarr files.
        
    Returns:
        List of paths to label zarr directories.
    """
    label_zarrs = []
    logger.debug(f"Scanning for label zarr directories in: {base_path}")
    
    # Find all .zarr directories first
    zarr_pattern = os.path.join(base_path, "**", "*.zarr")
    zarr_dirs = [path for path in glob.glob(zarr_pattern, recursive=True) if os.path.isdir(path)]
    
    for zarr_dir in zarr_dirs:
        labels_dir = os.path.join(zarr_dir, "labels")
        if os.path.isdir(labels_dir):
            # Look for subdirectories in labels/ that are themselves zarr arrays
            for item in os.listdir(labels_dir):
                potential_label_zarr = os.path.join(labels_dir, item)
                if os.path.isdir(potential_label_zarr):
                    # Check if this is a valid zarr array (has .zattrs or .zgroup or zarr.json)
                    has_zattrs = os.path.exists(os.path.join(potential_label_zarr, ".zattrs"))
                    has_zgroup = os.path.exists(os.path.join(potential_label_zarr, ".zgroup"))
                    has_zarrjson = os.path.exists(os.path.join(potential_label_zarr, "zarr.json"))
                    
                    if has_zattrs or has_zgroup or has_zarrjson:
                        # Check if it already has .zarr extension
                        if not item.endswith('.zarr'):
                            # Rename the directory to have .zarr extension on disk
                            label_zarr_path = potential_label_zarr + ".zarr"
                            new_item_name = item + ".zarr"
                            try:
                                os.rename(potential_label_zarr, label_zarr_path)
                                logger.info(f"Renamed label zarr: {potential_label_zarr} -> {label_zarr_path}")
                                
                                # Update the .zattrs file in the labels directory to reflect the rename
                                labels_attrs_path = None
                                labels_zattrs_path = os.path.join(labels_dir, ".zattrs")
                                labels_zarrjson_path = os.path.join(labels_dir, "zarr.json")
                                if os.path.exists(labels_zarrjson_path):
                                    labels_attrs_path = labels_zarrjson_path
                                elif os.path.exists(labels_zattrs_path):
                                    labels_attrs_path = labels_zattrs_path

                                if labels_attrs_path:
                                    try:
                                        import json
                                        with open(labels_attrs_path, 'r') as f:
                                            zattrs_data = json.load(f)
                                        
                                        # Update the labels list if it exists
                                        if 'labels' in zattrs_data and isinstance(zattrs_data['labels'], list):
                                            # Replace old name with new name
                                            if item in zattrs_data['labels']:
                                                zattrs_data['labels'][zattrs_data['labels'].index(item)] = new_item_name
                                            elif new_item_name not in zattrs_data['labels']:
                                                # If old name not found but new name not present, add it
                                                zattrs_data['labels'].append(new_item_name)
                                        
                                        # Write back the updated attrs
                                        with open(labels_attrs_path, 'w') as f:
                                            json.dump(zattrs_data, f, indent=2)
                                        logger.info(f"Updated labels/attrs to reference {new_item_name}")
                                        
                                    except (json.JSONDecodeError, IOError) as e:
                                        logger.warning(f"Failed to update labels/attrs after rename: {e}")
                                
                                label_zarrs.append(label_zarr_path)
                                logger.debug(f"Found label zarr: {label_zarr_path}")
                            except OSError as e:
                                logger.error(f"Failed to rename label zarr {potential_label_zarr}: {e}")
                                # Still add the original path in case of failure
                                label_zarrs.append(potential_label_zarr)
                                logger.debug(f"Found label zarr (rename failed): {potential_label_zarr}")
                        else:
                            # Already has .zarr extension
                            label_zarrs.append(potential_label_zarr)
                            logger.debug(f"Found label zarr: {potential_label_zarr}")
    
    logger.info(f"Found {len(label_zarrs)} label zarr directories in {base_path}")
    return label_zarrs


def getOriginalFilename(name: str) -> str:
    """Extract original filename from processed file path.

    Extracts the base filename from a processed file path by removing workflow
    processing suffixes. Handles zarr directories and regular files.

    Args:
        name: Path/name of processed file.

    Returns:
        Original filename if pattern matches, otherwise input name.

    Examples:
        >>> getOriginalFilename("/../../Cells Apoptotic.png_merged_z01_t01.tiff")
        "Cells Apoptotic.png"
        >>> getOriginalFilename("/out/Cell-Granules.tif.zarr")
        "Cell-Granules.tif"
    """
    # Handle zarr directories - extract original filename by removing .zarr extension
    if name.endswith('.zarr'):
        basename = os.path.basename(name)
        # Remove .zarr extension to get original filename
        return basename.replace('.zarr', '')
    
    # Handle standard processed files (original logic for backward compatibility)
    match = re.match(pattern=".+\/(.+\.[A-Za-z]+).+\.[tiff|png]", string=name)
    if match:
        return match.group(1)

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
                csv_file_attachment = create_file_annotation_inplace(
                    conn, csv_path, mimetype, namespace, description)

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


def find_best_matching_image(
    og_name: str,
    input_images: List[Any],
    threshold: float = 0.5
) -> Optional[Any]:
    """Find the best matching OMERO image for a result filename by name similarity.

    Uses SequenceMatcher to compare the extracted original filename against
    each input image's OMERO name. This is a best-effort approach that handles
    workflows that rename output files (e.g. NucleiLabels -> CellsLabels).

    Args:
        og_name: Extracted original filename from the result file.
        input_images: List of OMERO Image objects (workflow input images).
        threshold: Minimum similarity ratio to consider a match. Defaults to 0.5.

    Returns:
        Best matching image if ratio >= threshold, else None.
    """
    if not input_images:
        return None
    best_image = None
    best_ratio = 0.0
    og_lower = og_name.lower()
    for image in input_images:
        img_name = image.getName()
        ratio = difflib.SequenceMatcher(None, og_lower, img_name.lower()).ratio()
        logger.debug(f"Similarity '{og_name}' vs '{img_name}': {ratio:.2f}")
        if ratio > best_ratio:
            best_ratio = ratio
            best_image = image
    if best_ratio >= threshold:
        logger.info(
            f"Best match for '{og_name}': '{best_image.getName()}' (ratio={best_ratio:.2f})")
        return best_image
    logger.info(
        f"No good match for '{og_name}' among input images (best={best_ratio:.2f}), "
        f"falling back to name lookup")
    return None


def match_results_to_inputs(
    result_keys: List[str],
    input_images: List[Any],
    og_name_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Optional[Any]]:
    """Map each result file (or imported image name) to its source input OMERO image.

    Single source of truth for parent-matching used by ALL output paths
    (file attachments and biomero-importer dataset imports).

    Matching priority per result, in order:

    1. Single input  — trivially attach everything to it.
    2. Prefix match  — input name is a proper prefix of the result name.
                       E.g. input "base" produced result "base.0.tif"; the
                       workflow appended ".0.tif".  Longest prefix wins when
                       multiple inputs share a common prefix ("base" vs "base.0.tif"
                       both being inputs → "base.0.tif" grabs "base.0.tif.0.tif").
    3. Similarity    — difflib ratio against remaining inputs (threshold 0.5).
                       Handles workflows that rename outputs (e.g. Nuclei→Cells).
    4. Positional    — similarity failed, all remaining inputs share the same
                       name (OMERO exports them as base.tif / base_(1).tif …).
                       Results are sorted by pre-rename basename so their order
                       matches the original submission order of input_images.

    Scenario 4 (renamed files): og_name_map[file_path] = pre-rename basename.
    resolve_match_name() does a reverse-lookup so imported image names (stored
    by OMERO as the post-rename filename) still resolve to the original stem
    before any matching is attempted.

    Args:
        result_keys: File paths or OMERO image names — one entry per result.
        input_images: Workflow input images in original submission order. NOT sorted.
        og_name_map: {current_file_path: pre_rename_basename} from
            rename_files_in_importer_storage. Pass None when no rename ran.

    Returns:
        Dict mapping each result_key to its matched source Image (or None).
    """
    if not input_images:
        return {k: None for k in result_keys}

    # Scenario 4 prep: reverse-lookup so post-rename basenames resolve to pre-rename names.
    # og_name_map keys are full paths; basename(key) → pre_rename covers imported image names.
    reverse_og: Dict[str, str] = {}
    if og_name_map:
        for path, pre_rename in og_name_map.items():
            reverse_og[os.path.basename(path)] = pre_rename

    def resolve_match_name(key: str) -> str:
        """Pre-rename name for this key, used for sorting and similarity matching."""
        if og_name_map and key in og_name_map:   # full file path hit
            return og_name_map[key]
        bn = os.path.basename(key)
        if bn in reverse_og:                     # imported image name (post-rename basename)
            return reverse_og[bn]
        return getOriginalFilename(bn)            # no rename: extract stem from workflow suffix

    # Scenario 4 / positional: sort results by pre-rename name so order matches submission order.
    sorted_keys = sorted(result_keys, key=resolve_match_name) if len(input_images) > 1 else list(result_keys)
    logger.debug(f"Results sorted for source matching: {[os.path.basename(k) for k in sorted_keys]}")

    remaining = list(input_images)  # consumed — each input claimed at most once
    mapping: Dict[str, Optional[Any]] = {}

    for key in sorted_keys:
        if not remaining:
            mapping[key] = None
            continue

        match_name = resolve_match_name(key)

        if len(remaining) == 1:  # scenario 1
            mapping[key] = remaining.pop(0)
        else:
            # Scenario 2a — prefix match (higher priority than similarity).
            # Finds the input whose name is the longest proper prefix of match_name.
            # Needed when inputs are "base" and "base.0.tif": similarity would
            # incorrectly score result "base.0.tif" as 1.0 against input "base.0.tif",
            # but "base" is the true parent (the workflow appended ".0.tif").
            mn_lower = match_name.lower()
            prefix_match = None
            prefix_len = -1
            for img in remaining:
                img_lower = img.getName().lower()
                if mn_lower.startswith(img_lower) and len(img_lower) < len(mn_lower):
                    if len(img_lower) > prefix_len:  # longest prefix wins
                        prefix_len = len(img_lower)
                        prefix_match = img

            if prefix_match:
                remaining.remove(prefix_match)
                matched = prefix_match
                logger.info(
                    f"Prefix match for '{match_name}': '{matched.getName()}' (id={matched.getId()})")
            else:
                # Scenario 2b — similarity match for uniquely-named inputs.
                matched = find_best_matching_image(match_name, remaining)
                if matched:
                    remaining.remove(matched)
                else:
                    # Scenario 3 — same-name inputs: positional fallback.
                    # Results were sorted by pre-rename name above, so pop order
                    # mirrors the original submission order of input_images.
                    matched = remaining.pop(0)
                    logger.info(
                        f"No name match for '{match_name}'; positional fallback → id={matched.getId()}")
            mapping[key] = matched

    return mapping


def saveImagesToOmeroAsAttachments(
    conn: BlitzGateway,
    folder: str,
    client: Any,
    metadata_files: Optional[List[str]] = None,
    wf_id: Optional[str] = None,
    input_images: Optional[List[Any]] = None,
    og_name_map: Optional[Dict[str, str]] = None
) -> str:
    """Save image from a (unzipped) folder to OMERO as attachments.

    Args:
        conn: Connection to OMERO.
        folder: Unzipped folder path.
        client: OMERO client to attach output.
        metadata_files: List of metadata CSV file paths to attach. Defaults to None.
        wf_id: Workflow ID for metadata. Defaults to None.
        input_images: Known workflow input images for attachment matching. Defaults to None.
        og_name_map: Pre-rename filename map from rename_files_in_importer_storage.
            Keys are current file paths, values are the original filenames before
            any rename pattern was applied. Used so similarity matching still works
            when files have been renamed to an arbitrary pattern. Defaults to None.

    Returns:
        Message to add to script output.
    """
    # Handle both regular image files and ZARR directories
    files = find_supported_image_paths(folder)
    logger.info(f"Found the following files in {folder}: {files}")
    namespace = NSCREATED + "/SLURM/SLURM_GET_RESULTS"
    job_id = unwrap(client.getInput(
        constants.results.OUTPUT_SLURM_JOB_ID)).strip()
    msg = ""
    message = ""

    # Build result → source mapping once; used for both attachment and legacy paths.
    # match_results_to_inputs owns all scenario logic (prefix, similarity, positional).
    source_map = match_results_to_inputs(files, input_images, og_name_map) if input_images else {}

    for name in files:
        # og_name only needed for the legacy (no input_images) OMERO name-search path.
        og_name = (og_name_map.get(name) if og_name_map else None) or getOriginalFilename(os.path.basename(name))

        if input_images is not None:
            matched = source_map.get(name)
            if matched is None:  # no remaining inputs or genuinely unmatched
                logger.info(f"No source mapped for '{og_name}'; skipping attachment")
                continue
            images = [matched]
        else:
            # Legacy path (no workflow tracking): search OMERO by extracted name.
            images = list(conn.getObjects("Image", attributes={"name": og_name}))

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
                source_img = images[0] if images else None
                file_ann = create_file_annotation_inplace(
                    conn, name, mimetype=f"image/{ext}",
                    namespace=namespace,
                    description=f"Result from job {job_id}" + (f" (Workflow {wf_id})" if wf_id else "") + f" | analysis {folder}" + (f" | source: {source_img.getName()} (id: {source_img.getId()})" if source_img else ""))

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


def getUserScreens() -> List[Any]:
    """Get OMERO Screens that user has access to.

    Returns:
        List of screen IDs and names formatted as strings.

    Raises:
        Exception: If connection or query fails.
    """
    try:
        client = omero.client()
        client.createSession()
        conn = omero.gateway.BlitzGateway(client_obj=client)
        conn.SERVICE_OPTS.setOmeroGroup(-1)
        objparams = [rstring('%d: %s' % (d.id, d.getName()))
                     for d in conn.getObjects('Screen')
                     if type(d) == omero.gateway.ScreenWrapper]
        if not objparams:
            objparams = [rstring('<No objects found>')]
        return objparams
    except Exception as e:
        return ['Exception: %s' % e]
    finally:
        client.closeSession()


def get_current_user_groups(conn: BlitzGateway) -> List[str]:
    """Get OMERO groups that the current user belongs to.
    
    Args:
        conn: Existing OMERO BlitzGateway connection.
    
    Returns:
        List of group names the current user belongs to.
        
    Raises:
        Exception: If connection or query fails.
    """
    try:
        user_groups = []
        
        # Get current group from connection context
        current_group = conn.getGroupFromContext()
        if current_group:
            user_groups.append(current_group.getName())
        
        # Get admin service to query group memberships
        admin_service = conn.getAdminService()
        user = conn.getUser()
        user_id = user.getId()
        
        # Get all groups the user belongs to
        group_list = admin_service.containedGroups(user_id)
        for group in group_list:
            # Extract group name (handle both string and RString cases)
            if hasattr(group, 'getName'):
                group_name_obj = group.getName()
                if hasattr(group_name_obj, 'getValue'):
                    group_name = group_name_obj.getValue()
                else:
                    group_name = str(group_name_obj)
            else:
                group_name = str(group)
                
            if group_name and group_name not in user_groups:
                user_groups.append(group_name)
        
        return user_groups
        
    except Exception as e:
        logger.warning(f"Failed to get user groups: {e}")
        return []


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
        logger.error(f"Failed to cleanup tmp files: {e}")
        message += f" Failed to cleanup tmp files: {e}"

    return message


def create_file_annotation_inplace(
    conn: BlitzGateway,
    file_path: str,
    mimetype: str,
    namespace: str,
    description: str,
) -> Any:
    """Create a FileAnnotation using in-place (symlink) import if available.

    When ``omero_upload`` is installed and ``USE_INPLACE_ATTACHMENTS`` is
    enabled, the file is registered via a symlink inside the OMERO managed
    repository so no data is copied.  The managed repository root is read
    from OMERO's own ``omero.data.dir`` config value; the ``OMERO_DATA_DIR``
    environment variable is used only as a last-resort fallback.
    Falls back to a regular byte-copy upload when in-place is unavailable.

    Args:
        conn: Open OMERO BlitzGateway connection.
        file_path: Absolute path to the file to register.
        mimetype: MIME type string (e.g. ``"text/csv"``).
        namespace: OMERO annotation namespace.
        description: Human-readable description stored on the annotation.

    Returns:
        A :class:`omero.gateway.FileAnnotationWrapper` for the new annotation.
    """
    if INPLACE_ATTACHMENTS_AVAILABLE and USE_INPLACE_ATTACHMENTS:
        # Resolve OMERO data dir from server config, fall back to env var
        try:
            omero_data_dir = conn.getConfigService().getConfigValue("omero.data.dir")
        except Exception:
            omero_data_dir = None
        if not omero_data_dir:
            omero_data_dir = os.getenv("OMERO_DATA_DIR", "/OMERO")
        logger.debug(f"Creating in-place file annotation for {file_path} (data dir: {omero_data_dir})")
        fo = upload_ln_s(conn.c, file_path, omero_data_dir, mimetype)
        fa = omero.model.FileAnnotationI()
        fa.setFile(fo._obj)
        fa.setNs(rstring(namespace))
        fa.setDescription(rstring(description))
        fa = conn.getUpdateService().saveAndReturnObject(fa)
        return omero.gateway.FileAnnotationWrapper(conn, fa)
    else:
        logger.debug(f"Creating regular (upload) file annotation for {file_path}")
        return conn.createFileAnnfromLocalFile(
            file_path, mimetype=mimetype, ns=namespace, desc=description)


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
        annotation = create_file_annotation_inplace(
            conn, file, mimetype, namespace, description)
        # Already have other output in this script
        # But you could add this as output if you wanted log instead
        # client.setOutput("File_Annotation", robject(annotation._obj))

        # Build a fully-qualified URL so it works from any browser location.
        # omero.client.web.host (e.g. "https://omero.example.com/omero/") is
        # the canonical absolute base; fall back to omero.web.prefix for a
        # path-absolute URL, then bare path as last resort.
        obj_id = annotation.getFile().getId()
        try:
            config = conn.getConfigService()
            web_host = (config.getConfigValue("omero.client.web.host") or "").rstrip("/")
            if not web_host:
                web_prefix = (config.getConfigValue("omero.web.prefix") or "").rstrip("/")
                web_host = web_prefix  # may still be empty → path-absolute
        except Exception:
            web_host = ""
        url = f"{web_host}/webclient/get_original_file/{obj_id}/"
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

        annotation = create_file_annotation_inplace(
            conn, renamed_metadata_file, mimetype, namespace, description)

        # Refresh objects to avoid UnloadedEntityException (works for Projects, Plates, Images)
        for obj in projects:
            # Detect object type dynamically
            if hasattr(obj, '_obj') and hasattr(obj._obj, '__class__'):
                obj_class_name = obj._obj.__class__.__name__
                if 'Project' in obj_class_name:
                    object_type = "Project"
                elif 'Dataset' in obj_class_name:
                    object_type = "Dataset"
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
        zip_annotation = create_file_annotation_inplace(
            conn, f"{folder}.zip", mimetype, namespace, description)

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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results_path = os.path.join(
        base_path, '.analyzed', wf_id, timestamp)
    return results_path


def extract_slurm_results_zip(
    slurmClient: SlurmClient,
    slurm_job_id: str,
    local_tmp_storage: str,
    group_name: str,
    wf_id: UUID,
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

    # Normalize to absolute path - log may contain a relative path (e.g. "my-scratch/...")
    # which would break the 7z zip command (cd changes CWD, making relative paths wrong)
    if not slurm_data_path.startswith('/'):
        abs_result = slurmClient.run_commands([f'realpath "{slurm_data_path}"'])
        if abs_result.ok and abs_result.stdout.strip():
            slurm_data_path = abs_result.stdout.strip()
            logger.info(f"Resolved to absolute path: {slurm_data_path}")
        else:
            logger.warning(f"Could not resolve absolute path for '{slurm_data_path}', proceeding with relative path")

    # Poll SLURM until data/out contains files (NFS flush lag after job COMPLETED)
    out_dir = f"{slurm_data_path}/data/out"
    poll_interval = 15  # seconds
    poll_max_attempts = 20  # 20 * 15s = 5 minutes max
    for attempt in range(1, poll_max_attempts + 1):
        check_result = slurmClient.run_commands(
            [f"ls \"{out_dir}\" 2>/dev/null | wc -l"])
        file_count = 0
        if check_result.ok:
            try:
                file_count = int(check_result.stdout.strip())
            except (ValueError, AttributeError):
                file_count = 0
        if file_count > 0:
            logger.info(
                f"Output directory ready: {out_dir} has {file_count} file(s) (attempt {attempt})")
            break
        logger.warning(
            f"Output directory empty on SLURM (attempt {attempt}/{poll_max_attempts}): "
            f"{out_dir} — waiting {poll_interval}s for filesystem flush...")
        import time
        time.sleep(poll_interval)
    else:
        raise RuntimeError(
            f"SLURM output directory still empty after {poll_max_attempts * poll_interval}s: "
            f"{out_dir}. Job may have failed to write output.")

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
        copy_result = slurmClient.copy_zip_locally(local_tmp_storage, f"{slurm_data_path}/{filename}")

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
    image_files = find_supported_image_paths(target_path)

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


def poll_import_status(uuid: str, conn: BlitzGateway = None, timeout: int = 3600, poll_interval: int = 5) -> Tuple[bool, str]:
    """Poll the importer database for import completion status.

    Args:
        uuid: The UUID of the import order to monitor
        conn: OMERO BlitzGateway connection (optional, for keepAlive in long polls)
        timeout: Maximum time to wait in seconds (default: 1 hour)
        poll_interval: Time between polls in seconds (default: 5 seconds)

    Returns:
        Tuple of (success: bool, message: str)
        success=True if import completed successfully
        success=False if import failed or timed out
    """
    if not IMPORTER_ENABLED or not IMPORTER_AVAILABLE:
        return False, "Importer not available for status polling"

    # Ensure uuid is always a string to avoid PostgreSQL type casting issues
    uuid_str = str(uuid)
    logger.info(f"Starting import status polling for UUID {uuid_str}")

    try:
        import time
        start_time = time.time()
        tracker = get_ingest_tracker()

        if not tracker:
            return False, "Could not get ingest tracker instance"

        Session = tracker.Session

        while (time.time() - start_time) < timeout:
            try:
                with Session() as session:
                    # Get the latest status for this UUID
                    latest_entry = session.query(IngestionTracking)\
                        .filter(IngestionTracking.uuid == uuid_str)\
                        .order_by(IngestionTracking.timestamp.desc())\
                        .first()

                    if not latest_entry:
                        logger.warning(
                            f"No import entries found for UUID {uuid_str}")
                        time.sleep(poll_interval)
                        continue

                    current_stage = latest_entry.stage
                    logger.debug(
                        f"Current import stage for {uuid_str}: {current_stage}")

                    if current_stage == STAGE_IMPORTED:
                        elapsed = time.time() - start_time
                        return True, f"Import completed successfully after {elapsed:.1f} seconds"

                    elif current_stage == STAGE_INGEST_FAILED:
                        error_msg = latest_entry.description or "Import failed with unknown error"
                        return False, f"Import failed: {error_msg}"

                    # Still in progress (STAGE_NEW_ORDER, STAGE_INGEST_STARTED, etc.)

            except Exception as db_error:
                logger.warning(
                    f"Database query error during polling: {db_error}")

            # Keep OMERO connection alive during long waits (similar to batched script)
            if conn:
                try:
                    conn.keepAlive()
                except Exception as keepalive_error:
                    logger.warning(
                        f"Failed to keep OMERO connection alive: {keepalive_error}")

            # Wait before next poll
            time.sleep(poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        return False, f"Import polling timed out after {elapsed:.1f} seconds"

    except Exception as e:
        logger.error(f"Error during import status polling: {e}", exc_info=True)
        return False, f"Error during polling: {str(e)}"


def wait_for_import_completion(upload_orders: List[Dict[str, Any]],
                               conn: BlitzGateway = None,
                               timeout: int = 3600,
                               poll_interval: int = 5,
                               task_id: UUID = None,
                               slurmClient: SlurmClient = None) -> Tuple[bool, str, List[str]]:
    """Wait for completion of multiple import orders.

    Args:
        upload_orders: List of upload order dictionaries
        conn: OMERO BlitzGateway connection (optional, for keepAlive during long waits)
        timeout: Maximum time to wait per order in seconds
        poll_interval: Time between polls in seconds
        task_id: Task ID for workflow tracking (optional)
        slurmClient: SLURM client for task status updates (optional)

    Returns:
        Tuple of (all_success: bool, summary_message: str, failed_uuids: List[str])
    """
    if not upload_orders:
        return True, "No upload orders to wait for", []

    logger.info(
        f"Waiting for completion of {len(upload_orders)} import orders")

    # Note: Removed unnecessary IMPORTING_DATA status update to reduce noise

    successful_imports = []
    failed_imports = []

    for order in upload_orders:
        uuid = order.get('UUID')
        if not uuid:
            failed_imports.append("Unknown UUID")
            continue

        # Ensure uuid is always a string to avoid PostgreSQL type casting issues
        uuid_str = str(uuid)
        logger.info(f"Polling import status for UUID {uuid_str}")
        success, message = poll_import_status(
            uuid_str, conn, timeout, poll_interval)

        if success:
            successful_imports.append(uuid_str)
            logger.info(f"Import successful for {uuid_str}: {message}")
        else:
            failed_imports.append(uuid_str)
            logger.error(f"Import failed for {uuid_str}: {message}")

    total_orders = len(upload_orders)
    success_count = len(successful_imports)
    failed_count = len(failed_imports)

    summary = f"Import results: {success_count}/{total_orders} successful"
    if failed_count > 0:
        summary += f", {failed_count} failed"

    all_successful = failed_count == 0

    return all_successful, summary, failed_imports


def create_upload_orders_for_results(
    group_name: str,
    username: str,
    destination_type: str,
    destination_id: int,
    results_path: str,
    wf_id: UUID,
    client: Any = None
) -> List[Dict[str, Any]]:
    """Create upload orders for SLURM results (images and optionally label zarrs).

    Args:
        group_name: OMERO group name.
        username: Username creating the order.
        destination_type: Destination type ('Dataset', 'Screen', etc.).
        destination_id: Destination ID in OMERO.
        results_path: Path to results in importer storage.
        wf_id: Workflow UUID for tracking.
        client: OMERO script client for accessing input parameters (optional).

    Returns:
        List of created upload orders.
    """

    if not IMPORTER_AVAILABLE:
        logger.error(
            "Cannot create upload orders: biomero-importer not available")
        return []

    # Find only image files (CSV tables handled by zip workflow)
    image_files = find_supported_image_paths(results_path)

    if not image_files:
        logger.warning(
            f"No image files matched extensions {SUPPORTED_IMAGE_EXTENSIONS}")

    orders = []

    # Check if label zarr import is enabled
    import_label_zarrs = unwrap(client.getInput(constants.results.IMPORT_LABEL_ZARRS)) if client else True
    import_only_labels = unwrap(client.getInput(constants.results.IMPORT_ONLY_LABELS)) if client else True
    
    # First check if there are actually label zarr files available
    label_zarr_files = find_label_zarr_paths(results_path) if import_label_zarrs else []
    
    # Determine if we should skip main image import (only when both label options are true AND labels exist)
    skip_main_images = import_label_zarrs and import_only_labels and len(label_zarr_files) > 0
    
    # Safety check: if Import_Only_Labels=true but no label zarrs found, warn and import main images
    if import_label_zarrs and import_only_labels and len(label_zarr_files) == 0:
        logger.warning("Import_Only_Labels=true but no label zarr directories found. Falling back to main image import to avoid empty import.")
        skip_main_images = False

    # Create order for image files if any exist (unless we're only importing labels)
    if image_files and not skip_main_images:
        image_order = {
            "Group": group_name,
            "Username": username,
            "DestinationID": destination_id,
            "DestinationType": destination_type,
            # Generate unique order ID (importer needs string)
            "UUID": str(uuid.uuid4()),
            "Files": image_files,
            "wf_id": wf_id,
            "source": "SLURM_Results"
        }
        create_upload_order(image_order)
        orders.append(image_order)
        logger.info(f"Created image upload order for {len(image_files)} files")
    elif image_files and skip_main_images:
        logger.info(f"Skipping main image import for {len(image_files)} files (Import_Only_Labels=true, {len(label_zarr_files)} label zarr directories found)")
            
    # Create orders for label zarr directories if enabled and found
    if import_label_zarrs and label_zarr_files:
        label_order = {
            "Group": group_name,
            "Username": username,
            "DestinationID": destination_id,
            "DestinationType": destination_type,
            # Generate unique order ID (importer needs string)
            "UUID": str(uuid.uuid4()),
            "Files": label_zarr_files,
            "wf_id": wf_id,
            "source": "SLURM_Results_Labels"
        }
        create_upload_order(label_order)
        orders.append(label_order)
        logger.info(f"Created label zarr upload order for {len(label_zarr_files)} label zarr directories")
    elif import_label_zarrs:
        logger.info("No label zarr directories found (Import_Label_Zarrs=true but workflow likely produced non-zarr results)")
    else:
        logger.info("Label zarr import is disabled")

    return orders


def rename_files_in_importer_storage(client: Any, results_path: str) -> Tuple[str, Dict[str, str]]:
    """Rename files and directories in importer storage according to rename pattern.

    Handles both regular image files and zarr directories. Zarr directories
    are renamed as complete units while preserving their internal structure.

    Args:
        client: OMERO script client for accessing rename settings.
        results_path: Path to results in importer storage.

    Returns:
        Tuple of (status message, og_name_map) where og_name_map maps each
        new file path to its pre-rename original filename. Callers should pass
        this to saveImagesToOmeroAsAttachments so attachment matching still
        works even when files have been renamed to an arbitrary pattern.
    """

    # Check if renaming is enabled
    rename_enabled = unwrap(client.getInput(
        constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME))
    if not rename_enabled:
        return "\\nFile renaming skipped (disabled)", {}

    # Find all image files and zarr directories in results path
    all_files = []
    zarr_dirs = []
    
    for root, dirs, files in os.walk(results_path):
        # Check for zarr directories - collect them but do NOT descend into them.
        # Label zarrs (*.zarr/labels/*/zarr) are internal zarr structure and should
        # not be treated as independent rename targets. Walking into zarr dirs also
        # causes a path-invalidation bug: after the parent zarr is renamed, any
        # child zarr paths collected earlier no longer exist.
        zarr_subdirs = [d for d in dirs if d.endswith('.zarr')]
        for dir_name in zarr_subdirs:
            zarr_path = os.path.join(root, dir_name)
            zarr_dirs.append(zarr_path)
        # Prune zarr dirs so os.walk does not descend into them
        dirs[:] = [d for d in dirs if not d.endswith('.zarr')]

        # Check for regular image files
        for file in files:
            file_path = os.path.join(root, file)
            if any(file_path.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS):
                all_files.append(file_path)
    
    # Process zarr directories first (they take precedence)
    for zarr_path in zarr_dirs:
        all_files.append(zarr_path)

    renamed_count = 0
    message = ""
    og_name_map: Dict[str, str] = {}  # new_path -> pre-rename basename

    for file_path in all_files:
        try:
            # The pre-rename basename is the actual result filename before the user
            # pattern is applied — e.g. "fractal_cellpose_sam_segmentation_merged_z01_t01.tiff".
            # This is stored in og_name_map so similarity matching in
            # saveImagesToOmeroAsAttachments still works after files are renamed.
            # Do NOT use getOriginalFilename(file_path) here: its regex requires a dot
            # in the stem and produces garbage (e.g. "coreKrawczyk/.analyzed") for
            # common result filenames that lack an embedded extension in the stem.
            pre_rename_basename = os.path.basename(file_path)

            # Extract original filename for rename pattern substitution.
            # Pass only the basename — getOriginalFilename's regex can match
            # directory components (e.g. ".analyzed") if given a full path.
            original_filename = getOriginalFilename(pre_rename_basename)

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

                # Ensure target is unique — add a counter suffix if names collide
                # (e.g. a static rename pattern applied to multiple result files)
                if os.path.exists(new_file_path):
                    first_dot = new_filename.find('.')
                    name_base = new_filename[:first_dot] if first_dot != -1 else new_filename
                    name_rest = new_filename[first_dot:] if first_dot != -1 else ''
                    counter = 2
                    while os.path.exists(os.path.join(file_dir, f"{name_base}_{counter}{name_rest}")):
                        counter += 1
                    new_filename = f"{name_base}_{counter}{name_rest}"
                    new_file_path = os.path.join(file_dir, new_filename)
                    logger.info(f"Name collision; using unique name: {new_filename}")

                # For zarr directories, add small delay and permission check
                if file_path.endswith('.zarr'):
                    import time
                    time.sleep(1)  # Brief delay to let any file handles settle
                    
                    # Check basic permissions
                    try:
                        # Test if we can write to the parent directory
                        parent_dir = os.path.dirname(file_path)
                        test_file = os.path.join(parent_dir, '.test_write_permission')
                        with open(test_file, 'w') as f:
                            f.write('test')
                        os.remove(test_file)
                    except Exception as perm_e:
                        logger.error(f"Permission check failed for {parent_dir}: {perm_e}")
                        message += f"\nWarning: No write permission for zarr rename in {parent_dir}: {perm_e}"
                        continue

                # Perform the rename (works for both files and directories)
                os.rename(file_path, new_file_path)
                renamed_count += 1
                file_type = "directory" if os.path.isdir(new_file_path) else "file"
                logger.info(f"Successfully renamed {file_type}: {new_file_path}")
                og_name_map[new_file_path] = pre_rename_basename

            else:
                logger.debug(
                    f"No rename needed for: {os.path.basename(file_path)}")
                og_name_map[file_path] = pre_rename_basename

        except PermissionError as pe:
            logger.error(f"Permission denied renaming {file_path}: {pe}")
            logger.error(f"Source exists: {os.path.exists(file_path)}, Source is dir: {os.path.isdir(file_path)}")
            logger.error(f"Parent dir writable: {os.access(os.path.dirname(file_path), os.W_OK)}")
            if file_path.endswith('.zarr'):
                message += f"\\nWarning: Permission denied renaming zarr directory {os.path.basename(file_path)}. This is often due to filesystem permissions or the directory being in use. The zarr will be imported with its original name."
            else:
                message += f"\\nWarning: Permission denied renaming {os.path.basename(file_path)}: {pe}"
        except Exception as e:
            logger.error(f"Failed to rename file/directory {file_path}: {e}")
            message += f"\\nWarning: Failed to rename {os.path.basename(file_path)}: {e}"

    if renamed_count > 0:
        message += f"\\nRenamed {renamed_count} files/directories in importer storage"
        logger.info(f"Completed renaming {renamed_count} files/directories")
    else:
        message += "\\nNo files/directories needed renaming"
        logger.info("No files/directories required renaming")

    return message, og_name_map


def add_metadata_to_imported_images(
    conn: BlitzGateway,
    slurmClient: SlurmClient,
    destination_id: int,
    order_uuids: List[str],
    wf_id: str,
    job_id: str,
    destination_type: str = "Dataset",
    input_images: Optional[List[Any]] = None,
    og_name_map: Optional[Dict[str, str]] = None,
) -> str:
    """Add metadata to images/plates imported by biomero-importer using UUID search.

    For Dataset workflows: Searches for images and adds metadata to individual images.
    For Screen workflows: Searches for plates and adds metadata to plates themselves.

    Args:
        conn: OMERO BlitzGateway connection
        slurmClient: SlurmClient instance
        destination_id: ID of the dataset/screen containing imported images/plates
        order_uuids: List of upload order UUIDs to search for
        wf_id: Workflow UUID for metadata
        job_id: SLURM job ID for metadata
        destination_type: Type of destination ("Dataset" or "Screen")

    Returns:
        Status message describing metadata addition results
    """
    try:
        # For Screen destinations (plate workflows), add metadata to plates
        if destination_type.lower() == "screen":
            return add_metadata_to_imported_plates(
                conn, slurmClient, destination_id, order_uuids, wf_id, job_id)
        
        # For Dataset destinations (image workflows), add metadata to images
        if not order_uuids:
            return "No upload order UUIDs provided for image search"

        logger.info(f"Searching for images imported with UUIDs: {order_uuids}")

        # Search for images with any of the import order UUIDs in their annotations
        # biomero-importer adds the order UUID as metadata to each imported image
        query_service = conn.getQueryService()

        imported_images = []

        for uuid in order_uuids:
            # HQL to find images with this specific UUID in map annotations
            hql = """
            SELECT DISTINCT img.id, img.name
            FROM Image img
            JOIN img.annotationLinks ial
            JOIN ial.child ann
            JOIN ann.mapValue mv
            WHERE TYPE(ann) = MapAnnotation
            AND mv.value = :uuid
            """

            import omero.sys
            params = omero.sys.ParametersI()
            params.addString("uuid", uuid)

            results = query_service.projection(hql, params, conn.SERVICE_OPTS)

            if results:
                image_ids = [result[0].val for result in results]
                uuid_images = [conn.getObject("Image", img_id)
                               for img_id in image_ids]
                uuid_images = [img for img in uuid_images if img is not None]
                imported_images.extend(uuid_images)
                logger.info(f"Found {len(uuid_images)} images for UUID {uuid}")
            else:
                logger.warning(f"No images found with UUID {uuid}")

        # Remove duplicates (shouldn't happen but just in case)
        seen_ids = set()
        unique_images = []
        for img in imported_images:
            if img.getId() not in seen_ids:
                unique_images.append(img)
                seen_ids.add(img.getId())
        imported_images = unique_images

        if not imported_images:
            # Fallback for datasets: search images directly (dataset workflows only)
            logger.info(f"No images found with order UUIDs, checking dataset {destination_id}")
            dataset_obj = conn.getObject("Dataset", destination_id)
            if dataset_obj:
                imported_images = list(dataset_obj.listChildren())
                logger.info(f"Fallback: found {len(imported_images)} images in dataset")

        if not imported_images:
            return "No imported images found for metadata addition"

        logger.info(f"Adding metadata to {len(imported_images)} imported images")

        # Build result → source mapping using the same helper as the attachment path.
        # Imported image names are post-rename; og_name_map reverse-lookup resolves them
        # back to pre-rename basenames so similarity matching works correctly.
        source_map = match_results_to_inputs(
            [img.getName() for img in imported_images],
            input_images or [],
            og_name_map
        ) if input_images else {}

        # Add metadata to each imported image (same pattern as SLURM_Get_Results.py)
        metadata_added = 0
        for img in imported_images:
            try:
                # Use the same function as SLURM_Get_Results.py for consistency
                add_image_annotations(
                    conn, slurmClient, img.getId(), job_id, wf_id=wf_id)
                metadata_added += 1
                logger.debug(f"Added metadata to image {img.getId()}: {img.getName()}")
            except Exception as img_error:
                logger.warning(f"Failed to add metadata to image {img.getId()}: {img_error}")

            if input_images:
                try:
                    source_img = source_map.get(img.getName())
                    if source_img:
                        source_tag = f"source: {source_img.getName()} (id: {source_img.getId()})"
                        existing_desc = img.getDescription() or ""
                        if source_tag not in existing_desc:
                            new_desc = (existing_desc + " | " + source_tag).strip(" |")
                            img._obj.setDescription(rstring(new_desc))
                            conn.getUpdateService().saveAndReturnObject(img._obj)
                            logger.debug(
                                f"Updated description of image {img.getId()} with source: "
                                f"{source_img.getName()} (id={source_img.getId()})")
                except Exception as desc_error:
                    logger.warning(
                        f"Could not update description for image {img.getId()}: {desc_error}")

        message = f"Added workflow metadata to {metadata_added}/{len(imported_images)} imported images"
        logger.info(message)

        return message

    except Exception as e:
        error_msg = f"Failed to add metadata to imported images: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return error_msg


def add_metadata_to_imported_plates(
    conn: BlitzGateway,
    slurmClient: SlurmClient,
    destination_id: int,
    order_uuids: List[str],
    wf_id: str,
    job_id: str
) -> str:
    """Add metadata to plates imported by biomero-importer using UUID search.
    
    For plate workflows, add metadata to the plate itself rather than individual images.
    This is more appropriate as the workflow operates on the plate level.
    
    Args:
        conn: OMERO BlitzGateway connection
        slurmClient: SlurmClient instance
        destination_id: Screen ID containing imported plates
        order_uuids: List of upload order UUIDs to search for
        wf_id: Workflow UUID for metadata
        job_id: SLURM job ID for metadata
        
    Returns:
        Status message describing metadata addition results
    """
    try:
        if not order_uuids:
            return "No upload order UUIDs provided for plate search"

        logger.info(f"Searching for plates imported with UUIDs: {order_uuids}")

        # Search for plates with any of the import order UUIDs in their annotations
        query_service = conn.getQueryService()
        imported_plates = []

        for uuid in order_uuids:
            # HQL to find plates with this specific UUID in map annotations
            hql = """
            SELECT DISTINCT plate.id, plate.name
            FROM Plate plate
            JOIN plate.annotationLinks pal
            JOIN pal.child ann
            JOIN ann.mapValue mv
            WHERE TYPE(ann) = MapAnnotation
            AND mv.value = :uuid
            """

            import omero.sys
            params = omero.sys.ParametersI()
            params.addString("uuid", uuid)

            results = query_service.projection(hql, params, conn.SERVICE_OPTS)

            if results:
                plate_ids = [result[0].val for result in results]
                uuid_plates = [conn.getObject("Plate", plate_id)
                              for plate_id in plate_ids]
                uuid_plates = [plate for plate in uuid_plates if plate is not None]
                imported_plates.extend(uuid_plates)
                logger.info(f"Found {len(uuid_plates)} plates for UUID {uuid}")
            else:
                logger.warning(f"No plates found with UUID {uuid}")

        # Remove duplicates
        seen_ids = set()
        unique_plates = []
        for plate in imported_plates:
            if plate.getId() not in seen_ids:
                unique_plates.append(plate)
                seen_ids.add(plate.getId())
        imported_plates = unique_plates

        if not imported_plates:
            # Fallback: check screen for recently imported plates
            logger.info(f"No plates found with order UUIDs, checking screen {destination_id}")
            screen_obj = conn.getObject("Screen", destination_id)
            if screen_obj:
                # Get all plates and try to find ones with import UUIDs
                all_plates = list(screen_obj.listChildren())
                for plate in all_plates:
                    # Check if this plate has any of our UUIDs in its metadata
                    annotations = plate.listAnnotations()
                    for ann in annotations:
                        if hasattr(ann, 'getMapValue') and ann.getMapValue():
                            for key, value in ann.getMapValue():
                                if value in order_uuids:
                                    imported_plates.append(plate)
                                    break
                            if plate in imported_plates:
                                break
                logger.info(f"Fallback: found {len(imported_plates)} plates in screen with matching UUIDs")

        if not imported_plates:
            return "No imported plates found for metadata addition"

        logger.info(f"Adding metadata to {len(imported_plates)} imported plates")

        # Add metadata to each imported plate
        metadata_added = 0
        for plate in imported_plates:
            try:
                # Add annotations to the plate (using same pattern but for plate objects)
                add_plate_annotations(
                    conn, slurmClient, plate.getId(), job_id, wf_id=wf_id)
                metadata_added += 1
                logger.debug(f"Added metadata to plate {plate.getId()}: {plate.getName()}")
            except Exception as plate_error:
                logger.warning(f"Failed to add metadata to plate {plate.getId()}: {plate_error}")

        message = f"Added workflow metadata to {metadata_added}/{len(imported_plates)} imported plates"
        logger.info(message)
        return message

    except Exception as e:
        error_msg = f"Failed to add metadata to imported plates: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return error_msg


def rename_label_images_in_omero(
    conn: BlitzGateway,
    orders: List[Dict[str, Any]]
) -> str:
    """Rename label zarr images in OMERO to include their parent zarr name.

    When importing label zarrs, all labels from one workflow run share the same
    name (e.g. 'fractal_cellpose_sam_segmentation') with no way to tell which
    parent image they belong to. This function renames them in OMERO to:
        'ParentStem [label_name]'
    e.g. 'Cell-Granules_result.tif [fractal_cellpose_sam_segmentation]'

    This follows the same convention as multi-series files:
        'Swiss Rolls GM1748.lif [TileScan_Day10_730]'

    The path structure in the order is:
        /.../ParentName.zarr/labels/label_name.zarr

    Args:
        conn: OMERO BlitzGateway connection.
        orders: List of upload orders (only label orders are processed).

    Returns:
        Status message describing renames performed.
    """
    label_orders = [o for o in orders if o.get('source') == 'SLURM_Results_Labels']
    if not label_orders:
        return ""

    import omero.sys
    update_service = conn.getUpdateService()
    query_service = conn.getQueryService()
    renamed_count = 0
    message = ""

    for order in label_orders:
        order_uuid = order.get('UUID')
        files = order.get('Files', [])
        if not order_uuid or not files:
            continue

        # Build filepath -> new OMERO name from path structure
        filepath_to_name = {}
        for fpath in files:
            if '/labels/' not in fpath:
                continue
            parent_part, label_part = fpath.split('/labels/', 1)
            parent_stem = os.path.basename(parent_part)
            if parent_stem.endswith('.zarr'):
                parent_stem = parent_stem[:-5]
            label_stem = os.path.basename(label_part)
            if label_stem.endswith('.zarr'):
                label_stem = label_stem[:-5]
            filepath_to_name[fpath] = f"{parent_stem} [{label_stem}]"

        if not filepath_to_name:
            logger.warning(f"No label paths with /labels/ structure in order {order_uuid}")
            continue

        # Find images biomero-importer tagged with this order UUID
        hql = """
            SELECT DISTINCT img.id
            FROM Image img
            JOIN img.annotationLinks ial
            JOIN ial.child ann
            JOIN ann.mapValue mv
            WHERE TYPE(ann) = MapAnnotation
            AND mv.value = :uuid
        """
        params_i = omero.sys.ParametersI()
        params_i.addString("uuid", order_uuid)
        results = query_service.projection(hql, params_i, conn.SERVICE_OPTS)
        if not results:
            logger.warning(f"No images found for label order UUID {order_uuid}")
            continue

        image_ids = [r[0].val for r in results]
        new_names = list(filepath_to_name.values())

        if len(image_ids) == len(files):
            # 1-to-1 match by order position
            for img_id, fpath in zip(image_ids, files):
                new_name = filepath_to_name.get(fpath)
                if not new_name:
                    continue
                img = conn.getObject("Image", img_id)
                if img and img.getName() != new_name:
                    img_obj = img._obj
                    img_obj.name = rstring(new_name)
                    update_service.saveObject(img_obj, conn.SERVICE_OPTS)
                    renamed_count += 1
                    logger.info(f"Renamed label image {img_id}: '{img.getName()}' -> '{new_name}'")
        else:
            # Count mismatch: match by label stem in current image name
            logger.warning(
                f"Image count ({len(image_ids)}) != file count ({len(files)}) "
                f"for label order {order_uuid}, falling back to name-based matching")
            for img_id in image_ids:
                img = conn.getObject("Image", img_id)
                if not img:
                    continue
                current_name = img.getName()
                matched_name = None
                for fpath, new_name in filepath_to_name.items():
                    label_stem = os.path.basename(fpath.split('/labels/')[-1])
                    if label_stem.endswith('.zarr'):
                        label_stem = label_stem[:-5]
                    if label_stem and label_stem in current_name:
                        matched_name = new_name
                        break
                if matched_name and current_name != matched_name:
                    img_obj = img._obj
                    img_obj.name = rstring(matched_name)
                    update_service.saveObject(img_obj, conn.SERVICE_OPTS)
                    renamed_count += 1
                    logger.info(f"Renamed label image {img_id}: '{current_name}' -> '{matched_name}'")

    if renamed_count > 0:
        message = f"\nRenamed {renamed_count} label image(s) in OMERO to include parent zarr name"
    logger.info(f"Label image OMERO rename: {renamed_count} image(s) renamed")
    return message


def add_plate_annotations(conn, slurmClient, plate_id, job_id, wf_id=None):
    """Add workflow metadata annotations to a plate"""
    add_object_annotations(conn, slurmClient, "Plate", plate_id, job_id, wf_id)


def add_image_annotations(conn, slurmClient, object_id, job_id, wf_id=None):
    """Add workflow metadata annotations to an image"""
    add_object_annotations(conn, slurmClient, "Image", object_id, job_id, wf_id)


def add_object_annotations(conn, slurmClient, object_type, object_id, job_id, wf_id=None):
    """Generic function to add workflow metadata annotations to any OMERO object (Image, Plate, etc.)"""
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
                # task metadata - SLURM_Import_Results.py uses the SLURM_Get_Results.py
                # namespace so the batch supervisor (find_output_images_for_batch) can
                # locate images regardless of which import script was used.
                # The 'Name' key in the annotation dict still identifies the true source.
                # All other tasks keep their own namespace.
                if task.task_name == 'SLURM_Import_Results.py':
                    ns_task = ns_wf + "/task/SLURM_Get_Results.py"
                else:
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


def process_importer_workflow(
    client: Any,
    conn: BlitzGateway,
    slurmClient: SlurmClient,
    slurm_job_id: str,
    group_name: str,
    username: str,
    permanent_storage_path: str,
    wf_id: Optional[str],
    task_id: Optional[UUID] = None,
    input_images: Optional[List[Any]] = None,

) -> Tuple[str, Dict[str, str]]:
    """Process dataset or screen image imports via biomero-importer from comprehensive permanent storage.

    This function processes image imports using the biomero-importer while ALL workflow
    results (images, CSVs, metadata, etc.) are preserved in permanent storage for
    complete data archival and future analysis. Supports both dataset and screen destinations.

    Args:
        client: OMERO script client.
        conn: OMERO BlitzGateway connection.
        slurmClient: BIOMERO SlurmClient instance.
        slurm_job_id: SLURM job ID.
        group_name: OMERO group name.
        username: Username for upload orders.
        permanent_storage_path: Path to permanent storage location.
        wf_id: Workflow ID for metadata. Defaults to None.
        task_id: Task ID for workflow tracking. Defaults to None.        

    Returns:
        Status message describing processing results.
    """
    message = ""
    og_name_map: Dict[str, str] = {}
    logger.info("Processing importer workflow...")

    # Determine destination type (dataset or screen)
    use_dataset = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET))
    use_screen = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_SCREEN))
    
    if use_dataset and use_screen:
        logger.warning("Both dataset and screen output selected - using dataset")
        use_screen = False
    elif not use_dataset and not use_screen:
        logger.error("Neither dataset nor screen output selected")
        return "Error: No valid destination type selected"
    
    destination_type = "Screen" if use_screen else "Dataset"
    logger.info(f"Selected destination type: {destination_type}")

    # Handle file renaming if enabled (only for datasets, skip for screens)
    if use_dataset:
        rename_enabled = unwrap(client.getInput(
            constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME))

        if rename_enabled:
            logger.info("Renaming files in importer storage...")
            rename_message, og_name_map = rename_files_in_importer_storage(client,
                                                                            permanent_storage_path)
            message += rename_message
            logger.info("File renaming completed")
        else:
            og_name_map = {}
            logger.info("Skipping file renaming (not enabled)")
    else:
        og_name_map = {}
        logger.info("Skipping file renaming for screen destination")

    # Get destination info from parameters
    if use_screen:
        destination_name = unwrap(client.getInput(
            constants.results.OUTPUT_ATTACH_NEW_SCREEN_NAME))
        create_new_destination = unwrap(client.getInput(
            constants.results.OUTPUT_ATTACH_NEW_SCREEN_DUPLICATE))
        explicit_destination_id = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID))
    else:
        destination_name = unwrap(client.getInput(
            constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME))
        create_new_destination = unwrap(client.getInput(
            constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE))
        explicit_destination_id = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_NEW_DATASET_ID))
    
    logger.info(f"Setting up {destination_type.lower()} '{destination_name}'...")
    destination_id = None

    # If an explicit ID is provided, use it directly — skip name lookup entirely
    if explicit_destination_id:
        destination_id = int(explicit_destination_id)
        create_new_destination = False
        logger.info(
            f"Using explicit {destination_type}_ID: {destination_id} (skipping name lookup)")
    # Check if destination exists or create new one
    elif not create_new_destination:
        try:
            existing_objects_w_name = [d.id for d in conn.getObjects(
                destination_type, attributes={"name": destination_name})]
            if existing_objects_w_name:
                destination_id = existing_objects_w_name[0]
                create_new_destination = False
                logger.info(f"Using existing {destination_type.lower()} ID: {destination_id}")
            else:
                create_new_destination = True
                logger.info(f"{destination_type} not found, will create new one")
        except Exception:
            create_new_destination = True
            logger.info(
                f"Error checking for existing {destination_type.lower()}, will create new one")

    if create_new_destination:
        logger.info(f"Creating new {destination_type.lower()}...")
        if use_screen:
            destination_obj = omero.model.ScreenI()
        else:
            destination_obj = omero.model.DatasetI()
        
        destination_obj.name = rstring(destination_name)
        desc = f"Images from SLURM job {slurm_job_id}"
        if wf_id:
            desc += f" (Workflow {wf_id})"
        destination_obj.description = rstring(desc)
        update_service = conn.getUpdateService()
        destination_obj = update_service.saveAndReturnObject(destination_obj)
        destination_id = destination_obj.id.val
        logger.info(f"Created new {destination_type.lower()} ID: {destination_id}")

    # Create upload order for images only
    logger.info("Creating upload orders for biomero-importer...")
    orders = create_upload_orders_for_results(
        group_name, username, destination_type, destination_id,
        permanent_storage_path, wf_id, client)

    if orders:
        message += f"\nCreated {len(orders)} upload orders for biomero-importer ({destination_type.lower()}):"
        for order in orders:
            file_count = len(order.get('Files', []))
            message += f"\n  - Order {order['UUID']}: {file_count} files"
            logger.info(
                f"Created upload order {order['UUID']} with {file_count} files")

        # NEW: Wait for import completion instead of returning immediately
        logger.info("Waiting for import completion...")
        message += f"\nWaiting for import completion of {len(orders)} orders..."

        # Configure timeout - can be made configurable via script parameters if needed
        import_timeout = 3600  # 1 hour default

        all_successful, summary, failed_uuids = wait_for_import_completion(
            orders, conn, timeout=import_timeout, poll_interval=10,
            task_id=task_id, slurmClient=slurmClient)

        message += f"\n{summary}"

        if all_successful:
            logger.info("All imports completed successfully")
            message += f"\nAll {destination_type.lower()} imports completed successfully!"

            # Post-processing: Add metadata to newly imported images/plates using UUID search
            logger.info(f"Adding metadata to imported {destination_type.lower()}s...")
            order_uuids = [order.get('UUID')
                           for order in orders if order.get('UUID')]
            post_processing_message = add_metadata_to_imported_images(
                conn, slurmClient, destination_id, order_uuids, wf_id, slurm_job_id, destination_type,
                input_images=input_images, og_name_map=og_name_map)
            if post_processing_message:
                message += f"\n{post_processing_message}"

            # Rename label zarr images in OMERO: 'label_name' -> 'ParentZarr [label_name]'
            rename_msg = rename_label_images_in_omero(conn, orders)
            if rename_msg:
                message += rename_msg
        else:
            logger.error(f"Some imports failed: {failed_uuids}")
            message += f"\nSome imports failed for UUIDs: {failed_uuids}"
            # Import failures are logged but don't stop the workflow
    else:
        error_msg = "CRITICAL: No image files found for importer processing - workflow failed!"
        logger.error(error_msg)
        logger.error(f"Searched in: {permanent_storage_path}")
        logger.error(f"Supported extensions: {SUPPORTED_IMAGE_EXTENSIONS}")
        sys.exit(1)  # Exit with failure code - this should trigger workflow failure

    return message, og_name_map


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
    wf_id: Optional[str],
    input_images: Optional[List[Any]] = None,
    og_name_map: Optional[Dict[str, str]] = None
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
        input_images: Known workflow input images for attachment matching. Defaults to None.
        og_name_map: Mapping of renamed file paths to their pre-rename original filenames.
            Produced by rename_files_in_importer_storage. Defaults to None.

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
                                             wf_id=wf_id,
                                             input_images=input_images,
                                             og_name_map=og_name_map)
        message += msg

    # Process project and plate zip attachments - EXACTLY like Get_Results
    if (unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PROJECT)) or
            unwrap(client.getInput(constants.results.OUTPUT_ATTACH_DATASET)) or
            unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE))):
        zip_path = os.path.join(permanent_storage_path, filename)
        message = upload_zip_to_omero(
            client, conn, message, slurm_job_id, projects, zip_path, wf_id)

        # Create and attach metadata CSV for ZIP attachments
        if projects and metadata_files:
            message = upload_metadata_csv_to_omero(
                client, conn, message, slurm_job_id, projects, metadata_files, wf_id)

    return message


def process_non_image_file_outputs(
    conn: BlitzGateway,
    permanent_storage_path: str,
    projects: List[Any],
    slurm_job_id: str,
    metadata_files: Optional[List[str]] = None,
    wf_id: Optional[str] = None,
) -> str:
    """Attach individual non-image, non-CSV output files as OMERO file annotations.

    Scans *permanent_storage_path* for files that are neither images (handled
    by the importer), nor CSV tables (handled by process_slurm_tables), nor the
    bulk zip archive, nor SLURM job logs.  Every remaining file — e.g. NumPy
    arrays (.npy/.npz), model weights (.pt/.h5/.pkl), JSON/YAML configs — is
    registered as a :class:`omero.model.FileAnnotation` (in-place when the
    importer is available) and linked to every object in *projects*.

    This enables bilayers-style workflows that declare ``array``, ``file``, or
    ``executable`` output types to surface those artefacts directly in OMERO
    without relying solely on the bulk zip.

    Args:
        conn: Open OMERO BlitzGateway connection.
        permanent_storage_path: Path to the permanent workflow storage directory
            (the ``.analyzed/{uuid}/{timestamp}`` directory).
        projects: List of OMERO projects/plates/datasets to link annotations to.
        slurm_job_id: SLURM job ID used in annotation descriptions.
        metadata_files: File paths that were already attached as metadata CSVs
            (skipped to avoid duplication). Defaults to None.
        wf_id: Workflow UUID used in annotation descriptions. Defaults to None.

    Returns:
        Status message describing what was attached.
    """
    if not projects:
        logger.warning("process_non_image_file_outputs: no OMERO objects provided; skipping")
        return "\nNo OMERO objects available for file annotation."

    # Build set of paths to skip (already handled by other processors)
    skip_paths: set = set(metadata_files or [])

    # Extensions that are handled by other processors
    skip_extensions = (
        tuple(SUPPORTED_IMAGE_EXTENSIONS)
        + ('.csv', '.zip', '.log')
    )

    namespace = NSCREATED + "/SLURM/SLURM_FILE_OUTPUTS"
    message = ""
    attached_count = 0
    skipped_count = 0

    for dirpath, _dirnames, filenames in os.walk(permanent_storage_path):
        for fname in filenames:
            file_path = os.path.join(dirpath, fname)

            # Skip already-handled paths
            if file_path in skip_paths:
                skipped_count += 1
                continue

            # Skip by extension
            lower = fname.lower()
            if any(lower.endswith(ext) for ext in skip_extensions):
                skipped_count += 1
                continue

            # Skip hidden/system files
            if fname.startswith('.'):
                skipped_count += 1
                continue

            # Guess mimetype; fall back to application/octet-stream
            import mimetypes
            mimetype, _ = mimetypes.guess_type(file_path)
            if mimetype is None:
                mimetype = "application/octet-stream"

            description = (
                f"Non-image output from SLURM job {slurm_job_id}"
                + (f" (Workflow {wf_id})" if wf_id else "")
            )

            try:
                file_ann = create_file_annotation_inplace(
                    conn, file_path, mimetype, namespace, description)
                logger.info(f"Created file annotation for {file_path} (id={file_ann.getId()})")
            except Exception as e:
                logger.error(f"Failed to create file annotation for {file_path}: {e}")
                message += f"\nFailed to annotate {fname}: {e}"
                continue

            # Link annotation to all target OMERO objects
            for obj in projects:
                try:
                    obj.linkAnnotation(file_ann)
                    logger.debug(f"Linked {fname} annotation to {type(obj).__name__} {obj.getId()}")
                except Exception as e:
                    logger.error(f"Failed to link annotation for {file_path} to {obj}: {e}")
                    message += f"\nFailed to link {fname} to {type(obj).__name__} {obj.getId()}: {e}"

            attached_count += 1

    summary = f"\nAttached {attached_count} non-image output file(s) as annotations (skipped {skipped_count} already-handled files)."
    logger.info(summary)
    return message + summary


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
    if unwrap(client.getInput(constants.CLEANUP)):
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

    # Step 4: Consistency validation
    # job_tracker can return stale data when SLURM recycles job IDs, so it is
    # treated as a lower-priority source.  If the authoritative sources
    # (script_parameter and/or slurm_log) agree with each other, trust them
    # and only warn about a job_tracker mismatch instead of failing hard.
    if validated_sources:
        unique_ids = set(validated_sources.values())
        if len(unique_ids) > 1:
            authoritative = {k: v for k, v in validated_sources.items()
                             if k in ('script_parameter', 'slurm_log')}
            authoritative_ids = set(authoritative.values())
            if len(authoritative_ids) <= 1 and authoritative:
                # Authoritative sources agree; job_tracker is likely stale
                # (SLURM recycles job IDs across workflows)
                logger.warning(
                    f"Workflow ID mismatch in job_tracker (likely stale SLURM job ID reuse). "
                    f"Trusting authoritative sources. All sources: {dict(validated_sources)}")
                wf_id = list(authoritative.values())[0]
            else:
                raise RuntimeError(
                    f"Workflow ID inconsistency: {dict(validated_sources)}")
        else:
            wf_id = list(validated_sources.values())[0]

        sources = list(validated_sources.keys())
        logger.info(f"Workflow ID validated across {sources}: {wf_id}")
        return wf_id

    # Step 5: Generate fallback (only if no sources were available)
    # Convert UUID to string to avoid PostgreSQL type casting issues
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
                logger.info(
                    f"Extracted workflow UUID from directory '{dir_name}': {extracted_uuid}")
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


def check_write_access_for_importer(group_name: str, test_wf_id: str = None, conn: BlitzGateway = None) -> Tuple[bool, str]:
    """
    Check if we have write access to create importer storage directories.
    
    This function validates that we can create the required directory structure
    and write files to the permanent storage location that will be needed for
    the importer workflow. This allows us to fail fast if there are permission
    issues before starting expensive SLURM operations.
    
    Args:
        group_name: Name of the OMERO group
        test_wf_id: Test workflow ID to use for path creation (optional)
        conn: Existing OMERO BlitzGateway connection (optional)
        
    Returns:
        Tuple of (success: bool, message: str) indicating if write access works
    """
    if not IMPORTER_ENABLED:
        return True, "Importer not enabled, write check not needed"
        
    if not IMPORTER_AVAILABLE:
        return False, "Importer is enabled but biomero-importer module is not available"
    
    try:
        # Use a test workflow ID if none provided
        if test_wf_id is None:
            import uuid
            test_wf_id = str(uuid.uuid4())
            
        # Get the path where we would need to write
        test_results_path = get_workflow_results_path(group_name, test_wf_id)
        
        # Try to create the directory structure
        os.makedirs(test_results_path, exist_ok=True)
        
        # Try to write a test file
        test_file_path = os.path.join(test_results_path, "write_access_test.txt")
        with open(test_file_path, 'w') as f:
            f.write("BIOMERO write access test")
            
        # Verify we can read it back
        with open(test_file_path, 'r') as f:
            content = f.read()
            if content != "BIOMERO write access test":
                return False, f"Write test failed: file content mismatch in {test_results_path}"
                
        # Clean up test file and directory
        os.remove(test_file_path)
        
        # Try to remove the test directory structure (only if empty)
        try:
            # Remove the timestamp directory if it's empty
            os.rmdir(test_results_path)
            # Try to remove the workflow ID directory if it's empty
            workflow_dir = os.path.dirname(test_results_path)
            try:
                os.rmdir(workflow_dir)
            except OSError:
                pass  # Directory not empty, that's fine
            # Try to remove the .analyzed directory if it's empty
            analyzed_dir = os.path.dirname(workflow_dir)
            try:
                os.rmdir(analyzed_dir)
            except OSError:
                pass  # Directory not empty, that's fine
        except OSError:
            pass  # Directories not empty or other issues, that's fine for cleanup
            
        return True, f"Write access confirmed for {test_results_path}"
        
    except PermissionError as e:
        # Handle permission errors with specific admin guidance
        try:
            base_path = get_importer_group_base_path(group_name)
        except Exception:
            base_path = "path determination failed"
            
        # Get user groups for security filtering (only show groups the user belongs to)
        user_groups = get_current_user_groups(conn) if conn else []
        
        # Simplify accessible mappings display 
        accessible_mappings = []
        try:
            with open('/opt/omero/server/biomero-config.json', 'r') as f:
                config = json.load(f)
            group_mappings = config.get('group_mappings', {})
            
            for group_id, mapping in group_mappings.items():
                group_name_mapped = mapping.get('groupName', 'unknown')
                if group_name_mapped in user_groups:
                    folder_mapped = mapping.get('folder', 'default')
                    accessible_mappings.append(f"  - {group_name_mapped} -> {folder_mapped}")
        except Exception as config_e:
            pass  # Config issues handled in resolution
        
        mappings_text = "\n".join(accessible_mappings) if accessible_mappings else "  (no configured mappings found for your groups)"
        
        error_message = f"""Permission denied writing to '{base_path}' for group '{group_name}'.

Your groups with configured storage mappings:
{mappings_text}

Resolution:
1) User: Try switching to a different group above (if write access works there)
2) Admin: Add or change storage mapping for '{group_name}' group in the biomero importer admin page.  
3) IT: Grant write permissions on '{base_path}' for the biomero user account"""
        
        return False, error_message
    except Exception as e:
        # Handle all other errors with simple, clear message
        error_message = f"Write access check failed: {str(e)}"
        if test_results_path:
            error_message += f"\nAttempted path: {test_results_path}"
        
        error_message += "\n\nContact your BIOMERO administrator to investigate this error."
        
        return False, error_message


def runScript() -> None:
    """Main entry point for SLURM results import with comprehensive data handling.

    Orchestrates the complete import workflow including data extraction, permanent
    storage setup, and execution of requested import operations (attachments,
    dataset imports, tables, etc.).

    The workflow handles both standard OMERO import workflows and importer-enabled
    dataset imports based on configuration.
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
            scripts.String(constants.results.WORKFLOW_UUID,
                           optional=True, grouping="01.2",
                           description="UUID of the workflow that generated these results (auto-extracted from SLURM log if not provided)"),
            scripts.String(constants.results.TASK_ID,
                           optional=True, grouping="01.3",
                           description="Task ID for biomero workflow tracking and status updates"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_PROJECT,
                         optional=False,
                         grouping="02",
                         description="Attach a bulk zip archive of all results to a project (backup/download-all). Use the individual file annotations option to access specific output files without downloading the full archive.",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_PROJECT_ID,
                         optional=True, grouping="02.1",
                         description="Project to attach workflow results to",
                         values=_projects),
            scripts.Bool(constants.results.OUTPUT_ATTACH_DATASET,
                         optional=False,
                         grouping="03",
                         description="Attach a bulk zip archive of all results to a dataset (used when dataset has no parent project). Use the individual file annotations option to access specific output files without downloading the full archive.",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_DATASET_ID,
                         optional=True, grouping="03.1",
                         description="Dataset to attach workflow results to",
                         values=getUserDatasets()),
            scripts.Bool(constants.results.OUTPUT_ATTACH_PLATE,
                         optional=False,
                         grouping="04",
                         description="Attach a bulk zip archive of all results to a plate. Use the individual file annotations option to access specific output files without downloading the full archive.",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_PLATE_ID,
                         optional=True, grouping="04.1",
                         description="Plate to attach workflow results to",
                         values=_plates),
            scripts.Bool(constants.results.OUTPUT_ATTACH_OG_IMAGES,
                         optional=False,
                         grouping="05",
                         description="Attach all results to original images as attachments",
                         default=False),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET,
                         optional=False,
                         grouping="05",
                         description="Import all result as a new dataset via biomero-importer",
                         default=True),
            scripts.String(constants.results.OUTPUT_ATTACH_NEW_DATASET_NAME,
                           optional=True,
                           grouping="05.1",
                           description="Name for the new dataset w/ results",
                           default="Imported_Results"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET_DUPLICATE,
                         optional=True,
                         grouping="05.2",
                         description="If there is already a dataset with this name, still create new one? (True) or add to it? (False) ",
                         default=False),
            scripts.Long(constants.results.OUTPUT_ATTACH_NEW_DATASET_ID,
                         optional=True,
                         grouping="05.25",
                         description="Pinpoint an exact Dataset by OMERO ID. If provided, this ID wins over name lookup and Allow duplicate settings."),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME,
                         optional=True,
                         grouping="05.3",
                         description="Rename all imported files as below. You can use variables {original_file}, {original_ext}, {file}, and {ext}. E.g. {original_file}_IMPORTED.{ext}",
                         default=True),
            scripts.String(constants.results.OUTPUT_ATTACH_NEW_DATASET_RENAME_NAME,
                           optional=True,
                           grouping="05.4",
                           description="A new name for the imported images.",
                           default="{original_file}_IMPORTED.{ext}"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_SCREEN,
                         optional=False,
                         grouping="06",
                         description="Import all result as a new screen via biomero-importer",
                         default=False),
            scripts.String(constants.results.OUTPUT_ATTACH_NEW_SCREEN_NAME,
                           optional=True,
                           grouping="06.1",
                           description="Name for the new screen w/ results",
                           default="Imported_Results"),
            scripts.Bool(constants.results.OUTPUT_ATTACH_NEW_SCREEN_DUPLICATE,
                         optional=True,
                         grouping="06.2",
                         description="If there is already a screen with this name, still create new one? (True) or add to it? (False) ",
                         default=False),
            scripts.Long(constants.results.OUTPUT_ATTACH_NEW_SCREEN_ID,
                         optional=True,
                         grouping="06.25",
                         description="Pinpoint an exact Screen by OMERO ID. If provided, this ID wins over name lookup and Allow duplicate settings."),
            scripts.Bool(constants.results.IMPORT_LABEL_ZARRS,
                         optional=True,
                         grouping="07",
                         description="Also import label zarr directories (segmentation results) as separate datasets",
                         default=True),
            scripts.Bool(constants.results.IMPORT_ONLY_LABELS,
                         optional=True,
                         grouping="07.1",
                         description="Import ONLY label zarr directories (requires Import_Label_Zarrs=true). Skips main image import.",
                         default=True),
            scripts.Bool(constants.results.OUTPUT_ATTACH_TABLE,
                         optional=False,
                         grouping="08",
                         description="Add all csv files as OMERO.tables to the chosen dataset",
                         default=True),
            scripts.Bool(constants.results.OUTPUT_ATTACH_TABLE_DATASET,
                         optional=True,
                         grouping="08.1",
                         description="Attach to the dataset chosen below",
                         default=True),
            scripts.List(constants.results.OUTPUT_ATTACH_TABLE_DATASET_ID,
                         optional=True,
                         grouping="08.2",
                         description="Dataset to attach workflow results to",
                         values=_datasets),
            scripts.Bool(constants.results.OUTPUT_ATTACH_TABLE_PLATE,
                         optional=True,
                         grouping="08.3",
                         description="Attach to the plate chosen below",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_TABLE_PLATE_ID,
                         optional=True,
                         grouping="08.4",
                         description="Plate to attach workflow results to",
                         values=_plates),
            scripts.Bool(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS,
                         optional=True,
                         grouping="09",
                         description="Attach individual non-image output files (e.g. NumPy arrays, model weights, JSON/YAML configs) as OMERO file annotations. Bilayers workflows with 'array', 'file', or 'executable' output types benefit most from this option. Images and CSVs are handled separately.",
                         default=False),
            scripts.Bool(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_DATASET,
                         optional=True,
                         grouping="09.1",
                         description="Attach to the dataset chosen below",
                         default=True),
            scripts.List(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_DATASET_ID,
                         optional=True,
                         grouping="09.2",
                         description="Dataset to attach non-image file outputs to",
                         values=_datasets),
            scripts.Bool(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_PLATE,
                         optional=True,
                         grouping="09.3",
                         description="Attach to the plate chosen below",
                         default=False),
            scripts.List(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_PLATE_ID,
                         optional=True,
                         grouping="09.4",
                         description="Plate to attach non-image file outputs to",
                         values=_plates),
            scripts.Bool(constants.CLEANUP,
                         optional=True,
                         grouping="10",
                         description="Cleanup temporary files after completion (default: True). Turn off for debugging.",
                         default=True),
            scripts.Bool(constants.results.TEST_WRITE_PERMISSIONS_ONLY,
                         optional=True,
                         grouping="10.1",
                         description="DRY-RUN ONLY: Test write permissions to importer storage and exit (no actual import). " + \
                                    "Use this to validate storage configuration before running expensive workflows.",
                         default=False),


            namespaces=[omero.constants.namespaces.NSDYNAMIC],
            version=VERSION,
            authors=["Torec Luik"],
            institutions=["Amsterdam UMC"],
            contact='cellularimaging@amsterdamumc.nl',
            authorsInstitutions=[[1]]
        )

        try:
            # Initialize all variables that could be used in exception handling/cleanup
            # This prevents UnboundLocalError if an early exception occurs
            task_id = None
            slurm_job_id = None
            wf_id = None
            slurm_data_path = None
            folder = None
            log_file = None
            script_failed = False
            
            scriptParams = client.getInputs(unwrap=True)
            conn = BlitzGateway(client_obj=client)

            message = ""
            logger.info(f"Import Results: {scriptParams}\n")

            # Check if this is a write access validation request (DRY-RUN ONLY)
            test_write_only = unwrap(client.getInput(constants.results.TEST_WRITE_PERMISSIONS_ONLY)) or False
            if test_write_only:
                logger.info("DRY-RUN: Testing write access for importer storage only")
                
                # Get group name for path resolution
                group_name = conn.getGroupFromContext().getName()
                
                # Perform the write access check with existing connection
                success, result_message = check_write_access_for_importer(group_name, conn=conn)
                client.setOutput("Message", rstring(result_message))
                if not success:
                    raise RuntimeError(result_message)
                logger.info(f"Write access check successful: {result_message}")
                return

            # Get task_id if provided for status updates
            task_id = None
            try:
                task_id_input = unwrap(client.getInput(constants.results.TASK_ID))
                if task_id_input and task_id_input.strip():
                    # Convert to UUID object
                    task_id = uuid.UUID(task_id_input.strip())
                    logger.info(f"Using task ID {task_id} for status updates")
                else:
                    logger.debug(
                        "No task ID provided - status updates disabled")
            except (ValueError, TypeError, AttributeError) as e:
                logger.debug(
                    f"No valid task ID provided - status updates disabled: {e}")

            # Job id
            slurm_job_id = unwrap(client.getInput(
                constants.results.OUTPUT_SLURM_JOB_ID)).strip()

            # Get and validate workflow ID using proper UUID parsing
            script_wf_id = None
            try:
                wf_uuid_input = unwrap(client.getInput(constants.results.WORKFLOW_UUID))
                if wf_uuid_input and wf_uuid_input.strip():
                    # Validate UUID format immediately (keep as string for re.match compatibility)
                    script_wf_id = str(uuid.UUID(wf_uuid_input.strip()))
                    logger.info(
                        f"Workflow UUID parameter validated: {script_wf_id}")
            except (ValueError, TypeError, AttributeError) as e:
                logger.warning(f"Invalid workflow UUID format: {e}")
                script_wf_id = None

            # Resolve workflow ID from all available sources with validation
            wf_id = resolve_workflow_id(
                script_wf_id, slurmClient, slurm_job_id)

            # Get group and user info
            group_name = conn.getGroupFromContext().getName()
            username = conn.getUser().getName()

            # Determine which operations are requested
            use_importer_for_datasets = unwrap(client.getInput(
                constants.results.OUTPUT_ATTACH_NEW_DATASET))
            use_importer_for_screens = unwrap(client.getInput(
                constants.results.OUTPUT_ATTACH_NEW_SCREEN))
            process_csv_tables = unwrap(client.getInput(
                constants.results.OUTPUT_ATTACH_TABLE))
            process_file_outputs = unwrap(client.getInput(
                constants.results.OUTPUT_ATTACH_FILE_OUTPUTS)) or False
            process_attachments = (unwrap(client.getInput(constants.results.OUTPUT_ATTACH_OG_IMAGES)) or
                                   unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PROJECT)) or
                                   unwrap(client.getInput(constants.results.OUTPUT_ATTACH_DATASET)) or
                                   unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)))

            # Debug the workflow decisions
            logger.info(
                f"Workflow decisions: use_importer_for_datasets={use_importer_for_datasets}, use_importer_for_screens={use_importer_for_screens}, process_csv_tables={process_csv_tables}, process_attachments={process_attachments}, process_file_outputs={process_file_outputs}")

            # Initialize importer only if needed for dataset or screen imports
            importer_initialized = False
            if use_importer_for_datasets or use_importer_for_screens:
                importer_initialized = initialize_importer()

            # Ask job State
            if unwrap(client.getInput(constants.results.OUTPUT_COMPLETED_JOB)):
                _, result = slurmClient.check_job_status([slurm_job_id])
                message += f"\n{result.stdout}"

            # Pull project from OMERO for import operations
            projects = []  # note, can also be plate or dataset now
            if unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PROJECT)):
                project_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PROJECT_ID))
                projects = [conn.getObject("Project", p.split(":")[0])
                            for p in project_ids]
            if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_DATASET)):
                dataset_ids = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_DATASET_ID))
                projects = [conn.getObject("Dataset", p.split(":")[0])
                            for p in dataset_ids]
            if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_PLATE)):
                plate_ids = unwrap(client.getInput(
                    constants.results.OUTPUT_ATTACH_PLATE_ID))
                projects = [conn.getObject("Plate", p.split(":")[0])
                            for p in plate_ids]

            # MAIN WORKFLOW EXECUTION - runs regardless of project/plate attachment settings
            folder = None
            log_file = None
            slurm_data_path = None

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

            # Update task status to IMPORTING if task_id is available
            if task_id and slurmClient.track_workflows:
                try:
                    slurmClient.workflowTracker.update_task_status(
                        task_id, "IMPORTING")
                    logger.info(f"Updated task {task_id} status to IMPORTING")
                except Exception as db_e:
                    logger.warning(f"Failed to update task status: {db_e}")

            logger.info("Copying logfile from SLURM...")
            local_tmp_storage = None
            _logfile_max_retries = 5
            _logfile_retry_delay = 10  # seconds between retries
            for _logfile_attempt in range(1, _logfile_max_retries + 1):
                try:
                    tup = slurmClient.get_logfile_from_slurm(slurm_job_id)
                    (local_tmp_storage, log_file, get_result) = tup
                    logger.info(
                        f"Logfile copied successfully on attempt {_logfile_attempt}: {log_file}")
                    break
                except FileNotFoundError as _logfile_err:
                    if _logfile_attempt < _logfile_max_retries:
                        logger.warning(
                            f"Logfile omero-{slurm_job_id}.log not yet available on SLURM "
                            f"(attempt {_logfile_attempt}/{_logfile_max_retries}), "
                            f"retrying in {_logfile_retry_delay}s...")
                        import time as _timesleep
                        _timesleep.sleep(_logfile_retry_delay)
                    else:
                        logger.error(
                            f"Logfile omero-{slurm_job_id}.log not found after "
                            f"{_logfile_max_retries} attempts: {_logfile_err}")
                        raise
            # Ensure local_tmp_storage has trailing slash (required for copy operations)
            if local_tmp_storage and not local_tmp_storage.endswith('/'):
                local_tmp_storage += '/'
            message += "\nSuccessfully copied logfile."
            logger.info(f"Logfile copied successfully: {log_file}")

            logger.info("Extracting SLURM results...")
            slurm_data_path, permanent_storage_path, temporary_zip_file_path, filename = (
                None, None, None, None)
            extraction_error = None
            try:
                slurm_data_path, permanent_storage_path, temporary_zip_file_path, filename, message = extract_slurm_results_zip(
                    slurmClient, slurm_job_id, local_tmp_storage, group_name, wf_id, message)
                message += f"\nSuccessfully extracted SLURM results: {permanent_storage_path}"
                logger.info(
                    f"SLURM results extracted and available: {permanent_storage_path}")
            except Exception as e:
                logger.error(f"Failed to extract SLURM results: {e}")
                message += f"\nFailed to extract SLURM results: {e}"
                extraction_error = e  # defer raise so log is still uploaded below

            # Copy log to permanent storage (only possible when extraction succeeded),
            # then upload from the best available path so the in-place symlink stays valid.
            # If extraction failed we fall back to the /tmp path so the log is still
            # attached to OMERO before we re-raise.
            log_to_upload = log_file  # fallback: tmp path
            if permanent_storage_path and log_file and os.path.exists(log_file):
                try:
                    log_dest = os.path.join(
                        permanent_storage_path, os.path.basename(log_file))
                    shutil.copy2(log_file, log_dest)
                    log_to_upload = log_dest  # prefer permanent path for in-place import
                    logger.info(
                        f"Copied log file to permanent storage: {log_dest}")
                    message += f"\nLog file copied to permanent storage."
                except Exception as _log_copy_err:
                    logger.warning(
                        f"Failed to copy log file to permanent storage: {_log_copy_err}")

            logger.info("Uploading logfile to OMERO...")
            try:
                message = upload_log_to_omero(
                    client, conn, message, slurm_job_id,
                    projects, log_to_upload, wf_id=wf_id)
            except Exception as _log_upload_err:
                # Log the failure but don't let it shadow a pending extraction error
                logger.warning(f"Log upload failed: {_log_upload_err}")

            # Always propagate extraction failure, even if log upload also failed
            if extraction_error:
                raise extraction_error

            # Load input images once — used for both importer description tagging
            # and attachment matching (scenarios 1-3 + 4 with rename).
            input_images = []
            _lookup_task_id = task_id
            if not _lookup_task_id and slurmClient.track_workflows and slurm_job_id:
                try:
                    _lookup_task_id = slurmClient.jobAccounting.get_task_id(slurm_job_id)
                    logger.info(f"Resolved task ID from job accounting: {_lookup_task_id}")
                except Exception as _te:
                    logger.debug(f"Could not resolve task ID from job accounting: {_te}")
            if slurmClient.track_workflows and _lookup_task_id:
                try:
                    _task = slurmClient.workflowTracker.repository.get(_lookup_task_id)
                    _input_data = _task.input_data if _task else None
                    if isinstance(_input_data, dict):
                        _image_ids = _input_data.get('IDs', [])
                    elif isinstance(_input_data, list):
                        _image_ids = _input_data
                    else:
                        _image_ids = []
                    if not _image_ids and wf_id and slurmClient.track_workflows:
                        try:
                            _wf = slurmClient.workflowTracker.repository.get(wf_id)
                            for _tid in _wf.tasks:
                                _t = slurmClient.workflowTracker.repository.get(_tid)
                                _td = _t.input_data if _t else None
                                if isinstance(_td, dict):
                                    _ids = _td.get('IDs', [])
                                elif isinstance(_td, list) and all(isinstance(x, int) for x in _td):
                                    _ids = _td
                                else:
                                    _ids = []
                                if _ids:
                                    _image_ids = _ids
                                    logger.info(
                                        f"Found image IDs in task '{_t.task_name}' ({_tid}): {_image_ids}")
                                    break
                        except Exception as _we:
                            logger.debug(f"Could not walk workflow tasks for image IDs: {_we}")
                    if _image_ids:
                        input_images = [
                            img for img in conn.getObjects(
                                "Image", ids=[int(i) for i in _image_ids])
                            if img
                        ]
                        logger.info(
                            f"Loaded {len(input_images)} input images for matching: "
                            f"{[img.getId() for img in input_images]}")
                except Exception as _ie:
                    logger.warning(f"Could not load input images from task for matching: {_ie}")

            # IMPORTER WORKFLOW - Dataset and screen image imports
            og_name_map = {}
            if use_importer_for_datasets or use_importer_for_screens:
                importer_message, og_name_map = process_importer_workflow(
                    client, conn, slurmClient, slurm_job_id,
                    group_name, username,
                    permanent_storage_path, wf_id, task_id,
                    input_images=input_images)
                message += importer_message

            # Create metadata CSV (after import, to avoid duplicate metadata in import dir)
            logger.info("Creating metadata CSV...")
            metadata_files = create_metadata_csv(
                conn, slurmClient, permanent_storage_path, slurm_job_id, wf_id)
            message += f"\nCreated metadata files: {metadata_files}"
            logger.info(f"Metadata files created: {metadata_files}")

            # SLURM TABLES PROCESSING
            if process_csv_tables:
                zip_message = process_slurm_tables(
                    client, conn, process_csv_tables,
                    permanent_storage_path, wf_id)
                message += zip_message

            # ZIP ATTACHMENT OPERATIONS
            if process_attachments:
                attachment_message = process_zip_attachments(
                    client, conn, permanent_storage_path,
                    filename, projects, slurm_job_id,
                    metadata_files, wf_id, input_images=input_images,
                    og_name_map=og_name_map)
                message += attachment_message

            # NON-IMAGE FILE OUTPUT ANNOTATIONS (bilayers array/file/executable outputs)
            if process_file_outputs:
                file_output_targets = []
                if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_DATASET)):
                    dataset_ids = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_DATASET_ID))
                    if dataset_ids:
                        file_output_targets += [conn.getObject("Dataset", d.split(":")[0]) for d in dataset_ids]
                if unwrap(client.getInput(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_PLATE)):
                    plate_ids = unwrap(client.getInput(constants.results.OUTPUT_ATTACH_FILE_OUTPUTS_PLATE_ID))
                    if plate_ids:
                        file_output_targets += [conn.getObject("Plate", p.split(":")[0]) for p in plate_ids]
                file_output_targets = [t for t in file_output_targets if t is not None]
                file_outputs_message = process_non_image_file_outputs(
                    conn, permanent_storage_path, file_output_targets,
                    slurm_job_id, metadata_files=metadata_files, wf_id=wf_id)
                message += file_outputs_message

            logger.info("Results imported successfully")
            message += "\nWorkflow execution completed successfully."

            # Update task status to IMPORTED on successful import
            if task_id and slurmClient and slurmClient.track_workflows:
                try:
                    slurmClient.workflowTracker.update_task_status(
                        task_id, "IMPORTED")
                    logger.info(f"Updated task {task_id} status to IMPORTED")
                except Exception as db_e:
                    logger.warning(
                        f"Failed to update task completion status: {db_e}")

        except Exception as e:
            script_failed = True
            logger.error(f"Script execution failed: {e}", exc_info=True)
            message += f"\nScript execution failed: {e}"

            # Update task status to FAILED if task_id is available
            if task_id and slurmClient and slurmClient.track_workflows:
                try:
                    slurmClient.workflowTracker.update_task_status(
                        task_id, "FAILED")
                    slurmClient.workflowTracker.fail_task(
                        task_id, f"Import failed: {str(e)}")
                    logger.info(f"Updated task {task_id} status to FAILED")
                except Exception as db_e:
                    logger.warning(
                        f"Failed to update task failure status: {db_e}")

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
            # Only run cleanup if we have the required variables
            if slurm_job_id is not None and slurmClient is not None:
                try:
                    cleanup_message = cleanup_resources(
                        client, slurmClient, slurm_job_id,
                        slurm_data_path, folder, log_file)
                    message += cleanup_message
                except Exception as cleanup_error:
                    logger.error(f"Cleanup failed: {cleanup_error}", exc_info=True)
                    # Don't add cleanup errors to message if script already failed
            else:
                logger.debug("Skipping cleanup - required variables not initialized")

            # Always try to set outputs - but don't overwrite failure messages
            try:
                if wf_id is not None:
                    client.setOutput(constants.results.WORKFLOW_UUID_OUTPUT, rstring(str(wf_id)))
                # Only set success message if we haven't already set a failure message
                if not script_failed:
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
