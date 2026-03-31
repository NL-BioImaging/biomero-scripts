#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OMERO script to run training workflows on SLURM via BIOMERO.

Supports two data input modes:
- "annotate": reads annotate_ai tracking table, rasterizes ROIs to masks
- "paired": matches images with masks by filename suffix

Generates config.yaml, calls run_workflow() with training_mode=true,
and uploads the trained model + results back to OMERO.
"""

import os
import sys
import random
import tempfile
import time as timesleep
import traceback
from io import BytesIO

import numpy as np
import omero
import omero.scripts as omscripts
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong, rbool, rfloat, rint, unwrap, wrap

from biomero import SlurmClient

VERSION = "0.1.0"

# Namespaces for storing training artifacts
NS_TRAINING_MODEL = "biomero.training.model"
NS_TRAINING_RESULTS = "biomero.training.results"


# ---------------------------------------------------------------------------
# Data preparation — annotate mode
# ---------------------------------------------------------------------------

def prepare_annotate_data(conn, dataset_ids, slurmClient, folder_name):
    """Prepare training data from annotate_ai tracking table.

    Reads the tracking table to get split assignments, exports images,
    and rasterizes ROIs into instance label masks.
    """
    data_path = slurmClient.slurm_data_path

    # Create directory structure on SLURM
    for split in ["train", "validation", "test"]:
        slurmClient.run_commands([
            f"mkdir -p {data_path}/{folder_name}/data/in/{split}",
            f"mkdir -p {data_path}/{folder_name}/data/gt/{split}",
        ])
    slurmClient.run_commands([
        f"mkdir -p {data_path}/{folder_name}/data/out",
    ])

    for dataset_id in dataset_ids:
        dataset = conn.getObject("Dataset", dataset_id)
        if not dataset:
            print(f"Dataset {dataset_id} not found, skipping")
            continue

        # Find the tracking table
        tracking_table = find_tracking_table(conn, dataset)
        if not tracking_table:
            print(f"No tracking table found for dataset {dataset_id}")
            continue

        # Read split assignments from the table
        assignments = read_split_assignments(conn, tracking_table)

        for image_id, split in assignments.items():
            if split not in ("train", "validation", "test"):
                continue

            image = conn.getObject("Image", image_id)
            if not image:
                print(f"Image {image_id} not found, skipping")
                continue

            stem = os.path.splitext(image.getName())[0]

            # Export image
            export_image_to_slurm(
                conn, image,
                f"{data_path}/{folder_name}/data/in/{split}/{stem}.tif",
                slurmClient)

            # Rasterize ROIs to mask
            mask = rasterize_rois(conn, image)
            if mask is not None:
                export_mask_to_slurm(
                    mask,
                    f"{data_path}/{folder_name}/data/gt/{split}/{stem}.tif",
                    slurmClient)
            else:
                print(f"WARNING: No ROIs found for image {image_id}")


def find_tracking_table(conn, dataset):
    """Find the annotate_ai tracking table attached to the dataset."""
    for ann in dataset.listAnnotations():
        if isinstance(ann, omero.gateway.FileAnnotationWrapper):
            ns = ann.getNs() or ""
            if "tracking" in ns.lower() or "annotate" in ns.lower():
                return ann
    return None


def read_split_assignments(conn, tracking_table):
    """Read image -> split assignments from the tracking table.

    Returns dict mapping image_id -> split name (train/validation/test).
    """
    assignments = {}
    try:
        orig_file = tracking_table.getFile()
        table_id = orig_file.getId()
        resources = conn.c.sf.sharedResources()
        table = resources.openTable(omero.model.OriginalFileI(table_id, False))
        if table:
            headers = table.getHeaders()
            n_rows = table.getNumberOfRows()

            # Find column indices
            image_col = None
            split_col = None
            for i, h in enumerate(headers):
                if h.name.lower() in ("image_id", "image"):
                    image_col = i
                elif h.name.lower() in ("split", "category", "set"):
                    split_col = i

            if image_col is not None and split_col is not None:
                data = table.read(
                    list(range(len(headers))), 0, n_rows)
                for row_idx in range(n_rows):
                    img_id = data.columns[image_col].values[row_idx]
                    split = data.columns[split_col].values[row_idx]
                    assignments[int(img_id)] = str(split).lower()

            table.close()
    except Exception as e:
        print(f"Error reading tracking table: {e}")

    return assignments


def rasterize_rois(conn, image):
    """Rasterize all ROIs on an image into an instance label mask.

    Returns a uint16 numpy array where each ROI gets a unique label,
    or None if no ROIs found.
    """
    import cv2

    roi_service = conn.getRoiService()
    result = roi_service.findByImage(image.getId(), None)

    if not result or not result.rois:
        return None

    size_x = image.getSizeX()
    size_y = image.getSizeY()
    mask = np.zeros((size_y, size_x), dtype=np.uint16)

    label = 1
    for roi in result.rois:
        for shape in roi.copyShapes():
            if isinstance(shape, omero.model.PolygonI):
                points_str = shape.getPoints().getValue()
                pts = parse_polygon_points(points_str)
                if len(pts) >= 3:
                    pts_array = np.array(pts, dtype=np.int32)
                    cv2.fillPoly(mask, [pts_array], color=int(label))
                    label += 1
            elif isinstance(shape, omero.model.RectangleI):
                x = int(shape.getX().getValue())
                y = int(shape.getY().getValue())
                w = int(shape.getWidth().getValue())
                h = int(shape.getHeight().getValue())
                mask[y:y+h, x:x+w] = label
                label += 1
            elif isinstance(shape, omero.model.EllipseI):
                cx = int(shape.getX().getValue())
                cy = int(shape.getY().getValue())
                rx = int(shape.getRadiusX().getValue())
                ry = int(shape.getRadiusY().getValue())
                cv2.ellipse(mask, (cx, cy), (rx, ry), 0, 0, 360,
                           color=int(label), thickness=-1)
                label += 1

    return mask if label > 1 else None


def parse_polygon_points(points_str):
    """Parse OMERO polygon points string to list of [x, y] pairs.

    OMERO stores polygon points as "x1,y1 x2,y2 x3,y3 ..."
    """
    points = []
    for pair in points_str.strip().split(" "):
        parts = pair.split(",")
        if len(parts) == 2:
            points.append([int(float(parts[0])), int(float(parts[1]))])
    return points


# ---------------------------------------------------------------------------
# Data preparation — paired mode
# ---------------------------------------------------------------------------

def prepare_paired_data(conn, dataset_ids, slurmClient, folder_name,
                        mask_suffix, val_split, test_split):
    """Prepare training data from paired image + mask datasets.

    Matches images with masks by suffix, randomly splits into
    train/validation/test, and uploads to SLURM.
    """
    data_path = slurmClient.slurm_data_path

    # Create directory structure on SLURM
    for split in ["train", "validation", "test"]:
        slurmClient.run_commands([
            f"mkdir -p {data_path}/{folder_name}/data/in/{split}",
            f"mkdir -p {data_path}/{folder_name}/data/gt/{split}",
        ])
    slurmClient.run_commands([
        f"mkdir -p {data_path}/{folder_name}/data/out",
    ])

    for dataset_id in dataset_ids:
        dataset = conn.getObject("Dataset", dataset_id)
        if not dataset:
            print(f"Dataset {dataset_id} not found, skipping")
            continue

        # Collect all images and identify pairs
        all_images = {}
        mask_images = {}
        for image in dataset.listChildren():
            name = image.getName()
            stem = os.path.splitext(name)[0]
            if mask_suffix in stem:
                # This is a mask — key by the image stem it belongs to
                image_stem = stem.replace(mask_suffix, "")
                mask_images[image_stem] = image
            else:
                all_images[stem] = image

        # Find pairs
        paired = []
        for stem, img in all_images.items():
            if stem in mask_images:
                paired.append((img, mask_images[stem], stem))
            else:
                print(f"WARNING: No mask found for {img.getName()} "
                      f"(expected {stem}{mask_suffix}.*)")

        if not paired:
            print(f"No paired images found in dataset {dataset_id}")
            continue

        # Random split
        random.shuffle(paired)
        n_total = len(paired)
        n_val = int(n_total * val_split)
        n_test = int(n_total * test_split)
        n_train = n_total - n_val - n_test

        splits = (
            [("train", p) for p in paired[:n_train]] +
            [("validation", p) for p in paired[n_train:n_train + n_val]] +
            [("test", p) for p in paired[n_train + n_val:]]
        )

        # Export each pair to SLURM
        for split, (img, mask, stem) in splits:
            export_image_to_slurm(
                conn, img,
                f"{data_path}/{folder_name}/data/in/{split}/{stem}.tif",
                slurmClient)
            export_image_to_slurm(
                conn, mask,
                f"{data_path}/{folder_name}/data/gt/{split}/{stem}.tif",
                slurmClient)

        print(f"Paired data prepared: {n_train} train, {n_val} val, "
              f"{n_test} test")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def export_image_to_slurm(conn, image, remote_path, slurmClient):
    """Export an OMERO image as TIFF and upload to SLURM."""
    from tifffile import imwrite

    pixels = image.getPrimaryPixels()
    plane = pixels.getPlane(0, 0, 0)

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        imwrite(tmp.name, plane)
        tmp_path = tmp.name

    try:
        slurmClient.put(tmp_path, remote_path)
    finally:
        os.remove(tmp_path)


def export_mask_to_slurm(mask, remote_path, slurmClient):
    """Save a numpy mask array as TIFF and upload to SLURM."""
    from tifffile import imwrite

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        imwrite(tmp.name, mask)
        tmp_path = tmp.name

    try:
        slurmClient.put(tmp_path, remote_path)
    finally:
        os.remove(tmp_path)


# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------

def write_config_to_slurm(slurmClient, folder_name, workflow,
                           model_name, n_epochs, learning_rate,
                           weight_decay, batch_size, channels,
                           dataset_ids, username):
    """Generate config.yaml and upload to SLURM."""
    import yaml

    config = {
        "training": {
            "pretrained_model": "cpsam",
            "n_epochs": n_epochs,
            "learning_rate": learning_rate,
            "weight_decay": weight_decay,
            "batch_size": batch_size,
            "model_name": model_name,
            "channels": channels,
        },
        "metadata": {
            "source_datasets": list(dataset_ids),
            "trained_by": username,
            "workflow_name": workflow,
        },
    }

    data_path = slurmClient.slurm_data_path
    remote_path = f"{data_path}/{folder_name}/data/config.yaml"

    with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False) as tmp:
        yaml.dump(config, tmp, default_flow_style=False)
        tmp_path = tmp.name

    try:
        slurmClient.put(tmp_path, remote_path)
    finally:
        os.remove(tmp_path)

    print(f"Config written to {remote_path}")


# ---------------------------------------------------------------------------
# Training job submission
# ---------------------------------------------------------------------------

def run_training_job(slurmClient, workflow, workflow_version, folder_name,
                     wf_id=None):
    """Submit a training job to SLURM with a training-specific sbatch script.

    Unlike inference, training needs:
    - TRAINING_MODE=true env var passed to the container
    - /data bound so training data is accessible
    - Models dir bound to /tmp/models for cpsam cache
    - No inference-specific CLI parameters

    Writes a temporary job script to SLURM and sbatches it, since the
    inference job scripts have wrong parameters for training.

    Returns:
        Tuple of (result, slurm_job_id, wf_id, task_id)
    """
    wf = workflow.lower()
    model_path = slurmClient.slurm_model_paths[wf]
    image = slurmClient.slurm_model_images[wf].split("/")[1]
    data_path = f"{slurmClient.slurm_data_path}/{folder_name}"
    image_path = f"{slurmClient.slurm_images_path}/{model_path}"
    sif_name = f"{image}_{workflow_version}.sif"

    # Models bind
    models_bind = ""
    models_path = ""
    if slurmClient.slurm_models_path:
        models_path = f"{slurmClient.slurm_models_path}/{model_path}"
        models_bind = f"--bind {models_path}:/tmp/models"

    # Data bind — needed because /data is not in default Singularity bind paths
    data_bind = "--bind /data:/data"
    if slurmClient.slurm_data_bind_path:
        data_bind = f"--bind {slurmClient.slurm_data_bind_path}"

    # Track task
    task_id = slurmClient.workflowTracker.add_task_to_workflow(
        wf_id, workflow, workflow_version, folder_name, {})
    slurmClient.workflowTracker.start_task(task_id)

    # Write a training job script to SLURM, then sbatch it.
    # (Heredocs don't work reliably through SSH/run_commands.)
    script_content = f"""#!/bin/bash
#SBATCH --job-name=omero-training-{workflow}
#SBATCH --cpus-per-task=4
#SBATCH --mem=5GB
#SBATCH --time=02:00:00
#SBATCH --output=omero-%j.log
#SBATCH --open-mode=append

echo "BIOMERO Training Job: {workflow} v{workflow_version}"
echo "Data: {data_path}"
module load singularity > /dev/null 2>&1 || true
mkdir -p {models_path}/cellpose 2>/dev/null || true
singularity run --nv {data_bind} {models_bind} \\
    --env TRAINING_MODE=true \\
    "{image_path}/{sif_name}" \\
    --infolder "{data_path}/data/in" \\
    --outfolder "{data_path}/data/out" \\
    --gtfolder "{data_path}/data/gt" \\
    --local -nmc
"""
    # Upload script to SLURM
    script_remote = f"{data_path}/train_job.sh"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh",
                                     delete=False) as tmp:
        tmp.write(script_content)
        tmp_path = tmp.name
    try:
        slurmClient.put(tmp_path, script_remote)
    finally:
        os.remove(tmp_path)

    # Make executable and submit
    print(f"Submitting training job for {workflow} on {folder_name}")
    result = slurmClient.run_commands([
        f"chmod +x {script_remote}",
        f"sbatch {script_remote}",
    ])
    slurm_job_id = slurmClient.extract_job_id(result)

    if task_id:
        slurmClient.workflowTracker.add_job_id(task_id, slurm_job_id)
        slurmClient.workflowTracker.add_result(task_id, result)

    return result, slurm_job_id, wf_id, task_id


# ---------------------------------------------------------------------------
# Job polling
# ---------------------------------------------------------------------------

def poll_job(slurmClient, slurm_job_id, conn, client):
    """Poll SLURM job until completion."""
    if slurm_job_id < 0:
        return "Failed to submit SLURM job"

    print(f"Polling SLURM job {slurm_job_id}...")
    while True:
        job_status_dict, _ = slurmClient.check_job_status([slurm_job_id])
        job_state = job_status_dict.get(slurm_job_id, "UNKNOWN")

        print(f"  Job {slurm_job_id}: {job_state}")

        if job_state == "COMPLETED":
            return f"Training completed (SLURM job {slurm_job_id})"
        elif job_state in ("FAILED", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY"):
            return f"Training {job_state} (SLURM job {slurm_job_id})"
        elif job_state == "UNKNOWN":
            return f"Job {slurm_job_id} status unknown"

        conn.keepAlive()
        timesleep.sleep(10)


# ---------------------------------------------------------------------------
# Result upload
# ---------------------------------------------------------------------------

def upload_results(conn, slurmClient, folder_name, dataset_ids, slurm_job_id):
    """Download training results from SLURM and upload to OMERO."""
    import yaml

    data_path = slurmClient.slurm_data_path
    out_path = f"{data_path}/{folder_name}/data/out"

    # Download training_results.yaml
    results = {}
    with tempfile.NamedTemporaryFile(
            suffix=".yaml", delete=False) as tmp:
        try:
            slurmClient.get(f"{out_path}/training_results.yaml", tmp.name)
            with open(tmp.name) as f:
                results = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Could not download training_results.yaml: {e}")
        finally:
            os.remove(tmp.name)

    model_id = results.get("model_id", f"model_{slurm_job_id}")

    # Find and download model zip
    zip_filename = f"{model_id}.zip"
    zip_path = None
    with tempfile.NamedTemporaryFile(
            suffix=".zip", delete=False) as tmp:
        try:
            slurmClient.get(f"{out_path}/{zip_filename}", tmp.name)
            zip_path = tmp.name
        except Exception as e:
            print(f"Could not download model zip: {e}")

    # Upload to OMERO for each source dataset
    for dataset_id in dataset_ids:
        dataset = conn.getObject("Dataset", dataset_id)
        if not dataset:
            continue

        group_id = dataset.getDetails().getGroup().getId()
        conn.SERVICE_OPTS.setOmeroGroup(group_id)

        # Upload model zip as FileAnnotation
        if zip_path and os.path.exists(zip_path):
            file_ann = conn.createFileAnnfromLocalFile(
                zip_path,
                mimetype="application/zip",
                ns=NS_TRAINING_MODEL,
                desc=f"Trained model: {model_id}",
            )
            # Rename file to zip_filename
            orig_file = file_ann.getFile()._obj
            orig_file.setName(rstring(zip_filename))
            conn.getUpdateService().saveObject(orig_file)

            dataset.linkAnnotation(file_ann)
            print(f"Model zip uploaded as FileAnnotation {file_ann.getId()}")

        # Upload results as MapAnnotation
        if results:
            map_data = [
                [str(k), str(v)] for k, v in results.items()
                if not isinstance(v, (dict, list))
            ]
            map_ann = omero.gateway.MapAnnotationWrapper(conn)
            map_ann.setNs(NS_TRAINING_RESULTS)
            map_ann.setValue(map_data)
            map_ann.save()
            dataset.linkAnnotation(map_ann)
            print(f"Results uploaded as MapAnnotation {map_ann.getId()}")

    # Cleanup zip
    if zip_path and os.path.exists(zip_path):
        os.remove(zip_path)


# ---------------------------------------------------------------------------
# Main script
# ---------------------------------------------------------------------------

def runScript():
    """Define and execute the OMERO training script."""
    client = omscripts.client(
        "SLURM_Run_Training",
        "Run a training workflow on SLURM via BIOMERO",

        omscripts.String(
            "Data_Type", optional=False, grouping="01",
            description="OMERO data type",
            values=wrap(["Dataset"]), default="Dataset"),

        omscripts.List(
            "IDs", optional=False, grouping="02",
            description="Dataset IDs").ofType(rlong(0)),

        omscripts.String(
            "Workflow", optional=False, grouping="03",
            description="Workflow to train with"),

        omscripts.String(
            "Workflow_Version", optional=False, grouping="04",
            description="Container version"),

        omscripts.String(
            "Data_Mode", optional=False, grouping="05",
            description="Data input mode",
            values=wrap(["paired", "annotate"]), default="paired"),

        omscripts.String(
            "Mask_Suffix", optional=True, grouping="06",
            description="Suffix for mask images (paired mode)",
            default="_label"),

        omscripts.Float(
            "Val_Split", optional=True, grouping="07",
            description="Validation fraction (paired mode)",
            default=0.2),

        omscripts.Float(
            "Test_Split", optional=True, grouping="08",
            description="Test fraction (paired mode)",
            default=0.0),

        omscripts.String(
            "Model_Name", optional=True, grouping="09",
            description="Model display name",
            default="my_model"),

        omscripts.Int(
            "N_Epochs", optional=True, grouping="10",
            description="Training epochs",
            default=100),

        omscripts.Float(
            "Learning_Rate", optional=True, grouping="11",
            description="Learning rate",
            default=0.00001),

        omscripts.Float(
            "Weight_Decay", optional=True, grouping="12",
            description="Weight decay",
            default=0.1),

        omscripts.Int(
            "Batch_Size", optional=True, grouping="13",
            description="Training batch size",
            default=1),

        omscripts.String(
            "Channels", optional=True, grouping="14",
            description="Cytoplasm,nucleus channel indices",
            default="0,0"),

        version=VERSION,
        authors=["BIOMERO"],
        institutions=["Amsterdam UMC"],
        contact="biomero@amsterdamumc.nl",
    )

    try:
        conn = BlitzGateway(client_obj=client)
        conn.keepAlive()

        # Extract parameters
        data_type = unwrap(client.getInput("Data_Type"))
        ids = unwrap(client.getInput("IDs"))
        workflow = unwrap(client.getInput("Workflow"))
        version = unwrap(client.getInput("Workflow_Version"))
        data_mode = unwrap(client.getInput("Data_Mode"))
        def get_input(name, default):
            val = unwrap(client.getInput(name))
            return default if val is None else val

        mask_suffix = get_input("Mask_Suffix", "_label")
        val_split = get_input("Val_Split", 0.2)
        test_split = get_input("Test_Split", 0.0)
        model_name = get_input("Model_Name", "my_model")
        n_epochs = get_input("N_Epochs", 100)
        learning_rate = get_input("Learning_Rate", 0.00001)
        weight_decay = get_input("Weight_Decay", 0.1)
        batch_size = get_input("Batch_Size", 1)
        channels_str = get_input("Channels", "0,0")

        channels = [int(c.strip()) for c in channels_str.split(",")]

        username = conn.getUser().getName()

        with SlurmClient.from_config() as slurmClient:
            # Start tracking the workflow
            user = conn.getUserId()
            group = conn.getGroupFromContext().id
            wf_id = slurmClient.workflowTracker.initiate_workflow(
                workflow,
                f"Training {workflow} v{version}",
                user,
                group
            )

            # Generate a unique folder name for this training run
            timestamp = int(timesleep.time())
            folder_name = f"training_{workflow}_{timestamp}"

            # Prepare data based on mode
            if data_mode == "annotate":
                prepare_annotate_data(
                    conn, ids, slurmClient, folder_name)
            else:
                prepare_paired_data(
                    conn, ids, slurmClient, folder_name,
                    mask_suffix, val_split, test_split)

            # Generate config.yaml on SLURM
            write_config_to_slurm(
                slurmClient, folder_name, workflow,
                model_name, n_epochs, learning_rate,
                weight_decay, batch_size, channels,
                ids, username)

            # Run training workflow (custom sbatch, not the inference job script)
            result, slurm_job_id, wf_id, task_id = run_training_job(
                slurmClient, workflow, version, folder_name, wf_id=wf_id,
            )

            # Poll for completion
            msg = poll_job(slurmClient, slurm_job_id, conn, client)

            # Upload results back to OMERO (only if completed)
            if "COMPLETED" in msg:
                upload_results(
                    conn, slurmClient, folder_name,
                    ids, slurm_job_id)
                slurmClient.workflowTracker.complete_workflow(wf_id)
            else:
                slurmClient.workflowTracker.fail_workflow(wf_id, msg)

        client.setOutput("Message", rstring(msg))

    except Exception as e:
        traceback.print_exc()
        if 'slurmClient' in locals() and 'wf_id' in locals() and wf_id is not None:
            try:
                slurmClient.workflowTracker.fail_workflow(wf_id, str(e))
            except Exception:
                pass
        client.setOutput("Message", rstring(f"Error: {traceback.format_exc()}"))
    finally:
        client.closeSession()


if __name__ == "__main__":
    runScript()
