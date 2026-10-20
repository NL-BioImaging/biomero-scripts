"""File attachments must not move data already registered in place."""
import ast
import glob
import logging
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import Mock

import pytest


ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
SOURCE = ROOT / "_data" / "SLURM_Import_Results.py"


def load():
    names = {
        "find_supported_image_paths", "getOriginalFilename",
        "saveImagesToOmeroAsAttachments",
    }
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = dict(
        Any=Any, Dict=Dict, List=List, Optional=Optional, BlitzGateway=object,
        os=os, glob=glob, re=re, logger=logging.getLogger(__name__),
        SUPPORTED_IMAGE_EXTENSIONS=(".tif", ".tiff", ".png", ".zarr"),
        NSCREATED="test", unwrap=lambda value: value, robject=lambda value: value,
        constants=SimpleNamespace(results=SimpleNamespace(
            OUTPUT_SLURM_JOB_ID="job_id")),
        upload_metadata_csv_to_omero=Mock(return_value="metadata linked"),
    )
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"),
         namespace)
    return namespace


def source_image():
    image = Mock()
    image.getName.return_value = "cells"
    image.getId.return_value = 42
    return image


class OriginalFile:
    """An OMERO filename is metadata; the registered bytes have a fixed path."""

    def __init__(self, path):
        self.path = Path(path)
        self.name = self.path.name
        self.saved_name = self.name

    def setName(self, value):
        self.name = value

    def save(self):
        self.saved_name = self.name


def attachment(path):
    original = OriginalFile(path)
    return SimpleNamespace(_obj=object(), getFile=lambda: original)


def run(namespace, folder, *, tracked=True, workflow_id="wf-123", images=None):
    conn = Mock()
    images = images or [source_image()]
    conn.getObjects.return_value = images
    client = Mock()
    client.getInput.return_value = "91"
    namespace["match_results_to_inputs"] = Mock(
        side_effect=lambda paths, inputs, originals: [
            inputs[min(index, len(inputs) - 1)]
            for index, _ in enumerate(paths)
        ])
    message = namespace["saveImagesToOmeroAsAttachments"](
        conn, str(folder), client, wf_id=workflow_id,
        input_images=images if tracked else None,
    )
    return message, images, client


@pytest.mark.parametrize("tracked", [True, False])
@pytest.mark.parametrize("node", ["0", "A/1/0/0"])
def test_registered_zarr_directory_is_never_attached_or_renamed(
        tmp_path, tracked, node):
    store = tmp_path / "cells.ome.zarr"
    chunk = store / node / "chunk"
    chunk.parent.mkdir(parents=True)
    chunk.write_bytes(b"registered pixels")
    namespace = load()
    namespace["create_file_annotation_inplace"] = Mock(
        side_effect=IsADirectoryError("a FileAnnotation requires a file"))

    message, images, client = run(namespace, tmp_path, tracked=tracked)

    assert store.is_dir(), "the registered store path must still resolve"
    assert chunk.read_bytes() == b"registered pixels"
    namespace["create_file_annotation_inplace"].assert_not_called()
    images[0].linkAnnotation.assert_not_called()
    client.setOutput.assert_not_called()
    assert "skipped" in message.lower()


@pytest.mark.parametrize("workflow_id", ["wf-123", None])
@pytest.mark.parametrize("tracked", [True, False])
def test_regular_file_attachment_uses_a_permanent_source_path(
        tmp_path, workflow_id, tracked):
    path = tmp_path / "cells.ome.tif"
    path.write_bytes(b"registered image")
    namespace = load()
    created = []

    def create(_conn, filename, **kwargs):
        assert Path(filename).read_bytes() == b"registered image"
        ann = attachment(filename)
        created.append((filename, ann, kwargs))
        return ann

    namespace["create_file_annotation_inplace"] = Mock(side_effect=create)
    _, images, client = run(
        namespace, tmp_path, workflow_id=workflow_id, tracked=tracked)

    filename, ann, kwargs = created[0]
    assert filename == str(path)
    assert ann.getFile().path.read_bytes() == b"registered image"
    assert ann.getFile().saved_name == (
        "cells.wf-123.ome.tif" if workflow_id else path.name)
    images[0].linkAnnotation.assert_called_once_with(ann)
    client.setOutput.assert_called_once_with("File_Annotation", ann._obj)
    if workflow_id:
        assert f"Workflow {workflow_id}" in kwargs["description"]


@pytest.mark.parametrize("failure_stage", ["create", "link"])
def test_failed_attachment_keeps_source_path_and_continues(
        tmp_path, failure_stage):
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    namespace = load()
    images = [source_image(), source_image()]
    if failure_stage == "link":
        images[0].linkAnnotation.side_effect = RuntimeError("link denied")

    def create(_conn, filename, **kwargs):
        if failure_stage == "create" and Path(filename).read_bytes() == b"first":
            raise RuntimeError("upload denied")
        return attachment(filename)

    namespace["create_file_annotation_inplace"] = Mock(side_effect=create)
    message, _, _ = run(namespace, tmp_path, images=images)

    assert first.read_bytes() == b"first"
    assert second.read_bytes() == b"second"
    images[1].linkAnnotation.assert_called_once()
    assert "denied" in message


def test_directory_does_not_consume_a_file_source_match(tmp_path):
    store = tmp_path / "a.ome.zarr"
    store.mkdir()
    path = tmp_path / "b.tif"
    path.write_bytes(b"file")
    namespace = load()
    namespace["create_file_annotation_inplace"] = Mock(
        side_effect=lambda _conn, filename, **kwargs: attachment(filename))

    _, images, _ = run(namespace, tmp_path)

    matched_paths = namespace["match_results_to_inputs"].call_args.args[0]
    assert matched_paths == [str(path)]
    namespace["create_file_annotation_inplace"].assert_called_once()
    images[0].linkAnnotation.assert_called_once()
    assert store.is_dir()


def test_attachment_name_does_not_overwrite_an_existing_disk_file(tmp_path):
    path = tmp_path / "cells.tif"
    path.write_bytes(b"result")
    collision = tmp_path / "cells.wf-123.tif"
    collision.write_bytes(b"other result")
    namespace = load()
    namespace["find_supported_image_paths"] = lambda _folder: [str(path)]
    namespace["create_file_annotation_inplace"] = Mock(
        side_effect=lambda _conn, filename, **kwargs: attachment(filename))

    run(namespace, tmp_path)

    assert path.read_bytes() == b"result"
    assert collision.read_bytes() == b"other result"
    assert namespace["create_file_annotation_inplace"].call_args.args[1] == str(path)
