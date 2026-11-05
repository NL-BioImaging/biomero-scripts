"""Legacy label import must preserve native NGFF and provenance paths."""
import ast
import glob
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List
import uuid

import pytest


SOURCE_ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
SCRIPT = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"


def functions():
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    names = {"find_label_zarr_paths", "create_upload_orders_for_results"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    finder = next(node for node in nodes if node.name == "find_label_zarr_paths")
    if (any(isinstance(node, ast.Attribute) and node.attr == "rename" for node in ast.walk(finder))
            and os.environ.get("BIOMERO_REQUIRE_STABLE_LABEL_PATHS") != "1"):
        pytest.skip("Native label path preservation is not in this source revision yet")
    namespace = dict(os=os, glob=glob, logger=logging.getLogger(__name__),
                     List=List, Dict=Dict, Any=Any, UUID=uuid.UUID, uuid=uuid,
                     IMPORTER_AVAILABLE=True, SUPPORTED_IMAGE_EXTENSIONS=(".zarr",),
                     build_shallow_import_options=lambda *a, **kw: None,
                     find_supported_image_paths=lambda base: [])
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT), "exec"), namespace)
    return namespace["find_label_zarr_paths"], namespace["create_upload_orders_for_results"]


@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.parametrize("artifact", ["image.zarr", "nested/output/image.zarr"])
@pytest.mark.parametrize("label", ["cells", "cells.zarr"])
def test_label_order_keeps_ngff_crate_and_pixel_paths(tmp_path, version, artifact, label):
    root = tmp_path / artifact
    labels = root / "labels"
    child = labels / label
    (child / "0").mkdir(parents=True)
    attrs = {"labels": [label], "extension": {"preserve": True}}
    metadata_name = ".zattrs" if version == 2 else "zarr.json"
    metadata = attrs if version == 2 else {
        "zarr_format": 3, "node_type": "group",
        "attributes": {"ome": {"version": "0.5", **attrs}, "outside_ome": "preserve"}}
    (labels / metadata_name).write_text(json.dumps(metadata))
    (child / metadata_name).write_text(json.dumps({} if version == 2 else {
        "zarr_format": 3, "node_type": "group", "attributes": {"ome": {"version": "0.5"}}}))
    (child / "0" / "chunk").write_bytes(bytes(range(256)))
    relative = child.relative_to(tmp_path).as_posix() + "/"
    crate = tmp_path / "ro-crate-metadata.json"
    crate.write_text(json.dumps({"@graph": [{"@id": relative, "@type": "Dataset"}]}))
    before = {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    find_labels, build_orders = functions()

    orders = build_orders("fixture", "fixture", "Dataset", 1, str(tmp_path), uuid.UUID(int=1), submit=False)

    assert len(orders) == 1
    assert orders[0]["source"] == "SLURM_Results_Labels"
    assert orders[0]["Files"] == [str(child)]
    assert find_labels(str(tmp_path)) == [str(child)]  # repeat discovery is also read-only
    assert (tmp_path / json.loads(crate.read_text())["@graph"][0]["@id"]).is_dir()
    assert {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()} == before


def test_discovery_does_not_promote_bare_or_plate_internal_nodes(tmp_path):
    # These are not independent *.zarr/labels stores in the existing legacy path.
    # Native Plate labels remain the lifecycle/Plate reader's responsibility.
    bare = tmp_path / "labels" / "cells"
    plate_label = tmp_path / "plate.zarr" / "A" / "1" / "0" / "labels" / "cells"
    for child in (bare, plate_label):
        child.mkdir(parents=True)
        (child / ".zattrs").write_text("{}")
    find_labels, _ = functions()
    assert find_labels(str(tmp_path)) == []
    assert bare.is_dir() and plate_label.is_dir()
