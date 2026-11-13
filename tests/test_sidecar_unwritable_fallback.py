"""An unwritable BIOMERO folder must never fail a workflow transfer.

Indexing an existing Zarr writes a sidecar into ``<zarr parent>/.processed``.
When that folder cannot be created, ``save_as_zarr`` must warn, skip the
indexing and package the existing Zarr directly, without exporting again.
"""
import ast
import logging
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
TRANSFER_PATH = SOURCE_ROOT / "_data" / "_SLURM_Image_Transfer.py"
FUNCTIONS = {
    "save_as_zarr", "is_compatible_zarr_source", "index_existing_image_zarr",
    "index_existing_plate_zarr", "locate_managed_zarr",
}


class OmeroObject:
    def __init__(self, object_id=42):
        self._id = object_id

    def getId(self):
        return self._id

    def getName(self):
        return "object"

    def keepAlive(self):
        return None


def _load_namespace(tmp_path, existing_zarr, calls):
    tree = ast.parse(TRANSFER_PATH.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS]

    def write_sidecar(path, _source, **_kwargs):
        # A real filesystem failure: a file sits where the folder must go.
        (Path(path).parent / ".processed" / ".biomero").mkdir(parents=True)

    def record(name):
        def recorder(*_args, **_kwargs):
            calls.append(name)
        return recorder

    class Popen:
        def __init__(self, *_args, **_kwargs):
            calls.append("export")
            raise AssertionError("the Zarr must not be exported again")

    guard = SimpleNamespace(
        shape=(1, 1, 1, 4, 4), dtype="uint16",
        axes=("t", "c", "z", "y", "x"), coordinate_transformations=())
    namespace = {
        "Path": Path,
        "PROCESSED_DATA_FOLDER": ".processed",
        "constants": SimpleNamespace(transfer=SimpleNamespace(
            DATA_TYPE_IMAGE="Image", DATA_TYPE_PLATE="Plate",
            OME_ZARR_VERSION_0_4="0.4")),
        "os": os,
        "shutil": shutil,
        "logger": logging.getLogger("sidecar-unwritable-test"),
        "log": lambda _message: None,
        "get_shallow_reference": lambda _obj: None,
        "get_shallow_plate_reference": lambda _obj: None,
        "select_zarr_source_path": lambda *_args: existing_zarr,
        "upgrade_reused_canonical": lambda *_args: pytest.fail("not reused"),
        "read_zarr_semantic_guard": lambda *_args: guard,
        "validate_omero_image_semantics": lambda *_args: None,
        "canonical_interchange_profile": lambda _guard: "ngff-0.4-zarr-v2",
        "IsccBioIdentityProvider": lambda: None,
        "CanonicalZarrSource": lambda **kwargs: SimpleNamespace(**kwargs),
        "build_canonical_plate_source": lambda *_args, **_kwargs: (
            SimpleNamespace(images=())),
        "write_indexed_canonical_marker": write_sidecar,
        "attach_canonical_source": record("attach_canonical_source"),
        "attach_canonical_plate_source": record("attach_canonical_plate_source"),
        "subprocess": SimpleNamespace(PIPE=object(), Popen=Popen),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(TRANSFER_PATH),
                 "exec"), namespace)
    namespace["read_zarr_semantic_guard"] = lambda *_args: guard
    return namespace


@pytest.mark.parametrize("data_type", ["Image", "Plate"])
def test_unwritable_biomero_folder_warns_and_packages_zarr_directly(
    tmp_path, monkeypatch, caplog, data_type
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    existing = tmp_path / "data" / "input.ome.zarr"
    existing.mkdir(parents=True)
    (existing / ".zgroup").write_text('{"zarr_format": 2}')
    (existing / "marker-content").write_text("original pixels")
    # The BIOMERO folder cannot be created: a file occupies its name.
    (existing.parent / ".processed").write_text("blocked")
    calls = []
    namespace = _load_namespace(tmp_path, existing, calls)
    namespace["is_compatible_zarr_source"] = lambda *_args: True
    packaged = {}

    with caplog.at_level(logging.WARNING, logger="sidecar-unwritable-test"):
        result = namespace["save_as_zarr"](
            SimpleNamespace(keepAlive=lambda: None), "session",
            OmeroObject(), "batch", data_type, "0.4",
            storage_roots={"group-0-data": tmp_path},
            shallow_zarr_storage=True, reconstruct_shallow_zarr=True,
            direct_directory_sources=packaged,
        )

    canonical_source = result[0]
    assert canonical_source is None
    assert calls == []  # no attach_*, no export subprocess
    assert packaged == {"object.zarr": existing}
    assert "Existing Zarr indexing failed" in caplog.text
    assert (existing.parent / ".processed").read_text() == "blocked"
    assert (existing / "marker-content").read_text() == "original pixels"
