"""Trust backing stores on reuse, verify newly exported Plate pixels."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_canonical_zarr_transfer import (
    SCRIPT_PATH, _load_canonical_functions, pixel_identity, plate_source, Object,
    source as image_source, annotation_for,
)


pytestmark = pytest.mark.skipif(
    "def verified_plate_source(" not in SCRIPT_PATH.read_text(encoding="utf-8"),
    reason="Canonical backing-store trust is not implemented in this source revision",
)


def helpers():
    ns = _load_canonical_functions()
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    names = {"verified_plate_source", "upgrade_reused_canonical",
             "verify_exported_plate_source"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)
             and n.name in names]
    ns["run_with_keepalive"] = lambda operation, *_args, **_kwargs: operation()
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT_PATH),
                 "exec"), ns)
    return ns


def test_existing_plate_is_verified_without_changing_identity(pixel_identity):
    ns = helpers()
    source = plate_source(pixel_identity)
    result = ns["verified_plate_source"](source)
    assert all(i.source.canonical_pixel_verified for i in result.images)
    assert result.images[0].source.pixel_identity == source.images[0].source.pixel_identity


@pytest.mark.parametrize("same_backing", [True, False])
def test_cached_plate_upgrade_requires_matching_backing_path(
    tmp_path, pixel_identity, same_backing
):
    ns = helpers()
    path = tmp_path / "plate.zarr"
    path.mkdir()
    source = plate_source(pixel_identity)
    source = source.model_copy(update={"images": tuple(
        i.model_copy(update={"source": i.source.model_copy(update={
            "canonical_pixel_verified": False})}) for i in source.images)})
    ns["get_legacy_zarr_path"] = lambda _: path if same_backing else tmp_path / "other.zarr"
    writes = []
    ns["write_indexed_canonical_marker"] = lambda *args: writes.append(args)
    ns["attach_canonical_plate_source"] = lambda *args: None
    result = ns["upgrade_reused_canonical"](None, object(), source, path)
    assert all(i.source.canonical_pixel_verified for i in result.images) == same_backing
    assert bool(writes) == same_backing
    assert result.source_generation == source.source_generation
    assert all(i.source.source_generation == result.source_generation for i in result.images)


@pytest.mark.parametrize("same_backing", [True, False])
def test_cached_image_upgrade_persists_unambiguous_generation(tmp_path, pixel_identity, same_backing):
    ns = helpers()
    source = image_source(pixel_identity).model_copy(update={"canonical_pixel_verified": False})
    image = Object(7, [annotation_for(source)])
    ns["get_legacy_zarr_path"] = lambda _: tmp_path if same_backing else None
    writes = []
    attach = ns["attach_canonical_source"]
    ns["attach_canonical_source"] = lambda conn, obj, kind, updated: attach(
        conn, obj, kind, updated,
        annotation_updater=lambda _annotation, values: writes.append(values))
    result = ns["upgrade_reused_canonical"](None, image, source, tmp_path)
    assert result.canonical_pixel_verified == same_backing
    assert result.pixel_identity == source.pixel_identity
    assert bool(writes) == same_backing
    assert result.source_generation == source.source_generation


@pytest.mark.parametrize("match", [True, False])
def test_new_plate_export_checks_omero_before_verifying(tmp_path, pixel_identity, match):
    ns = helpers()
    source = plate_source(pixel_identity)
    # The fixture has one image at A/1/0.
    (tmp_path / ".zattrs").write_text(json.dumps({"plate": {"wells": [
        {"path": "A/1", "rowIndex": 0, "columnIndex": 0}]}}))
    image = Object(42, [], shape=pixel_identity.shape, pixel_type=pixel_identity.dtype)
    well = SimpleNamespace(getRow=lambda: 0, getColumn=lambda: 0,
                           getImage=lambda index: image)
    plate = SimpleNamespace(listChildren=lambda: [well])
    calls = []
    expected = source.images[0].source.pixel_identity
    class Provider:
        def generate_omero(self, conn, **kwargs):
            calls.append(kwargs)
            return expected if match else expected.model_copy(update={"dtype": "uint8"})
    if match:
        result = ns["verify_exported_plate_source"](SimpleNamespace(keepAlive=lambda: True), plate, tmp_path, source, Provider())
        assert all(i.source.canonical_pixel_verified for i in result.images)
    else:
        with pytest.raises(ValueError, match="do not match"):
            ns["verify_exported_plate_source"](SimpleNamespace(keepAlive=lambda: True), plate, tmp_path, source, Provider())
    assert calls[0]["image_id"] == 42
