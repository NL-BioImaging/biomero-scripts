"""Reuse compatible inputs; export incompatible inputs through OMERO."""
import ast
import json
import logging
import os
from pathlib import Path
import shutil
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from test_shallow_conversion_material import load_save_as_zarr, ImageObject, TRANSFER_PATH

pytestmark = pytest.mark.skipif(
    "def is_compatible_zarr_source(" not in TRANSFER_PATH.read_text(encoding="utf-8")
    and os.getenv("BIOMERO_TEST_ZARR_POLICY") != "1",
    reason="Requires version-aware reuse and export fallback",
)


@pytest.mark.parametrize("source_version,requested", [(2, "0.5"), (3, "0.4")])
@pytest.mark.parametrize("kind", ["Image", "Plate"])
@pytest.mark.parametrize("shallow_enabled", [False, True])
@pytest.mark.parametrize("registered", [False, True])
def test_mismatch_uses_normal_export_without_canonical_promotion(
        tmp_path, monkeypatch, source_version, requested, kind, shallow_enabled, registered):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    source = tmp_path / "original.zarr"
    source.mkdir()
    metadata = source / ("zarr.json" if source_version == 3 else ".zgroup")
    metadata.write_text(json.dumps({"zarr_format": source_version}))
    before = metadata.read_bytes()
    save, commands, materializations, promotions, _, _ = load_save_as_zarr(
        tmp_path, shallow_reference=None, reusable_path=source)
    # Permit the old implementation to finish so the route assertions go red.
    save.__globals__["convert_zarr_for_transfer"] = (
        lambda src, dst, version: shutil.copytree(src, dst))
    upgrade = Mock(side_effect=AssertionError("no identity work for mismatched inputs"))
    save.__globals__["upgrade_reused_canonical"] = upgrade
    canonical = SimpleNamespace(interchange_profile=
        f"ngff-{'0.4' if source_version == 2 else '0.5'}-zarr-v{source_version}") if registered else None
    direct = {}

    result = save(SimpleNamespace(host="omero"), "session", ImageObject(), "batch",
                  kind, requested, canonical_source=canonical,
                  shallow_zarr_storage=shallow_enabled, direct_directory_sources=direct)

    assert result[0] is None
    assert result[2] == ()
    assert len(commands) == 1
    assert f"--format {requested}" in commands[0]
    assert f"{kind}:42" in commands[0]
    assert Path("batch", result[1]).is_dir()
    upgrade.assert_not_called()
    assert direct == {}
    assert materializations == promotions == []
    assert metadata.read_bytes() == before


@pytest.mark.parametrize("kind", ["Image", "Plate"])
@pytest.mark.parametrize("source_version,requested", [(2, "0.5"), (3, "0.4")])
def test_shallow_mismatch_exports_selected_pixels_without_reconstruction(
        tmp_path, monkeypatch, kind, source_version, requested):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    reference = SimpleNamespace(source=object(), interchange_profile=
        f"ngff-{'0.4' if source_version == 2 else '0.5'}-zarr-v{source_version}")
    save, commands, materializations, promotions, selections, _ = load_save_as_zarr(
        tmp_path, shallow_reference=reference, restore_available=False)

    source, artifact, labels = save(SimpleNamespace(host="omero"), "session",
        ImageObject(), "batch", kind, requested, shallow_zarr_storage=True)

    assert source is None
    assert labels == ()
    assert len(commands) == 1
    assert f"--format {requested}" in commands[0]
    assert f"{kind}:42" in commands[0]
    assert Path("batch", artifact).is_dir()
    assert materializations == promotions == selections == []


@pytest.mark.parametrize("version,requested", [(2, "0.4"), (3, "0.5")])
def test_matching_backing_store_keeps_direct_packaging(tmp_path, monkeypatch, version, requested):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    source = tmp_path / "canonical.zarr"
    source.mkdir()
    (source / ("zarr.json" if version == 3 else ".zgroup")).write_text(
        json.dumps({"zarr_format": version}))
    canonical = object()
    save, commands, materializations, promotions, _, _ = load_save_as_zarr(
        tmp_path, shallow_reference=None, reusable_path=source)
    direct = {}
    result = save(None, "session", ImageObject(), "batch", "Image", requested,
                  canonical_source=canonical, shallow_zarr_storage=True,
                  direct_directory_sources=direct)
    assert result[0] is canonical
    assert direct == {result[1]: source}
    assert commands == materializations == promotions == []


@pytest.mark.parametrize("kind", ["Image", "Plate"])
def test_batch_drops_discovered_identity_when_export_has_no_canonical(tmp_path, monkeypatch, kind):
    """A fresh compatibility export must not leak the old identity into results."""
    names = ("CHANNELS", "MERGED", "CHANNELS_GREY", "DATA_TYPE", "FOLDER", "FORMAT",
             "OME_VERSION", "Z", "RECONSTRUCT_SHALLOW_ZARR", "CHANNELS_NAMES", "ZOOM",
             "DATA_TYPE_DATASET", "FORMAT_OMEZARR", "FORMAT_OMETIFF")
    constants = SimpleNamespace(transfer=SimpleNamespace(
        **{name: name for name in names}, DATA_TYPE_PLATE="Plate"))
    monkeypatch.chdir(tmp_path)
    tree = ast.parse(TRANSFER_PATH.read_text(encoding="utf-8"))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "batch_image_export")
    selected = Mock(id=42)
    selected.getId.return_value = 42
    conn = Mock()
    conn.getDownloadAsMaxSizeSetting.return_value = 1000
    discovered = {42: object()}
    captured = {}

    class SnapshotReached(Exception):
        pass

    def snapshot(objects, object_type, sources, *args):
        captured.update(sources)
        raise SnapshotReached

    namespace = dict(os=os, Path=Path, constants=constants, SlurmClient=object,
        log=lambda _: None, logger=logging.getLogger(__name__),
        is_shallow_zarr_storage_enabled=lambda _: True,
        load_group_storage_roots=lambda: {},
        discover_canonical_inputs=lambda *_: (discovered.copy(), ()),
        script_utils=SimpleNamespace(get_objects=lambda *_: ([selected], "")),
        save_plate_as_zarr=lambda *_args, **_kwargs: (None, "input.zarr", ()),
        save_image_as_zarr=lambda *_args, **_kwargs: (None, "input.zarr", ()),
        write_logfile=lambda _: None, canonical_inputs_from_sources=snapshot)
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(TRANSFER_PATH), "exec"), namespace)
    t = constants.transfer
    params = {t.CHANNELS: True, t.MERGED: False, t.CHANNELS_GREY: False,
              t.DATA_TYPE: kind, t.FOLDER: "batch", t.FORMAT: t.FORMAT_OMEZARR,
              t.OME_VERSION: "0.4"}
    with pytest.raises(SnapshotReached):
        namespace["batch_image_export"](conn, params, None, "session", None)
    assert captured == {}
    assert 42 in discovered  # persistent/discovered registration is untouched
