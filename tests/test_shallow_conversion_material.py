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
RUNNER_PATH = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"


class ImageObject:
    def getId(self):
        return 42

    def getName(self):
        return "nuclei-mask"


def load_save_as_zarr(
    tmp_path,
    *,
    shallow_reference,
    reusable_path=None,
    shallow_plate_type=None,
    restore_available=True,
):
    tree = ast.parse(TRANSFER_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "save_as_zarr"
    )
    commands = []
    materializations = []
    promotions = []
    source_selections = []

    class Process:
        returncode = 0

        def __init__(self, command, **_kwargs):
            commands.append(command)
            exported = tmp_path / "batch" / "42.ome.zarr"
            exported.mkdir(parents=True)

        def communicate(self):
            return b"exported", b""

    def select_source(*_args):
        source_selections.append(_args)
        return reusable_path

    plate_type = shallow_plate_type or type("ShallowPlateReference", (), {})

    def materialize(*_args, **_kwargs):
        materializations.append((_args, _kwargs))
        return SimpleNamespace(
            collection=object(),
            labels=("managed-label",),
        )

    canonical_plate = SimpleNamespace(images=(object(),))
    namespace = {
        "Path": Path,
        "ShallowPlateReference": plate_type,
        "build_zarr_export_error": lambda *_args: "export failed",
        "canonical_plate_source_from_collection": (
            lambda *_args: canonical_plate
        ),
        "constants": SimpleNamespace(transfer=SimpleNamespace(
            DATA_TYPE_IMAGE="Image",
            DATA_TYPE_PLATE="Plate",
        )),
        "get_shallow_plate_reference": lambda _obj: shallow_reference,
        "get_shallow_reference": lambda _obj: shallow_reference,
        "log": lambda _message: None,
        "logger": logging.getLogger("shallow-conversion-material-test"),
        "load_group_storage_roots": lambda: {"group-0-data": tmp_path},
        "materialize_shallow_zarr": materialize,
        "os": os,
        "promote_exported_image_zarr": lambda *_args, **_kwargs: (
            promotions.append((_args, _kwargs))
        ),
        "promote_exported_plate_zarr": lambda *_args, **_kwargs: (
            promotions.append((_args, _kwargs))
        ),
        "select_zarr_source_path": select_source,
        "SHALLOW_ZARR_RESTORE_AVAILABLE": restore_available,
        "SHALLOW_ZARR_RESTORE_IMPORT_ERROR": ImportError(
            "BIOMERO.importer unavailable"
        ),
        "shutil": shutil,
        "subprocess": SimpleNamespace(
            PIPE=object(),
            Popen=Process,
        ),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(TRANSFER_PATH), "exec"), namespace)
    return (
        namespace["save_as_zarr"],
        commands,
        materializations,
        promotions,
        source_selections,
        canonical_plate,
    )


def test_tiff_bound_shallow_image_uses_fresh_omero_pixel_export(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    shallow = object()
    save, commands, materializations, promotions, selections, _plate = (
        load_save_as_zarr(tmp_path, shallow_reference=shallow)
    )

    source, artifact, labels = save(
        SimpleNamespace(host="omero"),
        "session",
        ImageObject(),
        "batch",
        "Image",
        "0.4",
        canonical_source=object(),
        storage_roots={"group-0-data": tmp_path},
        shallow_zarr_storage=True,
        reconstruct_shallow_zarr=False,
    )

    assert source is None
    assert artifact == "nuclei-mask.zarr"
    assert labels == ()
    assert materializations == []
    assert promotions == []
    assert selections == []
    assert len(commands) == 1
    assert "Image:42" in commands[0]


def test_conversion_mode_does_not_disable_normal_canonical_reuse(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    reusable = tmp_path / "canonical.zarr"
    reusable.mkdir()
    (reusable / ".zgroup").write_text('{"zarr_format":2}', encoding="utf-8")
    canonical = object()
    save, commands, materializations, promotions, selections, _plate = (
        load_save_as_zarr(
            tmp_path,
            shallow_reference=None,
            reusable_path=reusable,
        )
    )

    source, artifact, labels = save(
        SimpleNamespace(host="omero"),
        "session",
        ImageObject(),
        "batch",
        "Image",
        "0.4",
        canonical_source=canonical,
        storage_roots={"group-0-data": tmp_path},
        shallow_zarr_storage=True,
        reconstruct_shallow_zarr=False,
    )

    assert source is canonical
    assert artifact == "nuclei-mask.zarr"
    assert labels == ()
    assert len(selections) == 1
    assert commands == []
    assert materializations == []
    assert promotions == []
    assert (tmp_path / "batch" / "nuclei-mask.zarr" / ".zgroup").is_file()


def test_missing_reconstruction_flag_keeps_full_reconstruction(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    canonical = object()
    shallow = SimpleNamespace(source=canonical)
    save, commands, materializations, promotions, selections, _plate = (
        load_save_as_zarr(tmp_path, shallow_reference=shallow)
    )

    source, artifact, labels = save(
        SimpleNamespace(host="omero"),
        "session",
        ImageObject(),
        "batch",
        "Image",
        "0.4",
        storage_roots={"group-0-data": tmp_path},
        shallow_zarr_storage=True,
    )

    assert source is canonical
    assert artifact == "nuclei-mask.zarr"
    assert labels == ("managed-label",)
    assert len(materializations) == 1
    assert commands == []
    assert promotions == []
    assert selections == []


def test_existing_shallow_reconstructs_when_new_shallowing_is_disabled(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    canonical = object()
    shallow = SimpleNamespace(source=canonical)
    save, commands, materializations, promotions, selections, _plate = (
        load_save_as_zarr(tmp_path, shallow_reference=shallow)
    )

    source, artifact, labels = save(
        SimpleNamespace(host="omero"),
        "session",
        ImageObject(),
        "batch",
        "Image",
        "0.4",
        storage_roots={"group-0-data": tmp_path},
        shallow_zarr_storage=False,
        reconstruct_shallow_zarr=True,
    )

    assert source is canonical
    assert artifact == "nuclei-mask.zarr"
    assert labels == ("managed-label",)
    assert len(materializations) == 1
    assert commands == []
    assert promotions == []
    assert selections == []


def test_existing_shallow_fails_clearly_without_restore_support(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    shallow = SimpleNamespace(source=object())
    save, *_rest = load_save_as_zarr(
        tmp_path,
        shallow_reference=shallow,
        restore_available=False,
    )

    with pytest.raises(RuntimeError, match="reconstruction support"):
        save(
            SimpleNamespace(host="omero"),
            "session",
            ImageObject(),
            "batch",
            "Image",
            "0.4",
            shallow_zarr_storage=False,
            reconstruct_shallow_zarr=True,
        )


def test_plate_cannot_enter_selected_pixel_conversion_mode(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()

    class PlateReference:
        pass

    shallow = PlateReference()
    save, commands, materializations, promotions, selections, plate_source = (
        load_save_as_zarr(
            tmp_path,
            shallow_reference=shallow,
            shallow_plate_type=PlateReference,
        )
    )

    source, artifact, labels = save(
        SimpleNamespace(host="omero"),
        "session",
        ImageObject(),
        "batch",
        "Plate",
        "0.4",
        storage_roots={"group-0-data": tmp_path},
        shallow_zarr_storage=True,
        reconstruct_shallow_zarr=False,
    )

    assert source is plate_source
    assert artifact == "nuclei-mask.zarr"
    assert labels == ("managed-label",)
    assert len(materializations) == 1
    assert commands == []
    assert promotions == []
    assert selections == []


def test_workflow_forwards_zarr_preference_as_reconstruction_policy():
    source = RUNNER_PATH.read_text(encoding="utf-8")

    assert "reconstruct_shallow_zarr=use_zarr_format" in source
    assert "constants.transfer.RECONSTRUCT_SHALLOW_ZARR" in source
