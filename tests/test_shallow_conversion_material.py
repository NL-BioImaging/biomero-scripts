import ast
import json
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
FORMAT_ROUTING_AVAILABLE = "def is_legacy_tiff_conversion_source(" in TRANSFER_PATH.read_text(encoding="utf-8")
requires_format_routing = pytest.mark.skipif(
    not FORMAT_ROUTING_AVAILABLE, reason="Requires workflow format-routing source change")


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
    functions = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name in {
                     "save_as_zarr", "is_legacy_tiff_conversion_source"}]
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
            OME_ZARR_VERSION_0_4="0.4",
        )),
        "json": json,
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
        "upgrade_reused_canonical": lambda conn, obj, source, path: source,
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
    exec(compile(ast.Module(body=functions, type_ignores=[]),
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


@requires_format_routing
def test_tiff_bound_shallow_plate_exports_selected_pixels_as_v2(
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

    assert source is None
    assert artifact == "nuclei-mask.zarr"
    assert labels == ()
    assert materializations == []
    assert len(commands) == 1
    assert "--format 0.4" in commands[0]
    assert "Plate:42" in commands[0]
    assert promotions == []
    assert selections == []


@requires_format_routing
@pytest.mark.parametrize("shallow_enabled", [False, True])
@pytest.mark.parametrize("data_type", ["Image", "Plate"])
@pytest.mark.parametrize("requested", ["0.4", "0.5"])
def test_v3_source_for_tiff_is_exported_as_temporary_v2(
    tmp_path, monkeypatch, data_type, requested, shallow_enabled,
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    reusable = tmp_path / "canonical.zarr"
    reusable.mkdir()
    (reusable / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group"}))
    (reusable / "pixels").write_bytes(b"original pixels")
    before = {p.name: p.read_bytes() for p in reusable.iterdir()}
    canonical = object()
    save, commands, materializations, promotions, _selections, _plate = (
        load_save_as_zarr(tmp_path, shallow_reference=None,
                          reusable_path=reusable))
    direct_sources = {}

    source, _artifact, labels = save(
        SimpleNamespace(host="omero"), "session", ImageObject(), "batch",
        data_type, requested, canonical_source=canonical,
        storage_roots={"group-0-data": tmp_path}, shallow_zarr_storage=shallow_enabled,
        reconstruct_shallow_zarr=False,
        direct_directory_sources=direct_sources,
    )

    assert len(commands) == 1
    assert "--format 0.4" in commands[0]
    assert source is None
    assert labels == ()
    assert direct_sources == {}
    assert materializations == promotions == []
    assert {p.name: p.read_bytes() for p in reusable.iterdir()} == before


@pytest.mark.parametrize("version", [2, 3])
def test_zarr_workflow_reuses_existing_format_with_new_default(
    tmp_path, monkeypatch, version,
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    reusable = tmp_path / "canonical.zarr"
    reusable.mkdir()
    metadata = "zarr.json" if version == 3 else ".zgroup"
    (reusable / metadata).write_text(json.dumps({"zarr_format": version}))
    canonical = object()
    save, commands, _materializations, promotions, _selections, _plate = (
        load_save_as_zarr(tmp_path, shallow_reference=None,
                          reusable_path=reusable))
    source, artifact, _labels = save(
        SimpleNamespace(host="omero"), "session", ImageObject(), "batch",
        "Image", "0.5", canonical_source=canonical,
        shallow_zarr_storage=True, reconstruct_shallow_zarr=True,
    )
    assert source is canonical
    assert commands == promotions == []
    assert (tmp_path / "batch" / artifact / metadata).is_file()


@requires_format_routing
def test_fresh_tiff_input_ignores_v3_request_and_is_not_promoted(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    save, commands, _materializations, promotions, _selections, _plate = (
        load_save_as_zarr(tmp_path, shallow_reference=None))
    source, _artifact, _labels = save(
        SimpleNamespace(host="omero"), "session", ImageObject(), "batch",
        "Image", "0.5", shallow_zarr_storage=True,
        reconstruct_shallow_zarr=False,
    )
    assert "--format 0.4" in commands[0]
    assert source is None
    assert promotions == []


def test_workflow_forwards_zarr_preference_as_reconstruction_policy():
    source = RUNNER_PATH.read_text(encoding="utf-8")

    assert "reconstruct_shallow_zarr=use_zarr_format" in source
    assert "constants.transfer.RECONSTRUCT_SHALLOW_ZARR" in source


def test_shared_pipeline_owns_the_only_image_export():
    """Detached launch must queue before shallow-Zarr reconstruction starts."""
    tree = ast.parse(RUNNER_PATH.read_text(encoding="utf-8"))
    pipeline = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "execute_workflow_pipeline"
    )
    pipeline_nodes = set(ast.walk(pipeline))
    export_calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "exportImageToSLURM"
    ]

    assert len(export_calls) == 1
    assert export_calls[0] in pipeline_nodes
    reconstruction = next(
        keyword.value for keyword in export_calls[0].keywords
        if keyword.arg == "reconstruct_shallow_zarr"
    )
    assert isinstance(reconstruction, ast.Name)
    assert reconstruction.id == "use_zarr_format"


def test_detached_launcher_records_zarr_execution_policy():
    """The supervisor must receive the same Zarr choice as inline execution."""
    tree = ast.parse(RUNNER_PATH.read_text(encoding="utf-8"))
    run_script = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "runScript"
    )
    launcher = next(
        node for node in ast.walk(run_script)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "detached"
        and node.func.attr == "register_detached_launcher"
    )
    extra_params = launcher.args[6]
    assert isinstance(extra_params, ast.Dict)
    recorded = {
        key.value: value
        for key, value in zip(extra_params.keys, extra_params.values)
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }

    assert isinstance(recorded["use_zarr_format"], ast.Name)
    assert recorded["use_zarr_format"].id == "use_zarr_format"
    assert isinstance(recorded["ome_zarr_version"], ast.Name)
    assert recorded["ome_zarr_version"].id == "ome_zarr_version"
