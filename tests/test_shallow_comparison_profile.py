"""Current comparison metadata is lazy; source linkage never fabricates hashes."""
from pathlib import Path
import os
from types import SimpleNamespace

import pytest

from test_canonical_zarr_transfer import _load_canonical_functions

pytestmark = pytest.mark.skipif(
    "defer_identity" not in (Path(os.environ["BIOMERO_SCRIPTS_ROOT"]) /
                            "_data/_SLURM_Image_Transfer.py").read_text(encoding="utf-8"),
    reason="Lazy source comparison profile is not present in selected production source",
)


def load_functions(relative, names, **values):
    import ast
    import json
    import logging
    import os
    from uuid import UUID
    root = Path(os.environ["BIOMERO_SCRIPTS_ROOT"])
    tree = ast.parse((root / relative).read_text(encoding="utf-8"))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    namespace = dict(json=json, UUID=UUID, logger=logging.getLogger(__name__),
                     PROCESSED_DATA_FOLDER=".processed",
                     InvalidCanonicalStore=RuntimeError, **values)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), relative, "exec"), namespace)
    return namespace


def policy_helpers():
    constants = SimpleNamespace(
        CLEANUP="cleanup",
        workflow=SimpleNamespace(USE_ZARR_FORMAT="use_zarr",
            SKIP_ORIGINAL_PIXEL_COMPARISON="Skip_Original_Pixel_Comparison"),
        transfer=SimpleNamespace(DATA_TYPE="type", IDS="ids", OME_VERSION="version",
                                  OME_ZARR_VERSION_0_4="0.4"),
        results=SimpleNamespace(OUTPUT_ATTACH_NEW_DATASET_ID="dataset_id",
                                OUTPUT_ATTACH_NEW_SCREEN_ID="screen_id"))
    return load_functions("__workflows/SLURM_Run_Workflow.py",
        {"build_output_settings", "resolve_pipeline_inputs"}, constants=constants,
        OUTPUT_OPTIONS=(), unwrap=lambda value: value)


def test_parameter_key_matches_shared_core_contract_when_paired():
    if os.environ.get("BIOMERO_CORE_ROOT"):
        import importlib.util
        path = Path(os.environ["BIOMERO_CORE_ROOT"]) / "biomero/constants.py"
        spec = importlib.util.spec_from_file_location("paired_core_constants", path)
        core_constants = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(core_constants)
    else:
        core_constants = pytest.importorskip("biomero.constants")
    assert core_constants.workflow.SKIP_ORIGINAL_PIXEL_COMPARISON == (
        "Skip_Original_Pixel_Comparison")


@pytest.mark.parametrize("value", [None, False, True])
def test_direct_script_policy_comes_from_typed_parameter(value):
    ns = policy_helpers()
    inputs = {"Skip_Original_Pixel_Comparison": value}
    client = SimpleNamespace(getInput=inputs.get)
    settings = ns["build_output_settings"](client, {})
    assert settings["original_pixels_not_required"] is (value is True)


@pytest.mark.parametrize("recorded", [None, False, True])
@pytest.mark.parametrize("raw", [None, False, True])
def test_detached_resume_and_batch_child_keep_run_policy(recorded, raw):
    ns = policy_helpers()
    inputs = {"Skip_Original_Pixel_Comparison": raw}
    client = SimpleNamespace(getInput=inputs.get)
    params = {"selected_output": {}, "roi_label_pattern": "", "email": "test@example.org",
              "group": 1, "use_zarr_format": False}
    if recorded is not None:
        params["output_settings"] = {"original_pixels_not_required": recorded}
    resolved = ns["resolve_pipeline_inputs"](client, None, None, params)
    assert resolved["output_settings"]["original_pixels_not_required"] is (
        raw is True if recorded is None else recorded)


@pytest.mark.parametrize("script", ["SLURM_Run_Workflow.py", "SLURM_Run_Workflow_Batched.py"])
def test_normal_and_batched_script_register_optional_boolean(script):
    import ast
    path = Path(os.environ["BIOMERO_SCRIPTS_ROOT"]) / "__workflows" / script
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and node.args and isinstance(node.args[0], ast.Attribute)
             and node.args[0].attr == "SKIP_ORIGINAL_PIXEL_COMPARISON"
             and ast.unparse(node.func) == "omscripts.Bool"]
    assert len(calls) == 1
    assert ast.unparse(calls[0].func) == "omscripts.Bool"
    assert {k.arg: ast.literal_eval(k.value) for k in calls[0].keywords}["default"] is False


def test_batched_launcher_preserves_parameter_for_child_resolution():
    import ast
    path = (Path(os.environ["BIOMERO_SCRIPTS_ROOT"]) /
            "__workflows/SLURM_Run_Workflow_Batched.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    base_inputs = next(value for node in ast.walk(tree) if isinstance(node, ast.Dict)
                       for key, value in zip(node.keys, node.values)
                       if isinstance(key, ast.Constant) and key.value == "base_inputs")
    raw = {"Skip_Original_Pixel_Comparison": SimpleNamespace(val=True)}
    recorded = eval(compile(ast.Expression(base_inputs), str(path), "eval"), {
        "inputs": raw, "unwrap": lambda value: value.val})
    assert recorded == {"Skip_Original_Pixel_Comparison": True}
    ns = policy_helpers()
    settings = ns["build_output_settings"](SimpleNamespace(getInput=recorded.get), {})
    assert settings["original_pixels_not_required"] is True


def test_scripts_do_not_read_web_ui_policy():
    source = (Path(os.environ["BIOMERO_SCRIPTS_ROOT"]) /
              "__workflows/SLURM_Run_Workflow.py").read_text(encoding="utf-8")
    assert "original_pixels_not_required_workflows" not in source


def test_policy_is_read_from_executed_task_not_current_admin_config():
    from biomero_schema.zarr import CanonicalInputManifest
    from uuid import uuid4
    from test_canonical_snapshot_orchestration import canonical_input
    wf_id, task_id = uuid4(), uuid4()
    manifest = CanonicalInputManifest(workflow_id=wf_id, export_task_id=uuid4(),
                                     inputs=(canonical_input(),))
    task = SimpleNamespace(task_name="segment", task_version="v2", job_ids=[41],
                           params={"output_settings": {"original_pixels_not_required": True}})
    records = {wf_id: SimpleNamespace(tasks=[task_id]), task_id: task}
    tracker = SimpleNamespace(repository=SimpleNamespace(get=records.__getitem__),
                              get_canonical_input_manifest=lambda _: manifest)
    ns = load_functions("_data/SLURM_Import_Results.py", {"load_canonical_input_snapshot"})
    result = ns["load_canonical_input_snapshot"](
        SimpleNamespace(workflowTracker=tracker, track_workflows=True), wf_id)
    assert result.original_pixels_not_required is True
    assert result.workflow_name == "segment"
    assert result.workflow_version == "v2"


def test_index_existing_image_can_defer_unused_comparison(tmp_path):
    ns = _load_canonical_functions()
    store = tmp_path / "existing.ome.zarr"
    store.mkdir()
    image = SimpleNamespace(getId=lambda: 7)
    guard = SimpleNamespace(shape=(1, 1, 1, 8, 8), dtype="uint16",
                            axes=("t", "c", "z", "y", "x"),
                            coordinate_transformations=(),
                            ngff_version="0.4", zarr_format=2)
    ns["validate_omero_image_semantics"] = lambda *_: None
    ns["locate_managed_zarr"] = lambda *_: ("data", tmp_path, Path(store.name))
    ns["attach_canonical_source"] = lambda *_args, **_kwargs: None
    provider = SimpleNamespace(generate=lambda *_args, **_kwargs: pytest.fail(
        "Source-linked preparation must not read original pixels"))

    result = ns["index_existing_image_zarr"](
        None, image, store, {"data": tmp_path}, identity_provider=provider,
        semantic_guard_reader=lambda *_: guard, defer_identity=True)

    assert result.pixel_identity is None
    assert not result.canonical_pixel_verified
    assert result.relative_path == store.name
    assert store.is_dir()


def test_reuse_adopts_refreshed_current_cache_without_hashing(tmp_path):
    from biomero_schema.zarr import PIXEL_COMPARISON_PROFILE
    from test_canonical_zarr_transfer import Object, annotation_for
    from test_canonical_snapshot_orchestration import canonical_input
    old = canonical_input().source
    identity = old.pixel_identity.model_copy(update={
        "comparison_profile": PIXEL_COMPARISON_PROFILE, "schema_version": 2,
        "dataset_path": "s1", "comparison_shape": old.pixel_identity.shape})
    updated = old.model_copy(update={"pixel_identity": identity})
    ns = _load_canonical_functions()
    ns["load_canonical_marker"] = lambda _, **_kwargs: updated
    ns["get_legacy_zarr_path"] = lambda _: pytest.fail("Current cache should be reused")
    writes = []
    ns["_update_canonical_annotation"] = lambda annotation, values: writes.append(values)
    obj = Object(old.source_object_id, [annotation_for(old)])
    result = ns["upgrade_reused_canonical"](None, obj, old, tmp_path)
    assert result == updated
    assert len(writes) == 1
