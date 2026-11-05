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
    namespace = dict(json=json, UUID=UUID, logger=logging.getLogger(__name__), **values)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), relative, "exec"), namespace)
    return namespace


@pytest.mark.parametrize("raw, expected", [(None, []), ("", []), ("[]", []),
                                           ('["segment"]', ["segment"]),
                                           ('true', []), ('[1]', []), ('invalid', [])])
def test_workflow_shortcut_config_is_explicit(raw, expected):
    import configparser
    config = configparser.ConfigParser(allow_no_value=True)
    config["UI"] = {}
    if raw is not None:
        config["UI"]["original_pixels_not_required_workflows"] = raw
    ns = load_functions("__workflows/SLURM_Run_Workflow.py",
                        {"original_pixels_not_required_workflows"})
    assert ns["original_pixels_not_required_workflows"](config) == expected


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
    ns["load_canonical_marker"] = lambda _: updated
    ns["get_legacy_zarr_path"] = lambda _: pytest.fail("Current cache should be reused")
    writes = []
    ns["_update_canonical_annotation"] = lambda annotation, values: writes.append(values)
    obj = Object(old.source_object_id, [annotation_for(old)])
    result = ns["upgrade_reused_canonical"](None, obj, old, tmp_path)
    assert result == updated
    assert len(writes) == 1
