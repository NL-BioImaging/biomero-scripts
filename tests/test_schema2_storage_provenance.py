"""Real schema-2 evidence from managed result stores (scripts issue #41)."""
import copy
import hashlib
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from biomero_schema.zarr import (
    CanonicalZarrSource, PixelIdentity, ShallowBindings, ShallowCollection,
    ShallowImageBinding, ShallowImageNode, ShallowManifest,
)

from test_storage_provenance import SOURCE, connection, load

WF = "4f596081-5d48-41af-a539-a3c5e120ba66"
AVAILABLE = "ShallowManifest.from_dict" in SOURCE.read_text(encoding="utf-8")
pytestmark = pytest.mark.skipif(
    not AVAILABLE and not os.environ.get("BIOMERO_TEST_SCHEMA2_STORAGE"),
    reason="schema-2 storage provenance fix is not available",
)


def manifest_dict(profile="ngff-0.4-zarr-v2"):
    images, bindings = [], []
    for index, path in enumerate(("A/1/0", "A/2/0")):
        identity = PixelIdentity(
            node_path=path, role="image", iscc_code=f"ISCC:FIELD{index}",
            data_code="ISCC:GDATA", instance_code="ISCC:IINSTANCE",
            tool_version="0.2.0", imagewalk_revision="draft-2026-06",
            shape=(1, 1, 1, 64, 64), dtype="uint16",
            axes=("t", "c", "z", "y", "x"),
        )
        source = CanonicalZarrSource(
            storage_root="group-5-data", relative_path="source.zarr",
            node_path=path, source_object_type="Plate", source_object_id=101,
            source_generation=1, interchange_profile=profile,
            pixel_identity=identity, pixel_identity_origin="raw",
            canonical_pixel_verified=True,
        )
        images.append(ShallowImageNode(id=f"image-{index}", name=path, node_path=path))
        bindings.append(ShallowImageBinding(
            node_id=f"image-{index}", source=source, returned_pixel_identity=identity))
    return ShallowManifest(
        workflow_id=UUID(WF), transfer_artifact="result.zarr",
        interchange_profile=profile,
        collection=ShallowCollection(name="Plate result", images=tuple(images)),
        bindings=ShallowBindings(images=tuple(reversed(bindings))),
    ).to_dict()


@pytest.fixture
def managed_result(tmp_path, monkeypatch):
    monkeypatch.setenv("IMPORT_MOUNT_PATH", str(tmp_path))
    root = tmp_path / "result.zarr"
    root.mkdir()
    manifest = root / ".biomero-shallow.json"
    manifest.write_text(json.dumps(manifest_dict()), encoding="utf-8")
    reference = {"storageRoot": "import-mount-data", "relativePath": "result.zarr",
                 "workflowId": WF}
    return root, manifest, reference


def collect(reference, kind="Plate", extra=()):
    return load()["collect_import_storage_provenance"](
        connection([("biomero.zarr.shallow", reference), *extra]), kind, 301, WF)


@pytest.mark.parametrize("profile", ["ngff-0.4-zarr-v2", "ngff-0.5-zarr-v3"])
def test_plate_joins_graph_to_bindings_and_preserves_remote_receipt(managed_result, profile):
    root, manifest, reference = managed_result
    manifest.write_text(json.dumps(manifest_dict(profile)), encoding="utf-8")
    receipt = {"schema": 1, "workflow_id": WF, "storage": "shallow-zarr",
               "location": "remote", "container": "helper:0.1.0b4",
               "tool_version": "0.1.0b4"}
    (root / ".biomero-import-storage.json").write_text(json.dumps(receipt))
    evidence = collect(reference)
    assert evidence == {**receipt, "source_biocodes": ["ISCC:FIELD0", "ISCC:FIELD1"],
                        "manifest": "import-mount-data/result.zarr/.biomero-shallow.json",
                        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()}


def test_image_selects_its_graph_path_before_resolving_binding(managed_result):
    _, _, reference = managed_result
    evidence = collect({**reference, "imageNodePath": "A/2/0"}, "Image")
    assert evidence["source_biocodes"] == ["ISCC:FIELD1"]
    assert evidence["location"] == "unknown"
    assert "tool_version" not in evidence


@pytest.mark.parametrize("mutation", ["workflow", "missing-binding", "wrong-node",
                                    "wrong-path", "legacy-schema", "duplicate-binding"])
def test_invalid_manifest_is_rejected(managed_result, mutation):
    _, manifest, reference = managed_result
    data = json.loads(manifest.read_text())
    if mutation == "workflow":
        data["workflowId"] = str(uuid4())
    elif mutation == "missing-binding":
        data["bindings"]["images"].pop()
    elif mutation == "wrong-node":
        data["bindings"]["images"][0]["nodeId"] = "not-in-graph"
    elif mutation == "wrong-path":
        data["collection"]["images"][0]["nodePath"] = "A/3/0"
    elif mutation == "legacy-schema":
        data["schema"] = 1
    else:
        data["bindings"]["images"].append(copy.deepcopy(data["bindings"]["images"][0]))
    manifest.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        collect(reference)


@pytest.mark.parametrize("path", ["A/3/0", ""])
def test_unknown_image_path_is_rejected(managed_result, path):
    _, _, reference = managed_result
    with pytest.raises(ValueError):
        collect({**reference, "imageNodePath": path}, "Image")


def test_image_without_reference_does_not_inherit_all_plate_biocodes(managed_result):
    root, _, _ = managed_result
    # Legacy importer provenance can identify a field even without a shallow map.
    field = root / "A" / "2" / "0"
    field.mkdir(parents=True)
    evidence = load()["collect_import_storage_provenance"](
        connection([("biomero.import", {"Filepath": str(field)})]), "Image", 2, WF)
    assert evidence["source_biocodes"] == ["ISCC:FIELD1"]


def test_image_without_unambiguous_field_is_rejected(managed_result):
    root, _, reference = managed_result
    with pytest.raises(ValueError):
        collect(reference, "Image", [("biomero.import", {"Filepath": str(root)})])


@pytest.mark.parametrize("reference_change", [
    {"relativePath": "../outside.zarr"},
    {"storageRoot": "unmanaged"},
    {"workflowId": "another-workflow"},
])
def test_reference_guards(managed_result, reference_change):
    _, _, reference = managed_result
    with pytest.raises(ValueError):
        collect({**reference, **reference_change})


def test_duplicate_reference_is_rejected(managed_result):
    _, _, reference = managed_result
    with pytest.raises(ValueError):
        collect(reference, extra=[("biomero.zarr.shallow", reference)])


def test_missing_manifest_is_rejected(managed_result):
    _, manifest, reference = managed_result
    manifest.unlink()
    with pytest.raises(ValueError):
        collect(reference)


def tracker_fixture():
    tid, other = uuid4(), uuid4()
    task = SimpleNamespace(id=tid, task_name="SLURM_Import_Results.py", storage_provenance={})
    objects = {UUID(WF): SimpleNamespace(tasks=[tid, other]), tid: task,
               other: SimpleNamespace(id=other, task_name="_SLURM_Remote_Shallower")}
    tracker = Mock()
    tracker.repository.get.side_effect = objects.__getitem__
    tracker.record_storage_provenance.side_effect = (
        lambda task_id, target, evidence: task.storage_provenance.update({target: evidence}))
    return SimpleNamespace(workflowTracker=tracker), task


def test_backfill_is_previewed_idempotent_and_reads_only_evidence(managed_result):
    root, _, reference = managed_result
    conn = connection([("biomero.zarr.shallow", reference),
                       ("biomero/workflow", {"Workflow_ID": WF})])
    conn.isAdmin.return_value = True
    slurm, task = tracker_fixture()
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    repair = load()["backfill_import_storage_provenance"]
    preview = repair(conn, slurm, "Plate", 301, WF)
    assert preview["status"] == "planned"
    assert preview["evidence"]["source_biocodes"] == ["ISCC:FIELD0", "ISCC:FIELD1"]
    slurm.workflowTracker.record_storage_provenance.assert_not_called()
    assert repair(conn, slurm, "Plate", 301, WF, dry_run=False)["status"] == "recorded"
    assert task.storage_provenance["Plate:301"] == preview["evidence"]
    assert repair(conn, slurm, "Plate", 301, WF, dry_run=False)["status"] == "unchanged"
    slurm.workflowTracker.record_storage_provenance.assert_called_once()
    assert {p: p.read_bytes() for p in root.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("failure", ["not-admin", "no-evidence", "wrong-workflow-map",
                                   "ambiguous-import-task", "conflicting-evidence"])
def test_backfill_fails_before_writing_for_unsafe_targets(managed_result, failure):
    root, _, reference = managed_result
    anns = [("biomero.zarr.shallow", reference),
            ("biomero/workflow", {"Workflow_ID": WF})]
    if failure == "no-evidence":
        anns = anns[1:]
    elif failure == "wrong-workflow-map":
        anns[1][1]["Workflow_ID"] = str(uuid4())
    conn = connection(anns)
    conn.isAdmin.return_value = failure != "not-admin"
    slurm, task = tracker_fixture()
    if failure == "ambiguous-import-task":
        slurm.workflowTracker.repository.get(UUID(WF)).tasks.append(task.id)
    elif failure == "conflicting-evidence":
        task.storage_provenance["Plate:301"] = {"storage": "full-zarr"}
    with pytest.raises(ValueError):
        load()["backfill_import_storage_provenance"](conn, slurm, "Plate", 301, WF,
                                                   dry_run=False)
    slurm.workflowTracker.record_storage_provenance.assert_not_called()


@pytest.mark.parametrize("kind", ["Image", "Plate"])
@pytest.mark.parametrize("status", ["failed", "reduced"])
def test_partial_metadata_is_an_explicit_user_warning(monkeypatch, kind, status):
    from test_import_result_source_matching import load_functions
    from test_provenance_fallback import load as load_annotations
    omero = SimpleNamespace(sys=SimpleNamespace(ParametersI=Mock))
    monkeypatch.setitem(sys.modules, "omero", omero)
    monkeypatch.setitem(sys.modules, "omero.sys", omero.sys)
    ns = (load_annotations("Import") if kind == "Plate" else
          load_functions("add_metadata_to_imported_images"))
    target = Mock()
    target.getId.return_value = 301
    conn = Mock()
    conn.getQueryService.return_value.projection.return_value = [[SimpleNamespace(val=301)]]
    conn.getObject.return_value = target
    ns[f"add_{kind.lower()}_annotations"] = Mock(return_value=status)
    fn = ns[f"add_metadata_to_imported_{'plates' if kind == 'Plate' else 'images'}"]
    message = fn(conn, Mock(), 201, ["order"], WF, "15")
    assert "WARNING:" in message
    assert "metadata" in message.lower()
    assert "activity log" in message.lower()
    assert not message.startswith("FAILED:")


def test_schema2_storage_event_precedes_rendering_and_is_written_to_import_map(managed_result):
    from test_provenance_fallback import load as load_annotations
    _, _, reference = managed_result
    conn = connection([("biomero.zarr.shallow", reference)])
    slurm, task = tracker_fixture()
    slurm.track_workflows = True
    ns = load_annotations("Import")
    ns.update(load())
    posted = []

    def render(tracker, workflow_id, *, target_key):
        assert tracker is slurm.workflowTracker
        assert workflow_id == WF
        evidence = task.storage_provenance[target_key]  # Must already be persisted.
        return [SimpleNamespace(
            namespace=f"biomero/workflow/task/{task.id}",
            values={"Task_ID": str(task.id), "Storage_Format": evidence["storage"],
                    "Canonical_Biocodes": ", ".join(evidence["source_biocodes"])})]

    ns["render_workflow_metadata"] = render
    ns["post_provenance_map_annotation"] = lambda **kw: (posted.append(kw) or 1, "complete")
    assert ns["add_object_annotations"](conn, slurm, "Plate", 301, "15", WF) == "complete"
    assert posted[0]["kv_dict"]["Storage_Format"] == "shallow-zarr"
    assert posted[0]["kv_dict"]["Canonical_Biocodes"] == "ISCC:FIELD0, ISCC:FIELD1"


def test_bad_storage_evidence_still_writes_other_metadata(managed_result):
    from test_provenance_fallback import load as load_annotations
    _, manifest, reference = managed_result
    manifest.write_text('{"schema": 2}')
    conn = connection([("biomero.zarr.shallow", reference)])
    slurm, _ = tracker_fixture()
    slurm.track_workflows = True
    ns = load_annotations("Import")
    ns.update(load())
    ns["render_workflow_metadata"] = Mock(return_value=[
        SimpleNamespace(namespace="biomero/workflow", values={"Workflow_ID": WF})])
    ns["post_provenance_map_annotation"] = Mock(return_value=(1, "complete"))
    assert ns["add_object_annotations"](conn, slurm, "Plate", 301, "15", WF) == "failed"
    slurm.workflowTracker.record_storage_provenance.assert_not_called()
    ns["post_provenance_map_annotation"].assert_called_once()
