"""The scripts read the import-storage record and the canonical marker through
the biomero-schema models (biomero-scripts#93, biomero-schema#12 and #14).

Two kinds of test:

* source tests fail when a script goes back to its own file name literals or
  its own parsing;
* behaviour tests read files as earlier betas wrote them (they must keep being
  accepted, with the same evidence) and files the models reject.
"""

import ast
import json
import logging
import os
from pathlib import Path

import pytest
from biomero_schema.imports import (
    IMPORT_STORAGE_FILENAME,
    IMPORT_STORAGE_FULL,
    IMPORT_STORAGE_SCHEMA,
    IMPORT_STORAGE_SHALLOW,
    parse_import_storage_record,
)
from biomero_schema.zarr import (
    CANONICAL_MARKER_NAME,
    SHALLOW_COLLECTION_MANIFEST,
    SHALLOW_COLLECTION_NAMESPACE,
    CanonicalMarker,
    CanonicalZarrSource,
)

SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
IMPORT_RESULTS = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"
IMAGE_TRANSFER = SOURCE_ROOT / "_data" / "_SLURM_Image_Transfer.py"
MIGRATE = SOURCE_ROOT / "admin" / "BIOMERO_Migrate_Shallow_Storage.py"
WF = "4f596081-5d48-41af-a539-a3c5e120ba66"
OTHER_WF = "9a1c1d1e-0000-4000-8000-000000000001"

# TODO(biomero-scripts#109): remove this gate once the scripts adopted the
# models (biomero-scripts#112) are merged.
# The tests of the new behaviour wait for the scripts to adopt the models;
# until then only the compatibility tests (which hold for both) run.
ADOPTED = "parse_import_storage_record" in IMPORT_RESULTS.read_text(
    encoding="utf-8")
requires_adoption = pytest.mark.skipif(
    not ADOPTED and not os.environ.get("BIOMERO_TEST_SCHEMA_STORAGE_MODELS"),
    reason="scripts do not use the biomero-schema storage models yet",
)


def _tree(path):
    return ast.parse(path.read_text(encoding="utf-8"))


def _string_literals(path):
    return {node.value for node in ast.walk(_tree(path))
            if isinstance(node, ast.Constant) and isinstance(node.value, str)}


def _imported_names(path, module):
    return {alias.name for node in ast.walk(_tree(path))
            if isinstance(node, ast.ImportFrom) and node.module == module
            for alias in node.names}


@requires_adoption
def test_import_results_uses_the_import_storage_models():
    names = _imported_names(IMPORT_RESULTS, "biomero_schema.imports")
    assert {"IMPORT_STORAGE_FILENAME", "parse_import_storage_record"} <= names
    assert IMPORT_STORAGE_FILENAME not in _string_literals(IMPORT_RESULTS)


@requires_adoption
def test_import_results_names_the_shallow_files_through_schema_constants():
    names = _imported_names(IMPORT_RESULTS, "biomero_schema.zarr")
    assert {"SHALLOW_COLLECTION_MANIFEST", "SHALLOW_COLLECTION_NAMESPACE",
            "ShallowManifest"} <= names
    literals = _string_literals(IMPORT_RESULTS)
    assert SHALLOW_COLLECTION_MANIFEST not in literals
    assert SHALLOW_COLLECTION_NAMESPACE not in literals
    function_imports = [
        node for function in ast.walk(_tree(IMPORT_RESULTS))
        if isinstance(function, ast.FunctionDef)
        for node in ast.walk(function)
        if isinstance(node, ast.ImportFrom) and node.module == "biomero_schema.zarr"]
    assert function_imports == []


@requires_adoption
def test_image_transfer_names_the_marker_through_the_schema_constant():
    assert "CANONICAL_MARKER_NAME" in _imported_names(
        IMAGE_TRANSFER, "biomero_schema.zarr")
    assert CANONICAL_MARKER_NAME not in _string_literals(IMAGE_TRANSFER)


@requires_adoption
def test_migration_reads_the_marker_with_the_model():
    names = _imported_names(MIGRATE, "biomero_schema.zarr")
    assert {"CANONICAL_MARKER_NAME", "CanonicalMarker"} <= names
    assert CANONICAL_MARKER_NAME not in _string_literals(MIGRATE)
    assert "markerSchema" not in _string_literals(MIGRATE)


# --- import-storage record -------------------------------------------------

def _collect():
    from test_storage_provenance import load
    return load()["collect_import_storage_provenance"]


def _result_with_record(tmp_path, monkeypatch, record):
    monkeypatch.setenv("IMPORT_MOUNT_PATH", str(tmp_path))
    root = tmp_path / "result.zarr"
    root.mkdir()
    (root / ".zgroup").write_text("{}")
    (root / IMPORT_STORAGE_FILENAME).write_text(json.dumps(record))
    return root


def _read(root, record_workflow=WF):
    from test_storage_provenance import connection
    conn = connection([("biomero.import", {"Imported_from": str(root)})])
    return _collect()(conn, "Plate", 1, record_workflow)


# Records as BIOMERO.importer wrote them. Releases v1.5.0-beta.6 to beta.11 did
# not write decision_basis; beta.12 onwards does.
EARLIER_BETA_RECORDS = {
    "remote-without-decision-basis": {
        "schema": 1, "workflow_id": WF, "storage": "shallow-zarr",
        "location": "remote", "container": "ghcr.io/x/shallower:0.1.0b4",
        "tool_version": "0.1.0b4", "task_id": "task-1", "job_id": None,
        "report_sha256": "a" * 64},
    "remote-full-without-decision-basis": {
        "schema": 1, "workflow_id": WF, "storage": "full-zarr",
        "location": "remote", "container": "ghcr.io/x/shallower:0.1.0b4",
        "tool_version": "0.1.0b4", "task_id": "task-3", "job_id": None,
        "report_sha256": "c" * 64},
    # Beta.14 and later write the optional fields explicitly, also as null.
    "remote-full-explicit-nulls": {
        "schema": 1, "workflow_id": WF, "storage": "full-zarr",
        "location": "remote", "decision_basis": None,
        "container": "ghcr.io/x/shallower:0.1.0b11",
        "tool_version": "0.1.0b11", "task_id": "task-4", "job_id": None,
        "report_sha256": "d" * 64},
    "importer-shallow-explicit-null": {
        "schema": 1, "workflow_id": WF, "storage": "shallow-zarr",
        "location": "importer", "tool_version": "0.1.0b11",
        "decision_basis": None},
    "remote-with-job": {
        "schema": 1, "workflow_id": WF, "storage": "full-zarr",
        "location": "remote", "decision_basis": "pixel-comparison",
        "container": "ghcr.io/x/shallower:0.1.0b11",
        "tool_version": "0.1.0b11", "task_id": "task-2", "job_id": "12345",
        "report_sha256": "b" * 64},
    "importer-shallow-without-decision-basis": {
        "schema": 1, "workflow_id": WF, "storage": "shallow-zarr",
        "location": "importer", "tool_version": "0.1.0b9"},
    "importer-shallow": {
        "schema": 1, "workflow_id": WF, "storage": "shallow-zarr",
        "location": "importer", "tool_version": "0.1.0b11",
        "decision_basis": "pixel-comparison"},
    "importer-full": {
        "schema": 1, "workflow_id": WF, "storage": "full-zarr",
        "location": "importer", "tool_version": "0.1.0b9",
        "reason": "result pixels changed"},
}


def _result_store(tmp_path, monkeypatch, record):
    """A managed result store holding ``record``; shallow ones get a manifest."""
    from test_schema2_storage_provenance import manifest_dict
    monkeypatch.setenv("IMPORT_MOUNT_PATH", str(tmp_path))
    root = tmp_path / "result.zarr"
    root.mkdir()
    if record["storage"] == IMPORT_STORAGE_SHALLOW:
        (root / ".biomero-shallow.json").write_text(json.dumps(manifest_dict()))
    else:
        (root / ".zgroup").write_text("{}")
    (root / IMPORT_STORAGE_FILENAME).write_text(json.dumps(record))
    return root


@pytest.mark.parametrize("name", sorted(EARLIER_BETA_RECORDS))
def test_record_from_an_earlier_beta_gives_the_evidence_it_always_did(
        tmp_path, monkeypatch, name):
    """The evidence is persisted in the event store and compared on backfill,
    so a record must come through key for key, with no key added for a field
    the old file did not have (for example ``decision_basis``)."""
    record = EARLIER_BETA_RECORDS[name]
    root = _result_store(tmp_path, monkeypatch, record)

    evidence = _read(root)

    # A shallow result takes decision_basis from its manifest, not the record.
    expected = {key: value for key, value in record.items()
                if not (record["storage"] == IMPORT_STORAGE_SHALLOW
                        and key == "decision_basis")}
    assert {key: evidence[key] for key in expected} == expected
    assert set(evidence) - set(record) <= {
        "manifest", "manifest_sha256", "source_biocodes", "decision_basis"}
    if record["storage"] == IMPORT_STORAGE_FULL:
        assert set(evidence) == set(record)
        # An explicit null is part of the file, so it stays in the evidence.
        assert evidence.get("decision_basis") == record.get("decision_basis")


@pytest.mark.parametrize("name", sorted(EARLIER_BETA_RECORDS))
def test_earlier_beta_records_are_valid_for_the_schema_models(name):
    record = EARLIER_BETA_RECORDS[name]

    parsed = parse_import_storage_record(record)

    assert parsed.model_dump(by_alias=True, mode="json",
                             exclude_unset=True) == record
    assert parsed.schema_version == IMPORT_STORAGE_SCHEMA
    assert parsed.storage in (IMPORT_STORAGE_SHALLOW, IMPORT_STORAGE_FULL)


def test_record_of_another_workflow_is_still_rejected(tmp_path, monkeypatch):
    record = {**EARLIER_BETA_RECORDS["importer-full"], "workflow_id": OTHER_WF}
    root = _result_with_record(tmp_path, monkeypatch, record)

    with pytest.raises(ValueError, match="does not match"):
        _read(root)


@requires_adoption
@pytest.mark.parametrize("mutation", [
    "no-schema", "unknown-key", "unknown-location",
    "remote-without-report", "not-a-uuid",
])
def test_record_the_model_rejects_is_not_used_as_evidence(
        tmp_path, monkeypatch, mutation):
    """Stricter validation: a record that is no shape of the model is refused
    instead of being merged into the evidence as it was before."""
    record = dict(EARLIER_BETA_RECORDS["importer-full"])
    if mutation == "no-schema":
        del record["schema"]
    elif mutation == "unknown-key":
        record["surprise"] = True
    elif mutation == "unknown-location":
        record["location"] = "elsewhere"
    elif mutation == "remote-without-report":
        record = {**EARLIER_BETA_RECORDS["remote-with-job"]}
        del record["report_sha256"]
    else:
        record["workflow_id"] = "wf"
    root = _result_with_record(tmp_path, monkeypatch, record)

    with pytest.raises(ValueError):
        _read(root)


# --- canonical marker ------------------------------------------------------

def _canonical_source(relative_path):
    return CanonicalZarrSource(
        storage_root="group-5-data", relative_path=relative_path,
        node_path="A", source_object_type="Image", source_object_id=1,
        source_generation=1, interchange_profile="ngff-0.4-zarr-v2",
        pixel_identity_origin="raw", canonical_pixel_verified=False)


def _migration_helpers():
    from test_canonical_marker_atomic import _marker_helpers
    return _marker_helpers()


def _store_with_marker(tmp_path, marker_text):
    store = tmp_path / "image.zarr"
    store.mkdir()
    (store / CANONICAL_MARKER_NAME).write_text(marker_text, encoding="utf-8")
    return store


def _importer_marker_text(source):
    """The marker exactly as BIOMERO.importer writes it."""
    return json.dumps(
        {"markerSchema": 1, "state": "committed", "source": source.to_dict()},
        indent=2, sort_keys=True)


def _install_load_check(helpers, source):
    helpers["load_canonical_marker"] = lambda path, **_kwargs: (
        CanonicalMarker.from_dict(json.loads(
            (path / CANONICAL_MARKER_NAME).read_text(encoding="utf-8"))
        ).source)


def test_marker_written_by_the_importer_is_updated_in_the_same_format(tmp_path):
    helpers = _migration_helpers()
    store = _store_with_marker(
        tmp_path, _importer_marker_text(_canonical_source("old.zarr")))
    new_source = _canonical_source("new.zarr")
    _install_load_check(helpers, new_source)

    helpers["_write_canonical_marker"](store, new_source)

    written = (store / CANONICAL_MARKER_NAME).read_text(encoding="utf-8")
    assert written == _importer_marker_text(new_source)


@requires_adoption
@pytest.mark.parametrize("mutation", ["uncommitted", "no-marker-schema",
                                      "unknown-key", "unreadable-source"])
def test_marker_the_model_rejects_is_not_rewritten(tmp_path, mutation):
    helpers = _migration_helpers()
    marker = json.loads(_importer_marker_text(_canonical_source("old.zarr")))
    if mutation == "uncommitted":
        marker["state"] = "pending"
    elif mutation == "no-marker-schema":
        del marker["markerSchema"]
    elif mutation == "unknown-key":
        marker["surprise"] = True
    else:
        marker["source"] = {"relativePath": "old"}
    original = json.dumps(marker)
    store = _store_with_marker(tmp_path, original)

    with pytest.raises(ValueError):
        helpers["_write_canonical_marker"](store, _canonical_source("new.zarr"))

    assert (store / CANONICAL_MARKER_NAME).read_text(
        encoding="utf-8") == original



# --- failures stay contained ------------------------------------------------

@requires_adoption
def test_invalid_storage_record_warns_and_does_not_abort_the_import(
        tmp_path, monkeypatch, caplog):
    """A ValidationError from the record is logged by add_object_annotations
    and the other workflow metadata is still written."""
    from test_provenance_fallback import load as load_annotations
    from test_schema2_storage_provenance import tracker_fixture
    from test_storage_provenance import connection
    record = {**EARLIER_BETA_RECORDS["importer-full"], "surprise": True}
    root = _result_with_record(tmp_path, monkeypatch, record)
    conn = connection([("biomero.import", {"Imported_from": str(root)})])
    slurm, _ = tracker_fixture()
    slurm.track_workflows = True
    namespace = load_annotations("Import")
    namespace.update(_load_provenance_functions())
    namespace["logger"] = logging.getLogger("storage-record-test")
    posted = []
    namespace["render_workflow_metadata"] = lambda *args, **kwargs: [
        type("Annotation", (), {"namespace": "biomero/workflow",
                                "values": {"Workflow_ID": WF}})()]
    namespace["post_provenance_map_annotation"] = (
        lambda **kwargs: (posted.append(kwargs) or 1, "complete"))

    with caplog.at_level(logging.WARNING, logger="storage-record-test"):
        outcome = namespace["add_object_annotations"](
            conn, slurm, "Image", 1, "15", WF)

    assert outcome == "failed"
    assert any(entry.levelno == logging.WARNING
               and "Storage provenance unavailable" in entry.getMessage()
               for entry in caplog.records)
    slurm.workflowTracker.record_storage_provenance.assert_not_called()
    assert len(posted) == 1


def _load_provenance_functions():
    from test_storage_provenance import load
    return load()


@requires_adoption
def test_migration_failure_message_names_the_first_invalid_field(tmp_path):
    from pydantic import ValidationError
    tree = _tree(MIGRATE)
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef)
                    and node.name == "_failure_message")
    namespace = {"ValidationError": ValidationError}
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(MIGRATE), "exec"), namespace)
    marker = json.loads(_importer_marker_text(_canonical_source("a.zarr")))
    marker["state"] = "pending"
    marker["surprise"] = True
    with pytest.raises(ValidationError) as caught:
        CanonicalMarker.from_dict(marker)

    message = namespace["_failure_message"](caught.value)

    assert len(message.splitlines()) == 1
    assert message.startswith("invalid CanonicalMarker at ")
    assert namespace["_failure_message"](ValueError("plain")) == "plain"
