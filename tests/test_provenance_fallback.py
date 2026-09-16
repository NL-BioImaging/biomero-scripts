"""Full provenance is durable; indexed annotations are a recoverable view."""
import ast
import csv
import glob
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
pytestmark = pytest.mark.skipif(
    "def post_provenance_map_annotation" not in (
        ROOT / "_data" / "SLURM_Import_Results.py").read_text(encoding="utf-8"),
    reason="provenance fallback is not available in this source revision",
)


def load(route):
    path = ROOT / "_data" / f"SLURM_{route}_Results.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = {"add_image_annotations", "add_object_annotations",
             "upload_metadata_csv_to_omero", "create_metadata_csv",
             "add_metadata_to_imported_plates", "upload_contents_to_omero"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)
             and (n.name in names or "provenance" in n.name)]
    ns = dict(os=os, re=re, csv=csv, glob=glob, json=json,
              SUPPORTED_IMAGE_EXTENSIONS=(".tif",), logger=logging.getLogger("test"),
              ezomero=SimpleNamespace(post_map_annotation=Mock(return_value=19)),
              NSCREATED="test", Any=object, BlitzGateway=object, SlurmClient=object,
              List=list, Dict=dict, Optional=__import__("typing").Optional,
              Tuple=tuple)
    # The renderer is tested in core. This harness tests its writer contract,
    # without requiring an unpublished core branch in shared CI.
    def render(tracker, workflow_id, **kwargs):
        rows = [SimpleNamespace(namespace="biomero/workflow", values={
            "Workflow_ID": workflow_id})]
        for key in tracker.repository.get(workflow_id).tasks:
            task = tracker.repository.get(key)
            values = {"Task_ID": key, "Workflow_ID": workflow_id}
            values.update({"Param_" + k: str(v) for k, v in task.params.items()})
            namespace = "biomero/workflow/task/" + key
            rows.append(SimpleNamespace(namespace=namespace, values=values))
            rows.append(SimpleNamespace(namespace=namespace + "/job",
                                        values={"Task_ID": key, "Job_ID": key}))
        return rows
    ns["render_workflow_metadata"] = Mock(side_effect=render)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), ns)
    ns['record_import_storage_provenance'] = Mock()
    return ns


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_annotations_delegate_to_core_renderer(route):
    source = (ROOT / '_data' / f'SLURM_{route}_Results.py').read_text(encoding='utf-8')
    if 'from biomero.provenance import render_workflow_metadata' not in source:
        pytest.skip('versioned renderer is not available in this source revision')
    ns = load(route)
    client = tracker()
    ns["add_image_annotations"](object(), client, 401, 23, "wf")
    kwargs = {'target_key': 'Image:401'} if route == 'Import' and 'target_key=' in source else {}
    ns["render_workflow_metadata"].assert_called_once_with(
        client.workflowTracker, "wf", **kwargs)
    if route == 'Import' and 'def record_import_storage_provenance' in source:
        ns['record_import_storage_provenance'].assert_called_once()


def tracker():
    now = datetime.now(timezone.utc)
    wf = SimpleNamespace(name="run", description="BIOMERO 2.7.0", tasks=["a", "b"],
                         _created_on=now, _modified_on=now)
    tasks = {key: SimpleNamespace(
        _id=key, task_name=key, task_version="1", _created_on=now, _modified_on=now,
        status="DONE", input_data="input", job_ids=[key], result_message="ok",
        results=[{"command": "run", "env": {"VALUE": "ok"}}],
        params={"schema": "λ" * 5000, "selected": False}) for key in wf.tasks}
    return SimpleNamespace(track_workflows=True, workflowTracker=SimpleNamespace(
        repository=SimpleNamespace(get=lambda key: wf if key == "wf" else tasks[key])))


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_full_annotation_is_attempted_before_size_fallback(route):
    ns = load(route)
    calls = []

    def post(**kwargs):
        values = kwargs["kv_dict"]
        calls.append(dict(values))
        if any(len(str(v).encode("utf-8")) > 2704 for v in values.values()):
            raise RuntimeError("index row size 2800 exceeds btree version 4 maximum 2704")
        return len(calls)

    ns["ezomero"].post_map_annotation = post
    result = ns["add_image_annotations"](object(), tracker(), 401, 23, "wf")
    assert result == "reduced"
    assert len(calls) == 7  # workflow + two (full, fallback, job) groups
    assert calls[1]["Param_schema"] == "λ" * 5000
    assert calls[2]["Param_selected"] == "False"
    assert "metadata_wf.csv" in calls[2]["Param_schema"]
    assert "sha256" in calls[2]["Param_schema"]
    assert calls[-1]["Task_ID"] == "b"


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_unrelated_annotation_failure_does_not_block_later_jobs(route):
    ns = load(route)
    calls = []

    def post(**kwargs):
        calls.append(kwargs)
        if kwargs["ns"].endswith("/task/a"):
            raise RuntimeError("permission denied")
        return len(calls)

    ns["ezomero"].post_map_annotation = post
    assert ns["add_image_annotations"](object(), tracker(), 401, 23, "wf") == "failed"
    assert len(calls) == 5  # no retry for permission errors
    assert calls[-1]["kv_dict"]["Task_ID"] == "b"


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_large_compressible_values_are_unchanged_when_server_accepts_them(route):
    ns = load(route)
    assert ns["add_image_annotations"](object(), tracker(), 401, 23, "wf") == "complete"
    calls = ns["ezomero"].post_map_annotation.call_args_list
    assert len(calls) == 5
    assert calls[1].kwargs["kv_dict"]["Param_schema"] == "λ" * 5000


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_csv_link_failure_does_not_block_next_target_and_counts_actual_links(route, tmp_path):
    ns = load(route)
    source = tmp_path / "metadata.csv"
    source.write_text('Param_schema,"full, untruncated schema"\n', encoding="utf-8")
    annotation = object()
    conn = Mock()
    conn.createFileAnnfromLocalFile.return_value = annotation
    ns["create_file_annotation_inplace"] = Mock(return_value=annotation)
    targets = [Mock(OMERO_CLASS="Plate"), Mock(OMERO_CLASS="Plate")]
    for index, target in enumerate(targets):
        target.getId.return_value = index + 1
    targets[0].linkAnnotation.side_effect = RuntimeError("permission denied")
    conn.getObject.side_effect = lambda kind, ident: targets[ident - 1]
    message = ns["upload_metadata_csv_to_omero"](
        object(), conn, "", 23, targets, [str(source)], "wf")
    targets[1].linkAnnotation.assert_called_once_with(annotation)
    assert "1/2" in message
    assert "warning" in message.lower()
    assert source.read_bytes() == (tmp_path / "metadata_wf.csv").read_bytes()
    if route == "Import":
        ns["create_file_annotation_inplace"].assert_called_once()
        conn.createFileAnnfromLocalFile.assert_not_called()
    else:
        conn.createFileAnnfromLocalFile.assert_called_once()


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_full_csv_preserves_schema_and_selected_values(route, tmp_path):
    ns = load(route)
    ns["find_supported_image_paths"] = lambda path: []
    ns["find_image_files"] = lambda path: []
    conn = Mock()
    conn.getUser.return_value.getName.return_value = "user"
    conn.getGroupFromContext.return_value.getName.return_value = "group"
    client = tracker()
    evidence = {'Plate:12': {'storage': 'shallow-zarr', 'source_biocodes': ['ISCC:SOURCE']}}
    client.workflowTracker.repository.get('a').storage_provenance = evidence
    files = ns["create_metadata_csv"](conn, client, str(tmp_path), 23, "wf")
    with open(files[0], newline="", encoding="utf-8") as stream:
        rows = dict(csv.reader(stream))
    assert rows["Task_a_Param_schema"] == "λ" * 5000
    assert rows["Task_a_Param_selected"] == "False"
    assert rows["Task_a_Job_a_Command"] == "run"
    assert rows["Task_a_Job_a_Env_VALUE"] == "ok"
    source = (ROOT / '_data' / f'SLURM_{route}_Results.py').read_text(encoding='utf-8')
    if 'Storage_Provenance' in source:
        assert json.loads(rows['Task_a_Storage_Provenance']) == evidence


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_failed_retry_and_missing_id_are_reported_without_stopping_later_tasks(route):
    ns = load(route)
    post = ns["ezomero"].post_map_annotation
    post.side_effect = [None, RuntimeError("index row size exceeds maximum"),
                        RuntimeError("still unavailable"), 3, 4, 5]
    assert ns["add_image_annotations"](object(), tracker(), 401, 23, "wf") == "failed"
    assert post.call_count == 6
    assert post.call_args.kwargs["kv_dict"]["Task_ID"] == "b"


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_unicode_keys_and_values_are_bounded_and_original_mapping_is_unchanged(route):
    ns = load(route)
    original = {"Workflow_ID": "wf", "長" * 2000: "value", "array": "é" * 2000}
    ns["ezomero"].post_map_annotation.side_effect = [
        RuntimeError("index row size exceeds maximum"), 21]
    assert ns["post_provenance_map_annotation"](
        object(), "Plate", 401, original, "biomero/workflow") == (21, "reduced")
    reduced = ns["ezomero"].post_map_annotation.call_args.kwargs["kv_dict"]
    assert all(len((str(k) + str(v)).encode("utf-8")) <= 1024
               for k, v in reduced.items())
    assert original["array"] == "é" * 2000
    assert len(reduced) == len(original)


def test_plate_discovery_collects_actual_targets_even_if_annotations_fail(monkeypatch):
    ns = load("Import")
    params = Mock()
    omero = SimpleNamespace(sys=SimpleNamespace(ParametersI=lambda: params))
    monkeypatch.setitem(sys.modules, "omero", omero)
    monkeypatch.setitem(sys.modules, "omero.sys", omero.sys)
    plates = [Mock(), Mock(), Mock()]
    for ident, plate in enumerate(plates, 401):
        plate.getId.return_value = ident
    conn = Mock()
    conn.getQueryService.return_value.projection.return_value = [
        [SimpleNamespace(val=i)] for i in range(401, 404)]
    conn.getObject.side_effect = lambda kind, ident: plates[ident - 401]
    ns["add_plate_annotations"] = Mock(side_effect=["reduced", "failed", "complete"])
    targets = []
    message = ns["add_metadata_to_imported_plates"](
        conn, tracker(), 1, ["order"], "wf", "23", provenance_targets=targets)
    assert targets == plates
    assert "1 complete, 1 reduced, 1 incomplete" in message


class Constants:
    def __getattr__(self, name):
        return name


def test_classic_upload_collects_explicit_result_dataset_without_zip_options():
    ns = load("Get")
    ns["constants"] = SimpleNamespace(results=Constants())
    ns["unwrap"] = lambda value: value
    values = {"OUTPUT_ATTACH_NEW_DATASET": True, "OUTPUT_ATTACH_NEW_DATASET_ID": 51}
    client = Mock()
    client.getInput.side_effect = values.get
    conn = Mock()
    dataset = Mock()
    conn.getObject.return_value = dataset
    ns["saveImagesToOmeroAsDataset"] = Mock(return_value="images imported")
    targets = []
    ns["upload_contents_to_omero"](
        client, conn, tracker(), "", "folder", ["metadata_wf.csv"],
        wf_id="wf", provenance_targets=targets)
    assert targets == [dataset]
    conn.getObject.assert_called_once_with("Dataset", 51)


def statement_block(tree, called_name):
    """Find a real sequential runScript block containing the named call."""
    for parent in ast.walk(tree):
        for _, value in ast.iter_fields(parent):
            if isinstance(value, list):
                for node in value:
                    if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                            and isinstance(node.value.func, ast.Name)
                            and node.value.func.id == called_name):
                        return value
    raise AssertionError(f"No call to {called_name}")


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_main_flow_attaches_csv_outside_optional_output_blocks(route):
    path = ROOT / "_data" / f"SLURM_{route}_Results.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    run = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "runScript")
    called = "create_metadata_csv" if route == "Import" else "upload_contents_to_omero"
    block = statement_block(run, called)
    start = next(i for i, n in enumerate(block) if isinstance(n, ast.Assign)
                 and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                 and n.value.func.id == called)
    # Execute the actual result-flow statements up to the next optional section.
    finish = next((i for i in range(start + 1, len(block)) if isinstance(block[i], ast.If)), len(block))
    target = object()
    attach = Mock(return_value="CSV attached")
    ns = dict(message="", logger=logging.getLogger("test"), conn=object(),
              client=object(), slurmClient=object(), permanent_storage_path="folder",
              folder="folder", slurm_job_id=23, wf_id="wf", projects=[],
              provenance_targets=[target], importer_destination_target=object(),
              _log_targets=[object()], roi_pairs=[], input_images=[],
              metadata_files=["metadata_wf.csv"],
              create_metadata_csv=Mock(return_value=["metadata_wf.csv"]),
              upload_contents_to_omero=Mock(return_value="imported"),
              attach_provenance_csv=attach)
    statements = block[start:finish]
    if route == "Get":
        statements = [next(n for n in ast.walk(run) if isinstance(n, ast.Try)
                           and block is n.body)]
    exec(compile(ast.Module(body=statements, type_ignores=[]), str(path), "exec"), ns)
    attach.assert_called_once()
    assert attach.call_args.args[4] == [target]
    if route == "Get":
        attach.reset_mock()
        ns["upload_contents_to_omero"].side_effect = RuntimeError("result upload failed")
        with pytest.raises(RuntimeError, match="result upload failed"):
            exec(compile(ast.Module(body=statements, type_ignores=[]), str(path), "exec"), ns)
        attach.assert_called_once()


@pytest.mark.parametrize("route", ["Get", "Import"])
def test_csv_upload_failure_returns_visible_warning(route):
    ns = load(route)
    ns["upload_metadata_csv_to_omero"] = Mock(side_effect=RuntimeError("disk unavailable"))
    message = ns["attach_provenance_csv"](
        object(), object(), "Imported results", 23, [object()], ["metadata_wf.csv"], "wf")
    assert message.startswith("Imported results")
    assert "Provenance warning" in message
    assert "disk unavailable" in message
