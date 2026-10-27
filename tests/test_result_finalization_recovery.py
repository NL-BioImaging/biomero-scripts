"""Failure-boundary tests against the selected deployable script revision."""
import ast
import errno
import json
import logging
import os
from datetime import datetime
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock
import uuid

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
SCRIPT = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"
pytestmark = pytest.mark.skipif(
    "def write_result_import_state(" not in SCRIPT.read_text(encoding="utf-8"),
    reason="result recovery implementation is not present in selected source",
)
requires_opt_in = pytest.mark.skipif(
    "def result_recovery_enabled(" not in SCRIPT.read_text(encoding="utf-8"),
    reason="selected source predates the opt-in recovery switch",
)
requires_lock_fallback = pytest.mark.skipif(
    "def result_import_recovery(" not in SCRIPT.read_text(encoding="utf-8"),
    reason="selected source predates unsupported-lock fallback",
)


class Names:
    def __getattr__(self, name):
        return name


def load_functions():
    names = {
        "process_importer_workflow", "ResultFinalizationBusy",
        "read_result_import_state", "write_result_import_state",
        "result_import_request", "ensure_result_import_orders",
        "create_upload_orders_for_results", "cleanup_resources",
        "acquire_result_import_lock", "result_recovery_enabled",
        "result_import_recovery", "ResultFinalizationLockUnsupported",
        "_process_importer_workflow_locked",
    }
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    body = [node for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    namespace = dict(
        Any=Any, Dict=Dict, List=List, Optional=Optional, Tuple=Tuple,
        UUID=uuid.UUID, Path=Path, SlurmClient=object, BlitzGateway=object,
        os=os, json=json, uuid=uuid, datetime=datetime, errno=errno,
        logger=logging.getLogger(__name__),
        constants=SimpleNamespace(results=Names(), CLEANUP="CLEANUP"),
        unwrap=lambda value: value, IMPORTER_AVAILABLE=True,
        IMPORT_POLL_TIMEOUT_SECONDS=86400, IMPORT_POLL_INTERVAL_SECONDS=5,
        RESULT_IMPORT_STATE=".biomero-result-import.json",
        STAGE_NEW_ORDER="Import Pending",
        contextmanager=contextmanager, ExitStack=ExitStack,
    )
    exec(compile(ast.Module(body=body, type_ignores=[]), str(SCRIPT), "exec"), namespace)
    return namespace


@pytest.fixture
def flow(tmp_path, monkeypatch):
    monkeypatch.setenv("BIOMERO_RESULT_RECOVERY", "true")
    namespace = load_functions()
    values = {
        "OUTPUT_ATTACH_NEW_DATASET": True,
        "OUTPUT_ATTACH_NEW_DATASET_NAME": "Results",
        "OUTPUT_ATTACH_NEW_DATASET_ID": 42,
    }
    client = SimpleNamespace(getInput=lambda name: values.get(name))
    destination = SimpleNamespace(getId=lambda: 42)
    conn = SimpleNamespace(getObject=Mock(return_value=destination))
    slurm = SimpleNamespace(remote_shallow_zarr=False)
    submissions = []
    stored_orders = {}

    def submit(order):
        submissions.append(order)
        stored_orders[order["UUID"]] = dict(order)

    namespace.update(
        build_shallow_import_options=lambda *args, **kwargs: {"schema": 2},
        select_lifecycle_import_paths=lambda _: [str(tmp_path / "result.zarr")],
        create_upload_order=submit,
        get_existing_import_order=lambda order_id: stored_orders.get(order_id),
        find_previous_result_import_orders=lambda *_: [],
        wait_for_import_completion=Mock(return_value=(True, "1/1 imported", [])),
        add_metadata_to_imported_images=Mock(return_value="metadata complete"),
        rename_label_images_in_omero=Mock(return_value=""),
    )

    def run(**kwargs):
        return namespace["process_importer_workflow"](
            client, conn, slurm, "490", "group", "user", str(tmp_path),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", **kwargs)

    return SimpleNamespace(
        run=run, namespace=namespace, submissions=submissions,
        stored_orders=stored_orders, values=values, directory=tmp_path,
        conn=conn)


@pytest.mark.parametrize("summary", ["Import failed", "Import polling timed out"])
def test_failed_or_unknown_import_cannot_finalize_successfully(flow, summary):
    flow.namespace["wait_for_import_completion"].return_value = (
        False, summary, ["order-id"])
    with pytest.raises(RuntimeError, match="order-id"):
        flow.run()
    flow.namespace["add_metadata_to_imported_images"].assert_not_called()


@requires_opt_in
@pytest.mark.parametrize("value", [None, "", "false", "FALSE"])
@pytest.mark.parametrize("successful", [True, False])
def test_recovery_opt_out_needs_no_new_filesystem_or_database_operations(
    flow, monkeypatch, value, successful,
):
    if value is None:
        monkeypatch.delenv("BIOMERO_RESULT_RECOVERY", raising=False)
    else:
        monkeypatch.setenv("BIOMERO_RESULT_RECOVERY", value)
    for name in ("acquire_result_import_lock", "write_result_import_state",
                 "read_result_import_state", "find_previous_result_import_orders",
                 "get_existing_import_order"):
        flow.namespace[name] = Mock(side_effect=OSError("unsupported filesystem operation"))
    flow.namespace["wait_for_import_completion"].return_value = (
        successful, "import status", [] if successful else ["failed-order"])
    if successful:
        flow.run()
    else:
        with pytest.raises(RuntimeError, match="failed-order"):
            flow.run()
    assert len(flow.submissions) == 1
    assert not list(flow.directory.glob(".biomero*"))


@requires_opt_in
def test_recovery_cannot_be_disabled_to_duplicate_a_saved_import(flow, monkeypatch):
    flow.run()
    monkeypatch.setenv("BIOMERO_RESULT_RECOVERY", "false")
    with pytest.raises(RuntimeError, match="BIOMERO_RESULT_RECOVERY"):
        flow.run()
    assert len(flow.submissions) == 1


def fail_lock(monkeypatch, error):
    if os.name == "nt":
        import msvcrt
        monkeypatch.setattr(msvcrt, "locking", Mock(side_effect=error))
    else:
        import fcntl
        monkeypatch.setattr(fcntl, "flock", Mock(side_effect=error))


@requires_lock_fallback
@pytest.mark.parametrize("code", [errno.ENOSYS, errno.EOPNOTSUPP])
@pytest.mark.parametrize("successful", [True, False])
def test_unsupported_lock_uses_legacy_import_before_creating_a_plan(
    flow, monkeypatch, caplog, code, successful,
):
    fail_lock(monkeypatch, OSError(code, "unsupported lock"))
    flow.namespace["wait_for_import_completion"].return_value = (
        successful, "import status", [] if successful else ["failed-order"])
    if successful:
        flow.run()
    else:
        with pytest.raises(RuntimeError, match="failed-order"):
            flow.run()
    assert len(flow.submissions) == 1
    assert not (flow.directory / ".biomero-result-import.json").exists()
    assert "legacy" in caplog.text.lower()


@requires_lock_fallback
@pytest.mark.parametrize("code", [errno.EACCES, errno.EIO, errno.ENOLCK])
def test_other_lock_errors_never_downgrade_recovery(flow, monkeypatch, code):
    fail_lock(monkeypatch, OSError(code, "lock failed"))
    with pytest.raises((RuntimeError, OSError)):
        flow.run()
    assert not flow.submissions


@requires_lock_fallback
def test_unsupported_lock_cannot_bypass_existing_plan(flow, monkeypatch):
    flow.run()
    fail_lock(monkeypatch, OSError(errno.EOPNOTSUPP, "unsupported lock"))
    with pytest.raises(RuntimeError, match="recovery record"):
        flow.run()
    assert len(flow.submissions) == 1


@requires_lock_fallback
def test_workflow_lock_fallback_disables_result_recovery_for_same_attempt(flow, monkeypatch):
    fail_lock(monkeypatch, OSError(errno.EOPNOTSUPP, "unsupported lock"))
    with flow.namespace["result_import_recovery"](flow.directory) as recovery:
        assert recovery is False
        flow.namespace["acquire_result_import_lock"] = Mock(
            side_effect=AssertionError("retried unsupported lock"))
        flow.run(recovery=recovery)
    assert len(flow.submissions) == 1
    assert not (flow.directory / ".biomero-result-import.json").exists()


@requires_lock_fallback
def test_unsupported_operation_after_acquiring_lock_never_restarts_import(flow):
    poll = flow.namespace["wait_for_import_completion"]
    poll.side_effect = OSError(errno.EOPNOTSUPP, "operation failed after enqueue")
    with pytest.raises(OSError):
        flow.run()
    poll.assert_called_once()
    assert len(flow.submissions) == 1
    assert (flow.directory / ".biomero-result-import.json").exists()


def test_retry_after_poll_interruption_reuses_the_submitted_order(flow):
    poll = flow.namespace["wait_for_import_completion"]
    poll.side_effect = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        flow.run()
    first_id = flow.submissions[0]["UUID"]
    poll.side_effect = None
    flow.run()
    assert [order["UUID"] for order in flow.submissions] == [first_id]
    assert poll.call_args.args[0][0]["UUID"] == first_id


def test_retry_after_completed_import_does_not_register_again(flow):
    flow.run()
    flow.run()
    assert len(flow.submissions) == 1


def test_lost_submission_acknowledgement_does_not_duplicate_order(flow):
    submit = flow.namespace["create_upload_order"]

    def lost_ack(order):
        submit(order)
        raise ConnectionError("Database commit acknowledged too late")

    flow.namespace["create_upload_order"] = lost_ack
    with pytest.raises(ConnectionError):
        flow.run()
    flow.namespace["create_upload_order"] = submit
    flow.run()
    assert len(flow.submissions) == 1


def test_retry_cannot_redirect_an_existing_import_to_another_destination(flow):
    flow.run()
    flow.values["OUTPUT_ATTACH_NEW_DATASET_ID"] = 99
    with pytest.raises(RuntimeError, match="settings"):
        flow.run()
    assert len(flow.submissions) == 1


def test_database_uncertainty_prevents_new_registration(flow):
    flow.run()
    flow.namespace["get_existing_import_order"] = Mock(
        side_effect=ConnectionError("Database unavailable"))
    with pytest.raises(ConnectionError):
        flow.run()
    assert len(flow.submissions) == 1


def test_registered_order_missing_from_database_is_not_requeued(flow):
    flow.run()
    flow.stored_orders.clear()
    with pytest.raises(RuntimeError, match="missing"):
        flow.run()
    assert len(flow.submissions) == 1


def test_failed_finalization_retains_remote_and_local_recovery_material(tmp_path):
    namespace = load_functions()
    client = SimpleNamespace(getInput=lambda _: True)
    slurm = SimpleNamespace(cleanup_tmp_files=Mock())
    local_cleanup = Mock(return_value="removed")
    namespace["cleanup_tmp_files_locally"] = local_cleanup
    message = namespace["cleanup_resources"](
        client, slurm, "490", "/remote/results", str(tmp_path), "job.log")
    slurm.cleanup_tmp_files.assert_not_called()
    local_cleanup.assert_not_called()
    assert "preserved" in message.lower()


def test_successful_finalization_still_honors_requested_cleanup(tmp_path):
    namespace = load_functions()
    client = SimpleNamespace(getInput=lambda _: True)
    slurm = SimpleNamespace(cleanup_tmp_files=Mock())
    namespace["cleanup_tmp_files_locally"] = Mock(return_value="removed")
    namespace["cleanup_resources"](
        client, slurm, "490", "/remote/results", None, None,
        completed=True)
    slurm.cleanup_tmp_files.assert_called_once()


def test_saved_order_cannot_change_the_owner(flow):
    flow.run()
    record = flow.directory / ".biomero-result-import.json"
    state = json.loads(record.read_text(encoding="utf-8"))
    state["orders"][0]["Group"] = "another-group"
    record.write_text(json.dumps(state), encoding="utf-8")
    flow.stored_orders.clear()
    with pytest.raises(RuntimeError, match="recovery record"):
        flow.run()
    assert len(flow.submissions) == 1


def test_destination_lookup_failure_never_creates_a_replacement(flow):
    flow.values["OUTPUT_ATTACH_NEW_DATASET_ID"] = None
    flow.conn.getObjects = Mock(side_effect=ConnectionError("OMERO unavailable"))
    with pytest.raises(ConnectionError, match="OMERO unavailable"):
        flow.run()
    assert not flow.submissions


@pytest.mark.parametrize("payload", ["{broken", '{"schema": 999}',
                                    '{"schema": 1, "request": {}, "phase": "preparing"}'])
def test_unreadable_or_incomplete_recovery_state_never_submits(flow, payload):
    (flow.directory / ".biomero-result-import.json").write_text(payload, encoding="utf-8")
    with pytest.raises(RuntimeError, match="recovery record"):
        flow.run()
    assert not flow.submissions


def test_saved_state_is_required_before_any_order_is_submitted(flow):
    flow.namespace["write_result_import_state"] = Mock(side_effect=OSError("disk full"))
    with pytest.raises(OSError, match="disk full"):
        flow.run()
    assert not flow.submissions


def test_legacy_result_orders_require_review_instead_of_duplicate_import(flow):
    flow.namespace["find_previous_result_import_orders"] = lambda *_: ["legacy-order"]
    with pytest.raises(RuntimeError, match="legacy-order"):
        flow.run()
    assert not flow.submissions


def test_retry_does_not_rename_results_or_create_destination_again(flow):
    flow.values["OUTPUT_ATTACH_NEW_DATASET_RENAME"] = True
    rename = Mock(return_value=("renamed", {"new.zarr": "old.zarr"}))
    flow.namespace["rename_files_in_importer_storage"] = rename
    flow.run()
    flow.run()
    rename.assert_called_once()
    assert len(flow.submissions) == 1


def test_concurrent_finalization_is_rejected_without_submitting(flow):
    with flow.namespace["acquire_result_import_lock"](flow.directory):
        with pytest.raises(RuntimeError, match="lock|running"):
            flow.run()
    assert not flow.submissions
    flow.run()
    assert len(flow.submissions) == 1


def test_conflicting_committed_order_is_not_replaced(flow):
    flow.run()
    flow.stored_orders[flow.submissions[0]["UUID"]]["DestinationID"] = 99
    with pytest.raises(RuntimeError, match="conflict"):
        flow.run()
    assert len(flow.submissions) == 1
