import ast
import logging
import os
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch
import pytest

import biomero.constants as biomero_constants
from pathlib import Path


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
CHECK_SCRIPT = SOURCE_ROOT / "admin" / "SLURM_check_setup.py"
INIT_SCRIPT = SOURCE_ROOT / "admin" / "SLURM_Init_environment.py"
RUN_SCRIPT = SOURCE_ROOT / "__workflows" / "SLURM_Run_Workflow.py"


def load_function(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function = next((
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    ), None)
    if function is None:
        raise AssertionError(f"{name} is not available on this source revision")
    namespace = {"SlurmClient": object, "logger": logging.getLogger(__name__)}
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(path), "exec"), namespace)
    return namespace[name]


def sample_status():
    ready = [{
        "kind": "workflow",
        "name": f"workflow-{index}",
        "version": "v1",
        "state": "READY",
        "exit_code": 0,
        "reason": "built and validated",
        "destination": f"/images/workflow-{index}.sif",
    } for index in range(15)]
    failed = [{
        "kind": "workflow",
        "name": "imagej",
        "version": "v2",
        "state": "FAILED",
        "exit_code": 22,
        "reason": "manifest unknown",
        "destination": "/images/imagej.sif",
    }]
    return {
        "counts": {"READY": 15, "RUNNING": 0, "FAILED": 1},
        "images": ready + failed,
    }


@pytest.mark.skipif('def format_metadata_summary' not in INIT_SCRIPT.read_text(encoding='utf-8'),
                    reason='compact Init summary not present')
def test_metadata_summary_is_compact_and_counts_actual_changes():
    formatter = load_function(INIT_SCRIPT, 'format_metadata_summary')
    report = {'dry_run': True, 'view_version': 'v0', 'discovered': 4,
              'counts': {'skipped': 1, 'failed': 0}, 'results': [
                  {'status': 'planned', 'plan': {'annotations': [{'action': 'update'}]}},
                  {'status': 'planned', 'plan': {'annotations': [{'action': 'unchanged'}]}},
                  {'status': 'planned', 'plan': {'annotations': [{'action': 'unlink'}]}},
                  {'status': 'skipped', 'reason': 'private detailed reason'}]}
    result = formatter(report)
    assert 'Would update: 2' in result
    assert 'Unchanged: 1' in result
    assert 'Skipped: 1' in result
    assert 'No OMERO metadata was changed' in result
    assert 'private detailed reason' not in result
    assert len(result) < 500


def test_check_setup_formats_exact_ready_running_failed_counts():
    formatter = load_function(CHECK_SCRIPT, "format_image_pull_status")

    message = formatter(sample_status())

    assert "READY: 15" in message
    assert "RUNNING: 0" in message
    assert "FAILED: 1" in message
    assert "imagej:v2 - manifest unknown (exit 22)" in message
    assert "available version: ''" not in message


def test_initializer_reports_array_id_and_initial_counts():
    formatter = load_function(INIT_SCRIPT, "format_image_submission")

    message = formatter(98765, {
        "counts": {"READY": 15, "RUNNING": 1, "FAILED": 0},
        "images": [],
    })

    assert "Image pull array ID: 98765" in message
    assert "READY: 15" in message
    assert "RUNNING: 1" in message
    assert "FAILED: 0" in message


def test_check_setup_uses_structured_image_status_not_legacy_log():
    source = CHECK_SCRIPT.read_text(encoding="utf-8")

    assert "slurmClient.get_image_pull_status()" in source
    assert "format_image_pull_status(image_status)" in source
    assert "slurmClient.slurm_script_path}/image-pulls" in source
    assert "get_logfile_from_slurm" not in source
    assert "sing.log" not in source


def test_initializer_submits_one_combined_array_and_reads_its_status():
    source = INIT_SCRIPT.read_text(encoding="utf-8")

    assert "converter_specs = slurmClient.prepare_converters()" in source
    assert "extra_image_specs=converter_specs" in source
    assert "image_array_id = slurmClient.setup_container_images(" in source
    assert "image_status = slurmClient.get_image_pull_status()" in source
    assert "get_logfile_from_slurm" not in source
    assert "sing.log" not in source


def test_initializer_installs_the_helper_image_of_the_installed_shallower():
    source = INIT_SCRIPT.read_text(encoding="utf-8")

    assert "slurmClient.remote_shallow_zarr" in source
    assert "from biomero_shallower.capabilities import" in source
    assert "shallower_image = helper_image()" in source
    assert "image_spec(slurmClient, shallower_image)" in source
    # The helper image is no longer an INI setting.
    assert "remote_shallower_image" not in source
    assert "remote_shallower_version" not in source


@pytest.mark.skipif(
    "BIOMERO_SHALLOW_ZARR" not in
    INIT_SCRIPT.read_text(encoding="utf-8"),
    reason="initializer feature gates are not present on this source revision",
)
@pytest.mark.parametrize(
    "importer,shallow,remote,image,expected",
    [
        (None, None, True, "shallower.sif", False),
        ("false", "true", True, "shallower.sif", False),
        ("true", "false", True, "shallower.sif", False),
        ("true", "", True, "shallower.sif", False),
        ("true", "true", False, "shallower.sif", False),
        ("true", "true", True, None, False),
        ("true", "true", True, "shallower.sif", True),
        ("TRUE", "TRUE", True, "shallower.sif", True),
    ],
)
def test_initializer_only_acquires_enabled_remote_shallower(
    monkeypatch, importer, shallow, remote, image, expected,
):
    source = INIT_SCRIPT.read_text(encoding="utf-8")
    tree = ast.parse(source)
    # Exercise the actual image-acquisition branch without booting OMERO.
    branch = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and any(
            isinstance(statement, ast.ImportFrom)
            and statement.module == "biomero.remote_shallower"
            for statement in node.body
        )
    )
    for name, value in (("IMPORTER_ENABLED", importer),
                        ("BIOMERO_SHALLOW_ZARR", shallow)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    client = SimpleNamespace(remote_shallow_zarr=remote)
    helper = ModuleType("biomero.remote_shallower")
    spec = {"kind": "shallower", "destination": image}
    helper.image_spec = Mock(return_value=spec)
    capabilities = _capabilities(image)
    converter_spec = {"kind": "converter", "destination": "converter.sif"}
    specs = [converter_spec]
    namespace = {"os": os, "slurmClient": client, "converter_specs": specs,
                 "logger": logging.getLogger(__name__),
                 "constants": biomero_constants}

    with patch.dict(sys.modules, {
            "biomero.remote_shallower": helper,
            "biomero_shallower.capabilities": capabilities}):
        exec(compile(ast.Module(body=_flag_assignments(tree) + [branch],
                                type_ignores=[]),
                     str(INIT_SCRIPT), "exec"), namespace)

    assert specs == [converter_spec] + ([spec] if expected else [])
    if expected:
        # The image to install is the one the installed shallower names.
        helper.image_spec.assert_called_once_with(client, image)
    else:
        helper.image_spec.assert_not_called()


def _remote_shallower_module():
    module = ModuleType("biomero.remote_shallower")
    module.image_spec = Mock(return_value={})
    return module


def _capabilities(image):
    """Fake biomero_shallower.capabilities; no image means no helper image."""
    capabilities = ModuleType("biomero_shallower.capabilities")
    capabilities.helper_image = Mock(
        return_value=image, side_effect=None if image else ValueError("dev"))
    return capabilities


def _flag_assignments(tree):
    # The flag locals are computed once before the branch in newer sources.
    return [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name)
                and target.id in ("importer_enabled", "shallow_zarr_enabled")
                for target in node.targets)
    ]


@pytest.mark.parametrize("importer,shallow,image,level", [
    ("false", "true", "shallower.sif", logging.DEBUG),
    ("true", "false", "shallower.sif", logging.DEBUG),
    ("true", "true", None, logging.WARNING),
])
def test_initializer_logs_unstaged_shallower_by_reason(
    monkeypatch, caplog, importer, shallow, image, level,
):
    source = INIT_SCRIPT.read_text(encoding="utf-8")
    if "names no helper image" not in source:
        pytest.skip("Staging decision logging is not in source revision")
    tree = ast.parse(source)
    branch = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and any(isinstance(statement, ast.ImportFrom)
                and statement.module == "biomero.remote_shallower"
                for statement in node.body)
    )
    monkeypatch.setenv("IMPORTER_ENABLED", importer)
    monkeypatch.setenv("BIOMERO_SHALLOW_ZARR", shallow)
    client = SimpleNamespace(remote_shallow_zarr=True)
    specs = []
    namespace = {"os": os, "slurmClient": client, "converter_specs": specs,
                 "logger": logging.getLogger(__name__),
                 "constants": biomero_constants}
    with patch.dict(sys.modules, {
            "biomero.remote_shallower": _remote_shallower_module(),
            "biomero_shallower.capabilities": _capabilities(image),
    }), caplog.at_level(logging.DEBUG):
        exec(compile(ast.Module(body=_flag_assignments(tree) + [branch],
                                type_ignores=[]),
                     str(INIT_SCRIPT), "exec"), namespace)
    assert specs == []
    assert [record.levelno for record in caplog.records] == [level]


PREFLIGHT_IMAGE = "registry/helper:0.1.0"


def _run_preflight(client, remote_shallower, capabilities=None):
    validator = load_function(RUN_SCRIPT, "validate_remote_shallower_ready")
    validator.__globals__.update(
        IMPORTER_ENABLED=True,
        SHALLOW_ZARR_ENABLED=True,
        SHALLOW_ZARR_STORAGE_ENABLED=True,
    )
    with patch.dict(sys.modules, {
            "biomero.remote_shallower": remote_shallower,
            "biomero_shallower.capabilities": (
                capabilities or _capabilities(PREFLIGHT_IMAGE))}):
        validator(client, use_zarr_format=True)


def _preflight_client(destination, installed=True):
    existing = [{"destination": destination}]
    return SimpleNamespace(
        remote_shallow_zarr=True,
        _partition_existing_images=Mock(
            return_value=(existing, []) if installed else ([], existing)),
    )


def _preflight_module(destination, **overrides):
    remote_shallower = ModuleType("biomero.remote_shallower")
    remote_shallower.image_spec = Mock(return_value={"destination": destination})
    remote_shallower.validate_installed_tool = Mock(**overrides)
    return remote_shallower


def test_workflow_preflight_accepts_ready_remote_shallower():
    destination = "/images/shallower.sif"
    client = _preflight_client(destination)
    remote_shallower = _preflight_module(destination, return_value="0.1.0b4")

    _run_preflight(client, remote_shallower)

    # The image checked is the one the installed shallower names.
    remote_shallower.image_spec.assert_called_once_with(client, PREFLIGHT_IMAGE)
    client._partition_existing_images.assert_called_once_with(
        [{"destination": destination}]
    )
    remote_shallower.validate_installed_tool.assert_called_once_with(
        client, destination
    )


def test_workflow_preflight_warns_when_remote_shallower_is_not_installed(caplog):
    destination = "/images/shallower.sif"
    client = _preflight_client(destination, installed=False)
    remote_shallower = _preflight_module(destination)

    with caplog.at_level(logging.DEBUG):
        _run_preflight(client, remote_shallower)

    warnings = [record.getMessage() for record in caplog.records
                if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "SLURM Init Environment" in warnings[0]
    assert "full results" in warnings[0]
    remote_shallower.validate_installed_tool.assert_not_called()


def test_workflow_preflight_warns_when_shallower_names_no_helper_image(caplog):
    destination = "/images/shallower.sif"
    client = _preflight_client(destination)
    remote_shallower = _preflight_module(destination)

    with caplog.at_level(logging.DEBUG):
        _run_preflight(client, remote_shallower, _capabilities(None))

    assert [record.levelno for record in caplog.records] == [logging.WARNING]
    remote_shallower.image_spec.assert_not_called()
    client._partition_existing_images.assert_not_called()


def test_workflow_preflight_rejects_incompatible_remote_shallower():
    destination = "/images/shallower.sif"
    client = _preflight_client(destination)
    remote_shallower = _preflight_module(
        destination, side_effect=RuntimeError("unsupported manifest schema"))

    with pytest.raises(RuntimeError, match="unsupported manifest schema"):
        _run_preflight(client, remote_shallower)

    remote_shallower.validate_installed_tool.assert_called_once_with(
        client, destination
    )


@pytest.mark.parametrize(
    "importer_enabled,shallow_enabled,use_zarr,remote_enabled",
    [
        (False, True, True, True),
        (True, False, True, True),
        (True, True, False, True),
        (True, True, True, False),
    ],
)
def test_workflow_preflight_is_inert_unless_remote_shallow_path_is_enabled(
    importer_enabled, shallow_enabled, use_zarr, remote_enabled,
):
    validator = load_function(RUN_SCRIPT, "validate_remote_shallower_ready")
    client = SimpleNamespace(
        remote_shallow_zarr=remote_enabled,
        _partition_existing_images=Mock(),
    )
    validator.__globals__.update(
        IMPORTER_ENABLED=importer_enabled,
        SHALLOW_ZARR_ENABLED=shallow_enabled,
        SHALLOW_ZARR_STORAGE_ENABLED=importer_enabled and shallow_enabled,
    )

    validator(client, use_zarr_format=use_zarr)

    client._partition_existing_images.assert_not_called()
