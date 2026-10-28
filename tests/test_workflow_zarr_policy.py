"""Admin input policy applies before transfer, including reused/shallow stores."""
import ast
import os
from pathlib import Path
import shutil
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from test_shallow_conversion_material import load_save_as_zarr, ImageObject, RUNNER_PATH

ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
pytestmark = pytest.mark.skipif(
    "def resolve_workflow_input_version(" not in RUNNER_PATH.read_text(encoding="utf-8")
    and os.getenv("BIOMERO_TEST_ZARR_POLICY") != "1",
    reason="Requires administrator workflow Zarr version policy",
)


@pytest.mark.parametrize("source_version,requested", [(2, "0.5"), (3, "0.4")])
@pytest.mark.parametrize("kind", ["Image", "Plate"])
@pytest.mark.parametrize("shallow_enabled", [False, True])
def test_native_mismatch_uses_temporary_conversion_without_source_promotion(
        tmp_path, monkeypatch, source_version, requested, kind, shallow_enabled):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    source = tmp_path / "original.zarr"
    source.mkdir()
    metadata = "zarr.json" if source_version == 3 else ".zgroup"
    (source / metadata).write_text('{}')
    before = (source / metadata).read_bytes()
    save, commands, _, promotions, _, _ = load_save_as_zarr(
        tmp_path, shallow_reference=None, reusable_path=source)
    convert = Mock(side_effect=lambda src, dst, version: shutil.copytree(src, dst))
    save.__globals__["convert_zarr_for_transfer"] = convert
    upgrade = Mock(side_effect=AssertionError("no identity work for mismatched inputs"))
    save.__globals__["upgrade_reused_canonical"] = upgrade
    canonical = object()
    direct = {}

    result = save(SimpleNamespace(host="omero"), "session", ImageObject(), "batch",
                  kind, requested, canonical_source=canonical,
                  shallow_zarr_storage=shallow_enabled, direct_directory_sources=direct)

    assert result[0] is canonical
    convert.assert_called_once_with(source, str(Path("batch") / result[1]), requested)
    upgrade.assert_not_called()
    assert direct == {}
    assert commands == promotions == []
    assert (source / metadata).read_bytes() == before


def test_shallow_input_converts_reconstruction_and_keeps_labels(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    canonical = object()
    save, commands, materializations, promotions, _, _ = load_save_as_zarr(
        tmp_path, shallow_reference=SimpleNamespace(source=canonical))
    convert = Mock(side_effect=lambda src, dst, version: shutil.copytree(src, dst))
    save.__globals__["convert_zarr_for_transfer"] = convert

    source, artifact, labels = save(None, "session", ImageObject(), "batch", "Image", "0.5",
                                    storage_roots={"data": tmp_path}, shallow_zarr_storage=False)

    assert source is canonical
    assert labels == ("managed-label",)
    assert len(materializations) == 1
    convert.assert_called_once()
    assert convert.call_args.args[2] == "0.5"
    assert Path("batch", artifact).is_dir()
    assert not Path(convert.call_args.args[0]).exists()  # only temporary reconstruction cleaned
    assert commands == promotions == []


@pytest.mark.parametrize("versions, expected", [(["0.4"], "0.4"), (["0.5", "0.5"], "0.5")])
def test_workflow_policy_resolves_only_selected_descriptors(versions, expected):
    tree = ast.parse(RUNNER_PATH.read_text(encoding="utf-8"))
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name == "resolve_workflow_input_version")
    namespace = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(RUNNER_PATH), "exec"), namespace)
    resolver = Mock(side_effect=versions)
    client = SimpleNamespace(resolve_workflow_zarr_version=resolver)
    selected = {f"wf{i}": True for i in range(len(versions))}
    selected["unselected"] = False
    descriptors = {name: {"schema-version": "bilayers:0.1.0"} for name in selected}
    assert namespace["resolve_workflow_input_version"](client, selected, descriptors, True) == expected
    assert resolver.call_count == len(versions)
    for call in resolver.call_args_list:
        assert call.kwargs == {"descriptor": descriptors[call.args[0]], "use_zarr": True}


def test_mixed_requirements_stop_before_shared_transfer():
    tree = ast.parse(RUNNER_PATH.read_text(encoding="utf-8"))
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name == "resolve_workflow_input_version")
    namespace = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(RUNNER_PATH), "exec"), namespace)
    client = SimpleNamespace(resolve_workflow_zarr_version=Mock(side_effect=["0.4", "0.5"]))
    with pytest.raises(ValueError, match="separately"):
        namespace["resolve_workflow_input_version"](client, {"old": True, "new": True},
                                                    {"old": {}, "new": {}}, True)
