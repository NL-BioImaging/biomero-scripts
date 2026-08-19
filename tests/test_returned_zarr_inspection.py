import ast
from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = (
    Path(__file__).parents[1] / "_data" / "SLURM_Import_Results.py"
)


class RecordingLogger:
    def __init__(self):
        self.info_calls = []
        self.warning_calls = []

    def info(self, *args, **kwargs):
        self.info_calls.append((args, kwargs))

    def warning(self, *args, **kwargs):
        self.warning_calls.append((args, kwargs))


def _load_inspector(*, finder, evaluator, available=True):
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef)
        and item.name == "inspect_returned_zarrs"
    )
    logger = RecordingLogger()
    namespace = {
        "RETURNED_ZARR_SUPPORT_AVAILABLE": available,
        "find_returned_zarr_stores": finder,
        "evaluate_returned_zarr": evaluator,
        "logger": logger,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SCRIPT_PATH), "exec"), namespace)
    return namespace["inspect_returned_zarrs"], logger


def _load_normalizer(normalize):
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef)
        and item.name == "normalize_eligible_returned_zarrs"
    )
    logger = RecordingLogger()
    namespace = {
        "Path": Path,
        "normalize_returned_zarr": normalize,
        "logger": logger,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SCRIPT_PATH), "exec"), namespace)
    return namespace["normalize_eligible_returned_zarrs"], logger


def test_keep_mode_logs_eligible_result_without_mutating_it(tmp_path):
    store = tmp_path / "input.zarr"
    decision = SimpleNamespace(
        eligible=True,
        matched_inputs=(SimpleNamespace(
            selected_object_type="Image",
            selected_object_id=42,
            transfer_artifact="input.zarr",
        ),),
        image_identities=(SimpleNamespace(iscc_code="ISCC:KPIXELS"),),
        label_node_paths=("labels/cells",),
        reason="input-image-unchanged",
    )
    inspector, logger = _load_inspector(
        finder=lambda _path: (store,),
        evaluator=lambda _store, _manifest: decision,
    )

    decisions = inspector(tmp_path, object())

    assert decisions == (decision,)
    assert "[KEEP MODE]" in logger.info_calls[-1][0][0]
    assert "no files were changed" in logger.info_calls[-1][0][0]


def test_keep_mode_fails_open_when_discovery_raises(tmp_path):
    def fail(_path):
        raise RuntimeError("bad result")

    inspector, logger = _load_inspector(finder=fail, evaluator=None)

    assert inspector(tmp_path, object()) == ()
    assert "retaining every full result store" in logger.warning_calls[-1][0][0]


def test_return_inspection_requires_importer_and_feature_flag():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "if IMPORTER_ENABLED and SHALLOW_ZARR_ENABLED:" in script
    assert "from biomero_importer.utils.result_zarr import (" in script
    assert "IMPORTER_ENABLED and SHALLOW_ZARR_ENABLED" in script
    assert "inspect_returned_zarrs(" in script


def test_normalizes_only_eligible_results_inside_results_root(tmp_path):
    inside = SimpleNamespace(
        eligible=True,
        store_path=tmp_path / "result.zarr",
    )
    keep = SimpleNamespace(
        eligible=False,
        store_path=tmp_path / "keep.zarr",
    )
    outside = SimpleNamespace(
        eligible=True,
        store_path=tmp_path.parent / "outside.zarr",
    )
    committed = SimpleNamespace(
        store_path=inside.store_path,
        bytes_before=100,
        bytes_after=10,
        collection=SimpleNamespace(images=(SimpleNamespace(
            label_node_paths=("labels/cells",),
        ),)),
    )
    calls = []

    def normalize(decision, workflow_id):
        calls.append((decision, workflow_id))
        return committed

    normalizer, logger = _load_normalizer(normalize)

    results = normalizer((inside, keep, outside), "workflow-id", tmp_path)

    assert results == (committed,)
    assert calls == [(inside, "workflow-id")]
    assert "saved %s bytes" in logger.info_calls[-1][0][0]
    assert "retaining the full result" in logger.warning_calls[-1][0][0]


def test_normalization_occurs_after_label_path_compatibility_renames():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef)
        and item.name == "create_upload_orders_for_results"
    )
    calls = [
        item.func.id
        for item in ast.walk(function)
        if isinstance(item, ast.Call) and isinstance(item.func, ast.Name)
    ]

    assert calls.index("find_label_zarr_paths") < calls.index(
        "normalize_eligible_returned_zarrs"
    )


def test_shallow_mode_automates_primary_and_label_import_selection():
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "automatic_shallow_import = (" in source
    assert "import_label_zarrs = True" in source
    assert "import_only_labels = False" in source
    assert "unchanged_passthrough" in source
    assert "Automatic shallow result selection prepared" in source
