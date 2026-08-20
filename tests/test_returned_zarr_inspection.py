import ast
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID


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


def _load_functions(names, namespace):
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    nodes = [
        item for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name in names
    ]
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT_PATH), "exec"),
        namespace,
    )
    return tuple(namespace[name] for name in names)


class FakeOperation:
    def __init__(self, **values):
        self.kind = "biomero.shallow-zarr"
        self.values = values


class FakeEnvelope:
    def __init__(self, operations):
        self.operations = operations

    def to_dict(self):
        return {
            "schema": 2,
            "operations": [operation.values for operation in self.operations],
        }


def test_builds_typed_importer_operation_only_when_capable():
    manifest = SimpleNamespace(
        workflow_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        inputs=(object(),),
    )
    logger = RecordingLogger()
    namespace = {
        "IMPORTER_ENABLED": True,
        "SHALLOW_ZARR_ENABLED": True,
        "IMPORTER_ORDER_API_AVAILABLE": True,
        "SHALLOW_ZARR_OPERATION_AVAILABLE": True,
        "ShallowZarrImportOperation": FakeOperation,
        "ImportOptionsEnvelope": FakeEnvelope,
        "unwrap": lambda value: value,
        "constants": SimpleNamespace(results=SimpleNamespace(
            IMPORT_PLATE_LABEL_PREVIEW="preview",
            PLATE_LABEL_PREVIEW_NAME="label",
        )),
        "logger": logger,
    }
    (build_options,) = _load_functions(
        ("build_shallow_import_options",), namespace
    )
    client = SimpleNamespace(getInput=lambda key: {
        "preview": True,
        "label": "nuclei",
    }[key])

    result = build_options(manifest, client)

    assert result["schema"] == 2
    values = result["operations"][0]
    assert values["canonicalInputs"] is manifest
    assert values["importImageLabelViews"] is True
    assert values["importPlateLabelPreview"] is True
    assert values["plateLabelName"] == "nuclei"

    namespace["SHALLOW_ZARR_OPERATION_AVAILABLE"] = False
    assert build_options(manifest, client) is None


def test_missing_snapshot_preserves_legacy_import():
    logger = RecordingLogger()
    namespace = {
        "IMPORTER_ENABLED": True,
        "SHALLOW_ZARR_ENABLED": True,
        "IMPORTER_ORDER_API_AVAILABLE": True,
        "SHALLOW_ZARR_OPERATION_AVAILABLE": True,
        "logger": logger,
    }
    (build_options,) = _load_functions(
        ("build_shallow_import_options",), namespace
    )

    assert build_options(None) is None
    assert "no canonical" in logger.info_calls[-1][0][0]


def test_lifecycle_path_selection_prunes_nested_label_zarrs(tmp_path):
    result = tmp_path / "result.ome.zarr"
    label = result / "labels" / "nuclei.zarr"
    label.mkdir(parents=True)
    regular = tmp_path / "measurements.tif"
    regular.write_bytes(b"pixels")
    namespace = {
        "Path": Path,
        "find_supported_image_paths": lambda _path: [
            str(result), str(label), str(regular)
        ],
    }
    (select_paths,) = _load_functions(
        ("select_lifecycle_import_paths",), namespace
    )

    assert select_paths(tmp_path) == [str(result), str(regular)]


def test_schema_two_order_uses_public_importer_api():
    calls = []
    logger = RecordingLogger()
    namespace = {
        "IMPORTER_ENABLED": True,
        "IMPORTER_AVAILABLE": True,
        "IMPORTER_ORDER_API_AVAILABLE": True,
        "submit_import_order": lambda order: calls.append(("api", order)),
        "log_ingestion_step": lambda order, stage: calls.append((stage, order)),
        "STAGE_NEW_ORDER": "pending",
        "logger": logger,
        "Dict": dict,
        "Any": object,
    }
    (create_order,) = _load_functions(("create_upload_order",), namespace)
    lifecycle = {"UUID": "new", "ImportOptions": {"schema": 2}}
    legacy = {"UUID": "old"}

    create_order(lifecycle)
    create_order(legacy)

    assert calls == [
        ("api", lifecycle),
        ("pending", legacy),
    ]


def test_script_delegates_without_importing_result_zarr_helpers():
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "from biomero_importer.utils.result_zarr import" not in source
    assert "inspect_returned_zarrs(" not in source
    assert "normalize_returned_zarr(" not in source
    assert "Returned Zarr inspection is delegated" in source
    assert '"source": "SLURM_Results_Lifecycle"' in source
    assert "canonical_inputs=canonical_input_manifest" in source


def test_legacy_label_options_remain_in_fallback_path():
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "find_label_zarr_paths(results_path) if import_label_zarrs else []" in source
    assert "Import_Only_Labels=true but no label zarr directories found" in source
    assert '"source": "SLURM_Results_Labels"' in source
