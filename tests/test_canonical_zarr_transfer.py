import ast
import json
import logging
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from biomero_schema.zarr import (
    CANONICAL_PLATE_IMAGE_NAMESPACE,
    CANONICAL_PLATE_LABEL_NAMESPACE,
    CANONICAL_PLATE_SOURCE_NAMESPACE,
    CANONICAL_SOURCE_NAMESPACE,
    SHALLOW_COLLECTION_NAMESPACE,
    CanonicalInput,
    CanonicalPlateImage,
    CanonicalPlateImageRecord,
    CanonicalPlateIndex,
    CanonicalPlateLabelRecord,
    CanonicalPlateSource,
    CanonicalZarrSource,
    ManagedZarrNode,
    PixelIdentity,
    ShallowZarrReference,
    ZarrLabelComponent,
)


SCRIPT_PATH = (
    Path(__file__).parents[1] / "_data" / "_SLURM_Image_Transfer.py"
)


def _load_canonical_functions():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted = {
        "_annotation_namespace",
        "_annotation_values",
        "get_canonical_source",
        "get_canonical_plate_source",
        "get_shallow_reference",
        "discover_canonical_inputs",
        "load_group_storage_roots",
        "locate_managed_zarr",
        "resolve_managed_source_path",
        "get_legacy_zarr_path",
        "select_zarr_source_path",
        "select_object_storage_root",
        "derive_canonical_source_directory",
        "attach_canonical_source",
        "attach_canonical_plate_source",
        "promote_exported_image_zarr",
        "index_existing_image_zarr",
        "index_existing_plate_zarr",
        "promote_exported_plate_zarr",
        "canonical_inputs_from_sources",
        "discover_canonical_label_components",
        "build_canonical_plate_source",
        "validate_omero_image_semantics",
        "is_shallow_zarr_storage_enabled",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "CANONICAL_PLATE_IMAGE_NAMESPACE": CANONICAL_PLATE_IMAGE_NAMESPACE,
        "CANONICAL_PLATE_LABEL_NAMESPACE": CANONICAL_PLATE_LABEL_NAMESPACE,
        "CANONICAL_PLATE_SOURCE_NAMESPACE": CANONICAL_PLATE_SOURCE_NAMESPACE,
        "CANONICAL_SOURCE_NAMESPACE": CANONICAL_SOURCE_NAMESPACE,
        "SHALLOW_COLLECTION_NAMESPACE": SHALLOW_COLLECTION_NAMESPACE,
        "CanonicalInput": CanonicalInput,
        "CanonicalPlateImage": CanonicalPlateImage,
        "CanonicalPlateImageRecord": CanonicalPlateImageRecord,
        "CanonicalPlateIndex": CanonicalPlateIndex,
        "CanonicalPlateLabelRecord": CanonicalPlateLabelRecord,
        "CanonicalPlateSource": CanonicalPlateSource,
        "CanonicalZarrSource": CanonicalZarrSource,
        "ManagedZarrNode": ManagedZarrNode,
        "ShallowZarrReference": ShallowZarrReference,
        "ZarrLabelComponent": ZarrLabelComponent,
        "Path": Path,
        "json": json,
        "logging": logging,
        "logger": logging.getLogger(__name__),
        "log": lambda text: None,
        "pixel_identities_match": lambda left, right: left == right,
        "os": os,
        "shutil": shutil,
        "BIOMERO_CONFIG_FILE": "/missing/biomero-config.json",
        "GROUP_MAPPINGS_FILE": "/missing/group-mappings.json",
        "IMPORT_MOUNT_PATH": "/data",
        "IMPORT_MOUNT_STORAGE_ROOT": "import-mount-data",
        "IMPORTER_ENABLED": False,
        "SHALLOW_ZARR_ENABLED": False,
        "SHALLOW_ZARR_SUPPORT_AVAILABLE": True,
        "constants": SimpleNamespace(
            transfer=SimpleNamespace(FORMAT_OMEZARR="OME-ZARR")
        ),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT_PATH),
                 "exec"), namespace)
    missing = wanted.difference(namespace)
    assert not missing, f"Missing canonical helper functions: {sorted(missing)}"
    return namespace


def test_shallow_zarr_storage_requires_importer_capability():
    ns = _load_canonical_functions()

    assert ns["is_shallow_zarr_storage_enabled"]("OME-ZARR") is False

    ns["IMPORTER_ENABLED"] = True
    assert ns["is_shallow_zarr_storage_enabled"]("OME-ZARR") is False

    ns["SHALLOW_ZARR_ENABLED"] = True
    assert ns["is_shallow_zarr_storage_enabled"]("OME-ZARR") is True

    ns["SHALLOW_ZARR_SUPPORT_AVAILABLE"] = False
    assert ns["is_shallow_zarr_storage_enabled"]("OME-ZARR") is False
    assert ns["is_shallow_zarr_storage_enabled"]("OME-TIFF") is False


def test_importer_dependencies_are_only_imported_behind_capability_guard():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    top_level_imports = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert not any(
        getattr(node, "module", "") == "biomero_importer"
        or getattr(node, "module", "").startswith("biomero_importer.")
        for node in top_level_imports
    )

    guarded_imports = [
        node
        for conditional in tree.body
        if (
            isinstance(conditional, ast.If)
            and "IMPORTER_ENABLED" in ast.unparse(conditional.test)
            and "SHALLOW_ZARR_ENABLED" in ast.unparse(conditional.test)
        )
        for node in ast.walk(conditional)
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("biomero_importer.")
        )
    ]
    assert len(guarded_imports) == 4


class NamedValue:
    def __init__(self, name, value):
        self.name = name
        self.value = value


class Annotation:
    def __init__(self, namespace, values):
        self.namespace = namespace
        self.values = values

    def getNs(self):
        return self.namespace

    def getMapValue(self):
        return [NamedValue(key, value) for key, value in self.values.items()]


class Object:
    def __init__(
        self,
        object_id,
        annotations,
        group_id=3,
        shape=(1, 1, 1, 16, 16),
        pixel_type="uint16",
    ):
        self.object_id = object_id
        self.annotations = annotations
        self.group_id = group_id
        self.shape = shape
        self.pixel_type = pixel_type

    def getId(self):
        return self.object_id

    def listAnnotations(self):
        return self.annotations

    def getDetails(self):
        return Details(self.group_id)

    def getSizeT(self):
        return self.shape[0]

    def getSizeC(self):
        return self.shape[1]

    def getSizeZ(self):
        return self.shape[2]

    def getSizeY(self):
        return self.shape[3]

    def getSizeX(self):
        return self.shape[4]

    def getPrimaryPixels(self):
        return Pixels(self.pixel_type)


class PixelsType:
    def __init__(self, value):
        self.value = value

    def getValue(self):
        return self.value


class Pixels:
    def __init__(self, pixel_type):
        self.pixel_type = pixel_type

    def getPixelsType(self):
        return PixelsType(self.pixel_type)


class Group:
    def __init__(self, group_id):
        self.group_id = group_id

    def getId(self):
        return self.group_id


class Details:
    def __init__(self, group_id):
        self.group_id = group_id

    def getGroup(self):
        return Group(self.group_id)


@pytest.fixture
def pixel_identity():
    return PixelIdentity(
        node_path=".",
        role="image",
        iscc_code="ISCC:KPIXEL",
        data_code="ISCC:GDATA",
        instance_code="ISCC:IINSTANCE",
        tool_version="0.1.0",
        imagewalk_revision="draft-2026-06",
        shape=(1, 1, 1, 16, 16),
        dtype="uint16",
        axes=("t", "c", "z", "y", "x"),
    )


def source(pixel_identity, generation=1, node_path="."):
    return CanonicalZarrSource(
        storage_root="group-3-data",
        relative_path=(
            f".processed/Image-7.g{generation}.ome.zarr"
        ),
        node_path=node_path,
        source_object_type="Image",
        source_object_id=7,
        source_generation=generation,
        interchange_profile="ngff-0.4-zarr-v2",
        pixel_identity=pixel_identity,
        pixel_identity_origin="raw",
        canonical_pixel_verified=True,
    )


def annotation_for(source_record):
    return Annotation(
        CANONICAL_SOURCE_NAMESPACE,
        source_record.to_annotation_values(),
    )


def plate_source(pixel_identity, generation=1):
    relative = f".processed/Plate-9.g{generation}.ome.zarr"
    image_identity = pixel_identity.model_copy(update={
        "node_path": "A/1/0",
    })
    image_source = CanonicalZarrSource(
        storage_root="group-3-data",
        relative_path=relative,
        node_path="A/1/0",
        source_object_type="Plate",
        source_object_id=9,
        source_generation=generation,
        interchange_profile="ngff-0.4-zarr-v2",
        pixel_identity=image_identity,
        pixel_identity_origin="canonical-bootstrap",
        canonical_pixel_verified=False,
    )
    return CanonicalPlateSource(
        storage_root="group-3-data",
        relative_path=relative,
        source_object_id=9,
        source_generation=generation,
        interchange_profile="ngff-0.4-zarr-v2",
        images=(CanonicalPlateImage(
            image_node_path="A/1/0",
            source=image_source,
        ),),
    )


def plate_annotation_for(source_record):
    return Annotation(
        CANONICAL_PLATE_SOURCE_NAMESPACE,
        source_record.to_annotation_values(),
    )


def test_resolves_shallow_projection_reference(pixel_identity):
    ns = _load_canonical_functions()
    canonical = source(pixel_identity)
    reference = ShallowZarrReference(
        storage_root="import-mount-data",
        relative_path="Project A/.analyzed/run/result.zarr",
        workflow_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        transfer_artifact="result.zarr",
        image_node_path=".",
        label_node_paths=("labels/nuclei", "labels/cells"),
        source=canonical,
        interchange_profile="ngff-0.4-zarr-v2",
    )
    annotation = Annotation(
        SHALLOW_COLLECTION_NAMESPACE,
        reference.to_annotation_values(),
    )

    restored = ns["get_shallow_reference"](Object(99, [annotation]))

    assert restored == reference


def test_canonical_source_selection_is_independent_of_annotation_order(
    pixel_identity,
):
    ns = _load_canonical_functions()
    older = annotation_for(source(pixel_identity, generation=1))
    current = annotation_for(source(pixel_identity, generation=2))
    unrelated = Annotation("other.namespace", {"relativePath": "bad"})

    first = ns["get_canonical_source"](
        Object(7, [older, unrelated, current]), "Image")
    second = ns["get_canonical_source"](
        Object(7, [current, older, unrelated]), "Image")

    assert first == second
    assert first.source_generation == 2


def test_canonical_source_rejects_ambiguous_current_generation(pixel_identity):
    ns = _load_canonical_functions()
    first = source(pixel_identity, generation=2)
    second = first.model_copy(update={
        "relative_path": ".processed/other/Image-7.g2.ome.zarr"
    })

    with pytest.raises(ValueError, match="ambiguous"):
        ns["get_canonical_source"](
            Object(7, [annotation_for(first), annotation_for(second)]),
            "Image",
        )


def test_resolves_latest_canonical_plate_source(pixel_identity):
    ns = _load_canonical_functions()
    old = plate_source(pixel_identity, generation=1)
    current = plate_source(pixel_identity, generation=2)

    restored = ns["get_canonical_plate_source"](Object(
        9,
        [plate_annotation_for(current), plate_annotation_for(old)],
    ))

    assert restored == current


def test_split_canonical_plate_annotations_round_trip(pixel_identity):
    ns = _load_canonical_functions()
    canonical = plate_source(pixel_identity)
    writes = []
    plate = Object(9, [])

    result = ns["attach_canonical_plate_source"](
        "connection",
        plate,
        canonical,
        annotation_writer=lambda **kwargs: writes.append(kwargs) or len(writes),
    )
    plate.annotations.extend(
        Annotation(write["ns"], write["kv_dict"]) for write in writes
    )

    assert result == (1, 2)
    assert [write["ns"] for write in writes] == [
        CANONICAL_PLATE_IMAGE_NAMESPACE,
        CANONICAL_PLATE_SOURCE_NAMESPACE,
    ]
    assert "images" not in writes[-1]["kv_dict"]
    assert ns["get_canonical_plate_source"](plate) == canonical


def test_incomplete_split_plate_index_is_ignored(pixel_identity, caplog):
    ns = _load_canonical_functions()
    canonical = plate_source(pixel_identity)
    index = CanonicalPlateIndex.from_source(canonical)
    plate = Object(9, [Annotation(
        CANONICAL_PLATE_SOURCE_NAMESPACE,
        index.to_annotation_values(),
    )])

    with caplog.at_level(logging.WARNING):
        restored = ns["get_canonical_plate_source"](plate)

    assert restored is None
    assert "incomplete split canonical Plate" in caplog.text


def test_plate_discovery_builds_plate_input(pixel_identity):
    ns = _load_canonical_functions()
    canonical = plate_source(pixel_identity)
    sources, inputs = ns["discover_canonical_inputs"](
        [Object(9, [plate_annotation_for(canonical)])],
        "Plate",
    )

    assert sources == {9: canonical}
    assert inputs[0].source is None
    assert inputs[0].plate_source == canonical


def test_builds_per_image_and_label_plate_identities(tmp_path, pixel_identity):
    ns = _load_canonical_functions()
    nodes = (
        SimpleNamespace(
            node_path="A/1/0",
            role="image",
            parent_image_node_path=None,
        ),
        SimpleNamespace(
            node_path="A/1/0/labels/cells",
            role="label",
            parent_image_node_path="A/1/0",
        ),
    )
    guard = SimpleNamespace(
        shape=(1, 1, 1, 16, 16),
        dtype="uint16",
        axes=("t", "c", "z", "y", "x"),
        coordinate_transformations=(),
    )

    class Provider:
        def generate(self, _root, **values):
            return pixel_identity.model_copy(update={
                "node_path": values["node_path"],
                "role": values["role"],
            })

    ns["discover_ngff_nodes"] = lambda _root: nodes
    ns["read_zarr_v2_semantic_guard"] = lambda _root, _node: guard
    result = ns["build_canonical_plate_source"](
        Object(9, []),
        tmp_path / "plate.zarr",
        "group-3-data",
        ".processed/Plate-9.g1.ome.zarr",
        identity_provider=Provider(),
    )

    assert result.images[0].source.pixel_identity.node_path == "A/1/0"
    assert result.images[0].labels[0].logical_node_path == (
        "A/1/0/labels/cells"
    )
    assert result.images[0].labels[0].source.node_path == (
        "A/1/0/labels/cells"
    )


def test_discovery_only_returns_snapshot_when_every_object_has_source(
    pixel_identity,
):
    ns = _load_canonical_functions()
    canonical = source(pixel_identity)
    complete_sources, complete_inputs = ns["discover_canonical_inputs"](
        [Object(7, [annotation_for(canonical)])], "Image")

    assert complete_sources == {7: canonical}
    assert complete_inputs == (
        CanonicalInput(
            ordinal=0,
            selected_object_type="Image",
            selected_object_id=7,
            source=canonical,
        ),
    )

    sources, inputs = ns["discover_canonical_inputs"](
        [Object(7, [annotation_for(canonical)]), Object(8, [])], "Image")
    assert sources == {7: canonical}
    assert inputs == ()


def test_group_mapping_resolves_managed_root_under_import_mount(
    tmp_path, pixel_identity
):
    ns = _load_canonical_functions()
    import_mount = tmp_path / "import-mount"
    managed_root = import_mount / "Project A"
    canonical_path = managed_root / ".processed/Image-7.g1.ome.zarr"
    canonical_path.mkdir(parents=True)
    config_path = tmp_path / "biomero-config.json"
    config_path.write_text(json.dumps({
        "group_mappings": {
            "3": {"folder": "Project A", "groupName": "test"}
        }
    }), encoding="utf-8")
    dedicated_path = tmp_path / "missing-group-mappings.json"

    roots = ns["load_group_storage_roots"](
        config_path,
        dedicated_path,
        import_mount,
    )
    resolved = ns["resolve_managed_source_path"](
        source(pixel_identity), roots)

    assert resolved == canonical_path.resolve()


def test_dedicated_group_mapping_overrides_legacy_config(tmp_path):
    ns = _load_canonical_functions()
    import_mount = tmp_path / "import-mount"
    config_path = tmp_path / "biomero-config.json"
    config_path.write_text(json.dumps({
        "group_mappings": {"3": {"folder": "Old Project"}}
    }), encoding="utf-8")
    dedicated_path = tmp_path / "group-mappings.json"
    dedicated_path.write_text(json.dumps({
        "3": {"folder": "Current Project", "groupName": "test"}
    }), encoding="utf-8")

    roots = ns["load_group_storage_roots"](
        config_path,
        dedicated_path,
        import_mount,
    )

    assert roots == {
        "import-mount-data": import_mount.resolve(),
        "group-3-data": (import_mount / "Current Project").resolve()
    }


@pytest.mark.parametrize("folder", ["../escape", "/absolute"])
def test_group_mapping_cannot_escape_import_mount(tmp_path, folder):
    ns = _load_canonical_functions()
    config_path = tmp_path / "biomero-config.json"
    config_path.write_text(json.dumps({
        "group_mappings": {"3": {"folder": folder}}
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="IMPORT_MOUNT_PATH"):
        ns["load_group_storage_roots"](
            config_path,
            tmp_path / "missing-group-mappings.json",
            tmp_path / "import-mount",
        )


def test_missing_storage_root_mapping_does_not_guess(tmp_path, pixel_identity):
    ns = _load_canonical_functions()

    assert ns["resolve_managed_source_path"](
        source(pixel_identity), {}) is None


def test_canonical_source_precedes_legacy_annotation(
    tmp_path, pixel_identity, caplog
):
    ns = _load_canonical_functions()
    managed_root = tmp_path / "managed"
    canonical_path = managed_root / ".processed/Image-7.g1.ome.zarr"
    canonical_path.mkdir(parents=True)
    legacy_path = tmp_path / "legacy.zarr"
    legacy_path.mkdir()
    obj = Object(7, [Annotation("legacy", {
        "Filepath": str(legacy_path),
        "Imported_from": str(legacy_path),
    })])

    with caplog.at_level(logging.INFO):
        selected = ns["select_zarr_source_path"](
            obj,
            source(pixel_identity),
            {"group-3-data": managed_root},
        )

    assert selected == canonical_path.resolve()
    assert "Reusing canonical Zarr for Image 7" in caplog.text
    assert "pixel ISCC=ISCC:KPIXEL" in caplog.text
    assert "ISCC-BIO was not recalculated" in caplog.text


def test_legacy_zarr_reuse_logs_missing_cached_identity(
    tmp_path, pixel_identity, caplog
):
    ns = _load_canonical_functions()
    legacy_path = tmp_path / "legacy.zarr"
    legacy_path.mkdir()
    obj = Object(8, [Annotation("legacy", {
        "Imported_from": str(legacy_path),
    })])

    with caplog.at_level(logging.INFO):
        selected = ns["select_zarr_source_path"](obj, None, {})

    assert selected == legacy_path.resolve()
    assert "Reusing legacy Zarr path for OMERO object 8" in caplog.text
    assert "not yet a BIOMERO canonical; no cached ISCC" in caplog.text


def test_nested_canonical_source_falls_back_to_fresh_export(
    tmp_path, pixel_identity
):
    ns = _load_canonical_functions()
    legacy_path = tmp_path / "legacy.zarr"
    legacy_path.mkdir()
    obj = Object(7, [Annotation("legacy", {
        "Imported_from": str(legacy_path),
    })])

    selected = ns["select_zarr_source_path"](
        obj,
        source(pixel_identity, node_path="0"),
        {"group-3-data": tmp_path},
    )

    assert selected is None


def test_selects_storage_root_from_omero_group(tmp_path):
    ns = _load_canonical_functions()
    roots = {
        "group-0-data": tmp_path / "system",
        "group-3-data": tmp_path / "test",
    }

    storage_id, storage_root = ns["select_object_storage_root"](
        Object(7, [], group_id=3), roots
    )

    assert storage_id == "group-3-data"
    assert storage_root == tmp_path / "test"


def test_missing_group_storage_root_fails_closed(tmp_path):
    ns = _load_canonical_functions()

    with pytest.raises(ValueError, match="group 3"):
        ns["select_object_storage_root"](
            Object(7, [], group_id=3), {"group-0-data": tmp_path}
        )


def test_derives_source_directory_from_managed_provenance(tmp_path):
    ns = _load_canonical_functions()
    root = tmp_path / "group"
    raw_file = root / "project" / "dataset" / "source.lif"
    obj = Object(7, [Annotation("legacy", {
        "Imported_from": str(raw_file),
        "Filepath": str(root / "other" / "ignored.tif"),
    })])

    relative = ns["derive_canonical_source_directory"](obj, root)

    assert relative == Path("project/dataset")


def test_source_directory_falls_back_to_group_root(tmp_path):
    ns = _load_canonical_functions()
    root = tmp_path / "group"

    assert ns["derive_canonical_source_directory"](
        Object(7, []), root
    ) == Path(".")


def test_attaches_canonical_source_annotation_once(pixel_identity):
    ns = _load_canonical_functions()
    canonical = source(pixel_identity)
    obj = Object(7, [])
    writes = []

    result = ns["attach_canonical_source"](
        "connection",
        obj,
        "Image",
        canonical,
        annotation_writer=lambda **kwargs: writes.append(kwargs) or 99,
    )

    assert result == 99
    assert writes == [{
        "conn": "connection",
        "object_type": "Image",
        "object_id": 7,
        "kv_dict": canonical.to_annotation_values(),
        "ns": CANONICAL_SOURCE_NAMESPACE,
        "across_groups": False,
    }]

    obj.annotations.append(annotation_for(canonical))
    assert ns["attach_canonical_source"](
        "connection",
        obj,
        "Image",
        canonical,
        annotation_writer=lambda **kwargs: pytest.fail("duplicate write"),
    ) is None


def test_managed_zarr_location_uses_most_specific_root(tmp_path):
    ns = _load_canonical_functions()
    import_root = tmp_path / "import"
    group_root = import_root / "Project A"
    zarr_path = group_root / ".analyzed/workflow/image.zarr"
    zarr_path.mkdir(parents=True)

    storage_id, root, relative = ns["locate_managed_zarr"](
        zarr_path,
        {
            "import-mount-data": import_root,
            "group-3-data": group_root,
        },
    )

    assert storage_id == "group-3-data"
    assert root == group_root.resolve()
    assert relative == Path(".analyzed/workflow/image.zarr")


def test_promotes_verified_image_and_restores_task_copy(
    tmp_path, pixel_identity, caplog
):
    ns = _load_canonical_functions()
    root = tmp_path / "managed"
    export = tmp_path / "task" / "image.zarr"
    export.mkdir(parents=True)
    (export / ".zgroup").write_text("{}", encoding="utf-8")
    canonical_path = root / ".processed/Image-7.g1.ome.zarr"
    canonical = source(pixel_identity)
    identities = []
    writes = []

    class Provider:
        def generate(self, path, **guard):
            identities.append(("zarr", path, guard))
            return pixel_identity

        def generate_omero(self, conn, **guard):
            identities.append(("omero", conn, guard))
            return pixel_identity

    class Promotion:
        def promote(self, staging, **kwargs):
            canonical_path.parent.mkdir(parents=True)
            shutil.move(staging, canonical_path)
            (canonical_path / ".biomero-canonical.json").write_text(
                "{}", encoding="utf-8"
            )
            return SimpleNamespace(source=canonical, path=canonical_path)

    with caplog.at_level(logging.INFO):
        result = ns["promote_exported_image_zarr"](
            "connection",
            Object(7, []),
            export,
            {"group-3-data": root},
            identity_provider=Provider(),
            semantic_guard_reader=lambda path, node: SimpleNamespace(
                shape=(1, 1, 1, 16, 16),
                dtype="uint16",
                axes=("t", "c", "z", "y", "x"),
                coordinate_transformations=(),
            ),
            promotion_service_factory=lambda **kwargs: Promotion(),
            annotation_writer=lambda **kwargs: writes.append(kwargs) or 99,
        )

    assert result == canonical
    assert canonical_path.is_dir()
    assert export.is_dir()
    assert (export / ".zgroup").is_file()
    assert not (export / ".biomero-canonical.json").exists()
    assert [item[0] for item in identities] == ["omero", "zarr"]
    assert writes[0]["ns"] == CANONICAL_SOURCE_NAMESPACE
    assert "Calculating ISCC-BIO pixel identity from OMERO Pixels" in caplog.text
    assert "Calculated OMERO pixel identity" in caplog.text
    assert "ISCC=ISCC:KPIXEL" in caplog.text
    assert "Calculating ISCC-BIO pixel identity from exported Zarr" in caplog.text
    assert "ISCC-BIO verification matched" in caplog.text


def test_indexes_verified_existing_image_without_copying(
    tmp_path, pixel_identity, caplog
):
    ns = _load_canonical_functions()
    root = tmp_path / "managed"
    existing = root / ".analyzed/workflow/image.zarr"
    existing.mkdir(parents=True)
    writes = []

    class Provider:
        def generate(self, path, **guard):
            return pixel_identity

        def generate_omero(self, conn, **guard):
            return pixel_identity

    class Indexing:
        def index_existing(self, path, **kwargs):
            assert Path(path).resolve() == existing.resolve()
            assert kwargs["relative_path"] == Path(
                ".analyzed/workflow/image.zarr"
            )
            indexed = source(pixel_identity).model_copy(update={
                "relative_path": ".analyzed/workflow/image.zarr",
            })
            return SimpleNamespace(source=indexed, path=existing)

    with caplog.at_level(logging.INFO):
        indexed = ns["index_existing_image_zarr"](
            "connection",
            Object(7, []),
            existing,
            {"group-3-data": root},
            identity_provider=Provider(),
            semantic_guard_reader=lambda path, node: SimpleNamespace(
                shape=(1, 1, 1, 16, 16),
                dtype="uint16",
                axes=("t", "c", "z", "y", "x"),
                coordinate_transformations=(),
            ),
            promotion_service_factory=lambda **kwargs: Indexing(),
            annotation_writer=lambda **kwargs: writes.append(kwargs) or 99,
        )

    assert indexed.relative_path == ".analyzed/workflow/image.zarr"
    assert existing.is_dir()
    assert writes[0]["ns"] == CANONICAL_SOURCE_NAMESPACE
    assert "indexed generation 1 in place" in caplog.text


def test_rejects_ngff_shape_or_dtype_that_disagrees_with_omero():
    ns = _load_canonical_functions()
    image = Object(7, [], shape=(1, 2, 3, 16, 32), pixel_type="uint16")

    ns["validate_omero_image_semantics"](
        image,
        SimpleNamespace(
            axes=("t", "c", "z", "y", "x"),
            shape=(1, 2, 3, 16, 32),
            dtype="uint16",
        ),
    )

    with pytest.raises(ValueError, match="shape"):
        ns["validate_omero_image_semantics"](
            image,
            SimpleNamespace(
                axes=("t", "c", "z", "y", "x"),
                shape=(1, 2, 3, 32, 16),
                dtype="uint16",
            ),
        )
    with pytest.raises(ValueError, match="pixel type"):
        ns["validate_omero_image_semantics"](
            image,
            SimpleNamespace(
                axes=("t", "c", "z", "y", "x"),
                shape=(1, 2, 3, 16, 32),
                dtype="uint8",
            ),
        )


def test_builds_snapshot_from_promoted_and_existing_sources(pixel_identity):
    ns = _load_canonical_functions()
    objects = [Object(7, []), Object(8, [])]
    first = source(pixel_identity)
    second = first.model_copy(update={
        "source_object_id": 8,
        "relative_path": ".processed/Image-8.g1.ome.zarr",
    })

    inputs = ns["canonical_inputs_from_sources"](
        objects,
        "Image",
        {7: first, 8: second},
        {7: "first.zarr", 8: "second.zarr"},
    )

    assert [item.ordinal for item in inputs] == [0, 1]
    assert [item.source.source_object_id for item in inputs] == [7, 8]
    assert [item.transfer_artifact for item in inputs] == [
        "first.zarr",
        "second.zarr",
    ]
    assert ns["canonical_inputs_from_sources"](
        objects,
        "Image",
        {7: first},
        {7: "first.zarr", 8: "second.zarr"},
    ) == ()
    assert ns["canonical_inputs_from_sources"](
        objects,
        "Image",
        {7: first, 8: second},
        {7: "first.zarr"},
    ) == ()


def test_builds_snapshot_from_canonical_plate_source(pixel_identity):
    ns = _load_canonical_functions()
    canonical = plate_source(pixel_identity)

    inputs = ns["canonical_inputs_from_sources"](
        [Object(9, [])],
        "Plate",
        {9: canonical},
        {9: "plate.zarr"},
    )

    assert len(inputs) == 1
    assert inputs[0].plate_source == canonical
    assert inputs[0].source is None
    assert inputs[0].transfer_artifact == "plate.zarr"


def test_snapshot_hashes_labels_already_in_canonical_zarr(
    tmp_path,
    pixel_identity,
):
    ns = _load_canonical_functions()
    canonical = source(pixel_identity)
    canonical_root = tmp_path / canonical.relative_path
    canonical_root.mkdir(parents=True)
    label_identity = pixel_identity.model_copy(update={
        "node_path": "labels/nuclei",
        "role": "label",
        "instance_code": "ISCC:INUCLEI",
    })

    ns["discover_ngff_nodes"] = lambda _root: (
        SimpleNamespace(
            node_path=".", role="image", parent_image_node_path=None),
        SimpleNamespace(
            node_path="labels/nuclei",
            role="label",
            parent_image_node_path=".",
        ),
    )
    ns["read_zarr_v2_semantic_guard"] = lambda _root, _node: SimpleNamespace(
        shape=label_identity.shape,
        dtype=label_identity.dtype,
        axes=label_identity.axes,
        coordinate_transformations=label_identity.coordinate_transformations,
    )

    class Provider:
        def generate(self, _root, **_kwargs):
            return label_identity

    ns["IsccBioIdentityProvider"] = Provider
    inputs = ns["canonical_inputs_from_sources"](
        [Object(7, [])],
        "Image",
        {7: canonical},
        {7: "image.zarr"},
        {"group-3-data": tmp_path},
    )

    assert len(inputs) == 1
    assert inputs[0].labels == (ZarrLabelComponent(
        logical_node_path="labels/nuclei",
        pixel_identity=label_identity,
        source=ManagedZarrNode(
            storage_root="group-3-data",
            relative_path=canonical.relative_path,
            node_path="labels/nuclei",
        ),
    ),)


def test_image_transfer_publishes_canonical_inputs_output():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'CANONICAL_INPUTS_OUTPUT = "Canonical_Inputs"' in script
    assert "client.setOutput(" in script
    assert "CANONICAL_INPUTS_OUTPUT," in script


def test_fresh_image_exports_feed_promotion_and_final_snapshot():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "canonical_source = promote_exported_image_zarr(" in script
    assert (
        "promoted_source, transfer_artifact, label_components = "
        "save_image_as_zarr(" in script
    )
    assert "canonical_inputs = canonical_inputs_from_sources(" in script
    assert "transfer_artifacts," in script
    assert "materialize_shallow_zarr(" in script


def test_plate_exports_are_indexed_or_promoted_for_final_snapshot():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "canonical_source = index_existing_plate_zarr(" in script
    assert "canonical_source = promote_exported_plate_zarr(" in script
