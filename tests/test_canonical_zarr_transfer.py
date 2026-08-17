import ast
import json
import logging
import os
from pathlib import Path

import pytest

from biomero_schema.zarr import (
    CANONICAL_SOURCE_NAMESPACE,
    CanonicalInput,
    CanonicalZarrSource,
    PixelIdentity,
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
        "discover_canonical_inputs",
        "load_storage_roots",
        "resolve_managed_source_path",
        "get_legacy_zarr_path",
        "select_zarr_source_path",
        "select_object_storage_root",
        "derive_canonical_source_directory",
        "attach_canonical_source",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "CANONICAL_SOURCE_NAMESPACE": CANONICAL_SOURCE_NAMESPACE,
        "CanonicalInput": CanonicalInput,
        "CanonicalZarrSource": CanonicalZarrSource,
        "Path": Path,
        "json": json,
        "logging": logging,
        "logger": logging.getLogger(__name__),
        "os": os,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT_PATH),
                 "exec"), namespace)
    missing = wanted.difference(namespace)
    assert not missing, f"Missing canonical helper functions: {sorted(missing)}"
    return namespace


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
    def __init__(self, object_id, annotations, group_id=3):
        self.object_id = object_id
        self.annotations = annotations
        self.group_id = group_id

    def getId(self):
        return self.object_id

    def listAnnotations(self):
        return self.annotations

    def getDetails(self):
        return Details(self.group_id)


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


def test_storage_root_config_resolves_only_managed_existing_path(
    tmp_path, pixel_identity
):
    ns = _load_canonical_functions()
    managed_root = tmp_path / "group"
    canonical_path = managed_root / ".processed/Image-7.g1.ome.zarr"
    canonical_path.mkdir(parents=True)
    config_path = tmp_path / "biomero-config.json"
    config_path.write_text(json.dumps({
        "storage_roots": {"group-3-data": str(managed_root)}
    }), encoding="utf-8")

    roots = ns["load_storage_roots"](config_path)
    resolved = ns["resolve_managed_source_path"](
        source(pixel_identity), roots)

    assert resolved == canonical_path.resolve()


def test_missing_storage_root_mapping_does_not_guess(tmp_path, pixel_identity):
    ns = _load_canonical_functions()

    assert ns["resolve_managed_source_path"](
        source(pixel_identity), {}) is None


def test_canonical_source_precedes_legacy_annotation(
    tmp_path, pixel_identity
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

    selected = ns["select_zarr_source_path"](
        obj,
        source(pixel_identity),
        {"group-3-data": managed_root},
    )

    assert selected == canonical_path.resolve()


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


def test_image_transfer_publishes_canonical_inputs_output():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'CANONICAL_INPUTS_OUTPUT = "Canonical_Inputs"' in script
    assert "client.setOutput(" in script
    assert "CANONICAL_INPUTS_OUTPUT," in script
