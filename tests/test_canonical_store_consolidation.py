import ast
import json
import re
import os
from pathlib import Path

import pytest
from biomero_schema.zarr import (
    CanonicalPlateImage,
    CanonicalPlateSource,
    CanonicalZarrSource,
    PixelIdentity,
)


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "admin" / "BIOMERO_Migrate_Shallow_Storage.py"


def _load_helpers():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted = {
        "_stable_canonical_relative_path",
        "_canonical_content_signature",
        "_normalize_canonical_source",
        "_plan_canonical_consolidation",
        "_requires_locator_update",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "Path": Path,
        "json": json,
        "re": re,
        "CanonicalPlateSource": CanonicalPlateSource,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT_PATH),
                 "exec"), namespace)
    assert wanted.issubset(namespace)
    return namespace


@pytest.fixture
def identity():
    return PixelIdentity(
        nodePath="A/1/0",
        role="image",
        iscc="ISCC:KPIXEL",
        dataCode="ISCC:GDATA",
        instanceCode="ISCC:IINSTANCE",
        toolVersion="0.1.0",
        imagewalkRevision="draft-2026-06",
        shape=(1, 1, 16, 16),
        dtype="uint16",
        axes=("t", "c", "y", "x"),
    )


def plate_source(identity, generation):
    relative = f"project/.processed/Plate-1.g{generation}.ome.zarr"
    image = CanonicalZarrSource(
        storageRoot="group-3-data",
        relativePath=relative,
        nodePath="A/1/0",
        sourceObjectType="Plate",
        sourceObjectId=1,
        sourceGeneration=generation,
        interchangeProfile="ngff-0.4-zarr-v2",
        pixelIdentity=identity,
        pixelIdentityOrigin="canonical-bootstrap",
        canonicalPixelVerified=True,
    )
    return CanonicalPlateSource(
        storageRoot="group-3-data",
        relativePath=relative,
        sourceObjectId=1,
        sourceGeneration=generation,
        interchangeProfile="ngff-0.4-zarr-v2",
        images=(CanonicalPlateImage(imageNodePath="A/1/0", source=image),),
    )


def test_plans_one_stable_store_for_identical_generations(tmp_path, identity):
    helpers = _load_helpers()
    first = plate_source(identity, 1)
    second = plate_source(identity, 2)
    records = [
        {"source": first, "annotation_id": 10},
        {"source": second, "annotation_id": 11},
    ]

    plan = helpers["_plan_canonical_consolidation"](
        records, {"group-3-data": tmp_path},
    )

    assert plan["relative_path"] == Path(
        "project/.processed/Plate-1.ome.zarr"
    )
    assert plan["destination"] == (
        tmp_path / "project/.processed/Plate-1.ome.zarr"
    ).resolve()
    assert plan["source"].source_generation == 1
    assert plan["source"].relative_path == (
        "project/.processed/Plate-1.ome.zarr"
    )
    assert plan["source"].images[0].source.relative_path == (
        "project/.processed/Plate-1.ome.zarr"
    )
    assert plan["selected"]["annotation_id"] == 11


def test_refuses_to_merge_generations_with_different_pixels(tmp_path, identity):
    helpers = _load_helpers()
    first = plate_source(identity, 1)
    changed = identity.model_copy(update={"iscc_code": "ISCC:KOTHER"})
    second = plate_source(changed, 2)

    with pytest.raises(ValueError, match="different canonical content"):
        helpers["_plan_canonical_consolidation"](
            [
                {"source": first, "annotation_id": 10},
                {"source": second, "annotation_id": 11},
            ],
            {"group-3-data": tmp_path},
        )


def test_native_managed_zarr_remains_the_canonical_location(tmp_path, identity):
    helpers = _load_helpers()
    generated = plate_source(identity, 2)
    native_relative = "project/.processed/imported-screen.ome.zarr"
    native_image = generated.images[0].source.model_copy(update={
        "relative_path": native_relative,
        "source_generation": 1,
    })
    native = generated.model_copy(update={
        "relative_path": native_relative,
        "source_generation": 1,
        "images": (generated.images[0].model_copy(update={
            "source": native_image,
        }),),
    })

    plan = helpers["_plan_canonical_consolidation"](
        [
            {"source": generated, "annotation_id": 11},
            {"source": native, "annotation_id": 12},
        ],
        {"group-3-data": tmp_path},
    )

    assert plan["selected"]["annotation_id"] == 12
    assert plan["relative_path"] == Path(native_relative)


def test_refuses_to_choose_between_two_native_managed_zarrs(tmp_path, identity):
    helpers = _load_helpers()
    first = plate_source(identity, 1)
    native_one = first.model_copy(update={
        "relative_path": "project/.processed/import-one.ome.zarr",
        "images": (first.images[0].model_copy(update={
            "source": first.images[0].source.model_copy(update={
                "relative_path": "project/.processed/import-one.ome.zarr",
            }),
        }),),
    })
    native_two = native_one.model_copy(update={
        "relative_path": "project/.processed/import-two.ome.zarr",
        "images": (native_one.images[0].model_copy(update={
            "source": native_one.images[0].source.model_copy(update={
                "relative_path": "project/.processed/import-two.ome.zarr",
            }),
        }),),
    })

    with pytest.raises(ValueError, match="multiple managed input Zarrs"):
        helpers["_plan_canonical_consolidation"](
            [{"source": native_one}, {"source": native_two}],
            {"group-3-data": tmp_path},
        )


def test_locator_update_only_changes_old_paths_or_generations():
    helpers = _load_helpers()
    replacements = {
        ("group-3-data", "Plate-1.g2.ome.zarr"): "Plate-1.ome.zarr",
        ("group-3-data", "Plate-2.ome.zarr"): "Plate-2.ome.zarr",
    }

    assert helpers["_requires_locator_update"]({
        "storageRoot": "group-3-data",
        "relativePath": "Plate-1.g2.ome.zarr",
        "sourceGeneration": 2,
    }, replacements)
    assert helpers["_requires_locator_update"]({
        "storageRoot": "group-3-data",
        "relativePath": "Plate-2.ome.zarr",
        "sourceGeneration": 2,
    }, replacements)
    assert not helpers["_requires_locator_update"]({
        "storageRoot": "group-3-data",
        "relativePath": "Plate-2.ome.zarr",
        "sourceGeneration": 1,
    }, replacements)
