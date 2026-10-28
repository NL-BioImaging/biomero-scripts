"""Use the published reconstruction return type, including its real manifest."""
import ast
import logging
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from biomero_schema.zarr import (
    CanonicalPlateImage, CanonicalPlateSource, CanonicalZarrSource, ManagedZarrNode,
    PixelIdentity, ShallowBindings, ShallowCollection, ShallowImageBinding,
    ShallowImageNode, ShallowLabelBinding, ShallowLabelNode, ShallowManifest,
    ShallowPlateReference, ZarrLabelComponent,
)
from biomero_shallower.result_zarr import MaterializedShallowResult

ROOT = Path(os.environ.get("BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]))
SCRIPT = ROOT / "_data" / "_SLURM_Image_Transfer.py"
pytestmark = pytest.mark.skipif(
    'getattr(materialized, "manifest", None)' not in SCRIPT.read_text(encoding="utf-8")
    and os.getenv("BIOMERO_TEST_PLATE_REANALYSIS") != "1",
    reason="selected source predates schema-2 Plate reconstruction support",
)


def fixture(profile, labels):
    nodes, bindings, label_nodes, label_bindings, managed = [], [], [], [], []
    for i, path in enumerate(["A/1/0", "A/2/0"]):
        ident = PixelIdentity(node_path=path, role="image", iscc_code="ISCC:KPIXEL",
                              data_code="ISCC:GDATA", instance_code="ISCC:IINSTANCE",
                              tool_version="0.2.0", imagewalk_revision="test",
                              shape=(1, 1, 1, 8, 8), dtype="uint16", axes=("t", "c", "z", "y", "x"))
        source = CanonicalZarrSource(storage_root="data", relative_path="source.zarr",
            node_path=path, source_object_type="Plate", source_object_id=9,
            source_generation=1, interchange_profile=profile, pixel_identity=ident,
            pixel_identity_origin="canonical-bootstrap", canonical_pixel_verified=True)
        nodes.append(ShallowImageNode(node_id=f"i{i}", name=f"field{i}", node_path=path))
        bindings.append(ShallowImageBinding(node_id=f"i{i}", source=source, returned_pixel_identity=ident))
        if labels:
            label_path=path+"/labels/cells"
            component=ZarrLabelComponent(logical_node_path=label_path,
                pixel_identity=ident.model_copy(update={"node_path":label_path,"role":"label"}),
                source=ManagedZarrNode(storage_root="data",relative_path="result.zarr",node_path=label_path))
            managed.append(component)
            label_nodes.append(ShallowLabelNode(node_id=f"l{i}",name=f"cells{i}",node_path=label_path,source_image_id=f"i{i}"))
            label_bindings.append(ShallowLabelBinding(node_id=f"l{i}",component=component))
    manifest=ShallowManifest(workflow_id=uuid4(),transfer_artifact="plate.zarr",interchange_profile=profile,
        collection=ShallowCollection(name="Plate",images=tuple(nodes),labels=tuple(label_nodes)),
        bindings=ShallowBindings(images=tuple(bindings),labels=tuple(label_bindings)))
    reference=ShallowPlateReference(storage_root="data",relative_path="result.zarr",
        workflow_id=manifest.workflow_id,transfer_artifact="plate.zarr",source_object_id=9,
        source_generation=1,image_node_count=2,interchange_profile=profile)
    return manifest,reference,tuple(managed)


def load_save(reference, materialized):
    def materialize(*args):
        Path(args[1]).mkdir(parents=True)
        return materialized
    names={"save_as_zarr","canonical_plate_source_from_collection"}
    tree=ast.parse(SCRIPT.read_text(encoding="utf-8"))
    body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    namespace=dict(os=os,Path=Path,tempfile=tempfile,ShallowPlateReference=ShallowPlateReference,
        CanonicalPlateImage=CanonicalPlateImage,CanonicalPlateSource=CanonicalPlateSource,
        logger=logging.getLogger(__name__),log=lambda _:None,SHALLOW_ZARR_RESTORE_AVAILABLE=True,
        get_shallow_plate_reference=lambda _:reference,materialize_shallow_zarr=materialize,
        zarr_ome_version=lambda _: "0.5" if reference.interchange_profile.endswith("v3") else "0.4",
        constants=SimpleNamespace(transfer=SimpleNamespace(DATA_TYPE_IMAGE="Image",DATA_TYPE_PLATE="Plate",
                                                           OME_ZARR_VERSION_0_4="0.4")))
    exec(compile(ast.Module(body=body,type_ignores=[]),str(SCRIPT),"exec"),namespace)
    return namespace["save_as_zarr"]


@pytest.mark.parametrize("profile",["ngff-0.4-zarr-v2","ngff-0.5-zarr-v3"])
@pytest.mark.parametrize("labels",[False,True])
def test_plate_reanalysis_uses_manifest_bindings(tmp_path,profile,labels):
    manifest,reference,managed=fixture(profile,labels)
    materialized=MaterializedShallowResult(tmp_path/"plate.zarr",manifest,managed)
    save=load_save(reference,materialized)
    plate=SimpleNamespace(getName=lambda:"plate",getId=lambda:99)
    source,artifact,restored_labels=save(None,"session",plate,str(tmp_path),"Plate",
        ome_zarr_version="0.5" if profile.endswith("v3") else "0.4",
        storage_roots={"data":tmp_path},shallow_zarr_storage=False)
    assert source.source_object_id==9
    assert source.interchange_profile==profile
    assert artifact=="plate.zarr"
    assert restored_labels==managed
    assert tuple(image.image_node_path for image in source.images)==("A/1/0","A/2/0")
    for i,image in enumerate(source.images):
        assert image.source==manifest.image_binding(f"i{i}").source
        assert image.labels==((managed[i],) if labels else ())


def test_missing_managed_labels_stop_reanalysis(tmp_path):
    manifest,reference,managed=fixture("ngff-0.5-zarr-v3",True)
    save=load_save(reference,MaterializedShallowResult(tmp_path/"plate.zarr",manifest,managed[:1]))
    with pytest.raises(ValueError,match="lost labels"):
        save(None,"session",SimpleNamespace(getName=lambda:"plate",getId=lambda:99),
             str(tmp_path),"Plate",ome_zarr_version="0.5",storage_roots={"data":tmp_path})
