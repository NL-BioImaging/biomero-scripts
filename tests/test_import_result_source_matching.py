import ast
import difflib
import logging
import os
import re
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"
SOURCE_TEXT = SCRIPT_PATH.read_text(encoding="utf-8")
pytestmark = pytest.mark.skipif(
    "def get_images_in_id_order" not in SOURCE_TEXT,
    reason="ordered result-source matching is not available in this revision",
)


def load_functions(*names):
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted = set(names)
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "Any": Any,
        "BlitzGateway": object,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "SlurmClient": object,
        "Tuple": Tuple,
        "add_image_annotations": lambda *_args, **_kwargs: None,
        "add_metadata_to_imported_plates": lambda *_args, **_kwargs: "",
        "difflib": difflib,
        "logger": logging.getLogger("import-result-source-matching"),
        "os": os,
        "re": re,
        "rstring": lambda value: value,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace


class Image:
    def __init__(self, image_id, name, description=""):
        self.image_id = image_id
        self.name = name
        self.description = description
        self._obj = self

    def getId(self):
        return self.image_id

    def getName(self):
        return self.name

    def getDescription(self):
        return self.description

    def setDescription(self, description):
        self.description = description


def matching_functions():
    return load_functions(
        "getOriginalFilename",
        "find_best_matching_image",
        "match_results_to_inputs",
    )


def test_duplicate_result_names_keep_one_match_per_occurrence():
    match_results = matching_functions()["match_results_to_inputs"]
    sources = [
        Image(55, "Cell-Granules_1.tif"),
        Image(169, "Cell-Granules_2.tif"),
    ]

    matches = match_results(["labels_cells", "labels_cells"], sources)

    assert isinstance(matches, list)
    assert [image.getId() for image in matches] == [55, 169]


def test_image_lookup_restores_forwarded_id_order():
    functions = load_functions("get_images_in_id_order")
    assert "get_images_in_id_order" in functions
    get_images = functions["get_images_in_id_order"]
    returned = [
        Image(169, "Cell-Granules_2.tif"),
        Image(55, "Cell-Granules_1.tif"),
    ]
    conn = SimpleNamespace(
        getObjects=lambda object_type, ids: returned,
    )

    images = get_images(conn, [55, 169])

    assert [image.getId() for image in images] == [55, 169]


def test_image_lookup_skips_omero_query_for_empty_ids():
    get_images = load_functions("get_images_in_id_order")[
        "get_images_in_id_order"
    ]

    class ConnectionThatMustNotBeQueried:
        def getObjects(self, *_args, **_kwargs):
            raise AssertionError("empty image IDs must not query OMERO")

    assert get_images(ConnectionThatMustNotBeQueried(), []) == []


class Parameters:
    def addString(self, _key, _value):
        pass


class QueryService:
    def projection(self, _hql, _params, _options):
        return [
            [SimpleNamespace(val=251)],
            [SimpleNamespace(val=252)],
        ]


class UpdateService:
    def __init__(self):
        self.saved = []

    def saveAndReturnObject(self, obj):
        self.saved.append(obj)
        return obj


class Connection:
    SERVICE_OPTS = object()

    def __init__(self, images):
        self.images = {image.getId(): image for image in images}
        self.update_service = UpdateService()

    def getQueryService(self):
        return QueryService()

    def getObject(self, object_type, object_id):
        assert object_type == "Image"
        return self.images[object_id]

    def getUpdateService(self):
        return self.update_service


@pytest.mark.parametrize("object_type", ["Image", "Plate"])
def test_workflow_reference_is_searchable_and_idempotent(object_type):
    ensure_reference = load_functions(
        "ensure_searchable_workflow_reference"
    )["ensure_searchable_workflow_reference"]
    result = Image(251, "result", "existing provenance")
    update_service = UpdateService()

    class ResultConnection:
        def getObject(self, requested_type, object_id):
            assert requested_type == object_type
            assert object_id == 251
            return result

        def getUpdateService(self):
            return update_service

    conn = ResultConnection()

    assert ensure_reference(conn, object_type, 251, "workflow-uuid") is True
    assert result.getDescription() == (
        "existing provenance | BIOMERO workflow: workflow-uuid"
    )
    assert ensure_reference(conn, object_type, 251, "workflow-uuid") is False
    assert len(update_service.saved) == 1


def test_workflow_reference_ignores_missing_workflow_id():
    ensure_reference = load_functions(
        "ensure_searchable_workflow_reference"
    )["ensure_searchable_workflow_reference"]

    class ConnectionThatMustNotBeQueried:
        def getObject(self, *_args, **_kwargs):
            raise AssertionError("missing workflow ID must not query OMERO")

    assert ensure_reference(
        ConnectionThatMustNotBeQueried(), "Image", 251, None
    ) is False


def test_duplicate_imported_names_get_distinct_descriptions_and_roi_pairs(
        monkeypatch):
    functions = load_functions(
        "getOriginalFilename",
        "find_best_matching_image",
        "match_results_to_inputs",
        "add_metadata_to_imported_images",
    )
    add_metadata = functions["add_metadata_to_imported_images"]

    omero_module = ModuleType("omero")
    omero_sys_module = ModuleType("omero.sys")
    omero_sys_module.ParametersI = Parameters
    omero_module.sys = omero_sys_module
    monkeypatch.setitem(sys.modules, "omero", omero_module)
    monkeypatch.setitem(sys.modules, "omero.sys", omero_sys_module)

    imported = [Image(251, "labels_cells"), Image(252, "labels_cells")]
    sources = [
        Image(55, "Cell-Granules_1.tif"),
        Image(169, "Cell-Granules_2.tif"),
    ]
    conn = Connection(imported)
    roi_pairs = []

    message = add_metadata(
        conn,
        SimpleNamespace(),
        destination_id=10,
        order_uuids=["order-uuid"],
        wf_id="workflow-uuid",
        job_id="40943031",
        input_images=sources,
        roi_pairs=roi_pairs,
    )

    assert imported[0].getDescription() == (
        "source: Cell-Granules_1.tif (id: 55)")
    assert imported[1].getDescription() == (
        "source: Cell-Granules_2.tif (id: 169)")
    assert roi_pairs == [
        (251, 55, "labels_cells", "Cell-Granules_1.tif"),
        (252, 169, "labels_cells", "Cell-Granules_2.tif"),
    ]
    assert len(conn.update_service.saved) == 2
    assert message == "Added workflow metadata to 2/2 imported images"
