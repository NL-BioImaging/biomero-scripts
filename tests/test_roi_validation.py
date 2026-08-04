import ast
from pathlib import Path
from types import SimpleNamespace

import pytest


class Value:
    def __init__(self, value):
        self.val = value


class Script:
    def __init__(self, script_id, name="Labels2Rois"):
        self.id = Value(script_id)
        self.name = name

    def getName(self):
        return Value(self.name)


class Service:
    def __init__(self, scripts):
        self.scripts = scripts

    def getScripts(self):
        return self.scripts


class Connection:
    def __init__(self, scripts):
        self.service = Service(scripts)

    def getScriptService(self):
        return self.service


class Client:
    def __init__(self, values):
        self.values = values

    def getInput(self, name):
        return self.values.get(name)


def _load_validation_functions():
    path = (Path(__file__).parents[1] / "__workflows" /
            "SLURM_Run_Workflow.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    wanted = {
        "get_roi_script_capability",
        "validate_roi_output_request",
        "get_roi_target_image_ids",
    }
    functions = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name in wanted]
    module = ast.Module(body=functions, type_ignores=[])
    workflow = SimpleNamespace(
        OUTPUT_CREATE_ROIS="create",
        OUTPUT_NEW_DATASET="dataset",
        OUTPUT_NEW_SCREEN="screen",
        ROI_SHAPE="shape",
        ROI_LABEL_PATTERN="pattern",
    )
    namespace = {
        "BlitzGateway": object,
        "constants": SimpleNamespace(
            LABELS_TO_ROIS_SCRIPT="Labels2Rois",
            workflow=workflow,
            transfer=SimpleNamespace(
                DATA_TYPE_IMAGE="Image",
                DATA_TYPE_DATASET="Dataset",
                DATA_TYPE_PLATE="Plate",
                DATA_TYPE_SCREEN="Screen",
            ),
        ),
        "unwrap": lambda value: getattr(value, "val", value),
    }
    exec(compile(module, str(path), "exec"), namespace)
    return namespace, workflow


NS, WF = _load_validation_functions()
validate = NS["validate_roi_output_request"]


def test_missing_optional_script_disables_request_before_other_validation():
    selected = {WF.OUTPUT_CREATE_ROIS: True}

    pattern, warning = validate(Client({}), Connection([]), selected, [])

    assert pattern == ""
    assert selected[WF.OUTPUT_CREATE_ROIS] is False
    assert "not installed" in warning


def test_installed_script_requires_import_destination():
    selected = {WF.OUTPUT_CREATE_ROIS: True}

    with pytest.raises(ValueError, match="Dataset or Screen"):
        validate(Client({}), Connection([Script(7)]), selected, [])


def test_all_label_outputs_get_automatic_selector():
    selected = {
        WF.OUTPUT_CREATE_ROIS: True,
        WF.OUTPUT_NEW_DATASET: True,
    }
    descriptors = [{"outputs": [
        {"type": "image", "sub-type": ["label"]},
    ]}]

    pattern, warning = validate(
        Client({WF.ROI_SHAPE: "Polygon"}),
        Connection([Script(7)]), selected, descriptors)

    assert pattern == "*"
    assert warning is None


def test_mixed_outputs_require_user_pattern():
    selected = {
        WF.OUTPUT_CREATE_ROIS: True,
        WF.OUTPUT_NEW_DATASET: True,
    }
    descriptors = [{"outputs": [
        {"type": "image", "subtype": ["label"]},
        {"type": "image", "subtype": ["grayscale"]},
    ]}]

    with pytest.raises(ValueError, match="label image pattern"):
        validate(Client({WF.ROI_SHAPE: "Mask"}),
                 Connection([Script(7)]), selected, descriptors)


def test_plate_and_screen_inputs_expand_to_exact_image_ids():
    class Image:
        def __init__(self, image_id):
            self.image_id = image_id

        def getId(self):
            return self.image_id

    class Container:
        def __init__(self, children):
            self.children = children

        def listChildren(self):
            return iter(self.children)

    class WellSample:
        def __init__(self, image):
            self.image = image

        def getImage(self):
            return self.image

    images = [Image(101), Image(102)]
    plate = Container([Container([WellSample(images[0]), WellSample(images[1])])])
    screen = Container([plate])
    objects = {("Plate", 7): plate, ("Screen", 8): screen}

    class ObjectConnection:
        def getObject(self, object_type, object_id):
            return objects.get((object_type, object_id))

    expand = NS["get_roi_target_image_ids"]

    assert expand(ObjectConnection(), "Plate", [7]) == [101, 102]
    assert expand(ObjectConnection(), "Screen", [8]) == [101, 102]
