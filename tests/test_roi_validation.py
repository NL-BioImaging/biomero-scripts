import ast
import fnmatch
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


class Value:
    def __init__(self, value):
        self.val = value


class Script:
    def __init__(self, script_id, name="Labels2Rois.py"):
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
            LABELS_TO_ROIS_SCRIPT="Labels2Rois.py",
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


def test_mixed_outputs_use_automatic_best_effort_selector():
    selected = {
        WF.OUTPUT_CREATE_ROIS: True,
        WF.OUTPUT_NEW_DATASET: True,
    }
    descriptors = [{"outputs": [
        {"type": "image", "subtype": ["label"]},
        {"type": "image", "subtype": ["grayscale"]},
    ]}]

    pattern, warning = validate(
        Client({WF.ROI_SHAPE: "Mask"}),
        Connection([Script(7)]), selected, descriptors)

    assert pattern == ""
    assert warning is None


def _load_result_selector(script_name):
    path = Path(__file__).parents[1] / "_data" / script_name
    tree = ast.parse(path.read_text(encoding="utf-8"))
    wanted_functions = {"matches_roi_label_output", "select_roi_image_pairs"}
    nodes = [
        node for node in tree.body
        if ((isinstance(node, ast.FunctionDef)
             and node.name in wanted_functions)
            or (isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name)
                        and target.id == "ROI_LABEL_NAME_HINTS"
                        for target in node.targets)))
    ]
    namespace = {"fnmatch": fnmatch, "os": os}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"),
         namespace)
    return namespace["select_roi_image_pairs"]


@pytest.mark.parametrize("script_name", [
    "SLURM_Get_Results.py",
    "SLURM_Import_Results.py",
])
def test_auto_selector_uses_single_results_and_label_name_hints(script_name):
    select = _load_result_selector(script_name)
    candidates = [
        (11, 1, "cellA_result.tif", "cellA.tif"),
        (21, 2, "cellB_probability.tif", "cellB.tif"),
        (22, 2, "cellB_cp_masks.tif", "cellB.tif"),
        (31, 3, "cellC_first.tif", "cellC.tif"),
        (32, 3, "cellC_second.tif", "cellC.tif"),
    ]

    pairs, ambiguous = select(candidates)

    assert pairs == [(11, 1), (22, 2)]
    assert ambiguous == 1


@pytest.mark.parametrize("script_name", [
    "SLURM_Get_Results.py",
    "SLURM_Import_Results.py",
])
def test_advanced_glob_overrides_automatic_selection(script_name):
    select = _load_result_selector(script_name)
    candidates = [
        (21, 2, "cellB_probability.tif", "cellB.tif"),
        (22, 2, "cellB_cp_masks.tif", "cellB.tif"),
    ]

    pairs, ambiguous = select(candidates, "*_probability.tif")

    assert pairs == [(21, 2)]
    assert ambiguous == 0


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


def test_export_verification_is_not_tied_to_cellpose_config_key():
    source = (Path(__file__).parents[1] / "__workflows" /
              "SLURM_Run_Workflow.py").read_text(encoding="utf-8")

    assert "get_image_versions_and_data_files('cellpose')" not in source
    assert ("_, available_data_files = "
            "slurmClient.get_all_image_versions_and_data_files()") in source


def test_remote_conversion_is_not_tied_to_cellpose_config_key():
    source = (Path(__file__).parents[1] / "_data" /
              "SLURM_Remote_Conversion.py").read_text(encoding="utf-8")

    assert "get_image_versions_and_data_files(\n            'cellpose')" not in source
    assert ("_, _datafiles = "
            "slurmClient.get_all_image_versions_and_data_files()") in source


@pytest.mark.parametrize("script_name", [
    "SLURM_Get_Results.py",
    "SLURM_Import_Results.py",
])
def test_roi_postprocessing_forwards_label_image_cleanup(script_name):
    source = (Path(__file__).parents[1] / "_data" / script_name).read_text(
        encoding="utf-8")

    assert "delete_label_images=False" in source
    assert '"Delete_Label_Image": rbool(delete_label_images)' in source


@pytest.mark.parametrize("script_name", [
    "SLURM_Get_Results.py",
    "SLURM_Import_Results.py",
])
def test_roi_postprocessing_forwards_provenance_and_clear_options(script_name):
    source = (Path(__file__).parents[1] / "_data" / script_name).read_text(
        encoding="utf-8")

    assert 'roi_name_prefix=""' in source
    assert "clear_existing_rois=False" in source
    assert 'clear_roi_filter=""' in source
    assert '"ROI_Name_Prefix": rstring(roi_name_prefix)' in source
    assert '"Clear_Existing_ROIs": rbool(clear_existing_rois)' in source
    assert '"Clear_ROI_Filter": rstring(clear_roi_filter)' in source


def test_run_workflow_builds_filterable_roi_provenance_prefix():
    source = (Path(__file__).parents[1] / "__workflows" /
              "SLURM_Run_Workflow.py").read_text(encoding="utf-8")

    assert 'f"{workflow_name}__{wf_id}"' in source
    assert "constants.results.ROI_NAME_PREFIX" in source
    assert "constants.workflow.ROI_CLEAR_EXISTING" in source
    assert "constants.workflow.ROI_CLEAR_FILTER" in source
