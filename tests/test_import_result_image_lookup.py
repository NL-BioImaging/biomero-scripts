import ast
from pathlib import Path


SCRIPT_PATH = Path(__file__).parents[1] / "_data" / "SLURM_Import_Results.py"


def load_function():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    node = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "get_images_by_ids"
    )
    namespace = {}
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), str(SCRIPT_PATH), "exec"),
        namespace,
    )
    return namespace["get_images_by_ids"]


class Connection:
    def __init__(self):
        self.calls = []

    def getObjects(self, object_type, *, ids):
        self.calls.append((object_type, ids))
        return [None, f"image-{ids[0]}"]


def test_empty_image_ids_do_not_query_omero():
    connection = Connection()

    assert load_function()(connection, []) == []
    assert connection.calls == []


def test_nonempty_image_ids_are_normalized_and_loaded():
    connection = Connection()

    assert load_function()(connection, ["7"]) == ["image-7"]
    assert connection.calls == [("Image", [7])]
