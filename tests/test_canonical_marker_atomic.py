"""Regression coverage for canonical marker updates on CIFS-like mounts."""

import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT = SOURCE_ROOT / "admin" / "BIOMERO_Migrate_Shallow_Storage.py"
MARKER_NAME = ".biomero-canonical.json"


def _marker_helpers():
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    names = {"_write_json", "_canonical_marker_path", "_write_canonical_marker"}
    nodes = [node for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = {
        "Path": Path,
        "json": json,
        "os": os,
        "uuid4": uuid4,
        "CANONICAL_MARKER_NAME": MARKER_NAME,
        "external_canonical_marker_path": lambda path: path.parent / "external-marker",
        "write_indexed_canonical_marker": lambda *args: pytest.fail(
            "Existing marker must be updated, not created again"
        ),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT), "exec"),
         namespace)
    assert names.issubset(namespace)
    return namespace


def test_existing_canonical_marker_is_replaced_without_direct_overwrite(tmp_path):
    helpers = _marker_helpers()
    store = tmp_path / "image.zarr"
    store.mkdir()
    marker = store / MARKER_NAME
    marker.write_text(json.dumps({
        "state": "committed", "markerSchema": 1,
        "source": {"relativePath": "old"},
    }), encoding="utf-8")
    source = SimpleNamespace(to_dict=lambda: {"relativePath": "new"})
    helpers["load_canonical_marker"] = lambda path: (
        source if json.loads((path / MARKER_NAME).read_text(
            encoding="utf-8"
        ))["source"] == source.to_dict() else None
    )
    original_write_text = Path.write_text

    def reject_direct_overwrite(path, *args, **kwargs):
        if path == marker:
            raise PermissionError(13, "Direct overwrite denied", str(path))
        return original_write_text(path, *args, **kwargs)

    with patch.object(Path, "write_text", reject_direct_overwrite):
        helpers["_write_canonical_marker"](store, source)

    assert json.loads(marker.read_text(encoding="utf-8"))["source"] == (
        source.to_dict()
    )
    assert list(store.iterdir()) == [marker]


def test_failed_replacement_preserves_marker_and_cleans_temp_file(tmp_path):
    helpers = _marker_helpers()
    marker = tmp_path / MARKER_NAME
    original = '{"state": "committed", "source": "original"}'
    marker.write_text(original, encoding="utf-8")

    with patch.object(os, "replace", side_effect=PermissionError(13, "denied")):
        with pytest.raises(PermissionError):
            helpers["_write_json"](marker, {"source": "updated"})

    assert marker.read_text(encoding="utf-8") == original
    assert list(tmp_path.iterdir()) == [marker]
