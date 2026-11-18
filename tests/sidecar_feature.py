"""Tells whether the source under test keeps canonical sidecars in BIOMERO folders.

The sidecar tests are capability-gated: sources before that feature (master,
and the production PRs before it) have the same functions without the new
behaviour. The anchor is that the migration script imports the importer's
``remove_external_canonical_markers``: it is part of the feature, and a
regression in a handler or helper call that the tests are meant to catch cannot
remove it, so such a regression fails the tests instead of skipping them.
"""
import ast
from pathlib import Path


def sidecars_live_in_biomero_folders(source_root: Path) -> bool:
    """Return whether the source root has the BIOMERO-folder sidecar feature.

    Args:
        source_root (Path): Root of the biomero-scripts checkout under test.

    Returns:
        bool: True when the migration script imports
        ``remove_external_canonical_markers``.
    """
    script = source_root / "admin" / "BIOMERO_Migrate_Shallow_Storage.py"
    tree = ast.parse(script.read_text(encoding="utf-8"))
    return any(
        isinstance(node, ast.ImportFrom)
        and any(alias.name == "remove_external_canonical_markers"
                for alias in node.names)
        for node in ast.walk(tree))
