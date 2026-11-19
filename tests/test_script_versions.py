"""Every script's VERSION is the installed biomero release (lockstep rule).

The scripts' ``VERSION`` is locked to the core release they require
(biomero-scripts-dev, "Keep releases locked to the BIOMERO library"). Gated
like the output-name contract: it applies to the release-preparation source
(biomero-scripts#101), not to sources before it (follow-up: #109).
"""
import ast
from importlib.metadata import version

import pytest

from test_shared_output_names import SCRIPTS, relative, requires_core_constants


def declared_version(path):
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "VERSION"
                and isinstance(node.value, ast.Constant)):
            return node.value.value
    return None


@requires_core_constants
@pytest.mark.parametrize("path", SCRIPTS, ids=relative)
def test_script_version_is_the_installed_core_release(path):
    declared = declared_version(path)
    if declared is None:
        pytest.skip("the script declares no VERSION")
    assert declared == version("biomero")
