"""The shared output names, as the scripts read them from biomero.constants.

Tests that run script functions in a bare namespace provide this as
``constants.results`` (or merge it into their own). The values come from
core itself, so they cannot drift from it.
"""
from types import SimpleNamespace

from biomero.constants import results

OUTPUT_NAMES = SimpleNamespace(
    MESSAGE_OUTPUT=results.MESSAGE_OUTPUT,
    URL_OUTPUT=results.URL_OUTPUT,
    FILE_ANNOTATION_OUTPUT=results.FILE_ANNOTATION_OUTPUT,
    CANONICAL_INPUTS_OUTPUT=results.CANONICAL_INPUTS_OUTPUT,
    RESULTS_PATH_OUTPUT=results.RESULTS_PATH_OUTPUT,
)
