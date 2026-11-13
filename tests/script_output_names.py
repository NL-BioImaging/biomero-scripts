"""The shared output names, as the scripts read them from biomero.constants.

Tests that run script functions in a bare namespace provide this as
``constants.results`` (or merge it into their own), so they do not need core
installed. The values are pinned by biomero's own tests.
"""
from types import SimpleNamespace

OUTPUT_NAMES = SimpleNamespace(
    MESSAGE_OUTPUT="Message",
    URL_OUTPUT="URL",
    FILE_ANNOTATION_OUTPUT="File_Annotation",
    CANONICAL_INPUTS_OUTPUT="Canonical_Inputs",
)
