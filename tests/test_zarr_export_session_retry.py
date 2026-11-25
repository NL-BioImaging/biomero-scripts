"""Retry an OME-Zarr export after a transient "Bad session key" CLI failure.

``omero zarr -k <key>`` can fail to join a still-valid session. The export
then runs the same command again after each ``ZARR_EXPORT_RETRY_DELAYS`` wait.
"""
import ast
import logging
import os
from types import SimpleNamespace

import pytest

from test_shallow_conversion_material import (
    ImageObject, TRANSFER_PATH, load_save_as_zarr)

pytestmark = pytest.mark.skipif(
    'b"Bad session key"' not in TRANSFER_PATH.read_text(encoding="utf-8")
    and os.getenv("BIOMERO_TEST_ZARR_SESSION_RETRY") != "1",
    reason="Requires the OME-Zarr export Bad session key retry",
)

BAD_SESSION = b"Bad session key. Cannot join session on omeroserver.\n"


def load(tmp_path, monkeypatch, outcomes):
    """Load save_as_zarr with a CLI that plays the given (code, stderr) list."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    save, *_rest = load_save_as_zarr(tmp_path, shallow_reference=None)
    commands = []
    sleeps = []

    class Process:
        def __init__(self, command, **_kwargs):
            self.returncode, self.stderr = outcomes[len(commands)]
            commands.append(command)
            if self.returncode == 0:
                (tmp_path / "batch" / "42.ome.zarr").mkdir()

        def communicate(self):
            return b"exported", self.stderr

    save.__globals__["subprocess"] = SimpleNamespace(
        PIPE=object(), Popen=Process)
    save.__globals__["sleep"] = sleeps.append
    save.__globals__["build_zarr_export_error"] = (
        lambda _object, _data_type, stderr: stderr.decode())
    return save, commands, sleeps


def export(save):
    return save(SimpleNamespace(host="omeroserver"), "session",
                ImageObject(), "batch", "Image", "0.4")


def test_script_imports_sleep():
    """The tests inject sleep, so check the script provides it itself."""
    tree = ast.parse(TRANSFER_PATH.read_text(encoding="utf-8"))
    assert any(
        isinstance(node, ast.ImportFrom) and node.module == "time"
        and "sleep" in {alias.name for alias in node.names}
        for node in tree.body)


def test_success_first_try(tmp_path, monkeypatch):
    save, commands, sleeps = load(tmp_path, monkeypatch, [(0, b"")])

    _source, artifact, _labels = export(save)

    assert len(commands) == 1
    assert sleeps == []
    assert (tmp_path / "batch" / artifact).is_dir()


def test_bad_session_key_then_success_retries(tmp_path, monkeypatch, caplog):
    save, commands, sleeps = load(
        tmp_path, monkeypatch, [(11, BAD_SESSION), (0, b"")])

    with caplog.at_level(logging.WARNING):
        _source, artifact, _labels = export(save)

    assert len(commands) == 2
    assert commands[0] == commands[1]
    assert sleeps == [10]
    assert (tmp_path / "batch" / artifact).is_dir()
    assert "attempt 1/3" in caplog.text


def test_other_failure_is_not_retried(tmp_path, monkeypatch):
    save, commands, sleeps = load(
        tmp_path, monkeypatch, [(1, b"Error instantiating pixel buffer")])

    with pytest.raises(Exception, match="return code 1"):
        export(save)

    assert len(commands) == 1
    assert sleeps == []


def test_retries_exhausted_uses_existing_error(tmp_path, monkeypatch, caplog):
    last = b"Bad session key. Third attempt failed too.\n"
    save, commands, sleeps = load(
        tmp_path, monkeypatch,
        [(11, BAD_SESSION), (11, BAD_SESSION), (11, last)])

    with caplog.at_level(logging.WARNING), \
            pytest.raises(Exception, match="return code 11") as error:
        export(save)

    assert len(commands) == 3
    assert sleeps == [10, 30]
    assert "Third attempt failed too." in str(error.value)
    assert "attempt 2/3" in caplog.text
