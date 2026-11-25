"""Retry an OME-Zarr export after a transient "Bad session key" CLI failure.

``omero zarr -k <key>`` can fail to join a still-valid session. The export
then runs the same command again after a delay, three attempts in total.
"""
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
    """Load save_as_zarr with a CLI that plays the given (code, stderr) list.

    A failed attempt leaves partial output behind, like an interrupted CLI.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "batch").mkdir()
    save, *_rest = load_save_as_zarr(tmp_path, shallow_reference=None)
    attempts = []
    sleeps = []
    output = tmp_path / "batch" / "42.ome.zarr"

    class Process:
        def __init__(self, command, **_kwargs):
            returncode, stderr = outcomes[len(attempts)]
            attempts.append(SimpleNamespace(
                command=command, stale_output=output.exists()))
            output.mkdir()
            (output / ("complete" if returncode == 0 else "partial")).touch()
            self.returncode = returncode
            self.stderr = stderr

        def communicate(self):
            return b"exported", self.stderr

    save.__globals__["subprocess"] = SimpleNamespace(
        PIPE=object(), Popen=Process)
    save.__globals__["sleep"] = sleeps.append
    return save, attempts, sleeps


def export(save):
    return save(SimpleNamespace(host="omeroserver"), "session",
                ImageObject(), "batch", "Image", "0.4")


def test_success_first_try(tmp_path, monkeypatch):
    save, attempts, sleeps = load(tmp_path, monkeypatch, [(0, b"")])

    _source, artifact, _labels = export(save)

    assert len(attempts) == 1
    assert sleeps == []
    assert (tmp_path / "batch" / artifact / "complete").exists()


def test_bad_session_key_then_success_retries(tmp_path, monkeypatch, caplog):
    save, attempts, sleeps = load(
        tmp_path, monkeypatch, [(11, BAD_SESSION), (0, b"")])

    with caplog.at_level(logging.WARNING):
        _source, artifact, _labels = export(save)

    exported = tmp_path / "batch" / artifact
    assert len(attempts) == 2
    assert attempts[0].command == attempts[1].command
    assert attempts[1].stale_output is False
    assert (exported / "complete").exists()
    assert not (exported / "partial").exists()
    assert sleeps == [10]
    assert "attempt 1/3" in caplog.text


def test_other_failure_is_not_retried(tmp_path, monkeypatch):
    save, attempts, sleeps = load(
        tmp_path, monkeypatch, [(1, b"Error instantiating pixel buffer")])

    with pytest.raises(Exception, match="return code 1"):
        export(save)

    assert len(attempts) == 1
    assert sleeps == []


def test_retries_exhausted_uses_existing_error(tmp_path, monkeypatch, caplog):
    save, attempts, sleeps = load(
        tmp_path, monkeypatch, [(11, BAD_SESSION)] * 3)

    with caplog.at_level(logging.WARNING), \
            pytest.raises(Exception, match="return code 11"):
        export(save)

    assert len(attempts) == 3
    assert sleeps == [10, 30]
    assert [a.stale_output for a in attempts] == [False, False, False]
    assert "attempt 2/3" in caplog.text
