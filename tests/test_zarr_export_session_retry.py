"""Retry an OME-Zarr export whose CLI could not join a still-valid session.

``omero zarr -k <key>`` reports every join failure as "Bad session key"
(exit code 523, seen as 11 through a shell), hiding transient server errors and races in the CLI's local
session store. Each export gets a private session store; a join failure is
retried only while the script's own session is still alive.
"""
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_shallow_conversion_material import (
    ImageObject, TRANSFER_PATH, load_save_as_zarr)

pytestmark = pytest.mark.skipif(
    "def run_zarr_export(" not in TRANSFER_PATH.read_text(encoding="utf-8")
    and os.getenv("BIOMERO_TEST_ZARR_SESSION_RETRY") != "1",
    reason="Requires the OME-Zarr export session retry",
)

SESSION = "0123abcd-4567-89ef-0123-456789abcdef"
BAD_SESSION = (
    f"Bad session key. Cannot join {SESSION} on omeroserver.\n".encode())


class Connection:
    host = "omeroserver"

    def __init__(self, alive=True):
        self.alive = alive
        self.keepalive_calls = 0

    def keepAlive(self):
        self.keepalive_calls += 1
        return self.alive


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
        def __init__(self, command, **kwargs):
            returncode, stderr = outcomes[len(attempts)]
            session_dir = kwargs["env"]["OMERO_SESSIONDIR"]
            attempts.append(SimpleNamespace(
                command=command,
                session_dir=session_dir,
                session_dir_existed=Path(session_dir).is_dir(),
                stale_output=output.exists(),
            ))
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


def export(save, conn):
    return save(conn, SESSION, ImageObject(), "batch", "Image", "0.4")


def assert_private_session_dirs(attempts):
    dirs = [attempt.session_dir for attempt in attempts]
    assert len(set(dirs)) == len(dirs)
    assert all(attempt.session_dir_existed for attempt in attempts)
    assert not any(Path(path).exists() for path in dirs)
    home_store = str(Path.home() / "omero" / "sessions")
    assert all(path != home_store for path in dirs)


def test_success_first_try_uses_private_session_store(tmp_path, monkeypatch):
    save, attempts, sleeps = load(tmp_path, monkeypatch, [(0, b"")])
    conn = Connection()

    _source, artifact, _labels = export(save, conn)

    assert len(attempts) == 1
    assert sleeps == []
    assert conn.keepalive_calls == 0
    assert (tmp_path / "batch" / artifact / "complete").exists()
    assert_private_session_dirs(attempts)


@pytest.mark.parametrize("returncode", [11, 523])
def test_bad_session_key_with_live_session_retries(
        tmp_path, monkeypatch, caplog, returncode):
    save, attempts, sleeps = load(
        tmp_path, monkeypatch, [(returncode, BAD_SESSION), (0, b"")])
    conn = Connection()

    with caplog.at_level(logging.INFO):
        _source, artifact, _labels = export(save, conn)

    exported = tmp_path / "batch" / artifact
    assert len(attempts) == 2
    assert attempts[0].command == attempts[1].command
    assert f'-k "{SESSION}"' in attempts[1].command
    assert attempts[1].stale_output is False
    assert (exported / "complete").exists()
    assert not (exported / "partial").exists()
    assert sleeps == [5]
    assert conn.keepalive_calls == 1
    assert_private_session_dirs(attempts)
    assert "attempt 1/3" in caplog.text
    assert "Bad session key. Cannot join" in caplog.text
    assert SESSION not in caplog.text


def test_bad_session_key_with_dead_session_fails_without_retry(
        tmp_path, monkeypatch, caplog):
    save, attempts, sleeps = load(tmp_path, monkeypatch, [(523, BAD_SESSION)])
    conn = Connection(alive=False)

    with caplog.at_level(logging.INFO), \
            pytest.raises(ConnectionError, match="no longer alive") as error:
        export(save, conn)

    assert len(attempts) == 1
    assert sleeps == []
    assert conn.keepalive_calls == 1
    assert SESSION not in str(error.value)
    assert SESSION not in caplog.text
    assert_private_session_dirs(attempts)


def test_other_failure_is_not_retried(tmp_path, monkeypatch):
    save, attempts, sleeps = load(
        tmp_path, monkeypatch, [(1, b"Error instantiating pixel buffer")])
    conn = Connection()

    with pytest.raises(Exception, match="return code 1"):
        export(save, conn)

    assert len(attempts) == 1
    assert sleeps == []
    assert conn.keepalive_calls == 0
    assert_private_session_dirs(attempts)


@pytest.mark.parametrize("outcome", [(1, BAD_SESSION), (11, b"")])
def test_either_bad_session_signal_is_retried(tmp_path, monkeypatch, outcome):
    save, attempts, sleeps = load(tmp_path, monkeypatch, [outcome, (0, b"")])

    export(save, Connection())

    assert len(attempts) == 2
    assert sleeps == [5]


def test_retries_exhausted_fails_with_cli_error(
        tmp_path, monkeypatch, caplog):
    save, attempts, sleeps = load(
        tmp_path, monkeypatch, [(523, BAD_SESSION)] * 3)
    conn = Connection()

    with caplog.at_level(logging.INFO), \
            pytest.raises(Exception, match="return code 523") as error:
        export(save, conn)

    assert len(attempts) == 3
    assert sleeps == [5, 15]
    assert conn.keepalive_calls == 2
    assert [a.stale_output for a in attempts] == [False, False, False]
    assert_private_session_dirs(attempts)
    assert "attempt 2/3" in caplog.text
    assert SESSION not in caplog.text
    assert SESSION not in str(error.value)
