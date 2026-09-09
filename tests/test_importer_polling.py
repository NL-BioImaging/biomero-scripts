import ast
import os
from pathlib import Path


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "_data" / "SLURM_Import_Results.py"


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def time(self):
        return self.now

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeLogger:
    def info(self, *_args, **_kwargs):
        pass

    def debug(self, *_args, **_kwargs):
        pass

    def warning(self, *_args, **_kwargs):
        pass

    def error(self, *_args, **_kwargs):
        pass


class FakeColumn:
    def __eq__(self, _other):
        return self

    def desc(self):
        return self


class FakeIngestionTracking:
    uuid = FakeColumn()
    timestamp = FakeColumn()


class FakeEntry:
    def __init__(self, stage):
        self.stage = stage
        self.description = None


class FakeQuery:
    def __init__(self, clock, stage_at):
        self.clock = clock
        self.stage_at = stage_at

    def filter(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def first(self):
        stage = self.stage_at(self.clock.now)
        return None if stage is None else FakeEntry(stage)


class FakeSession:
    def __init__(self, clock, stage_at):
        self.clock = clock
        self.stage_at = stage_at

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def query(self, *_args):
        return FakeQuery(self.clock, self.stage_at)


class FakeTracker:
    def __init__(self, clock, stage_at):
        self.Session = lambda: FakeSession(clock, stage_at)


class FakeConnection:
    def __init__(self):
        self.keepalives = 0

    def keepAlive(self):
        self.keepalives += 1


class RemoveLocalTimeImport(ast.NodeTransformer):
    """Let both the old and new implementations use the deterministic clock."""

    def visit_Import(self, node):
        aliases = [alias for alias in node.names if alias.name != "time"]
        if not aliases:
            return None
        node.names = aliases
        return node


def load_poll(clock, stage_at):
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "poll_import_status"
    )
    function = RemoveLocalTimeImport().visit(function)
    ast.fix_missing_locations(function)
    namespace = {
        "BlitzGateway": object,
        "Tuple": tuple,
        "IMPORT_POLL_TIMEOUT_SECONDS": 86400,
        "IMPORT_POLL_INTERVAL_SECONDS": 5,
        "IMPORT_POLL_BACKOFF_AFTER_SECONDS": 60,
        "IMPORT_POLL_MAX_INTERVAL_SECONDS": 60,
        "IMPORTER_ENABLED": True,
        "IMPORTER_AVAILABLE": True,
        "IngestionTracking": FakeIngestionTracking,
        "STAGE_IMPORTED": "IMPORTED",
        "STAGE_INGEST_FAILED": "FAILED",
        "get_ingest_tracker": lambda: FakeTracker(clock, stage_at),
        "logger": FakeLogger(),
        "time": clock,
    }
    exec(
        compile(ast.Module(body=[function], type_ignores=[]),
                str(SCRIPT_PATH), "exec"),
        namespace,
    )
    return namespace["poll_import_status"]


def test_importer_polling_defaults_to_one_day_and_workflow_uses_it():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    timeout_assignment = next((
        node for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name)
            and target.id == "IMPORT_POLL_TIMEOUT_SECONDS"
            for target in node.targets
        )
    ), None)
    assert timeout_assignment is not None
    timeout = eval(compile(
        ast.Expression(timeout_assignment.value), str(SCRIPT_PATH), "eval"
    ))
    process = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "process_importer_workflow"
    )
    wait_call = next(
        node for node in ast.walk(process)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "wait_for_import_completion"
    )
    keywords = {keyword.arg: keyword.value for keyword in wait_call.keywords}

    assert timeout == 24 * 60 * 60
    assert isinstance(keywords["timeout"], ast.Name)
    assert keywords["timeout"].id == "IMPORT_POLL_TIMEOUT_SECONDS"
    assert isinstance(keywords["poll_interval"], ast.Name)
    assert keywords["poll_interval"].id == "IMPORT_POLL_INTERVAL_SECONDS"


def test_default_polling_can_observe_completion_after_one_hour():
    clock = FakeClock()
    poll = load_poll(
        clock,
        lambda now: "IMPORTED" if now >= 3700 else "STARTED",
    )

    success, _message = poll("long-import")

    assert success
    assert 3700 <= clock.now <= 3760


def test_fast_completion_is_observed_without_backoff_delay():
    clock = FakeClock()
    poll = load_poll(
        clock,
        lambda now: "IMPORTED" if now >= 10 else "STARTED",
    )
    connection = FakeConnection()

    success, message = poll("fast-import", conn=connection)

    assert success
    assert "10.0 seconds" in message
    assert clock.sleeps == [5.0, 5.0]
    assert connection.keepalives == 2


def test_unchanged_stage_backs_off_to_one_minute():
    clock = FakeClock()
    poll = load_poll(clock, lambda _now: "STARTED")

    success, message = poll("unchanged-import", timeout=200)

    assert not success
    assert "200.0 seconds" in message
    assert clock.sleeps == (
        ([5.0] * 12) + [10.0, 20.0, 40.0, 60.0, 10.0]
    )


def test_importer_stage_change_restores_fast_polling():
    clock = FakeClock()

    def stage_at(now):
        if now >= 95:
            return "IMPORTED"
        if now >= 90:
            return "UPLOADING"
        return "STARTED"

    poll = load_poll(clock, stage_at)

    success, message = poll("advancing-import", timeout=200)

    assert success
    assert "95.0 seconds" in message
    assert clock.sleeps == ([5.0] * 12) + [10.0, 20.0, 5.0]
