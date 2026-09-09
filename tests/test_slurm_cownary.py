import ast
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = SOURCE_ROOT / "admin" / "SLURM_Cownary.py"
pytestmark = pytest.mark.skipif(
    not SCRIPT_PATH.exists(),
    reason="the Slurm cownary admin script is not available in this revision",
)


def load_cownary_helpers():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted_functions = {
        "_cownary_environment",
        "_cownary_job_parameters",
        "_parse_job_id",
        "run_cownary",
    }
    wanted_constants = {
        "_LOLCOW_IMAGE",
        "_COWNARY_SBATCH_COMMAND",
        "_COWNARY_MARKERS",
        "_COWNARY_PROTECTED_SBATCH_FLAGS",
    }
    nodes = [
        node for node in tree.body
        if (
            isinstance(node, ast.FunctionDef)
            and node.name in wanted_functions
        ) or (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id in wanted_constants
                for target in node.targets
            )
        )
    ]
    namespace = {
        "logger": SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
        ),
        "re": __import__("re"),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["run_cownary"]


class FakeSlurmClient:
    slurm_data_path = "/configured/scratch/biomero data"
    slurm_data_bind_path = "/configured/scratch/biomero data"
    slurm_default_partition = "cpu-short"
    slurm_global_job_params = [
        " --account=imaging",
        " --qos=normal",
    ]
    apptainer_tmpdir = "/configured/apptainer tmp"
    apptainer_cachedir = "/configured/apptainer cache"

    def __init__(self, submit_ok=True, log_ok=True):
        self.submit_ok = submit_ok
        self.log_ok = log_ok
        self.cd_paths = []
        self.commands = []

    @contextmanager
    def cd(self, path):
        self.cd_paths.append(path)
        yield

    def run(self, command, **kwargs):
        self.commands.append((command, kwargs))
        if command.startswith("sbatch "):
            return SimpleNamespace(
                ok=self.submit_ok,
                stdout="731;test-cluster\n",
                stderr="job failed" if not self.submit_ok else "",
                exited=7 if not self.submit_ok else 0,
            )
        if command == 'cat "biomero-cownary-731.log"':
            return SimpleNamespace(
                ok=self.log_ok,
                stdout=(
                    "c2\n"
                    " ---------\n"
                    "< healthy >\n"
                    " ---------\n"
                    "        \\   ^__^\n"
                    "         \\  (oo)\\_______\n"
                    "            (__)\\       )\\/\\\n"
                    "                ||----w |\n"
                    "                ||     ||\n"
                ) if self.log_ok else "",
                stderr="cannot read log" if not self.log_ok else "",
                exited=1 if not self.log_ok else 0,
            )
        if command == 'rm -f "biomero-cownary-731.log"':
            return SimpleNamespace(ok=True, stdout="", stderr="", exited=0)
        raise AssertionError(f"Unexpected remote command: {command}")


class FakeWebClient:
    def __init__(self):
        self.outputs = {}
        self.closed = False

    def setOutput(self, name, value):
        self.outputs[name] = value

    def closeSession(self):
        self.closed = True


def test_slurm_cownary_uses_configured_data_path_and_returns_cow():
    run_cownary = load_cownary_helpers()
    slurm_client = FakeSlurmClient()

    success, message = run_cownary(slurm_client)

    assert success is True
    assert slurm_client.cd_paths == [slurm_client.slurm_data_path]
    submit_command, submit_kwargs = slurm_client.commands[0]
    assert "sbatch --parsable --wait" in submit_command
    assert "--partition=cpu-short" in submit_command
    assert "--account=imaging" in submit_command
    assert "--qos=normal" in submit_command
    assert "--nodes=1 --ntasks=1" in submit_command
    assert "singularity run docker://godlovedc/lolcow" in submit_command
    assert "/data" not in submit_command
    assert submit_kwargs == {
        "env": {
            "APPTAINER_BINDPATH": '"/configured/scratch/biomero data"',
            "APPTAINER_CACHEDIR": '"/configured/apptainer cache"',
            "APPTAINER_TMPDIR": '"/configured/apptainer tmp"',
            "SINGULARITY_CACHEDIR": '"/configured/apptainer cache"',
            "SINGULARITY_TMPDIR": '"/configured/apptainer tmp"',
        },
        "hide": True,
        "warn": True,
    }
    assert slurm_client.commands[1][0] == 'cat "biomero-cownary-731.log"'
    assert slurm_client.commands[2][0] == 'rm -f "biomero-cownary-731.log"'
    assert "COWNARY PASSED" in message
    assert "Slurm job: 731" in message
    assert "Slurm node: c2" in message
    assert "^__^" in message
    assert "(oo)\\_______" in message


def test_slurm_cownary_global_settings_cannot_override_fixed_job_scope():
    run_cownary = load_cownary_helpers()
    slurm_client = FakeSlurmClient()
    slurm_client.slurm_global_job_params = [
        " --account=imaging",
        " --job-name=not-the-cownary",
        " --nodes=99",
        " --ntasks=99",
        " --output=somewhere-else.log",
        " --parsable=no",
        " --wait=no",
        " --wrap=echo-bad",
    ]

    success, _message = run_cownary(slurm_client)

    assert success is True
    submit_command = slurm_client.commands[0][0]
    assert "--account=imaging" in submit_command
    assert "not-the-cownary" not in submit_command
    assert "--nodes=99" not in submit_command
    assert "--ntasks=99" not in submit_command
    assert "somewhere-else.log" not in submit_command
    assert "--parsable=no" not in submit_command
    assert "--wait=no" not in submit_command
    assert "echo-bad" not in submit_command
    assert submit_command.count("--nodes=") == 1
    assert submit_command.count("--ntasks=") == 1
    assert submit_command.count("--output=") == 1


def test_slurm_cownary_global_partition_wins_over_default_fallback():
    run_cownary = load_cownary_helpers()
    slurm_client = FakeSlurmClient()
    slurm_client.slurm_global_job_params = [" --partition=canary"]

    success, _message = run_cownary(slurm_client)

    assert success is True
    submit_command = slurm_client.commands[0][0]
    assert "--partition=canary" in submit_command
    assert "--partition=cpu-short" not in submit_command
    assert submit_command.count("--partition=") == 1


def test_slurm_cownary_reports_job_failure_but_still_returns_log():
    run_cownary = load_cownary_helpers()
    slurm_client = FakeSlurmClient(submit_ok=False)

    success, message = run_cownary(slurm_client)

    assert success is False
    assert "COWNARY FAILED" in message
    assert "job failed" in message
    assert "^__^" in message
    assert slurm_client.commands[-1][0] == 'rm -f "biomero-cownary-731.log"'


def test_cownary_admin_script_has_no_user_supplied_command_or_path_inputs():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    client_call = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "client"
    )

    assert len(client_call.args) == 2
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    assert ".isAdmin()" in source
    assert "slurm_client.slurm_data_path" in source
    assert "getInputs" not in source


def test_cownary_run_script_returns_the_cow_in_omero_web_message():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted_functions = {
        "_cownary_environment",
        "_cownary_job_parameters",
        "_parse_job_id",
        "run_cownary",
        "runScript",
    }
    wanted_constants = {
        "VERSION",
        "_LOLCOW_IMAGE",
        "_COWNARY_SBATCH_COMMAND",
        "_COWNARY_MARKERS",
        "_COWNARY_PROTECTED_SBATCH_FLAGS",
    }
    nodes = [
        node for node in tree.body
        if (
            isinstance(node, ast.FunctionDef)
            and node.name in wanted_functions
        ) or (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id in wanted_constants
                for target in node.targets
            )
        )
    ]
    web_client = FakeWebClient()
    slurm_client = FakeSlurmClient()

    class SlurmClientFactory:
        @staticmethod
        @contextmanager
        def from_config():
            yield slurm_client

    user = SimpleNamespace(isAdmin=lambda: True, getName=lambda: "root")
    connection = SimpleNamespace(
        getUser=lambda: user,
        getUserId=lambda: 1,
    )
    namespace = {
        "BlitzGateway": lambda client_obj: connection,
        "SlurmClient": SlurmClientFactory,
        "logger": SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
            exception=lambda *_args, **_kwargs: None,
        ),
        "omero": SimpleNamespace(
            constants=SimpleNamespace(
                namespaces=SimpleNamespace(NSDYNAMIC="dynamic")
            )
        ),
        "omscripts": SimpleNamespace(client=lambda *_args, **_kwargs: web_client),
        "re": __import__("re"),
        "rstring": lambda value: value,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)

    namespace["runScript"]()

    assert web_client.closed is True
    assert "COWNARY PASSED" in web_client.outputs["Message"]
    assert "Slurm node: c2" in web_client.outputs["Message"]
    assert "^__^" in web_client.outputs["Message"]
    assert "(oo)\\_______" in web_client.outputs["Message"]
