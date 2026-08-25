import ast
import os
from pathlib import Path
from types import SimpleNamespace


SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPT_PATH = (SOURCE_ROOT / "__workflows" /
               "SLURM_Run_Workflow.py")


def load_log_upload_function():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    wanted = {"upload_job_log_to_omero"}
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "NSCREATED": "openmicroscopy.org/omero/client/mapAnnotation",
        "logger": SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
        ),
        "robject": lambda value: value,
        "unwrap": lambda value: getattr(value, "val", value),
        "wrap": lambda value: value,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]),
                 str(SCRIPT_PATH), "exec"), namespace)
    return namespace["upload_job_log_to_omero"]


class ServiceOptions:
    def __init__(self, group=-1):
        self.group = group
        self.changes = []

    def getOmeroGroup(self):
        return self.group

    def setOmeroGroup(self, group):
        self.group = group
        self.changes.append(group)


class Annotation:
    _obj = "annotation-object"

    def getFile(self):
        return SimpleNamespace(getId=lambda: 99)


class Connection:
    def __init__(self, upload_error=None):
        self.SERVICE_OPTS = ServiceOptions()
        self.upload_error = upload_error
        self.group_during_upload = None

    def createFileAnnfromLocalFile(self, *_args, **_kwargs):
        self.group_during_upload = self.SERVICE_OPTS.group
        if self.upload_error is not None:
            raise self.upload_error
        return Annotation()

    def getConfigService(self):
        return SimpleNamespace(
            getConfigValue=lambda _key: "https://omero.example"
        )


class Client:
    def __init__(self):
        self.outputs = {}

    def setOutput(self, name, value):
        self.outputs[name] = value


def slurm_client():
    return SimpleNamespace(
        get_logfile_from_slurm=lambda _job_id: (
            "log contents", "omero-40942344.log", None
        )
    )


def test_log_upload_uses_active_group_then_restores_cross_group_context():
    upload = load_log_upload_function()
    client = Client()
    conn = Connection()

    message = upload(
        client, conn, slurm_client(), 40942344, "workflow-id", 17)

    assert conn.group_during_upload == 17
    assert conn.SERVICE_OPTS.changes == [17, -1]
    assert conn.SERVICE_OPTS.group == -1
    assert client.outputs["Job_Log"] == "annotation-object"
    assert "Uploaded log" in message


def test_log_upload_restores_cross_group_context_when_creation_fails():
    upload = load_log_upload_function()
    conn = Connection(upload_error=RuntimeError("upload failed"))

    message = upload(
        Client(), conn, slurm_client(), 40942344, "workflow-id", 17)

    assert conn.group_during_upload == 17
    assert conn.SERVICE_OPTS.changes == [17, -1]
    assert conn.SERVICE_OPTS.group == -1
    assert "failed to upload log" in message
