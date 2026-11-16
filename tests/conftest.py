import pytest
from biomero import constants


@pytest.fixture(autouse=True)
def default_result_folder(monkeypatch):
    """Every test starts from the default result folder, whatever the host."""
    monkeypatch.delenv(constants.slurm_env.ANALYZED_DATA_FOLDER, raising=False)
