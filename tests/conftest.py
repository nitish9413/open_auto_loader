import pytest


@pytest.fixture
def test_env(tmp_path):
    """
    Sets up a standardized environment for every test.
    Returns a dictionary of paths.
    """
    env = {
        "source": tmp_path / "source",
        "target": tmp_path / "target",
        "checkpoint": tmp_path / "checkpoint",
        "schema": tmp_path / "schema_root",
    }

    for path in env.values():
        path.mkdir()

    yield env

    # Optional: Cleanup after test is done
    # shutil.rmtree(tmp_path)
