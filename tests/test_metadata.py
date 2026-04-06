import pytest

from open_auto_loader.main import OpenAutoLoader


def test_reserved_metadata_collision():
    """Verify ValueError is raised when using reserved keys."""
    with pytest.raises(ValueError) as excinfo:
        OpenAutoLoader(
            source="test/",
            target="test/",
            check_point="cp",
            schema_path="sc",
            metadata={"_batch_id": "malicious_value"},
        )
    assert "Forbidden key '_batch_id'" in str(excinfo.value)


def test_valid_metadata_assignment():
    """Verify custom metadata is stored correctly."""
    meta = {"env": "prod", "owner": "nitish"}
    loader = OpenAutoLoader(
        source="test/",
        target="test/",
        check_point="cp",
        schema_path="sc",
        metadata=meta,
    )
    assert loader.metadata["env"] == "prod"
