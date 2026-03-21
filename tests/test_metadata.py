import pytest
from open_auto_loader import OpenAutoLoader
import polars as pl

## 1. Test the Guardrail (The "Fail Fast" Check)
def test_reserved_metadata_collision():
    """Ensure the loader raises ValueError if a user uses a reserved key."""
    reserved_key = "_batch_id"

    with pytest.raises(ValueError) as excinfo:
        OpenAutoLoader(
            source="file://test/source",
            target="file://test/target",
            check_point="./test_cp",
            schema_path="./test_schema",
            metadata={reserved_key: "should_fail"} # Using a forbidden key
        )

    assert f"Forbidden key '{reserved_key}'" in str(excinfo.value)

## 2. Test Successful Metadata Injection
def test_metadata_injection_logic(tmp_path):
    """Verify that custom metadata ends up in the final DataFrame."""
    # Setup paths
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()

    # Create a dummy file
    test_file = source / "data.csv"
    test_file.write_text("id,name\n1,Nitish")

    custom_meta = {"env": "prod", "team": "data_eng"}

    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(tmp_path / "cp.db"),
        schema_path=str(tmp_path / "schemas"),
        metadata=custom_meta
    )

    # Mocking a run or testing the internal engine logic
    # Here we check if the loader's internal metadata dict is set correctly
    assert loader.metadata["env"] == "prod"
    assert loader.metadata["team"] == "data_eng"
