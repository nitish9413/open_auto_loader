from pathlib import Path

import polars as pl
import pytest

from open_auto_loader import OpenAutoLoader
from open_auto_loader.exceptions import SchemaMismatchError


def setup_test_env(base_path: Path):
    """Helper to create directory structure."""
    source_dir = base_path / "raw"
    target_dir = base_path / "delta"
    checkpoint = base_path / "check.db"
    schema_path = base_path / "schema.json"
    source_dir.mkdir()
    return source_dir, target_dir, checkpoint, schema_path


def test_schema_mismatch_failure(tmp_path: Path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    # 1. First Run: Establish Schema (2 columns: id, name)
    initial_data = [{"id": 1, "name": "Alice"}]
    pl.DataFrame(initial_data).write_csv(source / "file1.csv")

    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        format_type="csv",
    )
    loader.run(batch_id="batch_01")

    # 2. Second Run: Introduce "Bad" Data (Missing 'name' column, added 'age')
    bad_data = [{"id": 2, "age": 25}]
    pl.DataFrame(bad_data).write_csv(source / "file2.csv")

    # This should raise a ValueError or SchemaError depending on your implementation
    with pytest.raises((ValueError, Exception)):
        loader.run(batch_id="batch_02")


def test_data_type_validation(tmp_path: Path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    # Establish schema with Integer 'id'
    pl.DataFrame({"id": [1]}).write_csv(source / "good.csv")

    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        format_type="csv",
    )
    loader.run(batch_id="b1")

    # Try to ingest a string in the 'id' column
    pl.DataFrame({"id": ["not_an_int"]}).write_csv(source / "bad_type.csv")

    with pytest.raises(Exception):  # noqa: B017, PT011
        loader.run(batch_id="b2")


def test_s3_path_parsing():
    from open_auto_loader.configs.storage import AWSConfig

    config = AWSConfig(
        aws_access_key_id="mock", aws_secret_access_key="mock", region_name="us-east-1"
    )

    # Verify that storage_options are correctly passed to fsspec
    options = config.get_options()
    assert options["key"] == "mock"
    assert options["secret"] == "mock"


def test_schema_enforcement(tmp_path: Path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    # Batch 1: 2 columns (id, name) -> Sets the schema
    pl.DataFrame({"id": [1], "name": ["A"]}).write_csv(source / "f1.csv")
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        format_type="csv",
    )
    loader.run(batch_id="b1")

    # Batch 2: 3 columns (id, name, age) -> Should Fail
    pl.DataFrame({"id": [2], "name": ["B"], "age": [25]}).write_csv(source / "f2.csv")

    with pytest.raises(Exception):  # Replace with   # noqa: B017, PT011
        loader.run(batch_id="b2")


def test_schema_mismatch_metadata(tmp_path: Path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    # 1. Setup base schema
    pl.DataFrame({"id": [1]}).write_csv(source / "base.csv")
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
    )
    loader.run(batch_id="b1")

    # 2. Add file with extra column
    pl.DataFrame({"id": [2], "unexpected": ["error"]}).write_csv(source / "drift.csv")

    with pytest.raises(SchemaMismatchError) as exc_info:
        loader.run(batch_id="b2")

    # Verify the structured context contains the right details
    assert "unexpected" in exc_info.value.context["extra_columns"]
    assert "drift.csv" in exc_info.value.context["file_path"]
