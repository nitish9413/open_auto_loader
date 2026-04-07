import polars as pl
import pytest

from open_auto_loader.configs.schema import SchemaEvolutionMode
from open_auto_loader.exceptions import SchemaMismatchError
from open_auto_loader.main import OpenAutoLoader
from tests.test_schema_enforcement import setup_test_env


def test_evolution_mode_add_new_columns(tmp_path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    # Batch 1: Initial schema (id, name)
    pl.DataFrame({"id": [1], "name": ["Alice"]}).write_csv(source / "f1.csv")
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
    )
    loader.run(batch_id="b1")

    # Batch 2: New column (age)
    pl.DataFrame({"id": [2], "name": ["Bob"], "age": [30]}).write_csv(source / "f2.csv")
    loader.run(batch_id="b2")

    # Assertions
    df = pl.read_delta(str(target))
    assert "age" in df.columns
    assert df.filter(pl.col("id") == 1)["age"][0] is None  # Check null handling


def test_evolution_mode_fail_on_new_columns(tmp_path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    # Setup initial contract
    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        evolution_mode=SchemaEvolutionMode.FAIL_ON_NEW_COLUMNS,
    )
    loader.run(batch_id="b1")

    # Add file with extra column
    pl.DataFrame({"id": [2], "extra": ["fail"]}).write_csv(source / "f2.csv")

    with pytest.raises(SchemaMismatchError) as exc_info:
        loader.run(batch_id="b2")

    assert "extra" in exc_info.value.context["extra_columns"]


def test_evolution_mode_none_ignores_extra(tmp_path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        evolution_mode=SchemaEvolutionMode.NONE,
    )
    loader.run(batch_id="b1")

    # File has 'secret' column, but mode is 'none'
    pl.DataFrame({"id": [2], "secret": ["ignore_me"]}).write_csv(source / "f2.csv")
    loader.run(batch_id="b2")

    df = pl.read_delta(str(target))
    assert "secret" not in df.columns
    assert df.height == 2


def test_evolution_still_fails_on_missing_columns(tmp_path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    pl.DataFrame({"id": [1], "name": ["A"]}).write_csv(source / "f1.csv")
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        evolution_mode=SchemaEvolutionMode.ADD_NEW_COLUMNS,
    )
    loader.run(batch_id="b1")

    # File is missing the 'name' column
    pl.DataFrame({"id": [2]}).write_csv(source / "f2.csv")

    with pytest.raises(SchemaMismatchError) as exc_info:
        loader.run(batch_id="b2")

    assert "name" in exc_info.value.context["missing_columns"]


def test_evolution_mode_rescue(tmp_path):
    source, target, checkpoint, schema_path = setup_test_env(tmp_path)

    # Init with 'id'
    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_path),
        evolution_mode=SchemaEvolutionMode.RESCUE,
    )
    loader.run(batch_id="b1")

    # File with extra 'secret'
    pl.DataFrame({"id": [2], "secret": ["hush"]}).write_csv(source / "f2.csv")
    loader.run(batch_id="b2")

    df = pl.read_delta(str(target))
    assert "_rescued_data" in df.columns
    # Verify JSON content
    assert '"hush"' in df.filter(pl.col("id") == 2)["_rescued_data"][0]
