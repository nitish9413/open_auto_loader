import polars as pl
import pytest

from open_auto_loader import OpenAutoLoader


def test_full_workflow(tmp_path):
    # 1. Setup paths using only tmp_path
    source = tmp_path / "source"
    target = tmp_path / "target"
    checkpoint = tmp_path / "checkpoint"
    schema = tmp_path / "schema"

    source.mkdir()

    # 2. Create a dummy CSV
    df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    file1 = source / "data1.csv"
    df.write_csv(file1)

    # 3. Initialize Loader
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema),
    )

    # 4. Run Batch 1
    loader.run(batch_id="batch_01")

    # FIX: Use the 'target' variable defined above, NOT test_env
    assert target.exists()

    # 5. ASSERTIONS
    result_df = pl.read_delta(str(target))
    assert result_df.height == 2
    assert "_batch_id" in result_df.columns

    # Check if schema was locked
    assert (schema / "schema" / "schema_contract.json").exists()

    # 6. Run Batch 2 (Duplicate check)
    loader.run(batch_id="batch_02")
    # Height should still be 2 because no new files were added
    assert pl.read_delta(str(target)).height == 2


def test_schema_mismatch_fails(tmp_path):
    # 1. Setup paths
    source = tmp_path / "source"
    target = tmp_path / "target"
    checkpoint = tmp_path / "cp"
    schema_dir = tmp_path / "schema"

    source.mkdir()

    # 2. Bootstrap with a 'good' file
    good_df = pl.DataFrame({"id": [1], "val": [10.5]})
    good_df.write_csv(source / "good.csv")

    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema_dir),
    )
    loader.run(batch_id="b1")

    # 3. Create a 'bad' file (missing a column)
    bad_df = pl.DataFrame({"id": [2]})
    bad_df.write_csv(source / "bad.csv")

    # 4. ASSERT that it raises an Exception
    with pytest.raises(
        ValueError
    ) as excinfo:  # Use ValueError if that's what your SchemaManager raises
        loader.run(batch_id="b2")

    assert "Schema Mismatch" in str(excinfo.value)


def test_parquet_loading(tmp_path):
    source = tmp_path / "source"
    source.mkdir()

    df = pl.DataFrame({"id": [1, 2]})
    # Write as Parquet instead of CSV
    df.write_parquet(source / "data.parquet")

    loader = OpenAutoLoader(
        source=str(source),
        target=str(tmp_path / "target"),
        check_point=str(tmp_path / "cp"),
        schema_path=str(tmp_path / "schema"),
        format_type="parquet",  # Set format
    )
    loader.run(batch_id="p1")

    assert pl.read_delta(str(tmp_path / "target")).height == 2


def test_recursive_discovery(tmp_path):
    source = tmp_path / "source"
    nested_dir = source / "year=2026" / "month=03"
    nested_dir.mkdir(parents=True)

    df = pl.DataFrame({"id": [1]})
    df.write_csv(nested_dir / "nested_data.csv")

    loader = OpenAutoLoader(
        source=str(source),
        target=str(tmp_path / "target"),
        check_point=str(tmp_path / "cp"),
        schema_path=str(tmp_path / "schema"),
    )

    loader.run(batch_id="recursive_test")

    # Assert that the nested file was found and processed
    result = pl.read_delta(str(tmp_path / "target"))
    assert result.height == 1


def test_data_type_mismatch_fails(tmp_path):
    source = tmp_path / "source"
    source.mkdir()

    # 1. Lock schema with an Integer
    df1 = pl.DataFrame({"age": [25, 30]})
    df1.write_csv(source / "file1.csv")

    loader = OpenAutoLoader(
        source=str(source),
        target=str(tmp_path / "target"),
        check_point=str(tmp_path / "cp"),
        schema_path=str(tmp_path / "schema"),
    )
    loader.run(batch_id="init")

    # 2. Try to process a file where 'age' is a String
    df2 = pl.DataFrame({"age": ["Unknown"]})
    df2.write_csv(source / "file2.csv")

    with pytest.raises(TypeError) as excinfo:
        loader.run(batch_id="fail_expected")

    assert "Type Drift" in str(excinfo.value)
