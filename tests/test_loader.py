import pytest
import polars as pl
from pathlib import Path
from open_auto_loader import OpenAutoLoader

def test_full_workflow(tmp_path):
    # tmp_path is a built-in pytest fixture for a clean temp folder
    source = tmp_path / "source"
    target = tmp_path / "target"
    checkpoint = tmp_path / "checkpoint"
    schema = tmp_path / "schema"

    source.mkdir()

    # 1. Create a dummy CSV
    df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    file1 = source / "data1.csv"
    df.write_csv(file1)

    # 2. Initialize Loader
    loader = OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema)
    )

    # 3. Run Batch 1
    loader.run(batch_id="batch_01")

    # 4. ASSERTIONS
    # Check if Delta table exists and has data
    result_df = pl.read_delta(str(target))
    assert result_df.height == 2
    assert "_batch_id" in result_df.columns

    # Check if schema was locked
    assert (schema / "schema" / "schema_contract.json").exists()

    # 5. Run Batch 2 (Duplicate check)
    loader.run(batch_id="batch_02")
    # Height should still be 2 because no new files were added
    assert pl.read_delta(str(target)).height == 2

def test_schema_mismatch_fails(tmp_path):
    # 1. Setup paths
    source = tmp_path / "source"
    schema_dir = tmp_path / "schema"
    source.mkdir()

    # 2. Bootstrap with a 'good' file
    good_df = pl.DataFrame({"id": [1], "val": [10.5]})
    good_df.write_csv(source / "good.csv")

    loader = OpenAutoLoader(
        source=str(source),
        target=str(tmp_path / "target"),
        check_point=str(tmp_path / "cp"),
        schema_path=str(schema_dir)
    )
    loader.run(batch_id="b1")

    # 3. Create a 'bad' file (missing a column or different type)
    bad_df = pl.DataFrame({"id": [2]}) # 'val' is missing
    bad_df.write_csv(source / "bad.csv")

    # 4. ASSERT that it raises an Exception
    import pytest
    with pytest.raises(Exception) as excinfo:
        loader.run(batch_id="b2")

    assert "Schema mismatch" in str(excinfo.value)
