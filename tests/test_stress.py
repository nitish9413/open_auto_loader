import os
import shutil
from pathlib import Path

import polars as pl
import pytest

from open_auto_loader import OpenAutoLoader


@pytest.fixture
def stress_env(tmp_path):
    """
    Setup and Teardown fixture.
    Creates directories and deletes them after the test finishes.
    """
    paths = {
        "source": Path("data"),  # Pointing to your 7GB folder
        "target": tmp_path / "target_delta",
        "checkpoint": tmp_path / "checkpoint",
        "schema": tmp_path / "schema",
    }

    # Create the temp dirs (source already exists with your 7GB file)
    paths["target"].mkdir()
    paths["checkpoint"].mkdir()
    paths["schema"].mkdir()

    yield paths

    # TEARDOWN: This runs after the test, even if it fails
    print("\nCleaning up stress test artifacts...")
    for key in ["target", "checkpoint", "schema"]:
        shutil.rmtree(paths[key], ignore_errors=True)


def test_large_file_streaming(stress_env):
    """
    Tests if the loader can handle a 7GB file without crashing.
    """
    loader = OpenAutoLoader(
        source=str(stress_env["source"]),
        target=str(stress_env["target"]),
        check_point=str(stress_env["checkpoint"]),
        schema_path=str(stress_env["schema"]),
        format_type="csv",
    )

    # Execution should be stable in memory
    loader.run(batch_id="stress_test_01")

    # Verify result without loading everything into memory
    # Only scan metadata to verify height
    result_df = pl.scan_delta(str(stress_env["target"]))
    count = result_df.select(pl.len()).collect().item()

    assert count > 0
    print(f"Successfully processed {count} rows from 7GB source.")
