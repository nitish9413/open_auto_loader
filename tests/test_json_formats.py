from pathlib import Path

import polars as pl
import pytest

from open_auto_loader import OpenAutoLoader


def create_test_data(base_path: Path, format_ext: str):
    """Helper function to create a clean directory for each test run."""
    source_dir = base_path / f"raw_{format_ext}"
    source_dir.mkdir(parents=True, exist_ok=True)

    data = [
        {"id": 1, "name": "Alice", "role": "Admin"},
        {"id": 2, "name": "Bob", "role": "User"},
    ]

    # Write only the file type we are testing
    file_path = source_dir / f"data.{format_ext}"
    pl.DataFrame(data).write_ndjson(file_path)

    target_dir = base_path / f"target_{format_ext}"
    checkpoint = base_path / f"checkpoint_{format_ext}.db"
    schema_path = base_path / f"schema_{format_ext}.json"

    return str(source_dir), str(target_dir), str(checkpoint), str(schema_path)


@pytest.mark.parametrize("format_ext", ["ndjson", "jsonl"])
def test_ingest_json_variants(tmp_path: Path, format_ext: str):
    # 1. Setup clean data for THIS specific format
    source, target, checkpoint, schema_path = create_test_data(tmp_path, format_ext)

    # 2. Initialize Loader
    loader = OpenAutoLoader(
        source=source,
        target=target,
        check_point=checkpoint,
        schema_path=schema_path,
        format_type=format_ext,
    )

    # 3. Run Ingestion
    loader.run(batch_id=f"test_{format_ext}")

    # 4. Verify results in Delta
    df = pl.read_delta(target)

    # Now this will be exactly 2, because each test has its own folder!
    assert df.height == 2
    assert "_batch_id" in df.columns
    assert df["_batch_id"][0] == f"test_{format_ext}"
