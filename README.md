# OpenAutoLoader 🚀

An open-source, efficient data ingestion library inspired by Databricks Autoloader.

## Features
- **Polars-powered:** Lightning-fast CSV ingestion.
- **SQLite Checkpointing:** Guarantees no duplicate files are processed.
- **Delta Lake Integration:** Sink data directly into a Lakehouse format.
- **Schema Strictness:** Fails fast if incoming data drifts.

## Usage
```python
from open_auto_loader import OpenAutoLoader

loader = OpenAutoLoader(
    source="./landing_zone",
    target="./delta_lake/my_table",
    check_point="./checkpoints",
    extension="csv"
)

loader.run(batch_id="manual_run_01")
