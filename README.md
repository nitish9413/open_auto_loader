# 🚀 OpenAutoLoader

**OpenAutoLoader** is a high-performance, incremental data ingestion library for Python. It provides a "Set and Forget" experience for ingesting raw files into professional Delta Lake tables, built entirely on the **Polars** engine.

It is designed for Data Engineers who need a lightweight, open-source alternative to proprietary ingestion tools while maintaining strict schema governance.

---

## ✨ Key Features

* **Incremental Loading**: Uses a SQLite-backed checkpoint system to ensure files are processed **exactly once**.
* **Schema Governance**: Automatically bootstraps a schema on the first run and enforces a strict JSON contract to prevent data poisoning.
* **Streaming Execution**: Leverages Polars' `sink_delta(streaming=True)` to process datasets larger than your RAM.
* **Audit-Ready**: Automatically injects metadata columns: `_batch_id`, `_processed_at`, and `_file_path`.
* **Recursive Discovery**: Scans deeply nested directory structures automatically.

---

## 🛠️ Architecture

OpenAutoLoader is built on a modular, "Strategy-based" architecture:

| Component | Responsibility |
| --- | --- |
| **`OpenAutoLoader`** | The Orchestrator. Coordinates discovery, validation, and execution. |
| **`FileScanner`** | Discovery. Recursively finds new files based on format extensions. |
| **`PolarsEngine`** | Execution. Handles the LazyFrame transformations and Delta sinks. |
| **`ReaderFactory`** | Abstraction. Maps file extensions to the correct Polars reader (CSV, Parquet, etc). |
| **`SchemaManager`** | Governance. Serializes the data contract to JSON and validates new batches. |
| **`CheckPointManager`** | Persistence. Tracks file hashes in SQLite to prevent duplicate processing. |

---

## 🚀 Quick Start

### 1. Installation

```bash
# Using uv (recommended)
uv add open_auto_loader

# Using pip
pip install open_auto_loader

```

### 2. Basic Usage

```python
from open_auto_loader import OpenAutoLoader

# Initialize the loader
loader = OpenAutoLoader(
    source="./raw_data/landing",    # Source directory
    target="./data_lake/silver",   # Target Delta table
    check_point="./metadata",      # Checkpoint database location
    schema_path="./contracts",     # JSON Schema storage
    format_type="parquet"          # Supports 'csv', 'parquet', 'ndjson'
)

# Run an ingestion batch
loader.run(batch_id="daily_ingestion_v1")

```

---

## 📋 Schema Management

OpenAutoLoader implements **Schema Locking**.

1. **Bootstrap**: On the first run, the library reads the first file found, infers its types, and saves a `schema_contract.json`.
2. **Enforcement**: On all subsequent runs, every file is checked against this contract.
3. **Validation**: If a file has missing columns, extra columns, or **Type Drift** (e.g., an Int arriving as a String), the batch is aborted before it touches the target table.

---

## 🧪 Development & Testing

We use `pytest` for all unit and integration tests.

```bash
# Run the test suite
uv run pytest

```

### Adding New Formats

To add a new format (like Avro):

1. Add a new Reader class in `readers.py` implementing the `FormatReader` Protocol.
2. Register the format and extensions in `ReaderFactory` and `FileScanner`.

---

## 📜 License

MIT License.
