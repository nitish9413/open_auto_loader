# 🚀 OpenAutoLoader

**OpenAutoLoader** is a high-performance, incremental data ingestion library for Python. It provides a "Set and Forget" experience for ingesting raw files from **Local Storage, AWS S3, Azure Blob, and GCP** into professional Delta Lake tables, built entirely on the **Polars** engine.

---

## ✨ Key Features

* **Incremental Loading**: SQLite-backed checkpoint system ensures files are processed **exactly once**.
* **Multi-Cloud Support**: Native support for `s3://`, `abfss://`, and `gs://` protocols.
* **Schema Governance**: Automatically bootstraps and enforces a strict JSON contract to prevent data poisoning.
* **Streaming Execution**: Leverages Polars' `sink_delta(streaming=True)` to process datasets larger than RAM.
* **Audit-Ready**: Automatically injects metadata: `_batch_id`, `_processed_at`, and `_file_path`.

---

## 🛠️ Architecture

OpenAutoLoader uses a modular architecture designed for extensibility:

| Component | Responsibility |
| --- | --- |
| **`OpenAutoLoader`** | The Orchestrator. Coordinates discovery, validation, and execution. |
| **`FileScanner`** | Discovery. Uses `fsspec` to recursively find new files across cloud providers. |
| **`PolarsEngine`** | Execution. Handles LazyFrame transformations and high-speed Delta sinks. |
| **`SchemaManager`** | Governance. Serializes the data contract to JSON and validates new batches. |
| **`CheckPointManager`** | Persistence. Tracks processed file paths to prevent duplicate ingestion. |

---

## 🚀 Quick Start

### 1. Installation

```bash
# Core library
pip install open_auto_loader

# With Cloud Drivers (Optional)
pip install s3fs adlfs gcsfs
```

### 2. Cloud Ingestion (AWS S3 Example)

```python
from open_auto_loader import OpenAutoLoader

# Define your cloud credentials
storage_options = {
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "aws_region": "ap-south-1"
}

loader = OpenAutoLoader(
    source="s3://my-raw-bucket/incoming/",
    target="s3://my-silver-bucket/tables/users",
    check_point="./metadata",       # Checkpoints stay local for speed
    schema_path="./contracts",      # Schemas stay local for governance
    format_type="csv",
    storage_options=storage_options
)

loader.run(batch_id="daily_batch_001")
```

---

## ☁️ Supported Cloud Protocols

| Provider | Protocol | Required Driver | `storage_options` keys |
| --- | --- | --- | --- |
| **Local** | `file://` | None | None |
| **AWS S3** | `s3://` | `s3fs` | `aws_access_key_id`, `aws_region` |
| **Azure Blob**| `abfss://`| `adlfs`| `account_name`, `account_key` |
| **GCP GCS** | `gs://` | `gcsfs` | `token` (path to JSON key) |

---

## 📋 Schema Management

OpenAutoLoader implements **Schema Locking**:

1. **Bootstrap**: On the first run, the library infers types from the first file found and saves a `schema_contract.json`.
2. **Enforcement**: Every subsequent file is validated against this contract before processing.
3. **Type Safety**: If a file exhibits **Type Drift** (e.g., an Integer column arriving as a String), the batch is aborted to maintain target table integrity.

---

## 📜 License

MIT License.
