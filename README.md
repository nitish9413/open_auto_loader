# 🚀 OpenAutoLoader

[![PyPI version](https://img.shields.io/pypi/v/open-auto-loader.svg?color=blue)](https://pypi.org/project/open-auto-loader/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Powered by Polars](https://img.shields.io/badge/powered%20by-polars-orange.svg)](https://pola.rs/)
![PyPI Downloads](https://img.shields.io/pepy/dt/open-auto-loader)

**OpenAutoLoader** is a high-performance, incremental data ingestion engine. It bridges the gap between raw cloud storage and production-ready Delta Lakes using the lightning-fast **Polars Rust engine**.

Stop writing complex Spark jobs for simple file ingestion. OpenAutoLoader provides a "Databricks-style" Auto Loader experience in a lightweight Python package.

-----

## 💡 Why OpenAutoLoader?

Traditional ingestion often requires heavy JVM clusters (Spark) or manual file tracking. OpenAutoLoader changes that:

  * **Zero-Spark Overhead**: Runs on standard Python environments with Rust-level performance.
  * **Exactly-Once Processing**: Integrated SQLite checkpointing ensures no duplicate data, even if a job restarts.
  * **Schema First**: Automatically infers, saves, and enforces JSON schema contracts to prevent data corruption.
  * **Cloud Native**: A single API for Local, S3, Azure Blob (ABFSS), and GCS.

-----

## 🛠️ Installation

```bash
# Core (Local files only)
pip install open-auto-loader

# Full Cloud Support (Recommended)
pip install "open-auto-loader[all]"
```

-----

## 🚀 Quick Start: S3 to Delta Lake

```python
from open_auto_loader import OpenAutoLoader

# Define your cloud credentials
storage_options = {
    "aws_access_key_id": "YOUR_ACCESS_KEY",
    "aws_secret_access_key": "YOUR_SECRET_KEY",
    "region": "ap-south-1"
}

# Initialize the loader
loader = OpenAutoLoader(
    source="s3://my-raw-bucket/incoming_logs/",
    target="s3://my-silver-bucket/tables/user_logs",
    check_point="./metadata/checkpoints.db",
    schema_path="./metadata/schemas/",
    storage_options=storage_options
)

# Run the ingestion batch
loader.run(batch_id="daily_run_2026_03_18")
```

-----

## 🏗️ Architecture: How it Works

1.  **Scanner**: Uses `fsspec` to identify new files since the last successful `batch_id`.
2.  **Schema Guard**: Checks the file header against the stored JSON contract in `schema_path`.
3.  **Polars Engine**: Streams the data using `sink_delta()`, minimizing memory footprint.
4.  **Metadata Injection**: Automatically adds `_batch_id`, `_processed_at`, and `_source_file` to every row for full auditability.
5.  **Committer**: Updates the SQLite checkpoint only after a successful Delta write.

-----

## 📋 Compatibility Matrix

| Feature | Local | AWS S3 | Azure Blob | Google GCS |
| :--- | :---: | :---: | :---: | :---: |
| **Incremental Loading** | ✅ | ✅ | ✅ | ✅ |
| **Schema Enforcement** | ✅ | ✅ | ✅ | ✅ |
| **Service Principal Auth**| N/A | ✅ | ✅ | ✅ |
| **Streaming Sink** | ✅ | ✅ | ✅ | ✅ |

-----

## 🤝 Contributing

Contributions are welcome\! Whether it's a bug fix, a new cloud provider, or performance tuning, feel free to open a PR.

Created with ❤️ by [Nitish Katkade](https://github.com/nitish9413)
