# 🚀 OpenAutoLoader

**OpenAutoLoader** is a high-performance, Polars-powered incremental data loader for Delta Lake.

[![PyPI version](https://img.shields.io/pypi/v/open-auto-loader.svg?color=blue)](https://pypi.org/project/open-auto-loader/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Powered by Polars](https://img.shields.io/badge/powered%20by-polars-orange.svg)](https://pola.rs/)
![PyPI Downloads](https://img.shields.io/pepy/dt/open-auto-loader)


Inspired by Databricks Auto Loader, it provides a simple, open-source way to ingest data from local or cloud storage into a structured Delta Lake table with built-in checkpointing and schema enforcement.

-----

## ✨ Why OpenAutoLoader?

  * **⚡ Performance**: Built on the **Polars Lazy API** for optimized, multi-threaded I/O and memory-efficient processing.
  * **🔄 Incremental Loading**: Tracks processed files via a robust SQLite checkpoint system to ensure you only ingest **new** data.
  * **🛡️ Schema Evolution & Rescue**: Supports multiple modes (`addNewColumns`, `failOnNewColumns`, `none`, and `rescue`). **Rescue Mode** preserves unknown data in a JSON blob to prevent ingestion failure.
  * **📑 Auditability**: Automatically enriches every row with technical metadata including `_batch_id`, `_processed_at`, and `_file_path`.
  * **🛠️ Custom Metadata**: Inject business context (like `env`, `source_system`, or `team`) as physical columns during ingestion.
  * **☁️ Cloud Native**: Native support for S3, GCS, and Azure Blob Storage via `fsspec` and Pydantic-validated storage configurations.
  * **💎 Reliability**: Leverages Delta Lake for **ACID-compliant**, atomic writes, ensuring your target table is never left in a corrupted state.

-----

## 🏗️ Architecture

OpenAutoLoader uses a decoupled architecture of Scanners, Readers, and Engines to manage the lifecycle of a data batch.

-----

## 📖 Quick Start

```python
from open_auto_loader import OpenAutoLoader, SchemaEvolutionMode

# Initialize the loader
loader = OpenAutoLoader(
    source="s3://raw-zone/events/",
    target="s3://silver-zone/events_table/",
    checkpoint_path="./checkpoints/events.db",
    evolution_mode=SchemaEvolutionMode.RESCUE,
    metadata={"env": "production", "source": "web_logs"}
)

# Run ingestion for a new batch
loader.run(batch_id="daily_sync_2026_04_07")
```

-----

## 🛠️ Installation

```bash
pip install open-auto-loader
```
