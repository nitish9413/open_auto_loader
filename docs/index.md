# 🚀 OpenAutoLoader

**OpenAutoLoader** is a high-performance, Polars-powered incremental data loader for Delta Lake.

Inspired by Databricks Auto Loader, it provides a simple, open-source way to ingest data from local or cloud storage into a structured Delta Lake table with built-in checkpointing and schema enforcement.

## Why OpenAutoLoader?
* **Performance**: Built on Polars Lazy API for optimized I/O.
* **Safety**: Pydantic-validated storage configurations.
* **Reliability**: Atomic Delta Lake writes with ACID guarantees.
* **Auditability**: Automatic injection of `_batch_id` and `_processed_at` metadata.
