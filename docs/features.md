# ✨ Features

OpenAutoLoader provides a robust set of features designed to make data ingestion predictable, efficient, and professional. Unlike a simple file-reading script, it manages the state and integrity of your data pipeline.

---

## 🔄 Incremental Loading
The core value of OpenAutoLoader is its ability to process only **new** data.

- **State Management**: Uses a local SQLite database to track every file that has been successfully ingested.
- **Efficient Scanning**: Rapidly identifies new files without needing to re-read the entire source directory or the target table.
- **Batch Isolation**: Every run is assigned a unique `batch_id`, allowing you to trace exactly when and how data entered your system.

---

## 🛡️ Schema Enforcement
Data pipelines often fail due to unexpected changes in source files (e.g., a missing column or a changed data type).

- **Schema Bootstrapping**: On the very first run, the library infers the schema from your source files and saves it as a `schema.json`.
- **Validation**: Every subsequent run validates new files against this saved schema.
- **Integrity**: If a new file does not match the expected structure, the ingestion will fail before corrupting your Delta Lake table.

---

## 📑 Audit Metadata
To ensure your "Gold" or "Silver" layer tables are ready for production, OpenAutoLoader automatically enriches every row with technical metadata.



| Column | Description |
| :--- | :--- |
| `_batch_id` | The unique ID provided during the `.run()` call. |
| `_processed_at` | The exact UTC timestamp when the row was ingested. |
| `_file_path` | The original source file path (useful for debugging data quality issues). |

---

## ⚡ Polars Engine
Built on top of the [Polars](https://pola.rs/) library, OpenAutoLoader is designed for high-speed I/O.

- **Lazy Evaluation**: The engine builds a query plan and only executes it at the final "Sink" step.
- **Memory Efficient**: Uses streaming and memory-mapping where possible to handle datasets larger than your RAM.
- **Native Delta Support**: Uses the Polars `sink_delta` implementation for atomic, ACID-compliant writes.

---

## ☁️ Cloud Native Strategy
OpenAutoLoader is storage-agnostic. By utilizing `fsspec`, it treats cloud paths exactly like local paths.

- **Unified Interface**: Use `s3://`, `gs://`, or `abfss://` protocols seamlessly.
- **Credential Management**: Pydantic-based storage configurations ensure your secrets are validated and correctly passed to the underlying cloud drivers.

---

## 🛠️ Extensible Architecture
The library is built using the **Strategy** and **Factory** design patterns.

- **Format Support**: Easily switch between `CSV`, `Parquet`, and `NDJSON/JSONL`.
- **Modular Components**: The Scanner, Engine, and Checkpoint systems are decoupled, making the library easy to maintain and extend for custom requirements.
