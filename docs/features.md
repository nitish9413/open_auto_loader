# ✨ Features

OpenAutoLoader provides a robust set of features designed to make data ingestion predictable, efficient, and professional. Unlike a simple file-reading script, it manages the state and integrity of your data pipeline.

---

## 🔄 Incremental Loading
The core value of OpenAutoLoader is its ability to process only **new** data.

- **State Management**: Uses a local SQLite database to track every file that has been successfully ingested.
- **Efficient Scanning**: Rapidly identifies new files without needing to re-read the entire source directory or the target table.
- **Batch Isolation**: Every run is assigned a unique `batch_id`, allowing you to trace exactly when and how data entered your system.

---

## 🛡️ Schema Evolution & Integrity
OpenAutoLoader provides a sophisticated engine to handle "Schema Drift"—when source files change structure. You can control this behavior using the `evolution_mode` parameter.



| Mode | Strategy | Physical Action |
| :--- | :--- | :--- |
| `addNewColumns` | **Evolve** | Updates the JSON contract and evolves the Delta table. |
| `failOnNewColumns` | **Strict** | Raises `SchemaMismatchError` to stop the pipeline. |
| `none` | **Ignore** | Physically drops extra columns to match the existing contract. |
| `rescue` | **Zero Loss** | Moves unknown columns into a `_rescued_data` JSON blob. |

### 🚀 Rescue Mode: The Safety Net
Rescue mode allows the pipeline to continue running even if new columns appear by "stashing" them into a single column. This ensures your Delta table schema remains stable while preserving all incoming data.

---

## 📑 Metadata & Traceability
To ensure your "Gold" or "Silver" layer tables are ready for production, OpenAutoLoader automatically enriches every row with technical and custom metadata.

### System Audit Columns
| Column | Description |
| :--- | :--- |
| `_batch_id` | Links rows to a specific execution run. |
| `_processed_at` | High-precision UTC timestamp of ingestion. |
| `_file_path` | Absolute source path for data lineage and debugging. |

### Custom Metadata
You can inject arbitrary key-value pairs during the loader initialization. These are appended as physical columns to every row, perfect for tracking organizational context:

```python
loader = OpenAutoLoader(
    ...
    metadata={
        "env": "production",
        "region": "us-east-1",
        "source_system": "sap_erp"
    }
)
```

---

## ⚡ Polars Engine
Built on top of the [Polars](https://pola.rs/) library, OpenAutoLoader is designed for high-speed I/O.

- **Lazy Evaluation**: The engine builds an optimized query plan and only executes it at the final "Sink" step.
- **Memory Efficient**: Uses streaming and memory-mapping to handle datasets larger than your RAM.
- **Native Delta Support**: Uses the Polars `sink_delta` implementation for atomic, ACID-compliant writes.

---

## ☁️ Cloud Native Strategy
OpenAutoLoader is storage-agnostic. By utilizing `fsspec`, it treats cloud paths exactly like local paths.

- **Unified Interface**: Use `s3://`, `gs://`, or `abfss://` protocols seamlessly.
- **Credential Management**: Pydantic-based storage configurations ensure your secrets are validated and correctly passed to the underlying cloud drivers.

---

## 🛠️ Extensible Architecture
The library is built using the **Strategy** and **Factory** design patterns.

- **Format Support**: Native support for `CSV`, `Parquet`, and `NDJSON/JSONL`.
- **Modular Components**: The Scanner, Engine, and Checkpoint systems are decoupled, making the library easy to maintain and extend for custom requirements.
