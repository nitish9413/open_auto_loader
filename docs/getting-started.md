# 🚀 Getting Started

This guide will walk you through your first incremental data ingestion using **OpenAutoLoader**. We will set up a local pipeline to ingest CSV data into a Delta Lake table.

---

## 📂 1. Prepare Your Project Structure
To keep your data organized, create a basic directory structure. OpenAutoLoader requires a source for your files and a place to store metadata (checkpoints and schemas).

```text
my_data_project/
├── data/
│   └── raw/          # Place your source CSV/Parquet files here
├── output/           # Where the Delta Lake table will be created
└── metadata/         # Stores ingestion state and JSON schemas
```

---

## 🛠️ 2. Create Your First Ingestion Script
Create a Python file named `ingest.py` in your project root.

```python
from open_auto_loader import OpenAutoLoader

# 1. Initialize the loader
loader = OpenAutoLoader(
    source="./data/raw/",
    target="./output/gold_table/",
    check_point="./metadata/checkpoint.db",
    schema_path="./metadata/schema.json",
    format_type="csv"
)

# 2. Run the ingestion
# This will automatically detect new files and append them to Delta
loader.run(batch_id="initial_batch_001")
```

---

## 🔍 3. How the Ingestion Cycle Works
When you execute the `.run()` method, the library follows a structured pipeline to ensure data integrity and incremental progress.



1.  **Scanning**: The `Scanner` identifies all files in the source directory matching your `format_type`.
2.  **Filtering**: The `Checkpoint` system compares these files against the SQLite database to identify only "New" files.
3.  **Schema Enforcement**: On the first run, it infers and saves the schema. On subsequent runs, it validates new data against this contract.
4.  **Metadata Injection**: It adds audit columns (`_batch_id`, `_processed_at`, `_file_path`) to every row.
5.  **Atomic Sink**: Data is written to the target as a Delta Lake table, ensuring ACID transactions.

---

## 📊 4. Verify the Results
Since OpenAutoLoader produces standard Delta Lake tables, you can read the output immediately using **Polars**.

```python
import polars as pl

# Read the generated Delta table
df = pl.read_delta("./output/gold_table/")

print("Ingested Data Sample:")
print(df.head())

print("\nAudit Metadata:")
print(df.select(["_batch_id", "_processed_at", "_file_path"]).head())
```

---

## 🔄 5. Adding New Data
To see the incremental logic in action:
1.  Drop a new CSV file into `./data/raw/`.
2.  Update the `batch_id` in your script (e.g., `"daily_update_002"`).
3.  Run the script again.

**OpenAutoLoader will ignore the old files and only process the new one.**
