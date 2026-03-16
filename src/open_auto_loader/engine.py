from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import json


class SchemaManager:
    def __init__(self, schema_root_path: str):
        self.schema_dir = Path(schema_root_path).resolve() / "schema"
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.schema_file = self.schema_dir / "schema_contract.json"

    def schema_exists(self) -> bool:
        return self.schema_file.exists()

    def _serialize_schema(self, schema: dict) -> dict:
        """Converts Polars DataType objects to strings."""
        # Example: { "id": pl.Int64 } -> { "id": "Int64" }
        return {col: str(dtype) for col, dtype in schema.items()}

    def _deserialize_schema(self, schema_dict: dict) -> dict:
        """Converts strings back to Polars DataType objects."""
        return {
            col: self._string_to_pl_type(dtype_str)
            for col, dtype_str in schema_dict.items()
        }

    def save_schema(self, schema: dict):
        """Saves the contract to the JSON file."""
        serialized = self._serialize_schema(schema)
        with open(self.schema_file, "w") as f:
            json.dump(serialized, f, indent=4)

    def load_schema(self) -> dict:
        """Returns the Polars-compatible schema dictionary."""
        with open(self.schema_file, "r") as f:
            data = json.load(f)
        return self._deserialize_schema(data)

    def validate(self, current_schema: dict):
        """
        Compares the incoming file schema against the locked JSON contract.
        """
        reference = self.load_schema()

        # Check for missing or extra columns
        if set(current_schema.keys()) != set(reference.keys()):
            missing = set(reference.keys()) - set(current_schema.keys())
            extra = set(current_schema.keys()) - set(reference.keys())
            raise Exception(f"Schema mismatch. Missing: {missing}, Extra: {extra}")

        # Check for type drift
        for col, dtype in reference.items():
            if current_schema[col] != dtype:
                raise Exception(
                    f"Type drift on {col}: Expected {dtype}, got {current_schema[col]}"
                )

    def _string_to_pl_type(self, type_str: str):
        """Helper to convert 'Int64' string to pl.Int64 object."""
        return getattr(pl, type_str)


class PolarsEngine:
    def __init__(self, target_path, table_type="delta"):
        self.target_path = target_path
        self.table_type = table_type

    def get_schema(self, file_path):
        """Infers schema from a local CSV file"""
        return pl.scan_csv(file_path).collect_schema()

    def validate_schema(self, file_path, reference_schema):
        """Strict check: fails if CSV doesn't match the Table/Reference"""
        source_schema = self.get_schema(file_path)
        if source_schema != reference_schema:
            raise Exception(f"Schema mismatch in {file_path}")

    def process_single_file(self, file_path, schema, batch_id):
        """The atomic unit of work: Read -> Validate -> Enrich -> Sink"""

        # 1. Start the lazy scan (Must use scan_csv for the source!)
        lf = pl.scan_csv(file_path, schema=schema)

        # 2. Add the Airflow metadata columns
        # This is vital for your 'Open' library users to audit their data
        lf = lf.with_columns(
            [
                pl.lit(batch_id).alias("_batch_id"),
                pl.lit(datetime.now(timezone.utc)).alias("_processed_at"),
                pl.lit(str(Path(file_path).resolve())).alias("_file_path"),
            ]
        )

        # 3. Sink to Delta Lake (Streaming mode)
        # This will fail if the write is interrupted, which is what we want
        lf.sink_delta(self.target_path, mode="append")
