from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from .factories import ReaderFactory


class PolarsEngine:
    def __init__(
        self, target_path: str, format_type: str = "csv", table_type: str = "delta"
    ):
        self.target_path = target_path
        self.table_type = table_type

        self.reader = ReaderFactory.get_reader_by_format(format_type)

    def get_inferred_schema(self, file_path: str):
        """Helper for the SchemaManager to bootstrap the JSON contract."""
        return self.reader.get_schema(file_path)

    def validate_schema(self, file_path, reference_schema):
        """Strict check: fails if CSV doesn't match the Table/Reference"""
        source_schema = self.get_schema(file_path)
        if source_schema != reference_schema:
            raise Exception(f"Schema mismatch in {file_path}")

    def process_single_file(self, file_path: str, schema_dict: dict, batch_id: str):
        """The core ETL step: Read -> Enrich -> Sink."""

        Path(self.target_path).mkdir(parents=True, exist_ok=True)

        # 1. Lazy Read using the Reader Strategy
        # Passing the schema_dict here ensures 'Type Safety'
        lf = self.reader.scan(file_path, schema=schema_dict)

        # 2. Add the Airflow metadata columns
        # This is vital for your 'Open' library users to audit their data
        abs_path = str(Path(file_path).resolve())

        lf = lf.with_columns(
            [
                pl.lit(batch_id).alias("_batch_id"),
                pl.lit(datetime.now(timezone.utc)).alias("_processed_at"),
                pl.lit(abs_path).alias("_file_path"),
            ]
        )

        # 3. Stream to Delta/Sink
        # Using streaming=True allows us to handle files larger than RAM
        if self.table_type == "delta":
            lf.sink_delta(self.target_path, mode="append")
        else:
            raise NotImplementedError(
                f"Table type {self.table_type} not yet supported."
            )
