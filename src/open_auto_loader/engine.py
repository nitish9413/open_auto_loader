from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from .factories import ReaderFactory


class PolarsEngine:
    def __init__(
        self,
        target_path: str,
        format_type: str = "csv",
        table_type: str = "delta",
        storage_options: dict = None,
    ):
        self.target_path = target_path
        self.table_type = table_type
        self.storage_options = storage_options or {}

        # Determine if target is cloud or local
        self.target_protocol = (
            target_path.split("://")[0] if "://" in target_path else "file"
        )

        self.reader = ReaderFactory.get_reader_by_format(format_type)

    def get_inferred_schema(self, file_path: str):
        """Helper for the SchemaManager to bootstrap the JSON contract."""
        return self.reader.get_schema(file_path, storage_options=self.storage_options)

    def process_single_file(
        self, file_path: str, schema_dict: dict, batch_id: str, metadata: dict
    ):
        """The core ETL step: Read -> Enrich -> Sink."""

        if self.target_protocol == "file":
            Path(self.target_path).mkdir(parents=True, exist_ok=True)

        lf = self.reader.scan(
            file_path, schema=schema_dict, storage_options=self.storage_options
        )

        metadata_cols = [
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(datetime.now(timezone.utc)).alias("_processed_at"),
            pl.lit(file_path).alias("_file_path"),
        ]

        for key, value in metadata.items():
            metadata_cols.append(pl.lit(value).alias(key))

        lf = lf.with_columns(metadata_cols)

        if self.table_type == "delta":
            lf.sink_delta(
                self.target_path,
                mode="append",
            )
        else:
            raise NotImplementedError(
                f"Table type {self.table_type} not yet supported."
            )
