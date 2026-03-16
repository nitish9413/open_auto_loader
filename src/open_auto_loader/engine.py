from datetime import datetime,timezone
from pathlib import Path

import polars as pl


class PolarsEngine:
    def __init__(self, target_path, table_type="delta"):
        self.target_path = target_path
        self.table_type = table_type

    def get_schema(self, file_path):
        """Infers schema from a local CSV file"""
        return pl.scan_csv(file_path).collect_schema()

    def get_reference_schema(self):
        target_path_resolve = Path(self.target_path).resolve()

        delta_check = [file.name for file in target_path_resolve.iterdir()]

        if "_delta_log" in delta_check:
            return pl.scan_delta(self.target_path)

        return None

    def validate_schema(self, file_path, reference_schema):
        """Strict check: fails if CSV doesn't match the Table/Reference"""
        source_schema = self.get_schema(file_path)
        if source_schema != reference_schema:
            raise Exception(f"Schema mismatch in {file_path}")

    def process_single_file(self, file_path, schema, batch_id):
        """The atomic unit of work: Read -> Validate -> Enrich -> Sink"""

        # 1. Start the lazy scan (Must use scan_csv for the source!)
        lf = pl.scan_csv(file_path)

        # 2. Add the Airflow metadata columns
        # This is vital for your 'Open' library users to audit their data
        lf = lf.with_columns(
            [
                pl.lit(batch_id).alias("_batch_id"),
                pl.lit(datetime.now(timezone.utc)).alias("_processed_at"),
            ]
        )

        # 3. Sink to Delta Lake (Streaming mode)
        # This will fail if the write is interrupted, which is what we want
        lf.sink_delta(self.target_path, mode="append")
