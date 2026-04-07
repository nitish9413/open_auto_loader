from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from open_auto_loader.configs.schema import SchemaEvolutionMode

from ..factories import ReaderFactory


class PolarsEngine:
    def __init__(
        self,
        target_path: str,
        format_type: str = "csv",
        table_type: str = "delta",
        storage_options: dict | None = None,
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
        self,
        file_path: str,
        schema_dict: dict,
        batch_id: str,
        metadata: dict,
        evolution_mode: SchemaEvolutionMode,
        selected_columns: list[str] | None = None,
    ):
        """The core ETL step: Read -> Enrich -> Sink with Rescue logic."""

        reserved = {"_batch_id", "_processed_at", "_file_path"}
        if overlap := reserved & metadata.keys():
            raise ValueError(f"Metadata keys conflict with audit columns: {overlap}")

        # 1. Build scan kwargs
        # If mode is RESCUE, we DON'T pass 'columns' to the reader because
        # we need to read 'extra' columns to bundle them into JSON.
        is_rescue = evolution_mode == SchemaEvolutionMode.RESCUE

        scan_kwargs: dict = {
            "schema": (
                {k: v for k, v in schema_dict.items() if k in selected_columns}
                if (selected_columns and not is_rescue)
                else schema_dict
            )
        }

        # Prune only if NOT rescuing
        if selected_columns and not is_rescue:
            scan_kwargs["columns"] = selected_columns

        # 2. Scan
        lf = self.reader.scan(file_path, **scan_kwargs)
        cast_exprs = [
            pl.col(c).cast(schema_dict[c])
            for c in schema_dict
            if c in lf.collect_schema().names()
        ]
        if cast_exprs:
            lf = lf.with_columns(cast_exprs)
        # 3. Handle Rescue Logic
        rescue_col_name = "_rescued_data"
        active_selection = selected_columns.copy() if selected_columns else None

        if is_rescue:
            all_file_cols = lf.collect_schema().names()
            known_cols = set(selected_columns or schema_dict.keys())
            extra_cols = [c for c in all_file_cols if c not in known_cols]

            if extra_cols:
                lf = lf.with_columns(
                    pl.struct(extra_cols)
                    .map_elements(json.dumps, return_dtype=pl.String)
                    .alias(rescue_col_name)
                )
                lf = lf.drop(extra_cols)  # drop raw extras, keep only _rescued_data
                if active_selection is not None:
                    active_selection.append(rescue_col_name)

        # 4. Audit + metadata columns
        processed_at = datetime.now(UTC)
        audit_cols = [
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(processed_at).alias("_processed_at"),
            pl.lit(file_path).alias("_file_path"),
            *[pl.lit(v).alias(k) for k, v in metadata.items()],
        ]
        lf = lf.with_columns(audit_cols)

        # 5. Final column selection
        audit_col_names = [*reserved, *metadata.keys()]
        if active_selection:
            lf = lf.select([*active_selection, *audit_col_names])

        # 6. Sink
        lf.sink_delta(
            self.target_path,
            mode="append",
            delta_write_options={"schema_mode": "merge"},
            storage_options=self.storage_options or None,
        )
