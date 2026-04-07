import json
from pathlib import Path
from typing import Any

import polars as pl

from open_auto_loader.exceptions import (
    SchemaDriftError,
    SchemaMismatchError,
    SchemaSerializationError,
)


class SchemaManager:
    def __init__(self, schema_root_path: str):
        self.schema_dir = Path(schema_root_path).resolve() / "schema"
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.schema_file = self.schema_dir / "schema_contract.json"

    def schema_exists(self) -> bool:
        return self.schema_file.exists()

    def save_schema(self, schema: dict[str, Any]):
        """Serializes Polars schema to JSON."""
        serialized = {col: str(dtype) for col, dtype in schema.items()}
        with open(self.schema_file, "w") as f:
            json.dump(serialized, f, indent=4)

    def load_schema(self) -> dict[str, Any]:
        """Deserializes JSON back to Polars schema."""
        with open(self.schema_file) as f:
            data = json.load(f)

        return {
            col: self._string_to_dtype(dtype_str) for col, dtype_str in data.items()
        }

    def validate(self, current_schema: dict[str, Any], file_path: str = "unknown"):
        """Strict validation against the locked contract."""
        reference = self.load_schema()

        # 1. Check for missing/extra columns
        if set(current_schema.keys()) != set(reference.keys()):
            missing = list(set(reference.keys()) - set(current_schema.keys()))
            extra = list(set(current_schema.keys()) - set(reference.keys()))

            raise SchemaMismatchError(
                f"Schema Mismatch in {file_path}!",
                missing_columns=missing,
                extra_columns=extra,
                file_path=file_path,
            )

        # 2. Check for type drift (Strict Mode)
        for col, expected_dtype in reference.items():
            actual_dtype = current_schema[col]
            if str(actual_dtype) != str(expected_dtype):
                raise SchemaDriftError(
                    f"Type Drift on column '{col}' in {file_path}",
                    column=col,
                    expected_type=str(expected_dtype),
                    actual_type=str(actual_dtype),
                    file_path=file_path,
                )

    def _string_to_dtype(self, dtype_str: str) -> Any:
        """Maps common string representations to Polars DataTypes."""
        try:
            return getattr(pl, dtype_str)
        except AttributeError:
            raise SchemaSerializationError(  # noqa: B904
                f"Could not deserialize Polars type: {dtype_str}", dtype_str=dtype_str
            )
