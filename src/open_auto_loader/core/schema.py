import json
from pathlib import Path
from typing import Any

import polars as pl

from open_auto_loader.configs.schema import SchemaEvolutionMode
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

    def validate(
        self,
        current_schema: dict[str, Any],
        evolution_mode: SchemaEvolutionMode,
        file_path: str = "unknown",
    ):
        """Strict validation against the locked contract, respecting evolution mode."""
        reference = self.load_schema()
        current_keys = set(current_schema.keys())
        reference_keys = set(reference.keys())

        # 1. Check for Missing Columns (Always Fail - Additive Only)
        missing = list(reference_keys - current_keys)
        if missing:
            raise SchemaMismatchError(
                f"Missing columns in {file_path}",
                missing_columns=missing,
                extra_columns=[],
                file_path=file_path,
            )

        # 2. Check for Extra Columns (Mode Dependent)
        extra = list(current_keys - reference_keys)
        if extra and evolution_mode == SchemaEvolutionMode.FAIL_ON_NEW_COLUMNS:
            raise SchemaMismatchError(
                f"Extra columns in {file_path}",
                missing_columns=[],
                extra_columns=extra,
                file_path=file_path,
            )

        # 3. Check for Type Drift (Always Fail - Type Lock)
        for col in reference_keys.intersection(current_keys):
            if str(current_schema[col]) != str(reference[col]):
                raise SchemaDriftError(
                    f"Type drift on {col}",
                    column=col,
                    expected_type=str(reference[col]),
                    actual_type=str(current_schema[col]),
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

    def check_differences(self, current_schema: dict[str, Any]) -> dict[str, list[str]]:
        """Returns missing and extra columns compared to the contract."""
        reference = self.load_schema()
        return {
            "missing": list(set(reference.keys()) - set(current_schema.keys())),
            "extra": list(set(current_schema.keys()) - set(reference.keys())),
        }

    def evolve_schema(self, new_columns_schema: dict[str, Any]) -> None:
        """
        Merges new columns from a source file into the existing contract.

        Args:
            new_columns_schema: The schema inferred from the current raw file.
        """
        current_contract = self.load_schema()

        updated_contract = {**new_columns_schema, **current_contract}

        if len(updated_contract) > len(current_contract):
            self.save_schema(updated_contract)
