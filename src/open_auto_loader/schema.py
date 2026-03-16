import json
from pathlib import Path
from typing import Any, Dict

import polars as pl


class SchemaManager:
    def __init__(self, schema_root_path: str):
        # Enforce the folder structure rule
        self.schema_dir = Path(schema_root_path).resolve() / "schema"
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.schema_file = self.schema_dir / "schema_contract.json"

    def schema_exists(self) -> bool:
        return self.schema_file.exists()

    def save_schema(self, schema: Dict[str, Any]):
        """Serializes Polars schema to JSON."""
        # Convert pl.DataType to string (e.g., pl.Int64 -> "Int64")
        serialized = {col: str(dtype) for col, dtype in schema.items()}
        with open(self.schema_file, "w") as f:
            json.dump(serialized, f, indent=4)

    def load_schema(self) -> Dict[str, Any]:
        """Deserializes JSON back to Polars schema."""
        with open(self.schema_file, "r") as f:
            data = json.load(f)

        # Convert strings back to Polars objects
        return {
            col: self._string_to_dtype(dtype_str) for col, dtype_str in data.items()
        }

    def validate(self, current_schema: Dict[str, Any]):
        """Strict validation against the locked contract."""
        reference = self.load_schema()

        if set(current_schema.keys()) != set(reference.keys()):
            missing = set(reference.keys()) - set(current_schema.keys())
            extra = set(current_schema.keys()) - set(reference.keys())
            raise ValueError(f"Schema Mismatch! Missing: {missing}, Extra: {extra}")

        # Basic type check (Optional: depends on how strict you want to be)
        for col, dtype in reference.items():
            if str(current_schema[col]) != str(dtype):
                raise TypeError(
                    f"Type Drift on {col}: Expected {dtype}, got {current_schema[col]}"
                )

    def _string_to_dtype(self, dtype_str: str) -> Any:
        """Maps common string representations to Polars DataTypes."""
        # This handles the basic types. For complex types, you'd expand this.
        return getattr(pl, dtype_str)
