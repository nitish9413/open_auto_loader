from typing import Any, Protocol

import polars as pl


class FormatReader(Protocol):
    def scan(self, file_path: str, **kwargs) -> pl.LazyFrame: ...

    def get_schema(self, file_path: str, **kwargs) -> dict[str, Any]: ...


class CSVReader:
    def scan(self, file_path: str, **kwargs) -> pl.LazyFrame:
        return pl.scan_csv(file_path, **kwargs)

    def get_schema(self, file_path: str, **kwargs) -> dict[str, Any]:
        # collect_schema() is fine for CSV as it has to scan to find headers anyway
        return pl.scan_csv(file_path, **kwargs).collect_schema()


class ParquetReader:
    def scan(self, file_path: str, **kwargs) -> pl.LazyFrame:
        return pl.scan_parquet(file_path, **kwargs)

    def get_schema(self, file_path: str, **kwargs) -> dict[str, Any]:
        # Optimization: Use the specialized metadata reader for Parquet
        return pl.read_parquet_schema(file_path)


class NDJsonReader:
    def scan(self, file_path: str, **kwargs) -> pl.LazyFrame:
        return pl.scan_ndjson(file_path, **kwargs)

    def get_schema(self, file_path: str, **kwargs) -> dict[str, Any]:
        return pl.scan_ndjson(file_path, **kwargs).collect_schema()
