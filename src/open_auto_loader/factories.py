from typing import Type

from .reader import CSVReader, FormatReader, NDJsonReader, ParquetReader


class ReaderFactory:
    _STATERGY_MAP: dict[str, Type[FormatReader]] = {
        "csv": CSVReader,
        "parquet": ParquetReader,
        "ndjson": NDJsonReader,
    }

    _EXTENSION_MAP = {
        ".csv": "csv",
        ".parquet": "parquet",
        ".pq": "parquet",
        ".ndjson": "ndjson",
        ".jsonl": "ndjson",
    }

    @classmethod
    def get_reader_by_format(cls, format_name: str) -> FormatReader:
        target = format_name.lower()
        if target not in cls._STATERGY_MAP:
            raise ValueError(
                f"Unsupported format: {format_name}. Supported: {list(cls._STRATEGY_MAP.keys())}"
            )
        return cls._STATERGY_MAP[target]()

    @classmethod
    def get_reader_by_extension(cls, extension: str) -> FormatReader:
        ext = (
            extension.lower() if extension.startswith(".") else f".{extension.lower()}"
        )

        format_name = cls._EXTENSION_MAP.get(ext)

        if not format_name:
            raise ValueError(f"No reader registered for extension: {extension}")

        return cls.get_reader_by_format(format_name)
