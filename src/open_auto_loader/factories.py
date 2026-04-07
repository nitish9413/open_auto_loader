from open_auto_loader.exceptions import UnsupportedFormatError

from .core.reader import CSVReader, FormatReader, NDJsonReader, ParquetReader


class ReaderFactory:
    _STRATEGY_MAP: dict[str, type[FormatReader]] = {
        "csv": CSVReader,
        "parquet": ParquetReader,
        "ndjson": NDJsonReader,
        "jsonl": NDJsonReader,
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
        # 2. Updated to use the correct variable name
        if target not in cls._STRATEGY_MAP:
            raise UnsupportedFormatError(
                f"Unsupported format: {format_name}",
                format_type=format_name,
                supported_formats=list(cls._STRATEGY_MAP.keys()),
            )
        return cls._STRATEGY_MAP[target]()

    @classmethod
    def get_reader_by_extension(cls, extension: str) -> FormatReader:
        # Handles both "csv" and ".csv" inputs
        ext = (
            extension.lower() if extension.startswith(".") else f".{extension.lower()}"
        )

        format_name = cls._EXTENSION_MAP.get(ext)

        if not format_name:
            raise ValueError(f"No reader registered for extension: {extension}")

        return cls.get_reader_by_format(format_name)
