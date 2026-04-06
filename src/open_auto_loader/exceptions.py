"""
open_auto_loader.exceptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Full exception hierarchy for the open_auto_loader library.

Catch-all:
    except OpenAutoLoaderError: ...

Specific:
    except SchemaDriftError: ...
    except (SchemaDriftError, SchemaMismatchError): ...

All exceptions carry a .context dict with structured data for logging:
    except SchemaDriftError as e:
        print(e.context["column"])
        print(e.context["file_path"])
"""

from __future__ import annotations

from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Base
# ──────────────────────────────────────────────────────────────────────────────


class OpenAutoLoaderError(Exception):
    """
    Base class for all open_auto_loader exceptions.

    All subclasses inherit .context — a dict of structured metadata
    that can be logged, serialised, or inspected programmatically.

    Usage:
        raise SomethingError("human message", key=value, ...) from original_exc

    Catching everything from this library:
        except OpenAutoLoaderError as e:
            logger.error(str(e))
            report(e.context)
    """

    def __init__(self, message: str, **context: Any) -> None:
        self.context: dict[str, Any] = context
        super().__init__(message)

    def __str__(self) -> str:
        base = super().__str__()
        if not self.context:
            return base
        ctx = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
        return f"{base} [{ctx}]"

    def __repr__(self) -> str:
        ctx = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
        return f"{self.__class__.__name__}({super().__str__()!r}, {ctx})"


# ──────────────────────────────────────────────────────────────────────────────
# Configuration errors
# Raised during OpenAutoLoader.__init__ — bad inputs at construction time
# ──────────────────────────────────────────────────────────────────────────────


class ConfigurationError(OpenAutoLoaderError):
    """
    Raised when OpenAutoLoader is constructed with invalid arguments.

    context keys:
        field       str   — name of the invalid parameter
        value       Any   — value that was rejected
        reason      str   — why it was rejected
    """


class ReservedMetadataKeyError(ConfigurationError):
    """
    Raised when a user-supplied metadata key collides with an
    internal audit column (_batch_id, _processed_at, _file_path).

    context keys:
        key             str       — the offending key
        reserved_keys   list[str] — full list of reserved names
    """


# ──────────────────────────────────────────────────────────────────────────────
# File discovery errors
# Raised inside FileScanner
# ──────────────────────────────────────────────────────────────────────────────


class FileDiscoveryError(OpenAutoLoaderError):
    """
    Raised when the scanner cannot list or access the source path.

    context keys:
        source_path   str — the path that could not be scanned
        protocol      str — e.g. "s3", "abfss", "gs", "file"
    """


class MissingDriverError(FileDiscoveryError):
    """
    Raised when the required cloud filesystem driver is not installed.

    context keys:
        protocol        str — e.g. "s3"
        install_command str — e.g. "pip install s3fs"
    """


# ──────────────────────────────────────────────────────────────────────────────
# Schema errors
# Raised inside SchemaManager / LocalJsonSchemaStore
# ──────────────────────────────────────────────────────────────────────────────


class SchemaError(OpenAutoLoaderError):
    """
    Base for all schema-related errors.
    Catch this to handle any schema problem without caring which kind.

    context keys:
        file_path   str — file being validated (when applicable)
    """


class SchemaDriftError(SchemaError):
    """
    Raised when a file's column has a different type than the locked contract.
    The batch is aborted to protect target table integrity.

    context keys:
        column          str — name of the drifted column
        expected_type   str — type in the locked contract e.g. "Int64"
        actual_type     str — type found in the current file e.g. "Utf8"
        file_path       str — file that triggered the drift
    """


class SchemaMismatchError(SchemaError):
    """
    Raised when a file's column names don't match the locked contract —
    columns are missing, extra, or renamed.

    context keys:
        missing_columns  list[str] — in contract but not in file
        extra_columns    list[str] — in file but not in contract
        file_path        str
    """


class SchemaSerializationError(SchemaError):
    """
    Raised when a schema cannot be saved to or loaded from the JSON contract.
    Common cause: complex Polars types (Datetime with tz, List, Struct)
    that cannot round-trip through str → getattr(pl, ...).

    context keys:
        column      str — column with the problematic type (when applicable)
        dtype_str   str — string that could not be deserialised
    """


# ──────────────────────────────────────────────────────────────────────────────
# Checkpoint errors
# Raised inside CheckpointStore implementations
# ──────────────────────────────────────────────────────────────────────────────


class CheckpointError(OpenAutoLoaderError):
    """
    Raised when the checkpoint backend is inaccessible, corrupted,
    or returns unexpected results.

    context keys:
        checkpoint_path   str — path to the checkpoint db / connection string
    """


class DuplicateFileError(CheckpointError):
    """
    Raised when mark_processed is called for a file that is already
    in the checkpoint. Should not happen in normal operation —
    indicates a logic bug in the orchestrator.

    context keys:
        file_path   str
        batch_id    str
    """


# ──────────────────────────────────────────────────────────────────────────────
# Engine errors
# Raised inside PolarsEngine (read / enrich / sink)
# ──────────────────────────────────────────────────────────────────────────────


class EngineError(OpenAutoLoaderError):
    """
    Raised when the engine fails to read, transform, or write data.

    context keys:
        file_path     str — file being processed (when applicable)
        target_path   str — Delta table path (when applicable)
    """


class UnsupportedTableTypeError(EngineError):
    """
    Raised when table_type is not supported by the current engine.

    context keys:
        table_type          str       — value that was passed
        supported_types     list[str] — what this engine supports
    """


# ──────────────────────────────────────────────────────────────────────────────
# Format / reader errors
# Raised inside FormatRegistry and reader implementations
# ──────────────────────────────────────────────────────────────────────────────


class FormatError(OpenAutoLoaderError):
    """
    Base for format and reader related errors.
    """


class UnsupportedFormatError(FormatError):
    """
    Raised when format_type is not registered in FormatRegistry.

    context keys:
        format_type       str       — value that was passed
        supported_formats list[str] — registered format names
    """


class UnsupportedExtensionError(FormatError):
    """
    Raised when a file extension cannot be mapped to any reader.
    Only relevant when format_type="auto".

    context keys:
        extension           str       — e.g. ".xlsx"
        supported_extensions list[str] — all registered extensions
    """


# ──────────────────────────────────────────────────────────────────────────────
# Batch errors
# Raised inside OpenAutoLoader.run()
# ──────────────────────────────────────────────────────────────────────────────


class BatchError(OpenAutoLoaderError):
    """
    Base for errors that occur at the batch / run() level.

    context keys:
        batch_id   str
    """


class BatchAbortedError(BatchError):
    """
    Raised when run() stops early because the number of failed files
    reached the max_failures threshold set in RunConfig.

    context keys:
        batch_id        str
        failed_count    int — how many files failed before abort
        max_failures    int — the threshold that was set
        failed_files    list[str] — paths of all failed files
    """
