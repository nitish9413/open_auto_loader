import logging

from .configs import get_storage_config
from .configs.storage import StorageConfig
from .engine import PolarsEngine
from .scanner import FileScanner
from .schema import SchemaManager
from .state import CheckPointManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OpenAutoLoader:
    _RESERVED_KEYS: set[str] = {"_file_path", "_processed_at", "_batch_id"}

    def __init__(
        self,
        source: str,
        target: str,
        check_point: str,
        schema_path: str,
        format_type: str = "csv",
        table_type: str = "delta",
        storage_config: StorageConfig | dict | None = None,
        metadata: dict | None = None,
    ):
        self.source = source
        self.target = target
        self.check_point = check_point
        self.schema_path = schema_path
        self.format_type = format_type

        # 1. Metadata setup & Validation
        self.metadata = metadata or {}
        self._validate_metadata()

        # 2. Standardize Storage Config
        if isinstance(storage_config, dict):
            self.config = get_storage_config(source, storage_config)
        else:
            self.config = storage_config

        # 3. Extract options for internal modules
        self.storage_options = (
            self.config.get_options() if hasattr(self.config, "get_options") else {}
        )

        # 4. Initialize Managers
        self.check_point_manager = CheckPointManager(check_point)
        self.schema_manager = SchemaManager(schema_path)

        # 5. Initialize Cloud-Aware Modules
        self.engine = PolarsEngine(
            target_path=target,
            format_type=format_type,
            table_type=table_type,
            storage_options=self.storage_options,
        )
        self.file_scanner = FileScanner(
            source, format_type, storage_options=self.storage_options
        )

    def _validate_metadata(self) -> None:
        """Ensures user metadata doesn't collide with internal audit columns."""
        for key in self.metadata:
            if key in self._RESERVED_KEYS:
                raise ValueError(
                    f"Forbidden key '{key}' found in metadata. "
                    f"Reserved keys are: {list(self._RESERVED_KEYS)}"
                )

    def run(self, batch_id: str) -> None:
        logger.info("Starting OpenAutoLoader batch", extra={"batch_id": batch_id})

        new_files = self.file_scanner.get_eligible_files(self.check_point_manager)

        if not new_files:
            logger.info("No new files found to process. Exiting gracefully.")
            return

        # Bootstrap Schema
        if not self.schema_manager.schema_exists():
            logger.info("No schema found. Bootstrapping...")
            inferred_schema = self.engine.get_inferred_schema(new_files[0])
            self.schema_manager.save_schema(inferred_schema)

        locked_schema = self.schema_manager.load_schema()

        for file_path in new_files:
            try:
                # Schema Enforcement
                current_file_schema = self.engine.get_inferred_schema(file_path)
                self.schema_manager.validate(current_file_schema)

                # Incremental Process
                self.engine.process_single_file(
                    file_path=file_path,
                    schema_dict=locked_schema,
                    batch_id=batch_id,
                    metadata=self.metadata,
                )

                # Commit State
                self.check_point_manager.mark_processed(file_path, batch_id=batch_id)
                logger.info("Processed:", extra={"file_path": file_path})

            except Exception as e:
                # Use logger.exception to automatically capture the stack trace
                logger.exception(
                    "Failed processing file",
                    extra={"file_path": file_path, "e": str(e)},
                )
                raise

        logger.info("Batch completed.", extra={"batch_id": batch_id})
