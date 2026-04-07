import logging

from open_auto_loader.exceptions import SchemaMismatchError

from .configs import get_storage_config
from .configs.schema import SchemaEvolutionMode
from .configs.storage import StorageConfig
from .core.engine import PolarsEngine
from .core.scanner import FileScanner
from .core.schema import SchemaManager
from .core.state import CheckPointManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OpenAutoLoader:
    """
    The main orchestrator for incremental data loading.

    Attributes:
        source (str): The source path (Local or Cloud URL).
        target (str): The target Delta Lake path.
        format_type (str): Format of source files (csv, parquet, ndjson).
    """

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
        evolution_mode: SchemaEvolutionMode = SchemaEvolutionMode.FAIL_ON_NEW_COLUMNS,
    ):
        self.source = source
        self.target = target
        self.check_point = check_point
        self.schema_path = schema_path
        self.format_type = format_type
        self.evolution_mode = evolution_mode

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
        """
        Executes the ingestion loop for all new files.

        Args:
            batch_id: A unique identifier for this processing run.
        """
        logger.info("Starting OpenAutoLoader batch", extra={"batch_id": batch_id})

        new_files = self.file_scanner.get_eligible_files(self.check_point_manager)

        if not new_files:
            logger.info("No new files found to process. Exiting gracefully.")
            return

        if not self.schema_manager.schema_exists():
            logger.info("No schema found. Bootstrapping...")
            inferred_schema = self.engine.get_inferred_schema(new_files[0])
            self.schema_manager.save_schema(inferred_schema)

        locked_schema = self.schema_manager.load_schema()

        for file_path in new_files:
            current_file_schema = self.engine.get_inferred_schema(file_path)
            diffs = self.schema_manager.check_differences(current_file_schema)

            columns_to_write = None
            if self.evolution_mode == SchemaEvolutionMode.NONE:
                columns_to_write = list(locked_schema.keys())

            if diffs["extra"]:
                if self.evolution_mode == SchemaEvolutionMode.FAIL_ON_NEW_COLUMNS:
                    raise SchemaMismatchError(
                        "New Columns detected",
                        extra_columns=diffs["extra"],
                        file_path=str(file_path),
                    )

                elif self.evolution_mode == SchemaEvolutionMode.ADD_NEW_COLUMNS:
                    logger.info(f"Evolving schema: adding {diffs['extra']}")
                    self.schema_manager.evolve_schema(current_file_schema)
                    locked_schema = self.schema_manager.load_schema()
                    columns_to_write = None

                elif self.evolution_mode == SchemaEvolutionMode.NONE:
                    logger.warning(f"Ignoring extra columns: {diffs['extra']}")

            self.schema_manager.validate(
                current_schema=current_file_schema,
                evolution_mode=self.evolution_mode,
                file_path=str(file_path),
            )

            try:
                self.engine.process_single_file(
                    file_path=file_path,
                    schema_dict=locked_schema,
                    batch_id=batch_id,
                    metadata=self.metadata,
                    evolution_mode=self.evolution_mode,
                    selected_columns=columns_to_write,
                )

                self.check_point_manager.mark_processed(file_path, batch_id=batch_id)
                logger.info("Processed successfully", extra={"file_path": file_path})

            except Exception as e:
                logger.exception(
                    "Operational failure during ingestion",
                    extra={"file_path": file_path, "error": str(e)},
                )
                raise

        logger.info("Batch completed.", extra={"batch_id": batch_id})
