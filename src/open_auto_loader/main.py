import logging

from .engine import PolarsEngine
from .scanner import FileScanner
from .schema import SchemaManager
from .state import CheckPointManager

# Setup logging to ensure Airflow users can see progress in task logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OpenAutoLoader:
    def __init__(
        self,
        source: str,
        target: str,
        check_point: str,
        schema_path: str,
        format_type: str = "csv",
        table_type: str = "delta",
    ):
        self.source = source
        self.target = target
        self.check_point = check_point
        self.schema_path = schema_path
        self.format_type = format_type

        # Initialize the sub-modules
        self.check_point_manager = CheckPointManager(check_point)
        self.schema_manager = SchemaManager(schema_path)
        # Fix: Using 'self.engine' consistently makes the run method cleaner
        self.engine = PolarsEngine(
            target_path=target, format_type=format_type, table_type=table_type
        )
        self.file_scanner = FileScanner(source, format_type)

    def run(self, batch_id: str):
        """
        Executes a single batch load.
        """
        logger.info(f"Starting OpenAutoLoader batch: {batch_id}")

        # 1. Discovery Phase
        new_files = self.file_scanner.get_eligible_files(self.check_point_manager)

        if not new_files:
            logger.info("No new files found to process. Exiting gracefully.")
            return

        logger.info(f"Found {len(new_files)} new files to process.")

        # 2. Schema Locking Phase
        if not self.schema_manager.schema_exists():
            logger.info("No schema found. Bootstrapping from first file...")
            # Fix: Calling 'self.engine' to match initialization
            inferred_schema = self.engine.get_inferred_schema(new_files[0])
            self.schema_manager.save_schema(inferred_schema)
            logger.info(f"Schema locked in {self.schema_manager.schema_file}")

        locked_schema = self.schema_manager.load_schema()

        # 3. Processing Phase (Atomic Loop)
        for file_path in new_files:
            try:
                logger.info(f"Processing: {file_path}")

                # 1. Validate the individual file's schema against the contract
                current_file_schema = self.engine.get_inferred_schema(file_path)
                self.schema_manager.validate(current_file_schema)

                # 2. Trigger the Engine to process and sink
                self.engine.process_single_file(
                    file_path=file_path, schema_dict=locked_schema, batch_id=batch_id
                )

                # 3. Mark as processed ONLY after successful engine sink
                # Fix: Using 'self.check_point_manager' instead of 'self.state'
                self.check_point_manager.mark_processed(file_path, batch_id=batch_id)
                logger.info(f"Successfully processed and checkpointed: {file_path}")

            except Exception as e:
                logger.error(f"Failed to process {file_path}: {str(e)}")
                raise e

        logger.info(f"Batch {batch_id} completed successfully.")
