import logging
from .state import CheckPointManager
from .engine import PolarsEngine
from .scanner import FileScanner

# Setup logging to ensure Airflow users can see progress in task logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OpenAutoLoader:
    def __init__(self, source: str, target: str, check_point: str, extension: str = "csv", table_type: str = "delta"):
        """
        The main orchestrator for the OpenAutoLoader library.

        :param source: Directory path where source files arrive.
        :param target: Path to the target Delta or Iceberg table.
        :param check_point: Directory path where the metadata.db will live.
        :param extension: File extension to filter for (default: csv).
        :param table_type: Format of the sink table (default: delta).
        """
        self.source = source
        self.target = target
        self.check_point = check_point
        self.extension = extension

        # Initialize the sub-modules (Dependency Injection)
        self.check_point_manager = CheckPointManager(check_point)
        self.polars_engine = PolarsEngine(target_path=target, table_type=table_type)
        self.file_scanner = FileScanner(source, extension)

    def run(self, batch_id: str):
        """
        Executes a single batch load.
        """
        logger.info(f"Starting OpenAutoLoader batch: {batch_id}")

        # 1. Discovery Phase
        files = self.file_scanner.get_eligible_files(self.check_point_manager)

        if not files:
            logger.info("No new files found to process. Exiting gracefully.")
            return

        logger.info(f"Found {len(files)} new files to process.")

        # 2. Schema Locking Phase
        # First, try to get the schema from the existing target table
        locked_schema = self.polars_engine.get_reference_schema()

        # If the table doesn't exist, infer the schema from the first new file
        if locked_schema is not None:
            logger.info("Target table not found or empty. Inferring schema from first file.")
            locked_schema = self.polars_engine.get_schema(files[0])

        # 3. Processing Phase (Atomic Loop)
        for file_path in files:
            try:
                # Step A: Validate the file matches the locked schema
                self.polars_engine.validate_schema(file_path, locked_schema)

                # Step B: Process, add metadata, and Sink to Delta
                self.polars_engine.process_single_file(file_path, locked_schema, batch_id)

                # Step C: Update Checkpoint (Only happens if Step B succeeded)
                self.check_point_manager.mark_processed(file_path, batch_id)
                logger.info(f"Successfully processed: {file_path}")

            except Exception as e:
                logger.error(f"Failed to process {file_path}. Error: {e}")
                # We raise the exception to stop the loop and fail the Airflow task
                raise e

        logger.info(f"Batch {batch_id} completed successfully.")
