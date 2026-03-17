import logging

from .engine import PolarsEngine
from .scanner import FileScanner
from .schema import SchemaManager
from .state import CheckPointManager

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
        storage_options: dict = None,
    ):
        self.source = source
        self.target = target
        self.check_point = check_point
        self.schema_path = schema_path
        self.format_type = format_type
        raw_options = storage_options or {}

        self.scanner_options = self._get_scanner_options(source, raw_options)
        self.engine_options = self._get_engine_options(source, raw_options)

        # 1. Initialize Checkpoint and Schema (These stay local for now)
        self.check_point_manager = CheckPointManager(check_point)
        self.schema_manager = SchemaManager(schema_path)

        # 2. Initialize Cloud-Aware Modules
        self.engine = PolarsEngine(
            target_path=target,
            format_type=format_type,
            table_type=table_type,
            storage_options=self.engine_options,
        )
        self.file_scanner = FileScanner(
            source, format_type, storage_options=self.scanner_options
        )

    def _get_scanner_options(self, source: str, options: dict) -> dict:
        """Filters options so only Python-friendly keys remain."""
        opt = options.copy()
        if source.startswith("s3://"):
            if "aws_access_key_id" in opt:
                opt["key"] = opt.pop("aws_access_key_id")
            if "aws_secret_access_key" in opt:
                opt["secret"] = opt.pop("aws_secret_access_key")
            if "aws_region" in opt:
                opt.setdefault("client_kwargs", {})["region_name"] = opt.pop(
                    "aws_region"
                )

            opt.pop("region", None)
        return opt

    def _get_engine_options(self, source: str, options: dict) -> dict:
        """Ensures Polars gets its specific 'aws_' keys."""
        opt = options.copy()
        if source.startswith("s3://"):
            if "key" in opt:
                opt["aws_access_key_id"] = opt.get("key")
            if "secret" in opt:
                opt["aws_secret_access_key"] = opt.get("secret")
            if "aws_region" in opt:
                opt["region"] = opt.get("aws_region")
        return opt

    def run(self, batch_id: str):
        logger.info(f"Starting OpenAutoLoader batch: {batch_id}")

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
            try:
                current_file_schema = self.engine.get_inferred_schema(file_path)
                self.schema_manager.validate(current_file_schema)

                self.engine.process_single_file(
                    file_path=file_path, schema_dict=locked_schema, batch_id=batch_id
                )

                self.check_point_manager.mark_processed(file_path, batch_id=batch_id)
                logger.info(f"Processed: {file_path}")

            except Exception as e:
                logger.error(f"Failed {file_path}: {str(e)}")
                raise e

        logger.info(f"Batch {batch_id} completed.")
