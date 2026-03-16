from pathlib import Path
from typing import TYPE_CHECKING, List

# Using TYPE_CHECKING to avoid circular imports during type hinting
if TYPE_CHECKING:
    from .state import CheckPointManager


class FileScanner:
    """
    Handles discovery of new files in the source directory.
    Supports recursive scanning, hidden file exclusion, and format-specific extensions.
    """

    # Mapping formal formats to sets of valid file extensions
    FORMAT_MAP = {
        "csv": {".csv", ".txt"},
        "parquet": {".parquet", ".pq"},
        "ndjson": {".ndjson", ".jsonl"},
    }

    def __init__(self, source_dir: str, format_type: str = "csv"):
        self.source_dir = Path(source_dir).resolve()

        if not self.source_dir.exists():
            raise FileNotFoundError(
                f"Source directory does not exist: {self.source_dir}"
            )

        # Get extensions for the chosen format, default to the format name itself if not in map
        self.valid_extensions = self.FORMAT_MAP.get(
            format_type.lower(), {f".{format_type.lower()}"}
        )

    def _list_all_files(self) -> List[Path]:
        """
        Scans source_dir recursively for non-hidden, non-empty files
        matching the valid extensions.
        """
        eligible = []

        # Iterate through each valid extension (e.g., .parquet and .pq)
        for ext in self.valid_extensions:
            # rglob enables recursive search through subdirectories
            for file in self.source_dir.rglob(f"*{ext}"):
                if (
                    not file.name.startswith((".", "_"))  # Skip hidden/metadata files
                    and file.is_file()  # Ensure it's not a directory
                    and file.stat().st_size > 0  # Skip empty files
                ):
                    eligible.append(file)

        # Deduplicate (in case a file matches multiple patterns) and
        # Sort by modification time to maintain chronological order
        unique_eligible = list({str(f.resolve()): f for f in eligible}.values())
        unique_eligible.sort(key=lambda x: x.stat().st_mtime)

        return unique_eligible

    def get_eligible_files(self, checkpoint_manager: "CheckPointManager") -> List[str]:
        """
        Discovers files and filters them against the CheckPointManager
        to return only those that haven't been processed yet.
        """
        # Convert Path objects to absolute strings for the Checkpoint Manager
        all_paths = [str(file.resolve()) for file in self._list_all_files()]

        # Delegate to the state manager to filter out already-processed files
        return checkpoint_manager.filter_new_files(all_paths)
