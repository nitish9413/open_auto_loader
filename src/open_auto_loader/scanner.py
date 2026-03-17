from pathlib import Path
from typing import TYPE_CHECKING, List

import fsspec

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

    def __init__(
        self, source_dir: str, format_type: str = "csv", storage_options: dict = None
    ):
        self.source_raw = source_dir
        self.storage_options = storage_options

        self.protocol = source_dir.split("://")[0] if "://" in source_dir else "file"

        try:
            self.fs = fsspec.filesystem(self.protocol, **self.storage_options)
        except ImportError:
            raise ImportError(
                f"Protocol '{self.protocol}' requires an extra dependency. "
                f"Please install it using: pip install open-auto-loader[{self.protocol}]"
            )

        if self.protocol == "file":
            self.source_path = str(Path(source_dir).resolve()).replace("\\", "/")
        else:
            self.source_path = source_dir.split("://")[-1].rstrip("/")

        self.valid_extensions = self.FORMAT_MAP.get(
            format_type.lower(), {f".{format_type.lower()}"}
        )

    def _list_all_files(self) -> List[str]:
        """Scans source_dir recursively using fsspec find."""
        eligible = []

        try:
            all_paths = self.fs.find(self.source_path)

            for path in all_paths:
                path_str = str(path)

                if any(path_str.endswith(ext) for ext in self.valid_extensions):
                    file_name = path_str.split("/")[-1]

                    if not file_name.startswith((".", "_")):
                        eligible.append(self._ensure_protocol(path_str))

        except Exception as e:
            raise RuntimeError(f"Failed to scan {self.protocol} storage: {e}")

        eligible.sort()
        return eligible

    def _ensure_protocol(self, path: str) -> list[str]:
        if "://" not in path and self.protocol != "file":
            return f"{self.protocol}://{path}"
        return path

    def get_eligible_files(self, checkpoint_manager: "CheckPointManager") -> List[str]:
        """
        Discovers files and filters them against the CheckPointManager
        to return only those that haven't been processed yet.
        """
        all_paths = self._list_all_files()

        return checkpoint_manager.filter_new_files(all_paths)
