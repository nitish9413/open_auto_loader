from pathlib import Path

class FileScanner:
    def __init__(self, source_dir: str, extension: str):
        self.source_dir = Path(source_dir).resolve()
        self.extension = f".{extension.lower()}" if not extension.startswith(".") else extension.lower()

    def _list_all_files(self):
        """Finds non-hidden, non-empty files recursively"""
        eligible = []
        for file in self.source_dir.rglob(f"*{self.extension}"):
            if not file.name.startswith(".") and file.stat().st_size > 0:
                eligible.append(file)
        return eligible

    def get_eligible_files(self, checkpoint_manager):
        """Filters discovered files against the provided checkpoint manager"""
        all_paths = [str(file.resolve()) for file in self._list_all_files()]

        return checkpoint_manager.filter_new_files(all_paths)
