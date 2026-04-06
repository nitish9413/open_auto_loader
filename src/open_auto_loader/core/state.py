from datetime import UTC, datetime
import hashlib
from pathlib import Path

from sqlalchemy import Column, DateTime, String, Text, create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class ProcessedFile(Base):
    __tablename__ = "processed_files"

    path_hash = Column(String(64), primary_key=True)
    original_path = Column(Text, nullable=False)
    batch_id = Column(String(100), nullable=False)
    processed_at = Column(DateTime, default=lambda: datetime.now(UTC))


class CheckPointManager:
    def __init__(self, check_point_path: str):
        self.check_point_path = check_point_path
        self.abs_dir, self.db_file = self._resolve_checkpoint_dir()

        # Connect to SQLite with isolation_level to ensure thread safety during writes
        self.engine = create_engine(
            f"sqlite:///{self.db_file}", isolation_level="SERIALIZABLE"
        )
        Base.metadata.create_all(bind=self.engine)

        self.SessionFactory = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def _resolve_checkpoint_dir(self):
        raw_path = Path(self.check_point_path)
        abs_path = raw_path.resolve() / "checkpoint"
        abs_path.mkdir(parents=True, exist_ok=True)

        db_file = abs_path / "metadata.db"
        if not db_file.exists():
            db_file.touch()

        return abs_path, db_file

    def get_session(self):
        return self.SessionFactory()

    def _get_hash(self, path: str):
        return hashlib.sha256(path.encode()).hexdigest()

    def filter_new_files(self, file_paths: list[str]):
        if not file_paths:
            return []

        path_map = {self._get_hash(p): p for p in file_paths}
        incoming_hashes = list(path_map.keys())

        with self.get_session() as session:
            stmt = select(ProcessedFile.path_hash).where(
                ProcessedFile.path_hash.in_(incoming_hashes)
            )
            existing_hashes = session.execute(stmt).scalars().all()

        return [path_map[h] for h in incoming_hashes if h not in existing_hashes]

    def mark_processed(self, file_path: str, batch_id: str | None = None):
        """Commits the file record to SQLite to prevent double-processing."""
        bid = batch_id or f"manual_{datetime.now().strftime('%Y%m%d%H%M')}"

        processed_file = ProcessedFile(
            path_hash=self._get_hash(file_path),
            original_path=file_path,
            batch_id=bid,
        )

        with self.get_session() as session:
            session.add(processed_file)
            session.commit()
