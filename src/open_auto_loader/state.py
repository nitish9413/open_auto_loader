from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from sqlalchemy import create_engine, Column, String, DateTime, Text, select
from sqlalchemy.orm import sessionmaker, declarative_base
import hashlib

Base = declarative_base()


class ProcessedFile(Base):
    __tablename__ = "processed_files"

    path_hash = Column(String(64), primary_key=True)
    original_path = Column(Text, nullable=False)
    batch_id = Column(String(100), nullable=False)
    processed_at = Column(DateTime, default=datetime.now(timezone.utc))


class CheckPointManager:
    def __init__(self, check_point_path: str):
        self.check_point_path = check_point_path
        # Use the resolver to get paths and ensure the .db file exists
        self.abs_dir, self.db_file = self._resolve_checkpoint_dir()

        # Connect to SQLite
        self.engine = create_engine(f"sqlite:///{self.db_file}")

        # CRITICAL: Bind the engine here so the table is actually created
        Base.metadata.create_all(bind=self.engine)

        self.SessionFactory = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def _resolve_checkpoint_dir(self):
        raw_path = Path(self.check_point_path)
        # Standardizes the location to /user/path/checkpoint/
        abs_path = raw_path.resolve() / "checkpoint"
        abs_path.mkdir(parents=True, exist_ok=True)

        db_file = abs_path / "metadata.db"
        if not db_file.exists():
            db_file.touch()

        return abs_path, db_file

    def get_session(self):
        return self.SessionFactory()

    def _get_hash(self, path: str):
        # Convert path to string, encode to bytes, return sha256 hexdigest
        return hashlib.sha256(path.encode()).hexdigest()

    def filter_new_files(self, file_paths: List[str]):
        path_map = {self._get_hash(p): p for p in file_paths}
        incoming_hashes = list(path_map.keys())

        with self.get_session() as session:
            stmt = select(ProcessedFile.path_hash).where(
                ProcessedFile.path_hash.in_(incoming_hashes)
            )
            existing_hashes = session.execute(stmt).scalars().all()

        return [path_map[h] for h in incoming_hashes if h not in existing_hashes]

    def mark_processed(self, file_path: str, batch_id: Optional[str] = None):
        processed_file = ProcessedFile(
            path_hash=self._get_hash(file_path),
            original_path=file_path,
            batch_id=batch_id,
        )

        session = self.get_session()
        session.add(processed_file)
        session.commit()
