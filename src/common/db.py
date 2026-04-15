from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

from src.common.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_SYSTEM_DB,
    POSTGRES_USER,
)

class DatabaseManager:
    """Класс для работы с PostgreSQL."""

    def __init__(
        self,
        user: str = POSTGRES_USER,
        password: str = POSTGRES_PASSWORD,
        host: str = POSTGRES_HOST,
        port: str = POSTGRES_PORT,
        app_db: str = POSTGRES_DB,
        system_db: str = POSTGRES_SYSTEM_DB,
    ) -> None:
        self.user = user
        self.password = password
        self.host = host
        self.port = int(port)
        self.app_db = app_db
        self.system_db = system_db

    def _build_url(self, database: str) -> URL:
        return URL.create(
            drivername="postgresql+psycopg2",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=database,
        )

    def get_system_engine(self) -> Engine:
        return create_engine(self._build_url(self.system_db), isolation_level="AUTOCOMMIT")

    def get_app_engine(self) -> Engine:
        return create_engine(self._build_url(self.app_db))

    def create_database_if_not_exists(self) -> None:
        engine = self.get_system_engine()
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                {"db_name": self.app_db},
            ).scalar()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{self.app_db}"'))


def get_system_engine() -> Engine:
    return DatabaseManager().get_system_engine()


def get_engine() -> Engine:
    return DatabaseManager().get_app_engine()


def create_database_if_not_exists() -> None:
    DatabaseManager().create_database_if_not_exists()
