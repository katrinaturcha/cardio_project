from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "onkron1603"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"

POSTGRES_SYSTEM_DB = "postgres"
APP_DB = "zakupki"


def get_system_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        database=POSTGRES_SYSTEM_DB,
    )
    return create_engine(url, isolation_level="AUTOCOMMIT")


def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        database=APP_DB,
    )
    return create_engine(url)


def create_database_if_not_exists():
    engine = get_system_engine()
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
            {"db_name": APP_DB},
        ).scalar()

        if not exists:
            conn.execute(text(f'CREATE DATABASE "{APP_DB}"'))
