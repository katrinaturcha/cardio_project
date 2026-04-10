"""
Шаг 2. Этот модуль загружает сырой файл в PostgreSQL.
Он приводит названия столбцов к рабочему виду и защищает таблицу от дублей.
"""

import pandas as pd
from sqlalchemy import text
from psycopg2.extras import execute_values

from src.common.config import RAW_MERGED_CSV
from src.common.constants import COLUMN_MAPPING
from src.common.db import get_engine
from src.common.helpers import add_hash_key, read_csv_with_fallback
from src.common.logging_utils import get_logger

logger = get_logger("module_02_load_raw", "module_02_load_raw.log")


def prepare_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.loc[:, ~df.columns.duplicated()].copy()

    renamed_columns = {}
    extra_counter = 1
    for col in df.columns:
        if col in COLUMN_MAPPING:
            renamed_columns[col] = COLUMN_MAPPING[col]
        else:
            renamed_columns[col] = f"extra_col_{extra_counter}"
            extra_counter += 1

    df = df.rename(columns=renamed_columns)

    for col in df.columns:
        df[col] = df[col].fillna("").astype(str)

    df = add_hash_key(df)
    df = df.drop_duplicates(subset=["hash_key"]).reset_index(drop=True)
    return df


def create_raw_table_if_not_exists(engine, df: pd.DataFrame) -> None:
    columns_sql = []
    for col in df.columns:
        if col == "hash_key":
            columns_sql.append(f'"{col}" TEXT UNIQUE')
        else:
            columns_sql.append(f'"{col}" TEXT')

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS raw_contracts (
        id BIGSERIAL PRIMARY KEY,
        {', '.join(columns_sql)},
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))


def insert_without_duplicates(engine, df: pd.DataFrame) -> None:
    insert_columns = list(df.columns)
    quoted_columns = ", ".join(f'"{col}"' for col in insert_columns)

    values = [tuple(row[col] for col in insert_columns) for _, row in df.iterrows()]

    insert_sql = f"""
    INSERT INTO raw_contracts ({quoted_columns})
    VALUES %s
    ON CONFLICT (hash_key) DO NOTHING;
    """

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(
                cur,
                insert_sql,
                values,
                page_size=1000,
            )
        raw_conn.commit()
    finally:
        raw_conn.close()


def main():
    logger.info("Начат шаг 2. Загрузка сырого слоя в БД")
    engine = get_engine()

    df = read_csv_with_fallback(RAW_MERGED_CSV)
    logger.info("Прочитан сырой файл. Строк: %s", len(df))

    prepared_df = prepare_raw_dataframe(df)
    logger.info("После подготовки. Строк: %s", len(prepared_df))

    create_raw_table_if_not_exists(engine, prepared_df)
    insert_without_duplicates(engine, prepared_df)
    logger.info("Шаг 2 завершён")


if __name__ == "__main__":
    main()
