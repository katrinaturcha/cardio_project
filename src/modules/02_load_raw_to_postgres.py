"""
Шаг 2. Этот модуль загружает сырой файл в PostgreSQL.
Он создаёт базу при первом запуске, приводит названия столбцов к рабочему виду и защищает таблицу от дублей.
"""

import pandas as pd
from sqlalchemy import text
from src.common.config import RAW_MERGED_CSV
from src.common.constants import COLUMN_MAPPING
from src.common.db import create_database_if_not_exists, get_engine
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
            columns_sql.append(f'{col} TEXT UNIQUE')
        else:
            columns_sql.append(f'{col} TEXT')

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
    stage_table = "raw_contracts_stage"

    with engine.begin() as conn:
        df.to_sql(stage_table, conn, if_exists="replace", index=False)

        insert_columns = [col for col in df.columns]
        select_columns = [f's.{col}' for col in df.columns]

        insert_sql = f"""
        INSERT INTO raw_contracts ({', '.join(insert_columns)})
        SELECT {', '.join(select_columns)}
        FROM {stage_table} s
        LEFT JOIN raw_contracts t ON s.hash_key = t.hash_key
        WHERE t.hash_key IS NULL;
        """

        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))


def main():
    logger.info("Начат шаг 2. Загрузка сырого слоя в БД")
    # create_database_if_not_exists()
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
