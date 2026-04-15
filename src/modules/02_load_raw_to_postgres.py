"""
Шаг 2. Этот модуль загружает сырой файл в PostgreSQL.
Он приводит названия столбцов к рабочему виду и защищает таблицу от дублей.
"""

from __future__ import annotations

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.base import BasePipelineStep
from src.common.config import RAW_MERGED_CSV
from src.common.constants import COLUMN_MAPPING
from src.common.db import DatabaseManager
from src.common.helpers import add_hash_key, read_csv_with_fallback


class RawContractsLoader(BasePipelineStep):
    logger_name = "module_02_load_raw"
    log_file_name = "module_02_load_raw.log"

    def __init__(self) -> None:
        super().__init__()
        self.raw_file_path = RAW_MERGED_CSV
        self.db_manager = DatabaseManager()
        self.db_manager.create_database_if_not_exists()
        self.engine = self.db_manager.get_app_engine()

    @staticmethod
    def prepare_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        prepared = df.copy()
        prepared = prepared.loc[:, ~prepared.columns.duplicated()].copy()

        renamed_columns: dict[str, str] = {}
        extra_counter = 1
        for column in prepared.columns:
            if column in COLUMN_MAPPING:
                renamed_columns[column] = COLUMN_MAPPING[column]
            else:
                renamed_columns[column] = f"extra_col_{extra_counter}"
                extra_counter += 1

        prepared = prepared.rename(columns=renamed_columns)
        for column in prepared.columns:
            prepared[column] = prepared[column].fillna("").astype(str)

        prepared = add_hash_key(prepared)
        prepared = prepared.drop_duplicates(subset=["hash_key"]).reset_index(drop=True)
        return prepared

    @staticmethod
    def create_raw_table_if_not_exists(engine: Engine, df: pd.DataFrame) -> None:
        columns_sql = []
        for column in df.columns:
            if column == "hash_key":
                columns_sql.append(f'"{column}" TEXT UNIQUE')
            else:
                columns_sql.append(f'"{column}" TEXT')

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS raw_contracts (
            id BIGSERIAL PRIMARY KEY,
            {', '.join(columns_sql)},
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        with engine.begin() as conn:
            conn.execute(text(create_sql))

    @staticmethod
    def insert_without_duplicates(engine: Engine, df: pd.DataFrame) -> None:
        insert_columns = list(df.columns)
        quoted_columns = ", ".join(f'"{column}"' for column in insert_columns)
        values = [tuple(row[column] for column in insert_columns) for _, row in df.iterrows()]

        insert_sql = f"""
        INSERT INTO raw_contracts ({quoted_columns})
        VALUES %s
        ON CONFLICT (hash_key) DO NOTHING;
        """

        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                execute_values(cur, insert_sql, values, page_size=1000)
            raw_conn.commit()
        finally:
            raw_conn.close()

    def run(self) -> None:
        self.logger.info("Начат шаг 2. Загрузка сырого слоя в БД")
        raw_df = read_csv_with_fallback(self.raw_file_path)
        self.log_rows("Прочитан сырой файл", raw_df)

        prepared_df = self.prepare_raw_dataframe(raw_df)
        self.log_rows("После подготовки", prepared_df)

        self.create_raw_table_if_not_exists(self.engine, prepared_df)
        self.insert_without_duplicates(self.engine, prepared_df)
        self.logger.info("Шаг 2 завершён")


def main() -> None:
    RawContractsLoader().run()


if __name__ == "__main__":
    main()
