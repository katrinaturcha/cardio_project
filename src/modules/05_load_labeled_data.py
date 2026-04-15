"""
Шаг 5. Этот модуль загружает размеченный файл в БД.
Он оставляет только текст, метку и комментарий, затем пишет их в таблицу labeled_contracts.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.common.base import BasePipelineStep
from src.common.config import LABELED_FILE_PATH
from src.common.db import DatabaseManager


class LabeledContractsLoader(BasePipelineStep):
    logger_name = "module_05_load_labels"
    log_file_name = "module_05_load_labels.log"

    REQUIRED_COLUMNS = ["purchase_object_name", "label", "label_comment"]

    def __init__(self) -> None:
        super().__init__()
        self.engine = DatabaseManager().get_app_engine()
        self.file_path = LABELED_FILE_PATH

    def load_labeled_excel(self) -> pd.DataFrame:
        df = pd.read_excel(self.file_path)
        missing = [column for column in self.REQUIRED_COLUMNS if column not in df.columns]
        if missing:
            raise ValueError(f"В файле отсутствуют столбцы: {missing}")
        return df[self.REQUIRED_COLUMNS].copy()

    @staticmethod
    def clean_labeled_df(df: pd.DataFrame) -> pd.DataFrame:
        cleaned = df.copy()
        cleaned["purchase_object_name"] = cleaned["purchase_object_name"].astype(str).str.strip()
        cleaned = cleaned[cleaned["purchase_object_name"] != ""].copy()
        cleaned = cleaned[cleaned["label"].notna()].copy()
        cleaned["label"] = cleaned["label"].astype(int)
        cleaned = cleaned[cleaned["label"].isin([0, 1])].copy()
        cleaned["label_comment"] = cleaned["label_comment"].fillna("").astype(str).str.strip()
        cleaned = cleaned.drop_duplicates(subset=["purchase_object_name"]).reset_index(drop=True)
        return cleaned

    def run(self) -> None:
        self.logger.info("Начат шаг 5. Загрузка размеченного файла")
        labeled_df = self.load_labeled_excel()
        self.log_rows("Прочитан размеченный файл", labeled_df)
        labeled_df = self.clean_labeled_df(labeled_df)
        self.log_rows("После очистки", labeled_df)

        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS labeled_contracts"))

        labeled_df.to_sql("labeled_contracts", self.engine, if_exists="replace", index=False)
        self.logger.info("Таблица labeled_contracts обновлена")


def main() -> None:
    LabeledContractsLoader().run()


if __name__ == "__main__":
    main()
