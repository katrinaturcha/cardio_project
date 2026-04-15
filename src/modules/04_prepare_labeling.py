"""
Шаг 4. Этот модуль готовит файл для ручной разметки.
Он выбирает записи со словами про кардиоплегию, убирает повторы и сохраняет простой Excel для проверки человеком.
"""

from __future__ import annotations

import pandas as pd

from src.common.base import BasePipelineStep
from src.common.config import LABELING_FILE_PATH
from src.common.db import DatabaseManager


class LabelingPreparationStep(BasePipelineStep):
    logger_name = "module_04_prepare_labeling"
    log_file_name = "module_04_prepare_labeling.log"

    def __init__(self) -> None:
        super().__init__()
        self.engine = DatabaseManager().get_app_engine()
        self.output_path = LABELING_FILE_PATH

    def load_silver_data(self) -> pd.DataFrame:
        return pd.read_sql("SELECT purchase_object_name FROM silver_contracts", self.engine)

    @staticmethod
    def filter_candidates(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["purchase_object_name"] = result["purchase_object_name"].fillna("").astype(str)
        mask = result["purchase_object_name"].str.contains("кардиоплег", case=False, na=False)
        result = result[mask].copy()
        result = result.drop_duplicates(subset=["purchase_object_name"]).reset_index(drop=True)
        result["label"] = None
        result["label_comment"] = None
        return result[["purchase_object_name", "label", "label_comment"]]

    def run(self) -> None:
        self.logger.info("Начат шаг 4. Подготовка файла для разметки")
        silver_df = self.load_silver_data()
        self.log_rows("Прочитано строк из silver_contracts", silver_df)
        labeling_df = self.filter_candidates(silver_df)
        labeling_df.to_excel(self.output_path, index=False)
        self.log_rows("Файл для разметки сохранён", labeling_df)
        self.logger.info("Путь к файлу: %s", self.output_path)


def main() -> None:
    LabelingPreparationStep().run()


if __name__ == "__main__":
    main()
