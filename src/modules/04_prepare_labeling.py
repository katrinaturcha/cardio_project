"""
Шаг 4. Этот модуль готовит файл для ручной разметки.
Он выбирает записи со словами про кардиоплегию, убирает повторы и сохраняет простой Excel для проверки человеком.
"""

import pandas as pd
from src.common.config import LABELING_FILE_PATH
from src.common.db import get_engine
from src.common.logging_utils import get_logger

logger = get_logger("module_04_prepare_labeling", "module_04_prepare_labeling.log")


def load_silver_data() -> pd.DataFrame:
    engine = get_engine()
    query = "SELECT purchase_object_name FROM silver_contracts"
    return pd.read_sql(query, engine)


def filter_candidates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["purchase_object_name"] = df["purchase_object_name"].fillna("").astype(str)
    mask = df["purchase_object_name"].str.contains("кардиоплег", case=False, na=False)
    df = df[mask].copy()
    df = df.drop_duplicates(subset=["purchase_object_name"]).reset_index(drop=True)
    df["label"] = None
    df["label_comment"] = None
    return df[["purchase_object_name", "label", "label_comment"]]


def main():
    logger.info("Начат шаг 4. Подготовка файла для разметки")
    df = load_silver_data()
    logger.info("Прочитано строк из silver_contracts: %s", len(df))
    result = filter_candidates(df)
    result.to_excel(LABELING_FILE_PATH, index=False)
    logger.info("Файл для разметки сохранён: %s", LABELING_FILE_PATH)
    logger.info("Строк в файле для разметки: %s", len(result))


if __name__ == "__main__":
    main()
