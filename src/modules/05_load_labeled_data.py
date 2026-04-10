"""
Шаг 5. Этот модуль загружает размеченный файл в БД.
Он оставляет только текст, метку и комментарий, затем пишет их в таблицу labeled_contracts.
"""

import pandas as pd
from sqlalchemy import text
from src.common.config import LABELED_FILE_PATH
from src.common.db import get_engine
from src.common.logging_utils import get_logger

logger = get_logger("module_05_load_labels", "module_05_load_labels.log")


def load_labeled_excel(file_path):
    df = pd.read_excel(file_path)
    required = ["purchase_object_name", "label", "label_comment"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"В файле отсутствуют столбцы: {missing}")
    return df[required].copy()


def clean_labeled_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["purchase_object_name"] = df["purchase_object_name"].astype(str).str.strip()
    df = df[df["purchase_object_name"] != ""].copy()
    df = df[df["label"].notna()].copy()
    df["label"] = df["label"].astype(int)
    df = df[df["label"].isin([0, 1])].copy()
    df["label_comment"] = df["label_comment"].fillna("").astype(str).str.strip()
    df = df.drop_duplicates(subset=["purchase_object_name"]).reset_index(drop=True)
    return df


def main():
    logger.info("Начат шаг 5. Загрузка размеченного файла")
    engine = get_engine()
    df = load_labeled_excel(LABELED_FILE_PATH)
    logger.info("Прочитан размеченный файл. Строк: %s", len(df))
    df = clean_labeled_df(df)
    logger.info("После очистки. Строк: %s", len(df))

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS labeled_contracts"))

    df.to_sql("labeled_contracts", engine, if_exists="replace", index=False)
    logger.info("Таблица labeled_contracts обновлена")


if __name__ == "__main__":
    main()
