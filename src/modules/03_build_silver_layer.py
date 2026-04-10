"""
Шаг 3. Этот модуль строит silver-слой.
Он берёт сырые данные из БД, очищает строки, даты и числа, удаляет дубли и оставляет записи начиная с заданной даты.
"""

import pandas as pd
from sqlalchemy import text
from src.common.config import START_DATE
from src.common.constants import REQUIRED_COLUMNS
from src.common.db import get_engine
from src.common.helpers import clean_text, clean_float, clean_int
from src.common.logging_utils import get_logger

logger = get_logger("module_03_build_silver", "module_03_build_silver.log")

STRING_COLUMNS = [
    "registry_contract_id", "customer_name", "customer_inn", "customer_kpp", "budget_level",
    "budget_source_name", "extra_budget_source_type", "procurement_method", "notice_number",
    "supplier_result_date", "basis_document_details", "contract_number", "contract_subject",
    "budget_classification_code", "kosgu_code", "kvr_code", "ikz_code", "purchase_object_name",
    "purchase_object_code", "supplier_name", "supplier_inn", "supplier_kpp",
]
FLOAT_COLUMNS = ["contract_price", "unit_price_rub", "purchase_object_amount_rub"]
INT_COLUMNS = ["delivered_quantity"]


def extract_and_transform() -> pd.DataFrame:
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM raw_contracts", engine)
    logger.info("Прочитано строк из raw_contracts: %s", len(df))

    available_columns = [col for col in REQUIRED_COLUMNS if col in df.columns]
    df = df[available_columns].copy()
    logger.info("Оставлены нужные столбцы")

    df = df.drop_duplicates().reset_index(drop=True)

    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_text).astype("string")

    if "contract_date" in df.columns:
        df["contract_date"] = df["contract_date"].apply(clean_text)
        df["contract_date"] = pd.to_datetime(df["contract_date"], errors="coerce", dayfirst=True)
        df = df[df["contract_date"].notna()].copy()
        df = df[df["contract_date"] >= pd.Timestamp(START_DATE)].copy()
        logger.info("После фильтра по дате. Строк: %s", len(df))

    for col in FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_float)

    for col in INT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_int)

    df = df.drop_duplicates().reset_index(drop=True)
    logger.info("После финальной очистки. Строк: %s", len(df))
    return df


def load_to_silver(df: pd.DataFrame) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS silver_contracts"))
    df.to_sql("silver_contracts", engine, if_exists="replace", index=False)
    logger.info("Таблица silver_contracts обновлена")


def main():
    logger.info("Начат шаг 3. Построение silver-слоя")
    df = extract_and_transform()
    load_to_silver(df)
    logger.info("Шаг 3 завершён")


if __name__ == "__main__":
    main()
