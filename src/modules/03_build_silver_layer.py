"""
Шаг 3. Этот модуль строит silver-слой.
Он берёт сырые данные из БД, очищает строки, даты и числа, удаляет дубли и оставляет записи начиная с заданной даты.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from src.common.base import BasePipelineStep
from src.common.config import START_DATE
from src.common.constants import REQUIRED_COLUMNS
from src.common.db import DatabaseManager
from src.common.helpers import clean_float, clean_int, clean_text


class SilverLayerBuilder(BasePipelineStep):
    logger_name = "module_03_build_silver"
    log_file_name = "module_03_build_silver.log"

    STRING_COLUMNS = [
        "registry_contract_id", "customer_name", "customer_inn", "customer_kpp", "budget_level",
        "budget_source_name", "extra_budget_source_type", "procurement_method", "notice_number",
        "supplier_result_date", "basis_document_details", "contract_number", "contract_subject",
        "budget_classification_code", "kosgu_code", "kvr_code", "ikz_code", "purchase_object_name",
        "purchase_object_code", "supplier_name", "supplier_inn", "supplier_kpp",
    ]
    FLOAT_COLUMNS = ["contract_price", "unit_price_rub", "purchase_object_amount_rub"]
    INT_COLUMNS = ["delivered_quantity"]

    def __init__(self) -> None:
        super().__init__()
        self.engine = DatabaseManager().get_app_engine()

    def extract_and_transform(self) -> pd.DataFrame:
        df = pd.read_sql("SELECT * FROM raw_contracts", self.engine)
        self.log_rows("Прочитано строк из raw_contracts", df)

        available_columns = [column for column in REQUIRED_COLUMNS if column in df.columns]
        transformed = df[available_columns].copy().drop_duplicates().reset_index(drop=True)
        self.logger.info("Оставлены нужные столбцы")

        for column in self.STRING_COLUMNS:
            if column in transformed.columns:
                transformed[column] = transformed[column].apply(clean_text).astype("string")

        if "contract_date" in transformed.columns:
            transformed["contract_date"] = transformed["contract_date"].apply(clean_text)
            transformed["contract_date"] = pd.to_datetime(
                transformed["contract_date"], errors="coerce", dayfirst=True
            )
            transformed = transformed[transformed["contract_date"].notna()].copy()
            transformed = transformed[transformed["contract_date"] >= pd.Timestamp(START_DATE)].copy()
            self.log_rows("После фильтра по дате", transformed)

        for column in self.FLOAT_COLUMNS:
            if column in transformed.columns:
                transformed[column] = transformed[column].apply(clean_float)

        for column in self.INT_COLUMNS:
            if column in transformed.columns:
                transformed[column] = transformed[column].apply(clean_int)

        transformed = transformed.drop_duplicates().reset_index(drop=True)
        self.log_rows("После финальной очистки", transformed)
        return transformed

    def load_to_silver(self, df: pd.DataFrame) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS silver_contracts"))
        df.to_sql("silver_contracts", self.engine, if_exists="replace", index=False)
        self.logger.info("Таблица silver_contracts обновлена")

    def run(self) -> None:
        self.logger.info("Начат шаг 3. Построение silver-слоя")
        silver_df = self.extract_and_transform()
        self.load_to_silver(silver_df)
        self.logger.info("Шаг 3 завершён")


def main() -> None:
    SilverLayerBuilder().run()


if __name__ == "__main__":
    main()
