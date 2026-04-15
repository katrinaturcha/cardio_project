"""
Шаг 7. Этот модуль применяет лучшую модель ко всему silver-слою.
После предсказаний он сохраняет scored-таблицу и собирает gold-слой с итоговыми витринами.
"""

from __future__ import annotations

import joblib
import pandas as pd
from sqlalchemy import text

from src.common.base import BasePipelineStep
from src.common.config import ML_DIR, REPORTS_DIR
from src.common.db import DatabaseManager


class GoldLayerBuilder(BasePipelineStep):
    logger_name = "module_07_score_gold"
    log_file_name = "module_07_score_gold.log"

    def __init__(self) -> None:
        super().__init__()
        self.engine = DatabaseManager().get_app_engine()
        self.model_path = ML_DIR / "best_model.joblib"
        self.reports_dir = REPORTS_DIR

    def load_silver_data(self) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM silver_contracts", self.engine)

    @staticmethod
    def clean_scoring_df(df: pd.DataFrame) -> pd.DataFrame:
        cleaned = df.copy()
        cleaned["purchase_object_name"] = cleaned["purchase_object_name"].fillna("").astype(str).str.strip()
        cleaned = cleaned[cleaned["purchase_object_name"] != ""].copy()
        if "contract_date" in cleaned.columns:
            cleaned["contract_date"] = pd.to_datetime(cleaned["contract_date"], errors="coerce")
        for column in ["contract_price", "unit_price_rub", "purchase_object_amount_rub", "delivered_quantity"]:
            if column in cleaned.columns:
                cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")
        return cleaned

    @staticmethod
    def score_data(df: pd.DataFrame, model) -> pd.DataFrame:
        scored = df.copy()
        x_data = scored["purchase_object_name"]
        scored["predicted_label"] = model.predict(x_data)
        try:
            scored["predicted_probability"] = model.predict_proba(x_data)[:, 1]
        except Exception:
            scored["predicted_probability"] = None
        return scored

    @staticmethod
    def build_gold_items(df: pd.DataFrame) -> pd.DataFrame:
        gold_items = df[df["predicted_label"] == 1].copy()
        gold_items["contract_year"] = gold_items["contract_date"].dt.year
        return gold_items.reset_index(drop=True)

    @staticmethod
    def build_gold_year_stats(gold_items: pd.DataFrame) -> pd.DataFrame:
        return (
            gold_items.groupby("contract_year", dropna=False)
            .agg(
                contracts_count=("registry_contract_id", "count"),
                total_amount_rub=("purchase_object_amount_rub", "sum"),
                total_quantity=("delivered_quantity", "sum"),
                min_unit_price_rub=("unit_price_rub", "min"),
                max_unit_price_rub=("unit_price_rub", "max"),
                avg_unit_price_rub=("unit_price_rub", "mean"),
            )
            .reset_index()
            .sort_values("contract_year")
        )

    @staticmethod
    def build_gold_customer_stats(gold_items: pd.DataFrame) -> pd.DataFrame:
        return (
            gold_items.groupby(["customer_name", "customer_inn"], dropna=False)
            .agg(
                contracts_count=("registry_contract_id", "count"),
                total_amount_rub=("purchase_object_amount_rub", "sum"),
                total_quantity=("delivered_quantity", "sum"),
                min_unit_price_rub=("unit_price_rub", "min"),
                max_unit_price_rub=("unit_price_rub", "max"),
                avg_unit_price_rub=("unit_price_rub", "mean"),
            )
            .reset_index()
            .sort_values(["total_amount_rub", "contracts_count"], ascending=[False, False])
        )

    def save_tables(
        self,
        scored_df: pd.DataFrame,
        gold_items: pd.DataFrame,
        year_stats: pd.DataFrame,
        customer_stats: pd.DataFrame,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS silver_contracts_scored"))
            conn.execute(text("DROP TABLE IF EXISTS gold_cardioplegia_items"))
            conn.execute(text("DROP TABLE IF EXISTS gold_cardioplegia_year_stats"))
            conn.execute(text("DROP TABLE IF EXISTS gold_cardioplegia_customer_stats"))

        scored_df.to_sql("silver_contracts_scored", self.engine, if_exists="replace", index=False)
        gold_items.to_sql("gold_cardioplegia_items", self.engine, if_exists="replace", index=False)
        year_stats.to_sql("gold_cardioplegia_year_stats", self.engine, if_exists="replace", index=False)
        customer_stats.to_sql("gold_cardioplegia_customer_stats", self.engine, if_exists="replace", index=False)

        gold_items.to_excel(self.reports_dir / "gold_cardioplegia_items.xlsx", index=False)
        year_stats.to_excel(self.reports_dir / "gold_cardioplegia_year_stats.xlsx", index=False)
        customer_stats.to_excel(self.reports_dir / "gold_cardioplegia_customer_stats.xlsx", index=False)

    def run(self) -> None:
        self.logger.info("Начат шаг 7. Скоринг и сборка gold-слоя")
        silver_df = self.load_silver_data()
        self.log_rows("Строк в silver_contracts", silver_df)
        silver_df = self.clean_scoring_df(silver_df)
        model = joblib.load(self.model_path)
        scored_df = self.score_data(silver_df, model)
        self.logger.info("Распределение предсказанных классов:\n%s", scored_df["predicted_label"].value_counts())
        gold_items = self.build_gold_items(scored_df)
        year_stats = self.build_gold_year_stats(gold_items)
        customer_stats = self.build_gold_customer_stats(gold_items)
        self.save_tables(scored_df, gold_items, year_stats, customer_stats)
        self.log_rows("Шаг 7 завершён. Целевых строк", gold_items)


def main() -> None:
    GoldLayerBuilder().run()


if __name__ == "__main__":
    main()
