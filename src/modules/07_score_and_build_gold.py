"""
Шаг 7. Этот модуль применяет лучшую модель ко всему silver-слою.
После предсказаний он сохраняет scored-таблицу и собирает gold-слой с итоговыми витринами.
"""

import joblib
import pandas as pd
from sqlalchemy import text
from src.common.config import ML_DIR, REPORTS_DIR
from src.common.db import get_engine
from src.common.logging_utils import get_logger

logger = get_logger("module_07_score_gold", "module_07_score_gold.log")
MODEL_PATH = ML_DIR / "best_model.joblib"


def load_silver_data() -> pd.DataFrame:
    engine = get_engine()
    query = "SELECT * FROM silver_contracts"
    return pd.read_sql(query, engine)


def clean_scoring_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["purchase_object_name"] = df["purchase_object_name"].fillna("").astype(str).str.strip()
    df = df[df["purchase_object_name"] != ""].copy()
    if "contract_date" in df.columns:
        df["contract_date"] = pd.to_datetime(df["contract_date"], errors="coerce")
    for col in ["contract_price", "unit_price_rub", "purchase_object_amount_rub", "delivered_quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def score_data(df: pd.DataFrame, model) -> pd.DataFrame:
    df = df.copy()
    X = df["purchase_object_name"]
    df["predicted_label"] = model.predict(X)
    try:
        df["predicted_probability"] = model.predict_proba(X)[:, 1]
    except Exception:
        df["predicted_probability"] = None
    return df


def build_gold_items(df: pd.DataFrame) -> pd.DataFrame:
    gold_df = df[df["predicted_label"] == 1].copy()
    gold_df["contract_year"] = gold_df["contract_date"].dt.year
    return gold_df.reset_index(drop=True)


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


def save_tables(scored_df, gold_items, year_stats, customer_stats):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS silver_contracts_scored"))
        conn.execute(text("DROP TABLE IF EXISTS gold_cardioplegia_items"))
        conn.execute(text("DROP TABLE IF EXISTS gold_cardioplegia_year_stats"))
        conn.execute(text("DROP TABLE IF EXISTS gold_cardioplegia_customer_stats"))

    scored_df.to_sql("silver_contracts_scored", engine, if_exists="replace", index=False)
    gold_items.to_sql("gold_cardioplegia_items", engine, if_exists="replace", index=False)
    year_stats.to_sql("gold_cardioplegia_year_stats", engine, if_exists="replace", index=False)
    customer_stats.to_sql("gold_cardioplegia_customer_stats", engine, if_exists="replace", index=False)

    gold_items.to_excel(REPORTS_DIR / "gold_cardioplegia_items.xlsx", index=False)
    year_stats.to_excel(REPORTS_DIR / "gold_cardioplegia_year_stats.xlsx", index=False)
    customer_stats.to_excel(REPORTS_DIR / "gold_cardioplegia_customer_stats.xlsx", index=False)


def main():
    logger.info("Начат шаг 7. Скоринг и сборка gold-слоя")
    silver_df = load_silver_data()
    logger.info("Строк в silver_contracts: %s", len(silver_df))
    silver_df = clean_scoring_df(silver_df)
    model = joblib.load(MODEL_PATH)
    scored_df = score_data(silver_df, model)
    logger.info("Распределение предсказанных классов:
%s", scored_df["predicted_label"].value_counts())
    gold_items = build_gold_items(scored_df)
    year_stats = build_gold_year_stats(gold_items)
    customer_stats = build_gold_customer_stats(gold_items)
    save_tables(scored_df, gold_items, year_stats, customer_stats)
    logger.info("Шаг 7 завершён. Целевых строк: %s", len(gold_items))


if __name__ == "__main__":
    main()
