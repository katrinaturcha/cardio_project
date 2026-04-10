"""
Шаг 8. Этот модуль строит итоговые графики.
Он читает gold-таблицы и сохраняет картинки и Excel-отчёты для отчёта и презентации.
"""

import pandas as pd
import matplotlib.pyplot as plt
from src.common.config import PLOTS_DIR, REPORTS_DIR
from src.common.db import get_engine
from src.common.logging_utils import get_logger

logger = get_logger("module_08_visualize", "module_08_visualize.log")


def load_gold_data():
    engine = get_engine()
    year_stats = pd.read_sql("SELECT * FROM gold_cardioplegia_year_stats", engine)
    customer_stats = pd.read_sql("SELECT * FROM gold_cardioplegia_customer_stats", engine)
    return year_stats, customer_stats


def plot_total_amount_by_year(year_stats: pd.DataFrame):
    df = year_stats.copy().sort_values("contract_year")
    plt.figure(figsize=(8, 5))
    plt.bar(df["contract_year"].astype(str), df["total_amount_rub"])
    plt.title("Сумма закупок по годам")
    plt.xlabel("Год")
    plt.ylabel("Сумма, руб.")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "total_amount_by_year.png", dpi=150)
    plt.close()


def plot_total_quantity_by_year(year_stats: pd.DataFrame):
    df = year_stats.copy().sort_values("contract_year")
    plt.figure(figsize=(8, 5))
    plt.bar(df["contract_year"].astype(str), df["total_quantity"])
    plt.title("Количество закупленных позиций по годам")
    plt.xlabel("Год")
    plt.ylabel("Количество")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "total_quantity_by_year.png", dpi=150)
    plt.close()


def plot_avg_price_by_year(year_stats: pd.DataFrame):
    df = year_stats.copy().sort_values("contract_year")
    plt.figure(figsize=(8, 5))
    plt.plot(df["contract_year"].astype(str), df["avg_unit_price_rub"], marker="o")
    plt.title("Средняя цена по годам")
    plt.xlabel("Год")
    plt.ylabel("Средняя цена, руб.")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "avg_price_by_year.png", dpi=150)
    plt.close()


def plot_top_customers_by_amount(customer_stats: pd.DataFrame, top_n: int = 10):
    df = customer_stats.copy().head(top_n).sort_values("total_amount_rub", ascending=True)
    plt.figure(figsize=(10, 6))
    plt.barh(df["customer_name"], df["total_amount_rub"])
    plt.title(f"Топ-{top_n} заказчиков по сумме закупок")
    plt.xlabel("Сумма, руб.")
    plt.ylabel("Заказчик")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "top_customers_by_amount.png", dpi=150)
    plt.close()


def plot_top_customers_by_quantity(customer_stats: pd.DataFrame, top_n: int = 10):
    df = customer_stats.copy().head(top_n).sort_values("total_quantity", ascending=True)
    plt.figure(figsize=(10, 6))
    plt.barh(df["customer_name"], df["total_quantity"])
    plt.title(f"Топ-{top_n} заказчиков по количеству закупок")
    plt.xlabel("Количество")
    plt.ylabel("Заказчик")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "top_customers_by_quantity.png", dpi=150)
    plt.close()


def main():
    logger.info("Начат шаг 8. Построение графиков")
    year_stats, customer_stats = load_gold_data()
    year_stats.to_excel(REPORTS_DIR / "year_stats_report.xlsx", index=False)
    customer_stats.to_excel(REPORTS_DIR / "customer_stats_report.xlsx", index=False)
    plot_total_amount_by_year(year_stats)
    plot_total_quantity_by_year(year_stats)
    plot_avg_price_by_year(year_stats)
    plot_top_customers_by_amount(customer_stats)
    plot_top_customers_by_quantity(customer_stats)
    logger.info("Шаг 8 завершён")


if __name__ == "__main__":
    main()
