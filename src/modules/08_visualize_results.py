from __future__ import annotations

import json
import pandas as pd
from sqlalchemy import text

from src.common.base import BasePipelineStep
from src.common.config import REPORTS_DIR
from src.common.db import DatabaseManager


class SupersetJsonExportStep(BasePipelineStep):
    logger_name = "module_08_export_superset_json"
    log_file_name = "module_08_export_superset_json.log"

    def __init__(self) -> None:
        super().__init__()
        self.engine = DatabaseManager().get_app_engine()
        self.reports_dir = REPORTS_DIR
        self.output_path = self.reports_dir / "superset_dashboard_spec.json"

    def load_gold_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        with self.engine.connect() as conn:
            year_stats = pd.read_sql_query(
                text("SELECT * FROM gold_cardioplegia_year_stats ORDER BY contract_year"),
                conn,
            )
            customer_stats = pd.read_sql_query(
                text("SELECT * FROM gold_cardioplegia_customer_stats"),
                conn,
            )
        return year_stats, customer_stats

    @staticmethod
    def build_dashboard_spec(
        year_stats: pd.DataFrame,
        customer_stats: pd.DataFrame,
    ) -> dict:
        top_amount = (
            customer_stats.sort_values("total_amount_rub", ascending=False)
            .head(10)
            .sort_values("total_amount_rub", ascending=True)
            .copy()
        )

        top_quantity = (
            customer_stats.sort_values("total_quantity", ascending=False)
            .head(10)
            .sort_values("total_quantity", ascending=True)
            .copy()
        )

        return {
            "dashboard_title": "Аналитика закупок кардиоплегии",
            "source_tables": {
                "year_stats": "gold_cardioplegia_year_stats",
                "customer_stats": "gold_cardioplegia_customer_stats",
            },
            "charts": [
                {
                    "chart_id": "total_amount_by_year",
                    "title": "Сумма закупок по годам",
                    "chart_type": "bar",
                    "dataset": "gold_cardioplegia_year_stats",
                    "x_axis": "contract_year",
                    "y_axis": "total_amount_rub",
                    "x_label": "Год",
                    "y_label": "Сумма, руб.",
                    "data": year_stats[
                        ["contract_year", "total_amount_rub"]
                    ].to_dict(orient="records"),
                },
                {
                    "chart_id": "total_quantity_by_year",
                    "title": "Количество закупленных позиций по годам",
                    "chart_type": "bar",
                    "dataset": "gold_cardioplegia_year_stats",
                    "x_axis": "contract_year",
                    "y_axis": "total_quantity",
                    "x_label": "Год",
                    "y_label": "Количество",
                    "data": year_stats[
                        ["contract_year", "total_quantity"]
                    ].to_dict(orient="records"),
                },
                {
                    "chart_id": "avg_price_by_year",
                    "title": "Средняя цена по годам",
                    "chart_type": "line",
                    "dataset": "gold_cardioplegia_year_stats",
                    "x_axis": "contract_year",
                    "y_axis": "avg_unit_price_rub",
                    "x_label": "Год",
                    "y_label": "Средняя цена, руб.",
                    "data": year_stats[
                        ["contract_year", "avg_unit_price_rub"]
                    ].to_dict(orient="records"),
                },
                {
                    "chart_id": "top_customers_by_amount",
                    "title": "Топ-10 заказчиков по сумме закупок",
                    "chart_type": "barh",
                    "dataset": "gold_cardioplegia_customer_stats",
                    "x_axis": "total_amount_rub",
                    "y_axis": "customer_name",
                    "x_label": "Сумма, руб.",
                    "y_label": "Заказчик",
                    "limit": 10,
                    "sort_by": "total_amount_rub",
                    "sort_order": "desc",
                    "data": top_amount[
                        ["customer_name", "total_amount_rub"]
                    ].to_dict(orient="records"),
                },
                {
                    "chart_id": "top_customers_by_quantity",
                    "title": "Топ-10 заказчиков по количеству закупок",
                    "chart_type": "barh",
                    "dataset": "gold_cardioplegia_customer_stats",
                    "x_axis": "total_quantity",
                    "y_axis": "customer_name",
                    "x_label": "Количество",
                    "y_label": "Заказчик",
                    "limit": 10,
                    "sort_by": "total_quantity",
                    "sort_order": "desc",
                    "data": top_quantity[
                        ["customer_name", "total_quantity"]
                    ].to_dict(orient="records"),
                },
            ],
        }

    def run(self) -> None:
        self.logger.info("Начат шаг 8. Экспорт JSON-спецификации для Superset")
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        year_stats, customer_stats = self.load_gold_data()
        spec = self.build_dashboard_spec(year_stats, customer_stats)

        with open(self.output_path, "w", encoding="utf-8") as file:
            json.dump(spec, file, ensure_ascii=False, indent=2)

        self.logger.info("JSON-спецификация сохранена: %s", self.output_path)


def main() -> None:
    SupersetJsonExportStep().run()


if __name__ == "__main__":
    main()
