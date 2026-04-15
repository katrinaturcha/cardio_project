"""
Шаг 6. Этот модуль обучает несколько моделей и сравнивает их.
Он строит train, test и validation, считает метрики, сохраняет таблицу результатов, графики и лучшую модель.
"""

from __future__ import annotations

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.svm import LinearSVC

from src.common.base import BasePipelineStep
from src.common.config import ML_DIR, PLOTS_DIR
from src.common.db import DatabaseManager


class ModelTrainingStep(BasePipelineStep):
    logger_name = "module_06_train_models"
    log_file_name = "module_06_train_models.log"

    def __init__(self) -> None:
        super().__init__()
        self.engine = DatabaseManager().get_app_engine()
        self.ml_dir = ML_DIR
        self.plots_dir = PLOTS_DIR

    def load_labeled_data(self) -> pd.DataFrame:
        query = """
        SELECT purchase_object_name, label
        FROM labeled_contracts
        WHERE purchase_object_name IS NOT NULL AND label IS NOT NULL
        """
        df = pd.read_sql(query, self.engine)
        df["purchase_object_name"] = df["purchase_object_name"].astype(str).str.strip()
        df = df[df["purchase_object_name"] != ""].copy()
        df["label"] = df["label"].astype(int)
        df = df[df["label"].isin([0, 1])].copy()
        return df.drop_duplicates().reset_index(drop=True)

    @staticmethod
    def split_dataset(df: pd.DataFrame):
        x_data = df["purchase_object_name"]
        y_data = df["label"]
        x_train, x_temp, y_train, y_temp = train_test_split(
            x_data, y_data, test_size=0.30, random_state=42, stratify=y_data
        )
        x_val, x_test, y_val, y_test = train_test_split(
            x_temp, y_temp, test_size=2 / 3, random_state=42, stratify=y_temp
        )
        return x_train, x_val, x_test, y_train, y_val, y_test

    def save_split_plots(self, x_train, x_val, x_test, y_train, y_val, y_test) -> None:
        split_sizes = pd.DataFrame(
            {
                "Выборка": ["Train", "Test", "Validation"],
                "Количество строк": [len(x_train), len(x_test), len(x_val)],
            }
        )
        plt.figure(figsize=(8, 5))
        plt.bar(split_sizes["Выборка"], split_sizes["Количество строк"])
        plt.title("Разбиение датасета")
        plt.xlabel("Тип выборки")
        plt.ylabel("Количество строк")
        plt.tight_layout()
        plt.savefig(self.plots_dir / "dataset_split.png", dpi=150)
        plt.close()

        class_distribution = pd.DataFrame(
            {
                "Train": y_train.value_counts().sort_index(),
                "Test": y_test.value_counts().sort_index(),
                "Validation": y_val.value_counts().sort_index(),
            }
        ).fillna(0)
        class_distribution.index = ["Класс 0", "Класс 1"]
        class_distribution.T.plot(kind="bar", figsize=(8, 5))
        plt.title("Распределение классов по выборкам")
        plt.xlabel("Тип выборки")
        plt.ylabel("Количество строк")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(self.plots_dir / "split_class_distribution.png", dpi=150)
        plt.close()

    @staticmethod
    def build_models() -> dict[str, Pipeline]:
        return {
            "logreg": Pipeline(
                [
                    ("tfidf", TfidfVectorizer(lowercase=True, ngram_range=(1, 2), min_df=1, max_features=5000)),
                    ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42)),
                ]
            ),
            "random_forest": Pipeline(
                [
                    ("tfidf", TfidfVectorizer(lowercase=True, ngram_range=(1, 2), min_df=1, max_features=5000)),
                    ("clf", RandomForestClassifier(n_estimators=300, random_state=42, class_weight="balanced")),
                ]
            ),
            "linear_svc": Pipeline(
                [
                    ("tfidf", TfidfVectorizer(lowercase=True, ngram_range=(1, 2), min_df=1, max_features=5000)),
                    ("clf", LinearSVC(class_weight="balanced", random_state=42)),
                ]
            ),
        }

    def evaluate_model(self, model_name, model, x_train, y_train, x_test, y_test, x_val, y_val):
        model.fit(x_train, y_train)
        y_pred_test = model.predict(x_test)
        y_pred_val = model.predict(x_val)

        test_metrics = {
            "model_name": model_name,
            "dataset": "test",
            "accuracy": accuracy_score(y_test, y_pred_test),
            "precision": precision_score(y_test, y_pred_test, zero_division=0),
            "recall": recall_score(y_test, y_pred_test, zero_division=0),
            "f1": f1_score(y_test, y_pred_test, zero_division=0),
        }
        val_metrics = {
            "model_name": model_name,
            "dataset": "validation",
            "accuracy": accuracy_score(y_val, y_pred_val),
            "precision": precision_score(y_val, y_pred_val, zero_division=0),
            "recall": recall_score(y_val, y_pred_val, zero_division=0),
            "f1": f1_score(y_val, y_pred_val, zero_division=0),
        }

        self.logger.info("Модель: %s", model_name)
        self.logger.info("Метрики на test: %s", test_metrics)
        self.logger.info("Метрики на validation: %s", val_metrics)
        self.logger.info("Classification report (test):\n%s", classification_report(y_test, y_pred_test, zero_division=0))
        self.logger.info("Confusion matrix (test):\n%s", confusion_matrix(y_test, y_pred_test))
        return model, [test_metrics, val_metrics]

    def save_metrics_and_plots(self, results_df: pd.DataFrame) -> None:
        results_df.to_csv(self.ml_dir / "model_metrics.csv", index=False, encoding="utf-8-sig")
        results_df.to_excel(self.ml_dir / "model_metrics.xlsx", index=False)

        val_df = results_df[results_df["dataset"] == "validation"].copy()
        metrics = ["accuracy", "precision", "recall", "f1"]
        models = val_df["model_name"].tolist()
        x_positions = np.arange(len(models))
        width = 0.2

        plt.figure(figsize=(10, 6))
        for index, metric in enumerate(metrics):
            plt.bar(x_positions + index * width, val_df[metric], width=width, label=metric)
        plt.xticks(x_positions + width * 1.5, models)
        plt.ylim(0, 1.05)
        plt.title("Сравнение моделей по метрикам на validation")
        plt.xlabel("Модель")
        plt.ylabel("Значение метрики")
        plt.legend()
        plt.tight_layout()
        plt.savefig(self.plots_dir / "models_metrics_validation.png", dpi=150)
        plt.close()

        plt.figure(figsize=(8, 5))
        plt.bar(val_df["model_name"], val_df["f1"])
        plt.ylim(0, 1.05)
        plt.title("Сравнение моделей по F1-score")
        plt.xlabel("Модель")
        plt.ylabel("F1-score")
        plt.tight_layout()
        plt.savefig(self.plots_dir / "models_f1.png", dpi=150)
        plt.close()

    @staticmethod
    def choose_best_model(results_df: pd.DataFrame) -> str:
        val_df = results_df[results_df["dataset"] == "validation"].copy()
        val_df = val_df.sort_values(by=["f1", "recall", "precision", "accuracy"], ascending=False)
        return str(val_df.iloc[0]["model_name"])

    def run(self) -> None:
        self.logger.info("Начат шаг 6. Обучение моделей")
        labeled_df = self.load_labeled_data()
        self.log_rows("Размеченных строк", labeled_df)
        self.logger.info("Распределение классов:\n%s", labeled_df["label"].value_counts())

        x_train, x_val, x_test, y_train, y_val, y_test = self.split_dataset(labeled_df)
        self.save_split_plots(x_train, x_val, x_test, y_train, y_val, y_test)

        all_results: list[dict] = []
        trained_models: dict[str, Pipeline] = {}
        for model_name, model in self.build_models().items():
            trained_model, metrics_list = self.evaluate_model(
                model_name, model, x_train, y_train, x_test, y_test, x_val, y_val
            )
            trained_models[model_name] = trained_model
            all_results.extend(metrics_list)

        results_df = pd.DataFrame(all_results)
        self.save_metrics_and_plots(results_df)
        best_model_name = self.choose_best_model(results_df)
        joblib.dump(trained_models[best_model_name], self.ml_dir / "best_model.joblib")
        with open(self.ml_dir / "best_model_name.txt", "w", encoding="utf-8") as file:
            file.write(best_model_name)
        self.logger.info("Лучшая модель: %s", best_model_name)
        self.logger.info("Шаг 6 завершён")


def main() -> None:
    ModelTrainingStep().run()


if __name__ == "__main__":
    main()
