"""
Шаг 6. Этот модуль обучает несколько моделей и сравнивает их.
Он строит train, test и validation, считает метрики, сохраняет таблицу результатов, графики и лучшую модель.
"""

import os
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report, confusion_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import LinearSVC

from src.common.config import ML_DIR, PLOTS_DIR
from src.common.db import get_engine
from src.common.logging_utils import get_logger

logger = get_logger("module_06_train_models", "module_06_train_models.log")


def load_labeled_data() -> pd.DataFrame:
    engine = get_engine()
    query = """
    SELECT purchase_object_name, label
    FROM labeled_contracts
    WHERE purchase_object_name IS NOT NULL AND label IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    df["purchase_object_name"] = df["purchase_object_name"].astype(str).str.strip()
    df = df[df["purchase_object_name"] != ""].copy()
    df["label"] = df["label"].astype(int)
    df = df[df["label"].isin([0, 1])].copy()
    return df.drop_duplicates().reset_index(drop=True)


def split_dataset(df: pd.DataFrame):
    X = df["purchase_object_name"]
    y = df["label"]
    X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.30, random_state=42, stratify=y)
    X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=2/3, random_state=42, stratify=y_temp)
    return X_train, X_val, X_test, y_train, y_val, y_test


def save_split_plots(X_train, X_val, X_test, y_train, y_val, y_test):
    split_sizes = pd.DataFrame({
        "Выборка": ["Train", "Test", "Validation"],
        "Количество строк": [len(X_train), len(X_test), len(X_val)],
    })
    plt.figure(figsize=(8, 5))
    plt.bar(split_sizes["Выборка"], split_sizes["Количество строк"])
    plt.title("Разбиение датасета")
    plt.xlabel("Тип выборки")
    plt.ylabel("Количество строк")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "dataset_split.png", dpi=150)
    plt.close()

    class_distribution = pd.DataFrame({
        "Train": y_train.value_counts().sort_index(),
        "Test": y_test.value_counts().sort_index(),
        "Validation": y_val.value_counts().sort_index(),
    }).fillna(0)
    class_distribution.index = ["Класс 0", "Класс 1"]
    class_distribution.T.plot(kind="bar", figsize=(8, 5))
    plt.title("Распределение классов по выборкам")
    plt.xlabel("Тип выборки")
    plt.ylabel("Количество строк")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "split_class_distribution.png", dpi=150)
    plt.close()


def build_models():
    return {
        "logreg": Pipeline([
            ("tfidf", TfidfVectorizer(lowercase=True, ngram_range=(1, 2), min_df=1, max_features=5000)),
            ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42)),
        ]),
        "random_forest": Pipeline([
            ("tfidf", TfidfVectorizer(lowercase=True, ngram_range=(1, 2), min_df=1, max_features=5000)),
            ("clf", RandomForestClassifier(n_estimators=300, random_state=42, class_weight="balanced")),
        ]),
        "linear_svc": Pipeline([
            ("tfidf", TfidfVectorizer(lowercase=True, ngram_range=(1, 2), min_df=1, max_features=5000)),
            ("clf", LinearSVC(class_weight="balanced", random_state=42)),
        ]),
    }


def evaluate_model(model_name, model, X_train, y_train, X_test, y_test, X_val, y_val):
    model.fit(X_train, y_train)
    y_pred_test = model.predict(X_test)
    y_pred_val = model.predict(X_val)

    logger.info("Модель %s. Test classification report:
%s", model_name, classification_report(y_test, y_pred_test, zero_division=0))
    logger.info("Модель %s. Test confusion matrix:
%s", model_name, confusion_matrix(y_test, y_pred_test))

    test_metrics = {
        "model_name": model_name, "dataset": "test",
        "accuracy": accuracy_score(y_test, y_pred_test),
        "precision": precision_score(y_test, y_pred_test, zero_division=0),
        "recall": recall_score(y_test, y_pred_test, zero_division=0),
        "f1": f1_score(y_test, y_pred_test, zero_division=0),
    }
    val_metrics = {
        "model_name": model_name, "dataset": "validation",
        "accuracy": accuracy_score(y_val, y_pred_val),
        "precision": precision_score(y_val, y_pred_val, zero_division=0),
        "recall": recall_score(y_val, y_pred_val, zero_division=0),
        "f1": f1_score(y_val, y_pred_val, zero_division=0),
    }
    return model, [test_metrics, val_metrics]


def save_metrics_and_plots(results_df: pd.DataFrame):
    results_df.to_csv(ML_DIR / "model_metrics.csv", index=False, encoding="utf-8-sig")
    results_df.to_excel(ML_DIR / "model_metrics.xlsx", index=False)

    val_df = results_df[results_df["dataset"] == "validation"].copy()
    metrics = ["accuracy", "precision", "recall", "f1"]
    models = val_df["model_name"].tolist()
    x = np.arange(len(models))
    width = 0.2

    plt.figure(figsize=(10, 6))
    for i, metric in enumerate(metrics):
        plt.bar(x + i * width, val_df[metric], width=width, label=metric)
    plt.xticks(x + width * 1.5, models)
    plt.ylim(0, 1.05)
    plt.title("Сравнение моделей по метрикам на validation")
    plt.xlabel("Модель")
    plt.ylabel("Значение метрики")
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "models_validation_metrics.png", dpi=150)
    plt.close()

    plt.figure(figsize=(8, 5))
    plt.bar(val_df["model_name"], val_df["f1"])
    plt.ylim(0, 1.05)
    plt.title("Сравнение моделей по F1-score")
    plt.xlabel("Модель")
    plt.ylabel("F1-score")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "models_f1.png", dpi=150)
    plt.close()


def choose_best_model(results_df: pd.DataFrame) -> str:
    val_df = results_df[results_df["dataset"] == "validation"].copy()
    val_df = val_df.sort_values(by=["f1", "recall", "precision", "accuracy"], ascending=False)
    return val_df.iloc[0]["model_name"]


def main():
    logger.info("Начат шаг 6. Обучение моделей")
    df = load_labeled_data()
    logger.info("Размеченных строк: %s", len(df))
    logger.info("Распределение классов:
%s", df["label"].value_counts())

    X_train, X_val, X_test, y_train, y_val, y_test = split_dataset(df)
    save_split_plots(X_train, X_val, X_test, y_train, y_val, y_test)

    models = build_models()
    all_results = []
    trained_models = {}
    for model_name, model in models.items():
        trained_model, metrics_list = evaluate_model(model_name, model, X_train, y_train, X_test, y_test, X_val, y_val)
        trained_models[model_name] = trained_model
        all_results.extend(metrics_list)

    results_df = pd.DataFrame(all_results)
    save_metrics_and_plots(results_df)
    best_model_name = choose_best_model(results_df)
    joblib.dump(trained_models[best_model_name], ML_DIR / "best_model.joblib")
    with open(ML_DIR / "best_model_name.txt", "w", encoding="utf-8") as f:
        f.write(best_model_name)
    logger.info("Лучшая модель: %s", best_model_name)
    logger.info("Шаг 6 завершён")


if __name__ == "__main__":
    main()
