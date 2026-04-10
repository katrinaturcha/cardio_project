# Проект по анализу закупок систем кардиоплегии

Этот проект собирает данные из ЕИС, загружает их в PostgreSQL, очищает их, обучает модель по размеченному набору, строит итоговые витрины и сохраняет графики.

## Структура проекта

- `src/modules/01_extract_eis.py` — шаг 1. Скачивает выгрузки из ЕИС и собирает общий файл.
- `src/modules/02_load_raw_to_postgres.py` — шаг 2. Загружает сырой слой в PostgreSQL.
- `src/modules/03_build_silver_layer.py` — шаг 3. Очищает данные и строит silver-слой.
- `src/modules/04_prepare_labeling.py` — шаг 4. Готовит файл для ручной разметки.
- `src/modules/05_load_labeled_data.py` — шаг 5. Загружает размеченный файл в БД.
- `src/modules/06_train_models.py` — шаг 6. Обучает модели и сохраняет лучшую.
- `src/modules/07_score_and_build_gold.py` — шаг 7. Делает скоринг и собирает gold-слой.
- `src/modules/08_visualize_results.py` — шаг 8. Строит графики и сохраняет отчёты.

## Быстрый запуск в PyCharm

1. Открой проект в PyCharm.
2. Создай виртуальное окружение.
3. Установи зависимости:

```bash
pip install -r requirements.txt
```

4. Проверь значения в `.env`.
5. Подними PostgreSQL и Airflow через Docker:

```bash
docker compose up -d postgres airflow-init airflow-webserver airflow-scheduler
```

6. Запускай модули по порядку через PyCharm или через Docker.

## Ручной порядок запуска

```bash
python src/modules/01_extract_eis.py
python src/modules/02_load_raw_to_postgres.py
python src/modules/03_build_silver_layer.py
python src/modules/04_prepare_labeling.py
# После этого вручную разметь файл data/labeling/cardioplegia_labeling.xlsx
python src/modules/05_load_labeled_data.py
python src/modules/06_train_models.py
python src/modules/07_score_and_build_gold.py
python src/modules/08_visualize_results.py
```

## Оркестрация в Airflow

В проекте есть два DAG:

- `cardio_monthly_pipeline` — обновляет данные 1 числа каждого месяца.
- `cardio_retrain_pipeline` — запускается вручную, когда готов новый размеченный файл.

## Логи

Логи каждого шага пишутся в папку `logs/`.

## Замечание по разметке

В размеченном файле нужны только три столбца:

- `purchase_object_name`
- `label`
- `label_comment`
