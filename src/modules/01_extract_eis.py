"""
Шаг 1. Этот модуль скачивает выгрузки из ЕИС и собирает общий файл.
Здесь начинается весь проект. На выходе получаем один CSV и один Excel.
"""

import os
import time
import glob
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.common.config import SEARCH_TEXT, RAW_DIR, RAW_MERGED_CSV, RAW_MERGED_XLSX
from src.common.logging_utils import get_logger

logger = get_logger("module_01_extract", "module_01_extract.log")


def wait_for_downloads(download_dir: Path, timeout: int = 300) -> bool:
    """
    Ждём, пока закончатся загрузки.
    Для Chrome незавершённые файлы обычно имеют расширение .crdownload.
    """
    start = time.time()
    while True:
        temp_files = glob.glob(str(download_dir / "*.crdownload"))
        if not temp_files:
            return True
        if time.time() - start > timeout:
            raise TimeoutError("Не дождались завершения скачивания файлов")
        time.sleep(2)


def get_driver(download_dir: Path) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()

    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    # options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    # if os.path.exists("/usr/bin/chromium"):
    #     options.binary_location = "/usr/bin/chromium"

    driver = webdriver.Chrome(options=options)
    return driver


def merge_csv_files(download_dir: Path, result_csv: Path, result_xlsx: Path) -> pd.DataFrame:
    csv_files = sorted(glob.glob(str(download_dir / "*.csv")))
    if not csv_files:
        raise FileNotFoundError("CSV-файлы не найдены")

    dfs = []
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, sep=";", encoding="utf-8-sig")
        except Exception:
            try:
                df = pd.read_csv(file_path, sep=";", encoding="cp1251")
            except Exception:
                df = pd.read_csv(file_path)

        df["source_file"] = Path(file_path).name
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv(result_csv, index=False, encoding="utf-8-sig")
    final_df.to_excel(result_xlsx, index=False)

    return final_df


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Начат шаг 1. Скачивание данных из ЕИС")

    driver = get_driver(RAW_DIR)
    wait = WebDriverWait(driver, 60)

    try:
        # 1. Открываем страницу поиска контрактов
        driver.get("https://zakupki.gov.ru/epz/contract/search/search.html")

        # 2. Находим поле поиска и ищем текст из конфига
        search_input = wait.until(
            EC.presence_of_element_located((By.NAME, "searchString"))
        )
        search_input.clear()
        search_input.send_keys(SEARCH_TEXT)
        search_input.send_keys(Keys.ENTER)

        # 3. Ждём результатов
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.downLoad-search"))
        )

        time.sleep(3)

        # 4. Нажимаем кнопку выгрузки результатов поиска
        download_search_btn = driver.find_element(By.CSS_SELECTOR, "a.downLoad-search")
        driver.execute_script("arguments[0].click();", download_search_btn)

        # 5. Ждём открытия окна с параметрами выгрузки
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".dynatree-title"))
        )

        time.sleep(2)

        # 6. Ищем пункт "Все параметры" и кликаем по нему
        all_params = driver.find_element(
            By.XPATH,
            "//span[contains(@class,'dynatree-title') and normalize-space()='Все параметры']",
        )
        driver.execute_script("arguments[0].click();", all_params)

        time.sleep(2)

        # 7. Нажимаем кнопку "ДАЛЕЕ"
        next_btn = wait.until(
            EC.element_to_be_clickable((By.ID, "btn-primary"))
        )
        driver.execute_script("arguments[0].click();", next_btn)

        # 8. Ждём появления списка файлов для выгрузки
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#unload-block-list .csvDownload"))
        )

        time.sleep(3)

        # 9. Собираем все кнопки "Выгрузить"
        download_buttons = driver.find_elements(By.CSS_SELECTOR, "#unload-block-list .csvDownload")
        logger.info("Найдено диапазонов для выгрузки: %s", len(download_buttons))

        # 10. Кликаем каждую кнопку выгрузки
        for i in range(len(download_buttons)):
            buttons = driver.find_elements(By.CSS_SELECTOR, "#unload-block-list .csvDownload")
            btn = buttons[i]

            text = btn.text.strip()
            data_from = btn.get_attribute("data-from")
            data_to = btn.get_attribute("data-to")
            csv_name = btn.get_attribute("data-csvname")

            logger.info(
                "Скачиваю: %s | %s-%s | %s",
                text,
                data_from,
                data_to,
                csv_name,
            )

            driver.execute_script("arguments[0].click();", btn)
            time.sleep(5)

        # 11. Ждём завершения скачиваний
        wait_for_downloads(RAW_DIR, timeout=600)

        # 12. Объединяем CSV
        final_df = merge_csv_files(RAW_DIR, RAW_MERGED_CSV, RAW_MERGED_XLSX)

        logger.info("Шаг 1 завершён")
        logger.info("Итоговый CSV: %s", RAW_MERGED_CSV)
        logger.info("Итоговый Excel: %s", RAW_MERGED_XLSX)
        logger.info("Строк в итоговом файле: %s", len(final_df))

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
