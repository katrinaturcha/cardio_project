"""
Шаг 1. Этот модуль скачивает выгрузки из ЕИС и собирает общий файл.
Здесь начинается весь проект. На выходе получаем один CSV и один Excel.
"""

from __future__ import annotations

import glob
import time
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.common.base import BasePipelineStep
from src.common.config import RAW_DIR, RAW_MERGED_CSV, RAW_MERGED_XLSX, SEARCH_TEXT


class EISExtractor(BasePipelineStep):
    logger_name = "module_01_extract"
    log_file_name = "module_01_extract.log"

    def __init__(self) -> None:
        super().__init__()
        self.download_dir = RAW_DIR
        self.search_text = SEARCH_TEXT
        self.result_csv = RAW_MERGED_CSV
        self.result_xlsx = RAW_MERGED_XLSX

    def _wait_for_downloads(self, timeout: int = 300) -> bool:
        start = time.time()
        while True:
            temp_files = glob.glob(str(self.download_dir / "*.crdownload"))
            if not temp_files:
                return True
            if time.time() - start > timeout:
                raise TimeoutError("Не дождались завершения скачивания файлов")
            time.sleep(2)

    def _create_driver(self) -> webdriver.Chrome:
        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": str(self.download_dir.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        return webdriver.Chrome(options=options)

    def _merge_csv_files(self) -> pd.DataFrame:
        csv_files = sorted(glob.glob(str(self.download_dir / "*.csv")))
        if not csv_files:
            raise FileNotFoundError("CSV-файлы не найдены")

        dataframes: list[pd.DataFrame] = []
        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path, sep=";", encoding="utf-8-sig")
            except Exception:
                try:
                    df = pd.read_csv(file_path, sep=";", encoding="cp1251")
                except Exception:
                    df = pd.read_csv(file_path)
            df["source_file"] = Path(file_path).name
            dataframes.append(df)

        final_df = pd.concat(dataframes, ignore_index=True)
        final_df.to_csv(self.result_csv, index=False, encoding="utf-8-sig")
        final_df.to_excel(self.result_xlsx, index=False)
        return final_df

    def run(self) -> None:
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info("Начат шаг 1. Скачивание данных из ЕИС")

        driver = self._create_driver()
        wait = WebDriverWait(driver, 60)

        try:
            driver.get("https://zakupki.gov.ru/epz/contract/search/search.html")

            search_input = wait.until(EC.presence_of_element_located((By.NAME, "searchString")))
            search_input.clear()
            search_input.send_keys(self.search_text)
            search_input.send_keys(Keys.ENTER)

            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.downLoad-search")))
            time.sleep(3)

            download_search_btn = driver.find_element(By.CSS_SELECTOR, "a.downLoad-search")
            driver.execute_script("arguments[0].click();", download_search_btn)

            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".dynatree-title")))
            time.sleep(2)

            all_params = driver.find_element(
                By.XPATH,
                "//span[contains(@class,'dynatree-title') and normalize-space()='Все параметры']",
            )
            driver.execute_script("arguments[0].click();", all_params)
            time.sleep(2)

            next_btn = wait.until(EC.element_to_be_clickable((By.ID, "btn-primary")))
            driver.execute_script("arguments[0].click();", next_btn)

            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#unload-block-list .csvDownload"))
            )
            time.sleep(3)

            download_buttons = driver.find_elements(By.CSS_SELECTOR, "#unload-block-list .csvDownload")
            self.logger.info("Найдено диапазонов для выгрузки: %s", len(download_buttons))

            for index in range(len(download_buttons)):
                buttons = driver.find_elements(By.CSS_SELECTOR, "#unload-block-list .csvDownload")
                button = buttons[index]
                self.logger.info(
                    "Скачиваю: %s | %s-%s | %s",
                    button.text.strip(),
                    button.get_attribute("data-from"),
                    button.get_attribute("data-to"),
                    button.get_attribute("data-csvname"),
                )
                driver.execute_script("arguments[0].click();", button)
                time.sleep(5)

            self._wait_for_downloads(timeout=600)
            final_df = self._merge_csv_files()
            self.log_rows("Шаг 1 завершён", final_df)
            self.logger.info("Итоговый CSV: %s", self.result_csv)
            self.logger.info("Итоговый Excel: %s", self.result_xlsx)
        finally:
            driver.quit()


def main() -> None:
    EISExtractor().run()


if __name__ == "__main__":
    main()
