from pathlib import Path
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres").strip()
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "").strip()
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost").strip()
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432").strip()
POSTGRES_DB = os.getenv("POSTGRES_DB", "zakupki").strip()
POSTGRES_SYSTEM_DB = os.getenv("POSTGRES_SYSTEM_DB", "postgres").strip()

SEARCH_TEXT = os.getenv("SEARCH_TEXT", "кардиоплег")
START_DATE = os.getenv("START_DATE", "2024-01-01")

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
RAW_DIR = Path(os.getenv("RAW_DIR", str(DATA_DIR / "raw")))
LABELING_DIR = Path(os.getenv("LABELING_DIR", str(DATA_DIR / "labeling")))
ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", str(BASE_DIR / "artifacts")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(BASE_DIR / "logs")))

LABELED_FILE_NAME = os.getenv("LABELED_FILE_NAME", "cardioplegia_labeled.xlsx")
LABELED_FILE_PATH = LABELING_DIR / LABELED_FILE_NAME
LABELING_FILE_PATH = LABELING_DIR / "cardioplegia_labeling.xlsx"
RAW_MERGED_CSV = RAW_DIR / "cardiopleg_all.csv"
RAW_MERGED_XLSX = RAW_DIR / "cardiopleg_all.xlsx"
ML_DIR = ARTIFACTS_DIR / "ml"
PLOTS_DIR = ARTIFACTS_DIR / "plots"
REPORTS_DIR = ARTIFACTS_DIR / "reports"

for path in [DATA_DIR, RAW_DIR, LABELING_DIR, ARTIFACTS_DIR, LOG_DIR, ML_DIR, PLOTS_DIR, REPORTS_DIR]:
    path.mkdir(parents=True, exist_ok=True)
