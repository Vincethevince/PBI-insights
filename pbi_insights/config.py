"""Project-wide path constants."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data"
RAW_PBI_PATH = DATA_PATH / "pbi_files"
UNZIPPED_PATH = DATA_PATH / "unzipped_pbi_folders"
PBIR_PATH = DATA_PATH / "pbir_folders"
OUTPUT_PATH = PROJECT_ROOT / "output"
VECTOR_DB_PATH = PROJECT_ROOT / "vector_db"

