# src/config.py
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent

DATA_RAW = PROJECT_ROOT / "flight-analysis/data/raw"
DATA_CLEANED = PROJECT_ROOT / "flight-analysis/data/cleaned"

print(PROJECT_ROOT)
print(DATA_RAW)