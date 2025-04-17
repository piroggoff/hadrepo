# src/config.py
from pathlib import Path


class Paths:
    """Конфигурационная модель путей проекта."""

    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent

    DATA_DIR: Path = PROJECT_ROOT / "flight-analysis/data"
    DATA_RAW: Path = DATA_DIR / "raw"
    DATA_CLEANED: Path = DATA_DIR / "cleaned"


    @classmethod
    def print_all(cls):
        """Show all paths"""
        for attr in dir(cls):
            if not attr.startswith("_") and isinstance(getattr(cls, attr), Path):
                print(f"{attr}: {getattr(cls, attr)}")
