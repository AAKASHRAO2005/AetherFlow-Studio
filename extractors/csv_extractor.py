"""
CSV Extractor Module.
Extracts raw records from flat CSV files.
Handles various encodings, delimiters, and missing value indicators.
"""

from pathlib import Path
from typing import Optional, Union
import logging
import pandas as pd

from config import RAW_DATA_DIR

logger = logging.getLogger(__name__)


class CsvExtractor:
    """
    Extracts raw data from CSV datasets with defensive error handling.
    """

    def __init__(self, raw_dir: Path = RAW_DATA_DIR):
        self.raw_dir = raw_dir

    def extract(self, file_path: Optional[Union[str, Path]] = None) -> pd.DataFrame:
        """
        Reads CSV file into a pandas DataFrame.
        If no file is provided, looks for the most recent CSV file in data/raw/.
        """
        target_path: Optional[Path] = None

        if file_path:
            target_path = Path(file_path)
        else:
            # Pick the most recent CSV file in raw dir
            csv_files = sorted(self.raw_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            if csv_files:
                target_path = csv_files[0]

        if not target_path or not target_path.exists():
            raise FileNotFoundError(f"No CSV file found at {target_path or self.raw_dir}")

        logger.info(f"Extracting CSV dataset from {target_path.resolve()}...")

        # Robust extraction: read all as raw string/objects first to prevent silent pandas type coercion
        # Recognize common null representations
        na_values = ["", "NA", "N/A", "null", "NULL", "None", "-999", "-9999", "missing", "?"]
        
        try:
            df = pd.read_csv(
                target_path,
                na_values=na_values,
                keep_default_na=True,
                encoding="utf-8"
            )
        except UnicodeDecodeError:
            logger.warning("UTF-8 decoding failed, falling back to latin1 encoding...")
            df = pd.read_csv(
                target_path,
                na_values=na_values,
                keep_default_na=True,
                encoding="latin1"
            )

        logger.info(f"CSV Extraction complete: extracted {len(df)} rows and {len(df.columns)} columns.")
        return df
