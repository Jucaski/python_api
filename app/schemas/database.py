import pandas as pd
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class CSVDatabase:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df = None
        self.load_data()

    def load_data(self) -> None:
        """Load CSV data into memory using chunks for large files."""
        try:
            # Read CSV in chunks and concatenate
            chunks = []
            for chunk in pd.read_csv(self.csv_path, chunksize=100000):
                chunks.append(chunk)
            self.df = pd.concat(chunks)
            logger.info(f"Successfully loaded {len(self.df)} records from {self.csv_path}")
        except Exception as e:
            logger.error(f"Error loading CSV file: {str(e)}")
            raise

    def search(self, column: str, value: str, limit: int = 100) -> tuple[int, List[Dict[str, Any]]]:
        """Search for records where column matches value."""
        if column not in self.df.columns:
            raise ValueError(f"Column {column} not found in database")
        
        # Case-insensitive partial match
        mask = self.df[column].astype(str).str.contains(value, case=False, na=False)
        matched_records = self.df[mask]
        total_matches = len(matched_records)
        
        # Convert to list of dicts and limit results
        records = matched_records.head(limit).to_dict('records')
        return total_matches, records

    def get_columns(self) -> List[str]:
        """Return list of available columns."""
        return self.df.columns.tolist()

    def get_stats(self) -> Dict[str, Any]:
        """Return basic statistics about the database."""
        return {
            "total_records": len(self.df),
            "columns": self.get_columns(),
            "memory_usage": self.df.memory_usage(deep=True).sum() / 1024**2  # MB
        }