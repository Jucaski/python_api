from fastapi import FastAPI, HTTPException
from .schemas.database import CSVDatabase
from .models.basic_model import SearchParams, DataResponse
import os
import glob
from typing import List, Dict

app = FastAPI(title="Multi-CSV Database API")

class MultiCSVDatabase:
    def __init__(self, databases: List[CSVDatabase]):
        self.databases = databases
        
    def get_columns(self) -> List[str]:
        """Get unique columns from all databases."""
        columns = set()
        for db in self.databases:
            columns.update(db.get_columns())
        return sorted(list(columns))
        
    def search(self, column: str, value: str, limit: int) -> tuple:
        """Search across all databases."""
        total_matches = 0
        all_records = []
        
        for db in self.databases:
            try:
                matches, records = db.search(column=column, value=value, limit=limit-len(all_records))
                total_matches += matches
                all_records.extend(records)
                
                # If we've reached the limit, stop searching
                if len(all_records) >= limit:
                    break
                    
            except ValueError:
                # Skip databases that don't have the specified column
                continue
                
        return total_matches, all_records

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    db_path = os.path.join(os.path.dirname(__file__), "db")
    csv_files = glob.glob(os.path.join(db_path, "*.csv"))
    
    if not csv_files:
        raise Exception("No CSV files found in the db directory")
        
    databases = []
    for csv_file in csv_files:
        try:
            db = CSVDatabase(csv_file)
            databases.append(db)
        except Exception as e:
            print(f"Warning: Failed to load {csv_file}: {str(e)}")
            
    app.state.db = MultiCSVDatabase(databases)

@app.get("/")
async def root():
    """Get basic information about the API."""
    return {
        "message": "Multi-CSV Database API",
        "endpoints": [
            "/search - Search records across all databases",
            "/columns - List available columns from all databases",
        ]
    }

@app.get("/columns")
async def get_columns():
    """Get list of available columns across all databases."""
    return {"columns": app.state.db.get_columns()}

@app.get("/stats")
async def get_stats():
    """Get combined database statistics."""
    return app.state.db.get_stats()

@app.post("/search", response_model=DataResponse)
async def search(params: SearchParams, limit: int = 100):
    """Search records across all databases."""
    try:
        total_matches, records = app.state.db.search(
            column=params.column,
            value=params.value,
            limit=limit
        )
        return {
            "total_records": total_matches,
            "records": records
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))