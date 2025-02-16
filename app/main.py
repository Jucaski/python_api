from fastapi import FastAPI, HTTPException
from .database import CSVDatabase
from .models import SearchParams, DataResponse
import os

app = FastAPI(title="CSV Database API")

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    csv_path = os.getenv("CSV_PATH", "db/def00_19_v1.csv")
    app.state.db = CSVDatabase(csv_path)

@app.get("/")
async def root():
    """Get basic information about the API."""
    return {
        "message": "CSV Database API",
        "endpoints": [
            "/search - Search records",
            "/columns - List available columns",
            "/stats - Get database statistics"
        ]
    }

@app.get("/columns")
async def get_columns():
    """Get list of available columns in the database."""
    return {"columns": app.state.db.get_columns()}

@app.get("/stats")
async def get_stats():
    """Get database statistics."""
    return app.state.db.get_stats()

@app.post("/search", response_model=DataResponse)
async def search(params: SearchParams, limit: int = 100):
    """Search records in the database."""
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
