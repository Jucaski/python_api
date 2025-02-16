from pydantic import BaseModel
from typing import Optional, List

class SearchParams(BaseModel):
    column: str
    value: str
    
class DataResponse(BaseModel):
    total_records: int
    records: List[dict]