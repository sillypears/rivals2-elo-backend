from pydantic import BaseModel
from datetime import datetime
from ..base import ApiResponse
class SeasonLatest(BaseModel):
    id: int
    short_name: str
    display_name: str
    start_date: datetime
    end_date: datetime

    model_config = {"from_attributes": True}

SeasonLatestResponse = ApiResponse[SeasonLatest]
SeasonLatestResponse.model_rebuild()