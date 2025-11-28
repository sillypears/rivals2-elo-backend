from pydantic import BaseModel
from datetime import datetime
from typing import List
from .base import ApiResponse

class Seasons(BaseModel):
    id: int
    start_date: datetime
    end_date: datetime
    short_name: str
    display_name: str
    latest: bool

    model_config = {"from_attributes": True}  

Seasons.model_rebuild()

SeasonListResponse = ApiResponse[List[Seasons]]
SingleSeasonResponse = ApiResponse[Seasons]

SeasonListResponse.model_rebuild()
SingleSeasonResponse.model_rebuild()