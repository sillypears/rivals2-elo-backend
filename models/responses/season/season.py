from pydantic import BaseModel
from datetime import datetime
from typing import List
from ..base import ApiResponse

class Season(BaseModel):
    id: int
    start_date: datetime
    end_date: datetime
    short_name: str
    display_name: str
    latest: bool
    season_index: int|None
    steam_leaderboard: int|None
    model_config = {"from_attributes": True}  

Season.model_rebuild()

SeasonResponse = ApiResponse[Season]

SeasonResponse.model_rebuild()