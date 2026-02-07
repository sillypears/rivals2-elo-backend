from pydantic import BaseModel
from datetime import datetime
from typing import List
from ..base import ApiResponse

class BestWins(BaseModel):
    id: int
    ranked_game_number: int
    match_date: datetime
    elo_rank_old: int
    elo_rank_new: int
    elo_change: int
    total_wins: int
    win_streak_value: int
    opponent_elo: int
    season_id: int
    season_display_name: str
    model_config = {"from_attributes": True}  

BestWins.model_rebuild()

BestWinsListResponse = ApiResponse[List[BestWins]]

BestWinsListResponse.model_rebuild()
