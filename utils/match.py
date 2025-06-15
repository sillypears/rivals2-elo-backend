from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Match(BaseModel):
    match_date: datetime
    elo_rank_old: int
    elo_rank_new: int
    elo_change: int
    ranked_game_number: int
    total_wins: int
    win_streak_value: int
    opponent_elo: int
    game_1_char_pick: Optional[str] = "None"
    game_1_opponent_pick: Optional[str] = "None"
    game_1_stage: Optional[str] = "None"
    game_1_winner: Optional[int] = -1
    game_2_char_pick: Optional[str] = "None"
    game_2_opponent_pick: Optional[str] = "None"
    game_2_stage: Optional[str] = "None"
    game_2_winner: Optional[int] = -1
    game_3_char_pick: Optional[str] = "None"
    game_3_opponent_pick: Optional[str] = "None"
    game_3_stage: Optional[str] = "None"
    game_3_winner: Optional[int] = -1