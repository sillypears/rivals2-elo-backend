from pydantic.dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Match:
    match_date: datetime
    elo_rank_old: int
    elo_rank_new: int
    elo_change: int
    match_win: int
    ranked_game_number: int
    total_wins: int
    win_streak_value: int
    opponent_elo: int
    opponent_estimated_elo: int
    game_1_char_pick: Optional[int] = -1
    game_1_opponent_pick: Optional[int] = -1
    game_1_stage: Optional[int] = -1
    game_1_stage_name: Optional[int] = -1
    game_1_winner: Optional[int] = -1
    game_2_char_pick: Optional[int] = -1
    game_2_opponent_pick: Optional[int] = -1
    game_2_stage: Optional[int] = -1
    game_2_stage_name: Optional[int] = -1
    game_2_winner: Optional[int] = -1
    game_3_char_pick: Optional[int] = -1
    game_3_opponent_pick: Optional[int] = -1
    game_3_stage: Optional[int] = -1
    game_3_stage_name: Optional[int] = -1
    game_3_winner: Optional[int] = -1