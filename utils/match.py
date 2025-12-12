from pydantic.dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Match:
    match_date: datetime 
    match_id: int = -1
    elo_rank_new: int = -1
    elo_rank_old: int = -1
    elo_change: int = -9999
    match_win: int = -1
    match_forfeit: int = -1
    ranked_game_number: int = -1
    total_wins: int = -1
    win_streak_value: int = -1
    opponent_elo: int = -1
    opponent_estimated_elo: int = -1
    opponent_name: Optional[str] = ""
    game_1_char_pick: Optional[int] = -1
    game_1_opponent_pick: Optional[int] = -1
    game_1_stage: Optional[int] = -1
    game_1_stage_name: Optional[int] = -1
    game_1_winner: Optional[int] = -1
    game_1_final_move_id: Optional[int] = -1
    game_1_duration: Optional[int] = -1 
    game_2_char_pick: Optional[int] = -1
    game_2_opponent_pick: Optional[int] = -1
    game_2_stage: Optional[int] = -1
    game_2_stage_name: Optional[int] = -1
    game_2_winner: Optional[int] = -1
    game_2_final_move_id: Optional[int] = -1
    game_2_duration: Optional[int] = -1 
    game_3_char_pick: Optional[int] = -1
    game_3_opponent_pick: Optional[int] = -1
    game_3_stage: Optional[int] = -1
    game_3_stage_name: Optional[int] = -1
    game_3_winner: Optional[int] = -1
    game_3_final_move_id: Optional[int] = -1
    game_3_duration: Optional[int] = -1 
    season_id: Optional[int] = -1
    final_move_id: Optional[int] = -1
    notes: Optional[str] = ""