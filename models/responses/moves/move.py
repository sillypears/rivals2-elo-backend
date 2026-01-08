from pydantic import BaseModel
from datetime import datetime
from typing import List
from ..base import ApiResponse

class Move(BaseModel):
    id: int
    display_name: str
    short_name: str
    list_order: int
    category: str

    model_config = {"from_attributes": True}  

Move.model_rebuild()

MovesListResponse = ApiResponse[List[Move]]
SingleMoveResponse = ApiResponse[Move]

MovesListResponse.model_rebuild()
SingleMoveResponse.model_rebuild()