from pydantic import BaseModel
from datetime import datetime
from typing import List
from ..base import ApiResponse

class Character(BaseModel):
    id: int
    character_name: str
    display_name: str
    release_date: datetime
    list_order: int

    model_config = {"from_attributes": True}  

Character.model_rebuild()

CharactersListResponse = ApiResponse[List[Character]]
SingleCharacterResponse = ApiResponse[Character]

CharactersListResponse.model_rebuild()
SingleCharacterResponse.model_rebuild()