from pydantic import BaseModel
from datetime import datetime
from typing import List
from ..base import ApiResponse

class Servers(BaseModel):
    id: int
    short_name: str
    display_name: str
    model_config = {"from_attributes": True}  


ServersListResponse = ApiResponse[List[Servers]]

Servers.model_rebuild()
ServersListResponse.model_rebuild()
