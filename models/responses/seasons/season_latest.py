from pydantic import BaseModel
from datetime import datetime

class SeasonLatest(BaseModel):
    id: int
    short_name: str
    display_name: str
    start_date: datetime
    end_date: datetime

    model_config = {"from_attributes": True}

SeasonLatest.model_rebuild()