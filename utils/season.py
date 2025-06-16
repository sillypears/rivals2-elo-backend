from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Match(BaseModel):
    start_date: datetime
    end_date: datetime
    name: str
    short_name: str
    display_name: str

    def __init__(self, start_date: datetime, end_date: datetime, name: str, short_name: str, display_name: str):
        self.start_date = start_date
        self.end_date = end_date
        self.name = name
        self.short_name = short_name
        self.display_name = display_name
        return self

    def __str__(self):
        return f"Season ({self.display_name} starts on {self.start_date} and ends on {self.end_date})"
    
    def __json__(self):
        return {
            "startDate": self.start_date.isoformat(),
            "endDate": self.end_date.isoformat(),
            "name": self.name,
            "shortName": self.short_name,
            "displayName": self.display_name
        }