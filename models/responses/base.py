from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")

class ApiResponse(BaseModel, Generic[T]):
    status: str = "SUCCESS"
    data: T
    message: str | None = None

    model_config = {
        "json_encoders": {
            "datetime": lambda v: v.isoformat() if v else None
        }
    }

ApiResponse.model_rebuild()