from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import logging
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, validator
import traceback
import aiomysql

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom Exception Classes
class DatabaseError(Exception):
    """Raised when database operations fail"""
    pass

class ValidationError(Exception):
    """Raised when data validation fails"""
    pass

class MatchNotFoundError(Exception):
    """Raised when a match is not found"""
    pass

# Response Models
class ErrorResponse(BaseModel):
    status: str = "ERROR"
    message: str
    error_code: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

class SuccessResponse(BaseModel):
    status: str = "SUCCESS"
    data: Any
    message: Optional[str] = None

# Define allowed columns for updates (whitelist approach)
ALLOWED_UPDATE_COLUMNS = {
    'match_date', 'elo_rank_old', 'elo_rank_new', 'elo_change', 'match_win', 
    'match_forfeit', 'ranked_game_number', 'total_wins', 'win_streak_value', 
    'opponent_elo', 'opponent_estimated_elo', 'opponent_name',
    'game_1_char_pick', 'game_1_opponent_pick', 'game_1_stage', 'game_1_winner', 
    'game_1_final_move_id', 'game_2_char_pick', 'game_2_opponent_pick', 
    'game_2_stage', 'game_2_winner', 'game_2_final_move_id', 'game_3_char_pick', 
    'game_3_opponent_pick', 'game_3_stage', 'game_3_winner', 'game_3_final_move_id',
    'final_move_id'
}

# Pydantic model for request validation
class UpdateMatchRequest(BaseModel):
    row_id: int = Field(..., description="Match ID to update", gt=0)
    key: str = Field(..., description="Column name to update")
    value: Any = Field(..., description="New value for the column")

# Global Exception Handlers
async def database_exception_handler(request: Request, exc: DatabaseError):
    logger.error(f"Database error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            message="Database operation failed",
            error_code="DATABASE_ERROR"
        ).model_dump()
    )

async def validation_exception_handler(request: Request, exc: ValidationError):
    logger.warning(f"Validation error: {str(exc)}")
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            message=str(exc),
            error_code="VALIDATION_ERROR"
        ).model_dump()
    )

async def match_not_found_handler(request: Request, exc: MatchNotFoundError):
    return JSONResponse(
        status_code=404,
        content=ErrorResponse(
            message=str(exc),
            error_code="MATCH_NOT_FOUND"
        ).model_dump()
    )

async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Request validation error: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            message="Invalid request data",
            error_code="REQUEST_VALIDATION_ERROR",
            details={"validation_errors": exc.errors()}
        ).model_dump()
    )

async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unexpected error: {str(exc)}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            message="An unexpected error occurred",
            error_code="INTERNAL_SERVER_ERROR"
        ).model_dump()
    )

async def safe_db_fetch_all(request: Request, query: str, params: tuple = ()) -> Dict[str, Any]:
    """Safe database fetch with proper error handling - uses request.app.state.db_pool"""
    try:
        async with request.app.state.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()
                return SuccessResponse(data=rows or []).model_dump()
    except Exception as e:
        logger.error(f"Database query failed: {query[:100]}... Error: {str(e)}")
        raise DatabaseError(f"Failed to fetch data: {str(e)}")

async def safe_db_fetch_one(request: Request, query: str, params: tuple = ()) -> Dict[str, Any]:
    """Safe database fetch one with proper error handling"""
    try:
        async with request.app.state.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params)
                row = await cur.fetchone()
                if row is None:
                    raise MatchNotFoundError("No data found for the given criteria")
                return SuccessResponse(data=row).model_dump()
    except MatchNotFoundError:
        raise
    except Exception as e:
        logger.error(f"Database query failed: {query[:100]}... Error: {str(e)}")
        raise DatabaseError(f"Failed to fetch data: {str(e)}")

# Alternative: Database helper that takes db_pool directly (for use in main.py)
async def safe_db_execute_with_pool(db_pool, query: str, params: tuple = ()) -> Dict[str, Any]:
    """Execute query with direct pool access - for use in main.py functions"""
    try:
        async with db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params)
                return {"rowcount": cur.rowcount, "lastrowid": getattr(cur, 'lastrowid', None)}
    except Exception as e:
        logger.error(f"Database execute failed: {query[:100]}... Error: {str(e)}")
        raise DatabaseError(f"Failed to execute query: {str(e)}")

# Export everything needed
__all__ = [
    'DatabaseError', 'ValidationError', 'MatchNotFoundError',
    'ErrorResponse', 'SuccessResponse', 'UpdateMatchRequest',
    'ALLOWED_UPDATE_COLUMNS', 'safe_db_fetch_all', 'safe_db_fetch_one',
    'safe_db_execute_with_pool',
    'database_exception_handler', 'validation_exception_handler', 
    'match_not_found_handler', 'request_validation_exception_handler',
    'general_exception_handler', 'logger', 'RequestValidationError'
]