import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app 

@pytest.mark.asyncio
async def test_get_characters():
    # Dummy character data
    mock_characters = [{"id": 1, "character_name": "fors", "display_name": "Forsburn"}]

    # Mock the DB cursor behavior
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__.return_value.fetchall.return_value = mock_characters

    mock_conn = AsyncMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_pool = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    # Patch the database pool on the app
    with patch.object(app.state, "db_pool", mock_pool):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            response = await ac.get("/characters")

        assert response.status_code == 200
        assert response.json() == mock_characters
