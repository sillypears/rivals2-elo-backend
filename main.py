import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import aiomysql
from typing import List
from dotenv import load_dotenv
from utils.match import Match
import asyncio
import json

from pydantic import TypeAdapter
load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await aiomysql.create_pool(
        host=os.environ.get("DB_HOST"), 
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASS"), 
        db=os.environ.get("DDB_SCHEMA") if os.environ.get("DEBUG") else os.environ.get("DB_SCHEMA"),
        autocommit=True,
    )
    app.state.db_pool = pool
    try:
        yield
    finally:
        pool.close()
        await pool.wait_closed()

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://192.168.1.30:8006"],  # frontend dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

websockets: List[WebSocket] = []

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse('favicon.ico')

@app.get("/matches")
async def get_matches():
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM matches ORDER BY ranked_game_number DESC")
            rows = await cur.fetchall()
            return rows


@app.get("/matches/{limit}")
async def get_matches(limit:int=10):
    if limit < 1: limit = 1
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM matches ORDER BY ranked_game_number DESC LIMIT {limit}")
            rows = await cur.fetchall()
            return rows

@app.get("/matches/{offset}/{limit}")
async def get_matches(offset:int = 0, limit: int = 10):
    if limit < 1: limit = 1
    if offset < 0: offset = 0
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM matches ORDER BY ranked_game_number DESC LIMIT {limit} OFFSET {offset}")
            rows = await cur.fetchall()
            return rows

@app.get("/stats")
async def get_stats(limit: int = 10, skip: int = 0, match_win: bool = True):
    if limit < 1: limit = 10
    if skip < 0: skip = 0
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM matches WHERE match_win = {1 if match_win else 0} ORDER BY ranked_game_number DESC LIMIT {limit} OFFSET {skip}")
            rows = await cur.fetchall()
            return rows
        


@app.post("/insert-match")
async def insert_match(match: Match, debug: bool = 0):
    query = '''
        INSERT INTO matches (
            match_date, elo_rank_old, elo_rank_new, elo_change, match_win,
            ranked_game_number, total_wins, win_streak_value, opponent_elo,
            game_1_char_pick, game_1_opponent_pick, game_1_stage, game_1_winner,
            game_2_char_pick, game_2_opponent_pick, game_2_stage, game_2_winner,
            game_3_char_pick, game_3_opponent_pick, game_3_stage, game_3_winner
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    '''
    inserted_id = -1
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(query, (
                    match.match_date,
                    match.elo_rank_old, match.elo_rank_new, match.elo_change, match.match_win,
                    match.ranked_game_number, match.total_wins, match.win_streak_value, match.opponent_elo,
                    match.game_1_char_pick, match.game_1_opponent_pick, match.game_1_stage, match.game_1_winner,
                    match.game_2_char_pick, match.game_2_opponent_pick, match.game_2_stage, match.game_2_winner,
                    match.game_3_char_pick, match.game_3_opponent_pick, match.game_3_stage, match.game_3_winner
                ))
                await notify_websockets({'type': 'new_match', 'ranked_game_number': match.ranked_game_number})
                inserted_id = cur.lastrowid

            except aiomysql.IntegrityError as e:
                return {"status": "failed", "error": e.args}
    return {"status": "ok", "last_id": inserted_id}

@app.get("/seasons")
async def get_seasons():
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT start_date, end_date, short_name, display_name FROM seasons")
            rows = await cur.fetchall()
            return rows


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websockets.append(websocket)
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                await websocket.send_text("ping")  # optional keep-alive
    except WebSocketDisconnect:
        websockets.remove(websocket)
    except Exception:
        websockets.remove(websocket)  # catch-all for disconnects

async def notify_websockets(message: dict):
    for ws in websockets[:]: 
        try:
            await ws.send_json(message)
            print(f"Sent: {message}")
        except Exception:
            print("WebSocket failed; removing")
            websockets.remove(ws)