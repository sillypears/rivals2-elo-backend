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
    allow_origins=["http://192.168.1.30:8006", "http://192.168.1.30:8007"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

websockets: List[WebSocket] = []

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse('favicon.ico')

@app.get("/characters")
async def get_characters():
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM characters;")
            rows = await cur.fetchall()
            return rows

@app.get("/stages")
async def get_characters():
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM stages;")
            rows = await cur.fetchall()
            return rows

@app.get("/seasons")
async def get_seasons():
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT start_date, end_date, short_name, display_name FROM seasons")
            rows = await cur.fetchall()
            return rows
        
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
            await cur.execute(f"SELECT * FROM matches_vw ORDER BY ranked_game_number DESC LIMIT {limit}")
            rows = await cur.fetchall()
            return rows

@app.get("/matches/{offset}/{limit}")
async def get_matches(offset:int = 0, limit: int = 10):
    if limit < 1: limit = 1
    if offset < 0: offset = 0
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM matches_vw ORDER BY ranked_game_number DESC LIMIT {limit} OFFSET {offset}")
            rows = await cur.fetchall()
            return rows

@app.get("/stats")
async def get_stats(limit: int = 10, skip: int = 0, match_win: bool = True):
    if limit < 1: limit = 10
    if skip < 0: skip = 0
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM matches_vw WHERE match_win = {1 if match_win else 0} ORDER BY ranked_game_number DESC LIMIT {limit} OFFSET {skip}")
            rows = await cur.fetchall()
            return sorted(rows, key=lambda x: x['ranked_game_number'])
        
@app.get("/char-stats")
async def get_char_stats():
    query = '''
        SELECT
            opponent_pick,
            SUM(CASE WHEN game_winner = 1 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN game_winner = 2 THEN 1 ELSE 0 END) AS losses,
            COUNT(*) AS total_games,
            ROUND(SUM(CASE WHEN game_winner = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS win_percentage
        FROM (
            SELECT game_1_opponent_pick_name AS opponent_pick, game_1_winner AS game_winner FROM rivals2.matches_vw
            UNION ALL
            SELECT game_2_opponent_pick_name, game_2_winner FROM rivals2.matches_vw
            UNION ALL
            SELECT game_3_opponent_pick_name, game_3_winner FROM rivals2.matches_vw
        ) AS games
        WHERE opponent_pick NOT IN ('N/A', '') AND game_winner IN (1, 2)
        GROUP BY opponent_pick
        ORDER BY win_percentage DESC, total_games DESC;
            '''
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return rows

@app.get("/stage-stats")
async def get_stage_stats():
    query = '''
        SELECT
            stage_name,
            SUM(CASE WHEN game_winner = 1 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN game_winner = 2 THEN 1 ELSE 0 END) AS losses,
            COUNT(*) AS total_games,
            ROUND(SUM(CASE WHEN game_winner = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS win_percentage
        FROM (
            SELECT game_1_stage_name AS stage_name, game_1_winner AS game_winner FROM rivals2.matches_vw
            UNION ALL
            SELECT game_2_stage_name, game_2_winner FROM rivals2.matches_vw
            UNION ALL
            SELECT game_3_stage_name, game_3_winner FROM rivals2.matches_vw
        ) AS games
        WHERE stage_name NOT IN ('N/A', '') AND game_winner IN (1, 2)
        GROUP BY stage_name
        ORDER BY win_percentage DESC, total_games DESC;
            '''
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return rows

@app.get("/match-stats")
async def get_match_stats():
    query = ''' 
        SELECT
        game_count,
        match_win,
        COUNT(*) AS match_count
        FROM (
        SELECT
            id,
            match_win,
            -- Count how many of the 3 games have a valid winner (1 or 2)
            (CASE WHEN game_1_winner IN (1, 2) THEN 1 ELSE 0 END +
            CASE WHEN game_2_winner IN (1, 2) THEN 1 ELSE 0 END +
            CASE WHEN game_3_winner IN (1, 2) THEN 1 ELSE 0 END) AS game_count
        FROM rivals2.matches_vw
        ) AS counted_matches
        WHERE game_count > 0
        GROUP BY game_count, match_win
        ORDER BY game_count DESC, match_win DESC;
    '''
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query)
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
                await notify_websockets({'type': 'new_match', 'ranked_game_number': int(match.ranked_game_number)})
                if match.match_win == 1:
                    await notify_websockets({'type': 'new_win_stats', 'ranked_game_number': int(match.ranked_game_number)})
                elif match.match_win == 0:
                    await notify_websockets({'type': 'new_lose_stats', 'ranked_game_number': int(match.ranked_game_number)})

                inserted_id = cur.lastrowid

            except aiomysql.IntegrityError as e:
                return {"status": "failed", "error": e.args}
    return {"status": "ok", "last_id": inserted_id}

@app.patch("/update-match/")
async def update_match(update_value: dict):
    try:
        game_number = int(update_value['game_number'])
    except:
        return {"status": "failure", "message": "No ID provided"}
    match_id_exists = None

    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT id FROM matches WHERE ranked_game_number = {game_number}")
            rows = await cur.fetchone()
            match_id = rows["id"]

    if match_id:
        print(f"""UPDATE matches SET {update_value['key']}="{update_value['value']}" WHERE id = {match_id}""")
        async with app.state.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"""UPDATE matches SET {update_value['key']}="{update_value['value']}" WHERE id = {match_id}""")
                rows = await cur.fetchall()
                return {"status": "ok", "data": rows}
    return 0

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