import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import aiomysql
from typing import List
from dotenv import load_dotenv
from utils.match import Match
import asyncio
import json
from typing import Any, List, Dict
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


async def db_fetch_all(request: Request, query: str, params: tuple = ()) -> Dict[str, Any]:
    async with request.app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()
            if rows is None:
                return {"status": "FAIL", "data": []}
            return {"status": "OK", "data": rows}

async def db_fetch_one(request: Request, query: str, params: tuple = ()) -> Dict[str, Any]:
    async with request.app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, params)
            rows = await cur.fetchone()
            if rows is None:
                return {"status": "FAIL", "data": []}
            return {"status": "OK", "data": rows}

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
async def get_characters(req: Request):
    query = '''
        SELECT * FROM characters
    '''
    return await db_fetch_all(request=req, query=query)



@app.get("/stages")
async def get_characters(req: Request):
    query = '''
        SELECT * FROM stages
    '''
    return await db_fetch_all(request=req, query=query)


@app.get("/seasons")
async def get_seasons(req: Request):

    query = f'''
        SELECT 
            id,
            start_date, 
            end_date, 
            short_name, 
            display_name 
        FROM seasons
    '''
    return await db_fetch_all(request=req, query=query)


@app.get("/movelist")
async def get_movelist(req: Request):
    query = '''
        SELECT * FROM moves
    '''
    return await db_fetch_all(request=req, query=query)

@app.get("/matches")
@app.get("/matches/{limit}")
async def get_matches(req: Request, limit: int=None):
    try:
        if limit < 1: limit = 1
    except:
        limit = 0
    query = f'''
        SELECT * FROM matches_vw 
        {"LIMIT" if int(limit) else ''} {int(limit) if int(limit) else ''}
    '''
    return await db_fetch_all(request=req, query=query)


@app.get("/matches/{offset}/{limit}")
async def get_matches(req: Request, offset:int = 0, limit: int = 10):
    if limit < 1: limit = 1
    if offset < 0: offset = 0
    query = f'''
        SELECT * FROM matches_vw 
        LIMIT {limit} 
        OFFSET {offset}
    '''
    return await db_fetch_all(request=req, query=query)


@app.get("/stats")
async def get_stats(req: Request, limit: int = 10, skip: int = 0, match_win: bool = True):
    if limit < 1: limit = 10
    if skip < 0: skip = 0
    query = f'''
        SELECT * FROM matches_vw
        WHERE match_win = {1 if match_win else 0} 
        ORDER BY ranked_game_number DESC 
        LIMIT {limit} 
        OFFSET {skip}
    '''
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return sorted(rows, key=lambda x: x['ranked_game_number'])
        
@app.get("/char-stats")
async def get_char_stats(req: Request):
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
    return await db_fetch_all(request=req, query=query)


@app.get("/stage-stats")
async def get_stage_stats(req: Request):
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
    return await db_fetch_all(request=req, query=query)


@app.get("/match-stats")
async def get_match_stats(req: Request):
    query = ''' 
        SELECT
        game_count,
        match_win,
        COUNT(*) AS match_count
        FROM (
        SELECT
            id,
            match_win,
            match_forfeit,
            (
            CASE WHEN game_1_winner = 1 THEN 1 ELSE 0 END +
            CASE WHEN game_2_winner = 1 THEN 1 ELSE 0 END +
            CASE WHEN game_3_winner = 1 THEN 1 ELSE 0 END
            ) AS game_count
        FROM matches
        ) AS counted_matches
        WHERE NOT (
        (game_count = 0 AND match_win = 1 AND match_forfeit = 0)
        OR (game_count = 0 AND match_win = 0)
        )
        GROUP BY game_count, match_win
        ORDER BY game_count DESC, match_win DESC;
    '''
    return await db_fetch_all(request=req, query=query)

        
@app.get("/match-stage-stats")
async def get_match_stage_stats(req: Request):
    query = '''
        SELECT 
        s.id,
        s.display_name AS stage_name,
        COUNT(*) AS times_picked,
        CASE 
            WHEN m.stage_num = 1 THEN 'Starter'
            ELSE 'Counterpick'
        END AS pick_type
        FROM (
        SELECT game_1_stage AS stage_id, 1 AS stage_num, game_1_winner FROM matches_vw
        UNION ALL
        SELECT game_2_stage, 2, NULL FROM matches_vw
        UNION ALL
        SELECT game_3_stage, 3, NULL FROM matches_vw
        ) m
        JOIN stages s ON m.stage_id = s.id
        WHERE 
        (m.stage_num = 1 AND s.id != -1) -- only apply filter to game 1 rows
        OR (m.stage_num IN (2,3) AND s.counter_pick = 1)
        AND s.id != -1
        GROUP BY s.id, s.display_name, pick_type
        ORDER BY times_picked DESC, s.display_name, pick_type;

    '''
    return await db_fetch_all(request=req, query=query)
    
@app.get("/match/forfeits")
async def get_match_forfeits(req: Request):
    query = '''
        SELECT 
            COUNT(match_win) AS forfeits
        FROM
            matches_vw
        WHERE
            match_forfeit = 1
            AND match_win = 1
    '''
    return await db_fetch_one(request=req, query=query)


@app.get("/elo-change")
@app.get("/elo-change/{match_number}")
async def get_elo_changes(req: Request, match_number:int = 10):
    if match_number % 2 != 0: match_number += 1
    query = f'''
        SELECT
        (SELECT SUM(elo_change)
        FROM (
            SELECT elo_change
            FROM matches_vw m
            WHERE match_win = 1
            ORDER BY id DESC
            LIMIT {int(match_number)}
        ) AS wins) AS elo_change_plus,

        (SELECT SUM(elo_change)
        FROM (
            SELECT elo_change
            FROM matches_vw m
            WHERE match_win = 0
            ORDER BY id DESC
            LIMIT {int(match_number)}
        ) AS losses) AS elo_change_minus,
        (SELECT (elo_change_plus - abs(elo_change_minus))) as difference
    '''
    return await db_fetch_one(request=req, query=query)


@app.get("/final-move-stats")
async def get_final_move_stats(req: Request):
    query = '''
        SELECT 
            m.final_move_name, 
            COUNT(m.final_move_name) as final_move_count,
            m.final_move_short,
            m.season_display_name
        FROM
            matches_vw m
        WHERE
            m.match_win = 1 AND m.final_move_id != - 1
        GROUP BY m.final_move_name , m.season_display_name
    '''
    return await db_fetch_all(request=req, query=query)

@app.get("/all-seasons-stats")
async def get_all_seasons_stats(req: Request):
    query = '''
        SELECT
        season_display_name,
        COUNT(*) AS total_matches,
        SUM(CASE WHEN match_win = 1 THEN 1 ELSE 0 END) AS match_wins,
        SUM(CASE WHEN match_win = 0 THEN 1 ELSE 0 END) AS match_losses,
        SUM(elo_change) AS total_elo_change,
        ROUND(AVG(match_win) * 100, 2) AS win_rate_percent
        FROM matches_vw
        GROUP BY season_display_name
        ORDER BY season_id DESC;

    '''
    return await db_fetch_all(request=req, query=query)

@app.post("/insert-match")
async def insert_match(match: Match, debug: bool = 0):
    query = '''
        INSERT INTO matches (
            match_date, elo_rank_old, elo_rank_new, elo_change, match_win, match_forfeit,
            ranked_game_number, total_wins, win_streak_value, opponent_elo, opponent_estimated_elo, opponent_name,
            game_1_char_pick, game_1_opponent_pick, game_1_stage, game_1_winner,
            game_2_char_pick, game_2_opponent_pick, game_2_stage, game_2_winner,
            game_3_char_pick, game_3_opponent_pick, game_3_stage, game_3_winner, final_move_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    '''
    inserted_id = -1
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                print(                    match.match_date,
                    match.elo_rank_old, match.elo_rank_new, match.elo_change, match.match_win, match.match_forfeit,
                    match.ranked_game_number, match.total_wins, match.win_streak_value, match.opponent_elo, match.opponent_estimated_elo, match.opponent_name,
                    match.game_1_char_pick, match.game_1_opponent_pick, match.game_1_stage, match.game_1_winner,
                    match.game_2_char_pick, match.game_2_opponent_pick, match.game_2_stage, match.game_2_winner,
                    match.game_3_char_pick, match.game_3_opponent_pick, match.game_3_stage, match.game_3_winner, match.final_move_id)
                await cur.execute(query, (
                    match.match_date,
                    match.elo_rank_old, match.elo_rank_new, match.elo_change, match.match_win, match.match_forfeit,
                    match.ranked_game_number, match.total_wins, match.win_streak_value, match.opponent_elo, match.opponent_estimated_elo, match.opponent_name,
                    match.game_1_char_pick, match.game_1_opponent_pick, match.game_1_stage, match.game_1_winner,
                    match.game_2_char_pick, match.game_2_opponent_pick, match.game_2_stage, match.game_2_winner,
                    match.game_3_char_pick, match.game_3_opponent_pick, match.game_3_stage, match.game_3_winner, match.final_move_id
                ))
                await notify_websockets({'type': 'new_match', 'ranked_game_number': int(match.ranked_game_number)})
                if match.match_win == 1:
                    await notify_websockets({'type': 'new_win_stats', 'ranked_game_number': int(match.ranked_game_number)})
                elif match.match_win == 0:
                    await notify_websockets({'type': 'new_lose_stats', 'ranked_game_number': int(match.ranked_game_number)})

                inserted_id = cur.lastrowid

            except Exception as e:
                return {"status": "failed", "error": e.args}
    return {"status": "ok", "last_id": inserted_id}

@app.patch("/update-match/")
async def update_match(update_value: dict):
    try:
        match_id = int(update_value['row_id'])
    except (ValueError, KeyError):
        return {"status": "failure", "message": "No ID provided or invalid ID format"}
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
                nmessage = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                nmessage_data = {}
                try:
                    nmessage_data = json.loads(nmessage)
                except:
                    pass
                await notify_websockets(nmessage_data)
            except asyncio.TimeoutError:
                await websocket.send_text("ping")
    except WebSocketDisconnect:
        websockets.remove(websocket)
    except Exception:
        websockets.remove(websocket)

async def notify_websockets(message: dict):
    for ws in websockets[:]: 
        try:
            await ws.send_json(message)
            print(f"Sent: {message}")
        except Exception:
            print("WebSocket failed; removing")
            websockets.remove(ws)