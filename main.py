import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, status
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
from pydantic import TypeAdapter, BaseModel, Field
import utils.errors as err
from datetime import datetime

load_dotenv()
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


class UpdateMatchRequest(BaseModel):
    row_id: int = Field(..., description="Match ID to update", gt=0)
    key: str = Field(..., description="Column name to update")
    value: Any = Field(..., description="New value for the column")


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    pool = await aiomysql.create_pool(
        host=os.environ.get("DB_HOST"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASS"),
        db=os.environ.get("DDB_SCHEMA") if os.environ.get(
            "DEBUG") else os.environ.get("DB_SCHEMA"),
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
            return err.SuccessResponse(data=rows)


async def db_fetch_one(request: Request, query: str, params: tuple = ()) -> Dict[str, Any]:
    async with request.app.state.db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, params)
            rows = await cur.fetchone()
            if rows is None:
                return err.ErrorResponse(data=[])
            return err.SuccessResponse(data=rows)

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://192.168.1.30:8006", "http://192.168.1.30:8007"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_exception_handler(err.DatabaseError, err.database_exception_handler)
app.add_exception_handler(err.ValidationError, err.validation_exception_handler)
app.add_exception_handler(err.MatchNotFoundError, err.match_not_found_handler)
app.add_exception_handler(err.RequestValidationError, err.validation_exception_handler)
app.add_exception_handler(Exception, err.general_exception_handler)

websockets: List[WebSocket] = []


class DatabaseError(Exception):
    pass


@app.exception_handler(DatabaseError)
async def database_exception_handler(request, exc):
    raise HTTPException(status_code=500, detail="Database operation failed")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> FileResponse:
    return FileResponse('favicon.ico')

async def check_database(req: Request):
    try:
        async with req.app.state.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT 1")
                rows = await cur.fetchone()
        return {"status": "healthy", "message": "Database connection successful"}
    except Exception as e:
        return {"status": "unhealthy", "message": str(e)}

@app.get("/healthcheck", tags=["Health"])
async def health_check(req: Request):
    db_status = await check_database(req)
   
    if db_status["status"] == "unhealthy":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "timestamp": datetime.now().isoformat(),
                "status": "unhealthy",
                "details": {
                    "database": db_status
                }
            }
        )
    
    return {
        "timestamp": datetime.now().isoformat(),
        "status": "healthy",
        "details": {
            "database": db_status
        }
    }

@app.get("/characters", tags=["Characters", "Meta"])
async def get_characters(req: Request) -> dict:
    query = '''
        SELECT * FROM characters
    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/stages", tags=["Stages", "Meta"])
async def get_characters(req: Request) -> dict:
    query = '''
        SELECT * FROM stages
    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/seasons", tags=["Seasons", "Meta"])
async def get_seasons(req: Request) -> dict:

    query = f'''
        SELECT 
            id,
            start_date, 
            end_date, 
            short_name, 
            display_name 
        FROM seasons
    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/season/latest", tags=["Seasons", "Meta"])
async def get_latest_season(req: Request):
    query = '''
        SELECT id FROM seasons WHERE '%s' BETWEEN start_date AND end_date
    ''' % (datetime.now().isoformat())
    return await err.safe_db_fetch_one(request=req, query=query)

@app.get("/ranked_tiers", tags=["Ranked", "Meta"])
async def get_ranked_tier_list(req: Request) -> dict:

    query = f'''
        SELECT 
            *
        FROM tiers
    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/opponent_names", tags=["Ranked", "Meta"])
async def get_opponent_name_list(req: Request):
    try:
        query = f'''
            SELECT 
                DISTINCT(opponent_name),
                COUNT(opponent_name) as count
            FROM
                matches_vw
            WHERE
                opponent_name <> ''
            GROUP BY opponent_name
            ORDER BY opponent_name ASC
  
        '''
        opponent_list = await err.safe_db_fetch_all(request=req, query=query)

        data = {
            'names': [x['opponent_name'] for x in opponent_list['data']],
            'counts': {x['opponent_name']: x['count'] for x in opponent_list['data']}
        }
        return err.SuccessResponse(data=data)
    except Exception as e:
        return err.ErrorResponse(message=f"Could not get opponent_list: {e}")


@app.get("/current_tier", tags=["Performance"])
async def get_current_tier(req: Request) -> dict:
    try:
        tiers = await get_ranked_tier_list(req)
        query = f'''
            SELECT
                elo_rank_new,
                ranked_game_number,
                win_streak_value,
                total_wins
            FROM
                matches_vw
            ORDER BY id DESC
            LIMIT 1
        '''
        elo_raw = await err.safe_db_fetch_one(request=req, query=query)
        elo = int(elo_raw['data']['elo_rank_new'])
        game_no = elo_raw['data']['ranked_game_number']
        current_tier = {
            "current_elo": elo,
            "tier": "",
            "tier_short": "",
            "last_game_number": int(game_no),
            "win_streak_value": int(elo_raw['data']['win_streak_value']),
            "total_wins": int(elo_raw['data']['total_wins'])
        }
        for tier in tiers['data']:
            if elo > int(tier['min_threshold']) and elo < int(tier['max_threshold']):
                current_tier['tier'] = tier['tier_display_name']
                current_tier['tier_short'] = tier['tier_short_name']
        return err.SuccessResponse(data=current_tier).model_dump()

    except Exception as e:
        print(f"eeee: {e}")
        return err.ErrorResponse(message=str(e))


@app.get("/movelist", tags=["Moves", "Meta"])
async def get_movelist(req: Request) -> dict:
    query = '''
        SELECT * FROM moves
    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/movelist/top", tags=["Moves", "Meta"])
async def get_movelist(req: Request) -> dict:
    query = '''
        SELECT
            final_move_id,
            final_move_name,
            COUNT(*) AS usage_count
        FROM (
            SELECT
                game_1_final_move_id AS final_move_id,
                game_1_final_move_name AS final_move_name
            FROM matches_vw
            WHERE game_1_final_move_id != -1 AND LOWER(game_1_final_move_name) != 'n/a'

            UNION ALL

            SELECT
                game_2_final_move_id,
                game_2_final_move_name
            FROM matches_vw
            WHERE game_2_final_move_id != -1 AND LOWER(game_2_final_move_name) != 'n/a'

            UNION ALL

            SELECT
                game_3_final_move_id,
                game_3_final_move_name
            FROM matches_vw
            WHERE game_3_final_move_id != -1 AND LOWER(game_3_final_move_name) != 'n/a'
        ) AS all_moves
        GROUP BY
            final_move_id, final_move_name
        ORDER BY
            usage_count DESC
        LIMIT 5;
    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/matches", tags=["Matches"])
@app.get("/matches/{limit}", tags=["Matches"])
async def get_matches(req: Request, limit: int = None) -> dict:
    try:
        if limit < 1:
            limit = 1
    except:
        limit = 0
    query = f'''
        SELECT * FROM matches_vw 
        {"LIMIT" if int(limit) else ''} {int(limit) if int(limit) else ''}
    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/matches/{offset}/{limit}", tags=["Matches"])
async def get_matches(req: Request, offset: int = 0, limit: int = 10) -> dict:
    if limit < 1:
        limit = 1
    if offset < 0:
        offset = 0
    query = f'''
        SELECT * FROM matches_vw 
        LIMIT {limit} 
        OFFSET {offset}
    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/match", tags=["Matches"])
@app.get("/match/{id}", tags=["Matches"])
async def get_match(req: Request, id: int = -1) -> dict:
    if id < 1:
        try:
            data = await get_latest_match_id(req)
            id = data['data']['latest_id']
        except:
            id = 1
    query = f'''
        SELECT 
            *
        FROM
            matches_vw
        WHERE id = {int(id)}
    '''
    return await err.safe_db_fetch_one(request=req, query=query)

@app.get("/match-exists", tags=["Matches"])
async def get_match_exists(req: Request, match_number: int = 0):
    season = ""
    if match_number < 1:
        return err.ErrorResponse(message="Not Found", error_code=204)
    if season == "":
        s = await get_latest_season(req)
        if s['status'] == "SUCCESS":
            season = s['data']['id']
        else:
            season = -1
    query = '''
        SELECT m.id FROM matches_vw m LEFT JOIN seasons s ON m.season_id = s.id WHERE ranked_game_number = %s AND m.season_id = '%s'
    ''' % (match_number, season)
    print(query)
    return await err.safe_db_fetch_one(request=req, query=query)
    
@app.get("/match_forfeits", tags=["Matches"])
async def get_match_forfeits(req: Request) -> dict:
    query = '''
        SELECT 
            COUNT(match_win) AS forfeits
        FROM
            matches_vw
        WHERE
            match_forfeit = 1
            AND match_win = 1
    '''
    return await err.safe_db_fetch_one(request=req, query=query)


@app.get("/stats", tags=["Charts"])
async def get_stats(req: Request, limit: int = 10, skip: int = 0, match_win: bool = True) -> dict:
    if limit < 1:
        limit = 10
    if skip < 0:
        skip = 0
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
            return {"status": "SUCCESS", "data": sorted(rows, key=lambda x: x['ranked_game_number'])}


@app.get("/char-stats", tags=["Charts"])
async def get_char_stats(req: Request) -> dict:
    query = '''
        SELECT
            opponent_pick,
            CAST(SUM(CASE WHEN game_winner = 1 THEN 1 ELSE 0 END) AS INTEGER) AS wins,
            CAST(SUM(CASE WHEN game_winner = 2 THEN 1 ELSE 0 END) AS INTEGER) AS losses,
            COUNT(*) AS total_games,
            CAST(ROUND(SUM(CASE WHEN game_winner = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS FLOAT) AS win_percentage
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
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/char-stats-picked", tags=["Charts"])
async def get_chars_by_times_picked(req: Request):
    query = '''
    SELECT opponent_pick_name, COUNT(*) AS times_played, season_display_name
    FROM (
        SELECT game_1_opponent_pick_name AS opponent_pick_name,
        season_display_name
        FROM matches_vw
        WHERE game_1_opponent_pick_name <> 'N/A'
        
        UNION ALL
        
        SELECT game_2_opponent_pick_name,
        season_display_name
        FROM matches_vw
        WHERE game_2_opponent_pick_name <> 'N/A'
        
        UNION ALL
        
        SELECT game_3_opponent_pick_name,
        season_display_name
        FROM matches_vw
        WHERE game_3_opponent_pick_name <> 'N/A'
    ) AS all_picks
    GROUP BY opponent_pick_name, season_display_name
    ORDER BY times_played DESC;
    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/stage-stats", tags=["Charts"])
async def get_stage_stats(req: Request) -> dict:
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
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/match-stats", tags=["Charts"])
async def get_match_stats(req: Request) -> dict:
    season_raw = await get_seasons(req=req)
    season = season_raw['data'][-1]['display_name']
    query = f''' 
        SELECT 
            game_count,
            CASE WHEN match_win = 1 THEN 'WIN' ELSE 'LOSE' END AS match_win,
            COUNT(*) AS match_count,
            season_display_name
        FROM
            (SELECT 
                id,
                    match_win,
                    match_forfeit,
                    (CASE
                        WHEN game_1_winner IN (1 , 2) THEN 1
                        ELSE 0
                    END + CASE
                        WHEN game_2_winner IN (1 , 2) THEN 1
                        ELSE 0
                    END + CASE
                        WHEN game_3_winner IN (1 , 2) THEN 1
                        ELSE 0
                    END) AS game_count,
                    season_display_name
            FROM
                matches_vw) AS counted_matches
        WHERE
            NOT ((game_count = 0 AND match_win = 1
                AND match_forfeit = 0)
                OR (game_count = 0 AND match_win = 0))
            AND season_display_name = "{season}"
        GROUP BY season_display_name, game_count , match_win
        ORDER BY game_count DESC , match_win DESC;

    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/match-stage-stats", tags=["Charts"])
async def get_match_stage_stats(req: Request) -> dict:
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
        (m.stage_num = 1 AND s.id != -1) 
        OR (m.stage_num IN (2,3) AND s.counter_pick = 1)
        AND s.id != -1
        GROUP BY s.id, s.display_name, pick_type
        ORDER BY times_picked DESC, s.display_name, pick_type;

    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/elo-change", tags=["Charts"])
@app.get("/elo-change/{match_number}", tags=["Charts"])
async def get_elo_changes(req: Request, match_number: int = 10) -> dict:
    if match_number < 1:
        match_number = 1
    query = f'''
        SELECT
        elo_change_plus,
        elo_change_minus,
        (elo_change_plus - ABS(elo_change_minus)) AS difference
        FROM (
        SELECT
            COALESCE((
            SELECT SUM(elo_change)
            FROM (
                SELECT m.match_win, elo_change
                FROM matches_vw m
                ORDER BY id DESC
                LIMIT {int(match_number)}
            ) AS recent
            WHERE match_win = 1
            ), 0) AS elo_change_plus,

            COALESCE((
            SELECT SUM(elo_change)
            FROM (
                SELECT m.match_win, elo_change
                FROM matches_vw m
                ORDER BY id DESC
                LIMIT {int(match_number)}
            ) AS recent
            WHERE match_win = 0
            ), 0) AS elo_change_minus
        ) AS stats;

    '''
    return await err.safe_db_fetch_one(request=req, query=query)


@app.get("/final-move-stats", tags=["Charts"])
async def get_final_move_stats(req: Request) -> dict:
    query = '''
        SELECT 
            final_move_short,
            final_move_name,
            COUNT(final_move_name) AS final_move_count,
            season_display_name
        FROM
            (SELECT 
                game_1_final_move_name AS final_move_name,
                    game_1_final_move_short AS final_move_short,
                    season_display_name
            FROM
                matches_vw
            WHERE
                game_1_final_move_id <> - 1
                    AND game_1_winner = 1 UNION ALL SELECT 
                game_2_final_move_name AS final_move_name,
                    game_2_final_move_short AS final_move_short,
                    season_display_name
            FROM
                matches_vw
            WHERE
                game_2_final_move_id <> - 1
                    AND game_2_winner = 1 UNION ALL SELECT 
                game_3_final_move_name AS final_move_name,
                    game_3_final_move_short AS final_move_short,
                    season_display_name
            FROM
                matches_vw
            WHERE
                game_3_final_move_id <> - 1
                    AND game_3_winner = 1) AS all_winnings
        GROUP BY final_move_name
        ORDER BY final_move_count DESC;
    '''
    return await err.safe_db_fetch_all(request=req, query=query)


@app.get("/all-seasons-stats", tags=["Charts"])
async def get_all_seasons_stats(req: Request) -> dict:
    query = '''
        SELECT
        season_display_name,
        COUNT(*) AS total_matches,
        SUM(CASE WHEN match_win = 1 THEN 1 ELSE 0 END) AS match_wins,
        SUM(CASE WHEN match_win = 0 THEN 1 ELSE 0 END) AS match_losses,
        MIN(elo_rank_new) as min_elo,
        MAX(elo_rank_new) as max_elo,
        SUM(elo_change) AS total_elo_change,
        ROUND(AVG(match_win) * 100, 2) AS win_rate_percent
        FROM matches_vw
        GROUP BY season_display_name
        ORDER BY season_id DESC;

    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/elo-by-season", tags=["Charts"])
async def get_elo_by_season(req: Request) -> dict:
    query = '''
    SELECT 
        MIN(m.elo_rank_old) AS min_elo,
        CAST(ROUND(AVG(m.elo_rank_new)) AS INTEGER) AS avg_elo,
        CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(GROUP_CONCAT(elo_rank_new
                            ORDER BY elo_rank_new
                            SEPARATOR ','),
                        ',',
                        FLOOR(COUNT(*) / 2) + 1),
                ',',
                - 1) AS INTEGER) AS median_elo,
        MAX(m.elo_rank_new) AS max_elo,
        (SELECT m2.elo_rank_new 
        FROM matches_vw m2 
        WHERE m2.season_id = s.id 
        AND m2.ranked_game_number = 1
        ) AS first_elo,
        (MAX(m.elo_rank_new) - MIN(m.elo_rank_old)) AS elo_gain,
        COUNT(*) AS match_count,
        s.display_name

    FROM
        matches_vw m
            JOIN
        seasons s ON m.season_id = s.id
    WHERE
        m.season_id IS NOT NULL
    GROUP BY s.id , s.display_name
    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/head-to-head", tags=["Charts", "Matches"])
async def get_head_to_head_by_user(req: Request, opp_name: str = ""):
    if opp_name == "": return err.ErrorResponse(message="No user was given")
    all_data = {}
    query = '''
    SELECT 
        COUNT(*) as total_matches,
        SUM(match_win) as matches_won,
        COUNT(*) - SUM(match_win) as matches_lost,
        ROUND(CAST(SUM(match_win) AS DECIMAL) / COUNT(*) * 100, 1) as win_percentage,
        ROUND(AVG(ABS(elo_change))) as avg_elo_change,
        MIN(elo_rank_old) as my_lowest_elo,
        MAX(elo_rank_new) as my_highest_elo,
        MIN(opponent_elo) as opp_lowest_elo,
        MAX(opponent_elo) as opp_hightest_elo,
        COUNT(CASE WHEN match_forfeit = 1 THEN 1 END) as forfeits
    FROM matches_vw
    WHERE LOWER(opponent_name) = LOWER('%s');
    ''' % (opp_name)
    temp = await err.safe_db_fetch_one(request=req, query=query)
    if temp['status'] == "SUCCESS":
        all_data['overall'] = temp['data']
    else:
        return err.ErrorResponse(message="Something went wrong with the overall request").model_dump()

    query = '''
        SELECT 
            id,
            ranked_game_number,
            match_date,
            match_win,
            elo_rank_old,
            elo_rank_new,
            elo_change,
            opponent_elo,
            match_forfeit,
            season_display_name,
            game_1_char_pick_name,
            game_1_opponent_pick_name,
            game_1_opponent_pick_image,
            game_1_stage_name,
            game_1_winner,
            game_1_final_move_name,
            game_2_char_pick_name,
            game_2_opponent_pick_name,
            game_2_opponent_pick_image,
            game_2_stage_name,
            game_2_winner,
            game_2_final_move_name,
            game_3_char_pick_name,
            game_3_opponent_pick_name,
            game_3_opponent_pick_image,
            game_3_stage_name,
            game_3_winner,
            game_3_final_move_name,
            (CASE WHEN game_1_winner = 1 THEN 1 ELSE 0 END +
            CASE WHEN game_2_winner = 1 THEN 1 ELSE 0 END +
            CASE WHEN game_3_winner = 1 THEN 1 ELSE 0 END) as games_won,
            (CASE WHEN game_1_winner = 0 THEN 1 ELSE 0 END +
            CASE WHEN game_2_winner = 0 THEN 1 ELSE 0 END +
            CASE WHEN game_3_winner = 0 THEN 1 ELSE 0 END) as games_lost
        FROM matches_vw
        WHERE LOWER(opponent_name) = LOWER('%s')
        ORDER BY match_date DESC;
    ''' % (opp_name)

    temp = await err.safe_db_fetch_all(request=req, query=query)
    if temp['status'] == "SUCCESS":
        all_data['matches'] = temp['data']
    else:
        return err.ErrorResponse(message="Something went wrong with the matches request").model_dump()
    query = '''
        SELECT 
            stage_name,
            games_played,
            games_won,
            ROUND(CAST(games_won AS DECIMAL) / games_played * 100, 1) as win_rate
        FROM (
            SELECT 
                game_1_stage_name as stage_name,
                COUNT(*) as games_played,
                SUM(CASE WHEN game_1_winner = 1 THEN 1 ELSE 0 END) as games_won
            FROM matches_vw 
            WHERE LOWER(opponent_name) = LOWER('%(opponent)s') AND game_1_stage_name != 'N/A'
            GROUP BY game_1_stage_name
            
            UNION ALL
            
            SELECT 
                game_2_stage_name,
                COUNT(*),
                SUM(CASE WHEN game_2_winner = 1 THEN 1 ELSE 0 END)
            FROM matches_vw 
            WHERE LOWER(opponent_name) = LOWER('%(opponent)s') AND game_2_stage_name != 'N/A'
            GROUP BY game_2_stage_name
            
            UNION ALL
            
            SELECT 
                game_3_stage_name,
                COUNT(*),
                SUM(CASE WHEN game_3_winner = 1 THEN 1 ELSE 0 END)
            FROM matches_vw 
            WHERE LOWER(opponent_name) = LOWER('%(opponent)s') AND game_3_stage_name != 'N/A'
            GROUP BY game_3_stage_name
        ) stage_stats
        GROUP BY stage_name
        ORDER BY games_played DESC, win_rate DESC;
        
    ''' % {'opponent': opp_name}
    temp = await err.safe_db_fetch_all(request=req, query=query)
    if temp['status'] == "SUCCESS":
        all_data['stages'] = temp['data']
    else:
        return err.ErrorResponse(message="Something went wrong with the stages request").model_dump()

    query = '''
        SELECT 
            your_character,
            opponent_character,
            SUM(games_played) as games_played,
            SUM(games_won) as games_won,
            ROUND(CAST(SUM(games_won) AS DECIMAL) / SUM(games_played) * 100, 1) as win_rate
        FROM (
            SELECT 
                game_1_char_pick_name as your_character,
                game_1_opponent_pick_name as opponent_character,
                COUNT(*) as games_played,
                SUM(CASE WHEN game_1_winner = 1 THEN 1 ELSE 0 END) as games_won
            FROM matches_vw 
            WHERE LOWER(opponent_name) = LOWER('%(opponent)s')
            AND game_1_char_pick_name != 'N/A'
            AND game_1_opponent_pick_name != 'N/A'
            GROUP BY game_1_char_pick_name, game_1_opponent_pick_name
            
            UNION ALL
            
            SELECT 
                game_2_char_pick_name,
                game_2_opponent_pick_name,
                COUNT(*),
                SUM(CASE WHEN game_2_winner = 1 THEN 1 ELSE 0 END)
            FROM matches_vw 
            WHERE LOWER(opponent_name) = LOWER('%(opponent)s')
            AND game_2_char_pick_name != 'N/A'
            AND game_2_opponent_pick_name != 'N/A'
            GROUP BY game_2_char_pick_name, game_2_opponent_pick_name
            
            UNION ALL
            
            SELECT 
                game_3_char_pick_name,
                game_3_opponent_pick_name,
                COUNT(*),
                SUM(CASE WHEN game_3_winner = 1 THEN 1 ELSE 0 END)
            FROM matches_vw 
            WHERE LOWER(opponent_name) = LOWER('%(opponent)s')
            AND game_3_char_pick_name != 'N/A'
            AND game_3_opponent_pick_name != 'N/A'
            GROUP BY game_3_char_pick_name, game_3_opponent_pick_name
        ) matchup_stats
        GROUP BY your_character, opponent_character
        HAVING games_played > 0
        ORDER BY games_played DESC, win_rate DESC;
    ''' % {'opponent': opp_name}
    temp = await err.safe_db_fetch_all(request=req, query=query)
    if temp['status'] == "SUCCESS":
        all_data['matchup'] = temp['data']
    else:
        return err.ErrorResponse(message="Something went wrong with the matchup request").model_dump()

    if all_data:
        return err.SuccessResponse(data=all_data).model_dump()
    return err.ErrorResponse(message="Something went wrong").model_dump()

@app.get("/head-to-head/top", tags=["Charts", "Matches"])
async def get_top_matchups_by_name(req: Request):
    query = '''
    SELECT
        m.opponent_name,
        count(m.opponent_name) as count
    FROM
        matches_vw m
    WHERE 
        m.opponent_name != NULL
        OR m.opponent_name != ""
    GROUP BY
        m.opponent_name
    ORDER BY count DESC, m.opponent_name ASC
    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/heatmap-data", tags=["Charts"])
async def get_data_for_heatmap(req: Request):
    query = '''
        SELECT char_name, COUNT(*) AS pick_count
        FROM (
            SELECT game_1_opponent_pick_name AS char_name
            FROM matches_vw
            WHERE game_1_opponent_pick != -1

            UNION ALL

            SELECT game_2_opponent_pick_name
            FROM matches_vw
            WHERE game_2_opponent_pick != -1

            UNION ALL

            SELECT game_3_opponent_pick_name
            FROM matches_vw
            WHERE game_3_opponent_pick != -1
        ) AS picks
        GROUP BY char_name
        ORDER BY pick_count DESC;
    '''
    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/stagepick-data", tags=["Charts"])
async def get_data_for_heatmap(req: Request):
    query = '''
        WITH stage_data AS (
        SELECT 
            season_id,
            season_short_name,
            season_display_name,
            game_1_stage as stage_id,
            game_1_stage_name as stage_name
        FROM matches_vw
        WHERE game_1_stage != -1 AND LOWER(game_1_stage_name) != 'n/a'
        
        UNION ALL
        
        SELECT 
            season_id,
            season_short_name, 
            season_display_name,
            game_2_stage as stage_id,
            game_2_stage_name as stage_name
        FROM matches_vw
        WHERE game_2_stage != -1 AND LOWER(game_2_stage_name) != 'n/a'
        
        UNION ALL
        
        SELECT 
            season_id,
            season_short_name,
            season_display_name, 
            game_3_stage as stage_id,
            game_3_stage_name as stage_name
        FROM matches_vw
        WHERE game_3_stage != -1 AND LOWER(game_3_stage_name) != 'n/a'
        )

        SELECT 
        season_id,
        season_short_name,
        season_display_name,
        stage_id,
        stage_name,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY season_id), 2) as pick_percentage,
        COUNT(*) as pick_count
        FROM stage_data
        GROUP BY  season_display_name, stage_id, stage_name
        ORDER BY pick_count DESC;
    '''

    return await err.safe_db_fetch_all(request=req, query=query)

@app.get("/character-mu-data", tags=["Charts"])
async def get_character_matchup_data(req: Request):
    query = '''
    WITH individual_games AS (
        SELECT 
            m.season_display_name,
            m.season_id,
            m.elo_rank_new,
            m.game_1_opponent_pick_name as opponent_character,
            CASE WHEN m.game_1_winner = 1 THEN 1 ELSE 0 END as game_won
        FROM matches_vw m
        WHERE m.game_1_opponent_pick_name IS NOT NULL 
        AND m.game_1_opponent_pick_name != 'N/A' 
        AND m.game_1_opponent_pick != -1
        AND m.game_1_winner != -1
        AND m.match_forfeit = 0
        
        UNION ALL
        SELECT 
            m.season_display_name,
            m.season_id,
            m.elo_rank_new,
            m.game_2_opponent_pick_name as opponent_character,
            CASE WHEN m.game_2_winner = 1 THEN 1 ELSE 0 END as game_won
        FROM matches_vw m
        WHERE m.game_2_opponent_pick_name IS NOT NULL 
        AND m.game_2_opponent_pick_name != 'N/A' 
        AND m.game_2_opponent_pick != -1
        AND m.game_2_winner != -1
        AND m.match_forfeit = 0
        
        UNION ALL
        SELECT 
            m.season_display_name,
            m.season_id,
            m.elo_rank_new,
            m.game_3_opponent_pick_name as opponent_character,
            CASE WHEN m.game_3_winner = 1 THEN 1 ELSE 0 END as game_won
        FROM matches_vw m
        WHERE m.game_3_opponent_pick_name IS NOT NULL 
        AND m.game_3_opponent_pick_name != 'N/A' 
        AND m.game_3_opponent_pick != -1
        AND m.game_3_winner != -1
        AND m.match_forfeit = 0
    )

    SELECT 
        ig.season_display_name,
        ig.season_id,
        t.tier_display_name,
        t.tier_short_name,
        ig.opponent_character,
        COUNT(*) as total_games,
        SUM(ig.game_won) as games_won,
        COUNT(*) - SUM(ig.game_won) as games_lost,
        ROUND(
            CASE 
                WHEN COUNT(*) > 0 THEN (SUM(ig.game_won) * 100.0 / COUNT(*))
                ELSE 0 
            END, 2
        ) as win_percentage
    FROM individual_games ig
    INNER JOIN (
        SELECT 1 as id, 'Stone' as tier_display_name, 'stone' as tier_short_name, 0 as min_threshold, 499 as max_threshold
        UNION ALL SELECT 2, 'Bronze', 'bronze', 500, 699
        UNION ALL SELECT 3, 'Silver', 'silver', 700, 899
        UNION ALL SELECT 4, 'Gold', 'gold', 900, 1099
        UNION ALL SELECT 5, 'Platinum', 'plat', 1100, 1299
        UNION ALL SELECT 6, 'Diamond', 'diamond', 1300, 1499
        UNION ALL SELECT 7, 'Master', 'master', 1500, 1699
        UNION ALL SELECT 8, 'Grandmaster', 'gs', 1700, 1799
        UNION ALL SELECT 9, 'Aetherean', 'aeth', 1800, 99999
    ) t ON ig.elo_rank_new BETWEEN t.min_threshold AND t.max_threshold
    GROUP BY 
        ig.season_display_name,
        ig.season_id,
        t.id,
        t.tier_display_name,
        t.tier_short_name,
        ig.opponent_character
    ORDER BY 
        ig.season_id DESC,
        t.id ASC,
        win_percentage DESC,
        total_games DESC;
    '''
    return await err.safe_db_fetch_all(request=req, query=query)
# post


@app.post("/insert-match", tags=["Matches", "Mutable"])
async def insert_match(match: Match, debug: bool = 0) -> dict:
    print(match)
    query = '''
        INSERT INTO matches (
            match_date, elo_rank_old, elo_rank_new, elo_change, match_win, match_forfeit,
            ranked_game_number, total_wins, win_streak_value, opponent_elo, opponent_estimated_elo, opponent_name,
            game_1_char_pick, game_1_opponent_pick, game_1_stage, game_1_winner, game_1_final_move_id,
            game_2_char_pick, game_2_opponent_pick, game_2_stage, game_2_winner, game_2_final_move_id,
            game_3_char_pick, game_3_opponent_pick, game_3_stage, game_3_winner, game_3_final_move_id, 
            final_move_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s
        )
    '''
    inserted_id = -1
    async with app.state.db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(query, (
                    match.match_date,
                    match.elo_rank_old, match.elo_rank_new, match.elo_change, match.match_win, match.match_forfeit,
                    match.ranked_game_number, match.total_wins, match.win_streak_value, match.opponent_elo, match.opponent_estimated_elo, match.opponent_name,
                    match.game_1_char_pick, match.game_1_opponent_pick, match.game_1_stage, match.game_1_winner, match.game_1_final_move_id,
                    match.game_2_char_pick, match.game_2_opponent_pick, match.game_2_stage, match.game_2_winner, match.game_2_final_move_id,
                    match.game_3_char_pick, match.game_3_opponent_pick, match.game_3_stage, match.game_3_winner, match.game_3_final_move_id,
                    match.final_move_id
                ))
            except Exception as e:
                return err.ErrorResponse(message=f"{e}").model_dump()
            try:
                await notify_websockets(message={"user": "backend", "type": "new_match", "ranked_game_number": int(match.ranked_game_number)})
                print("""Sending: {"user": "backend", "type": "new_match", "ranked_game_number": %i }""" % (int(match.ranked_game_number)))
                if match.match_win == 1:
                    await notify_websockets(message={"user": "backend", "type": "new_win_stats", "ranked_game_number": int(match.ranked_game_number)})
                elif match.match_win == 0:
                    await notify_websockets(message={"user": "backend", "type": "new_lose_stats", "ranked_game_number": int(match.ranked_game_number)})
                inserted_id = cur.lastrowid
            except Exception as e:
                print(f"Insert fail: {e}")
                return err.ErrorResponse(message=f"{e}").model_dump()
    return err.SuccessResponse(data={"last_id": inserted_id}).model_dump()

# patch


@app.patch("/update-match/", tags=["Mutable"])
async def update_match(update_value: dict) -> dict:
    try:
        match_id = int(update_value['row_id'])
    except (ValueError, KeyError):
        return {"status": "failure", "message": "No ID provided or invalid ID format"}
    if match_id:
        print(
            f"""UPDATE matches SET {update_value['key']}="{update_value['value']}" WHERE id = {match_id}""")
        async with app.state.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"""UPDATE matches SET {update_value['key']}="{update_value['value']}" WHERE id = {match_id}""")
                rows = await cur.fetchall()
                return err.SuccessResponse(data=rows, message="hi").model_dump()
    return err.SuccessResponse(data={}).model_dump()

# delete


@app.delete("/match/{id}", tags=["Matches", "Mutable"])
async def delete_match(req: Request, id: int) -> dict:
    if type(id) != int:
        return {"status": "FAIL", "data": {"message": "Invalid match ID"}}
    query = f'''
        DELETE FROM matches WHERE id = {id} 
    '''
    await notify_websockets({"type": "new_match"})
    return await err.safe_db_fetch_one(request=req, query=query)

# websockets

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websockets.append(websocket)
    print(f"New connection: {websocket}")

    last_pong = asyncio.get_event_loop().time()  
    PING_INTERVAL = 60
    PONG_TIMEOUT = 10 
    try:
        while True:
            try:
                nmessage = await asyncio.wait_for(websocket.receive_text(), timeout=PING_INTERVAL)

                if nmessage == "pong":
                    last_pong = asyncio.get_event_loop().time()
                    continue  

                try:
                    nmessage_data = json.loads(nmessage)
                except json.JSONDecodeError:
                    nmessage_data = {}

                await notify_websockets(nmessage_data)

            except asyncio.TimeoutError:
                await websocket.send_text("ping")

                now = asyncio.get_event_loop().time()
                if now - last_pong > PING_INTERVAL + PONG_TIMEOUT:
                    print(f"Client {websocket} missed pong, closing.")
                    break 

    except WebSocketDisconnect:
        print(f"Client disconnected: {websocket}")
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        if websocket in websockets:
            websockets.remove(websocket)



async def notify_websockets(message: dict):
    for ws in websockets[:]:
        try:
            await ws.send_json(message)
            print(f"Sent: {message}")
        except Exception:
            print("WebSocket failed; removing")
            websockets.remove(ws)


async def get_latest_match_id(req: Request):
    query = f'''
        SELECT 
            MAX(id) as latest_id
        FROM
            matches_vw
    '''
    return await err.safe_db_fetch_one(request=req, query=query)
