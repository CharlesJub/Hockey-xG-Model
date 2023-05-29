import pandas as pd
from dataclasses import dataclass
import time
from datetime import datetime
import asyncio
import aiohttp
import requests
import json


async def fetch_data(session, url):
    async with session.get(url) as response:
        return await response.json()

async def get_game_json(game_id):
    shifts_url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
    game_url = f"https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live?site=en_nhl"

    async with aiohttp.ClientSession() as session:
        shifts_task = asyncio.create_task(fetch_data(session, shifts_url))
        game_task = asyncio.create_task(fetch_data(session, game_url))

        shifts, game = await asyncio.gather(shifts_task, game_task)

    return shifts, game

def get_sec(time_str):
    """Get seconds from time."""
    time_obj = datetime.strptime(time_str, '%M:%S')
    return time_obj.minute * 60 + time_obj.second

def get_shift_df(shifts_json):
    shifts_array = [
        [
            f"{shift['firstName']} {shift['lastName']}",
            shift['period'],
            get_sec(shift['startTime']),
            get_sec(shift['endTime']),
            shift['teamAbbrev']
        ]
        for shift in shifts_json['data']
    ]

    return pd.DataFrame.from_records(shifts_array, columns=['player', 'period', 'shift_start_seconds', 'shift_end_seconds', 'team'])

def get_players_on_ice(shifts_df, period, time_of_event, team, goalies_in_game):
    on_ice = shifts_df[
        (shifts_df['period'] == period) &
        (shifts_df['shift_start_seconds'] <= time_of_event) &
        (shifts_df['shift_end_seconds'] > time_of_event) &
        (shifts_df['team'] == team)
    ]
    goalie = on_ice.loc[on_ice['player'].isin(goalies_in_game), 'player'].iloc[0] if not on_ice.empty and any(on_ice['player'].isin(goalies_in_game)) else None
    skaters = on_ice[~on_ice['player'].isin(goalies_in_game)]['player'].values
    
    return goalie, skaters

def get_game_goalies(game_json):            
    return [
        player['fullName']
        for player in game_json['gameData']['players'].values()
        if "primaryPosition" in player.keys() and player['primaryPosition']['code'] == 'G'  
    ]

def json_to_array(shifts_df, game_json):
    play_by_play_events = []

    season = game_json['gameData']['game']['season']
    game_id = game_json['gamePk']
    game_date = game_json['gameData']['datetime']['dateTime']
    home_team = game_json['gameData']['teams']['home']['abbreviation']
    away_team = game_json['gameData']['teams']['away']['abbreviation']
    goalies_in_game = get_game_goalies(game_json=game_json)
    
    for play in game_json['liveData']['plays']['allPlays']:
        event_type = play['result']['eventTypeId']
        if event_type not in ['FACEOFF', 'HIT', 'BLOCKED_SHOT', 'MISSED_SHOT', 'GIVEAWAY', 'TAKEAWAY', 'GOAL']:
            continue
        event_index = play['about']['eventIdx']
        game_period = play['about']['period']
        game_seconds = get_sec(play['about']['periodTime'])
        
        event_description = play['result']['description']
        
        event_detail = play['result'].get('secondaryType')
        event_team = play.get('team', {}).get('triCode')
        
        event_players = play.get('players', [])
        event_player_1 = event_players[0]['player']['fullName'] if event_players else None
        event_player_2 = event_players[1]['player']['fullName'] if len(event_players) >= 2 else None
        event_player_3 = event_players[2]['player']['fullName'] if len(event_players) >= 3 else None
        
        coords = play['coordinates']
        coords_x = coords['x'] if 'x' in coords else None
        coords_y = coords['y'] if 'y' in coords else None

        home_goalie, home_skaters_on_ice = get_players_on_ice(shifts_df, game_period, game_seconds, home_team, goalies_in_game)
        away_goalie, away_skaters_on_ice = get_players_on_ice(shifts_df, game_period, game_seconds, away_team, goalies_in_game)
        home_skaters = len(home_skaters_on_ice)
        away_skaters = len(away_skaters_on_ice)

        home_skater_1 = home_skaters_on_ice[0] if home_skaters >= 1 else None
        home_skater_2 = home_skaters_on_ice[1] if home_skaters >= 2 else None
        home_skater_3 = home_skaters_on_ice[2] if home_skaters >= 3 else None
        home_skater_4 = home_skaters_on_ice[3] if home_skaters >= 4 else None
        home_skater_5 = home_skaters_on_ice[4] if home_skaters >= 5 else None
        home_skater_6 = home_skaters_on_ice[5] if home_skaters >= 6 else None
        away_skater_1 = away_skaters_on_ice[0] if away_skaters >= 1 else None
        away_skater_2 = away_skaters_on_ice[1] if away_skaters >= 2 else None
        away_skater_3 = away_skaters_on_ice[2] if away_skaters >= 3 else None
        away_skater_4 = away_skaters_on_ice[3] if away_skaters >= 4 else None
        away_skater_5 = away_skaters_on_ice[4] if away_skaters >= 5 else None
        away_skater_6 = away_skaters_on_ice[5] if away_skaters >= 6 else None

        home_goals = play['about']['goals']['home']
        away_goals = play['about']['goals']['away']

        play_by_play_events.append([season, game_id, game_date, event_index, game_period, game_seconds, event_type, event_description, event_detail, event_team, event_player_1, event_player_2,
                                    event_player_3, coords_x, coords_y, home_team, away_team, home_skater_1, home_skater_2, home_skater_3, home_skater_4, home_skater_5, home_skater_6, away_skater_1,
                                    away_skater_2, away_skater_3, away_skater_4, away_skater_5, away_skater_6, home_goalie, away_goalie, home_skaters, away_skaters, home_goals, away_goals])

    return play_by_play_events

def get_game_ids(start_date, end_date):
    # Season year is the first year in the season
    
    schedule = requests.get(f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={start_date}&endDate={end_date}").json()
    game_ids = []
    for day in schedule['dates']:
        for game in day['games']:
            if game['gameType'] == "R":
                game_ids.append(game['gamePk'])
    return game_ids
    

def main():
    all_games = []
    game_ids = get_game_ids('2022-10-07', '2023-04-14')
    print(len(game_ids))
    start_time = time.time()
    i = 0
    for game in game_ids:
        shifts_json, game_json = asyncio.run(get_game_json(game)) 
        shifts_df = get_shift_df(shifts_json)
        all_games += json_to_array(shifts_df, game_json)
        if i % 164 == 0:
            print("--- %s seconds ---" % (time.time() - start_time))
        i += 1
    df = pd.DataFrame(all_games, columns=["season", "game_id", "game_date", "event_index", "game_period", "game_seconds", "event_type", 
                                          "event_description", "event_detail", "event_team", "event_player_1", "event_player_2","event_player_3", 
                                          "coords_x", "coords_y", "home_team", "away_team", "home_skater_1", "home_skater_2", "home_skater_3", 
                                          "home_skater_4", "home_skater_5", "home_skater_6", "away_skater_1", "away_skater_2", "away_skater_3", 
                                          "away_skater_4", "away_skater_5", "away_skater_6", "home_goalie", "away_goalie", "home_skaters", "away_skaters", 
                                          "home_goals", "away_goals"])
    df.to_csv('202223_pbp_data.csv')
    
    
if __name__ == "__main__":
    main()
