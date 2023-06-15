import pandas as pd
from dataclasses import dataclass
import time
from datetime import datetime
import asyncio
import aiohttp
import requests
import numpy as np
from selectolax.parser import HTMLParser

def get_game_json(game_id):
    game_url = f"https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live?site=en_nhl"
    with requests.Session() as session:
        game = session.get(game_url).json()
    return  game

def get_sec(time_str):
    """Get seconds from time."""
    time_obj = datetime.strptime(time_str, '%M:%S')
    return time_obj.minute * 60 + time_obj.second

import numpy as np

def json_to_array(shifts_df, game_json):
    play_by_play_events = []

    season = game_json['gameData']['game']['season']
    game_id = game_json['gamePk']
    game_date = game_json['gameData']['datetime']['dateTime']
    home_team = game_json['gameData']['teams']['home']['abbreviation']
    away_team = game_json['gameData']['teams']['away']['abbreviation']

    # Convert the DataFrame columns to NumPy arrays for faster access
    periods = shifts_df['Period'].values
    times = shifts_df['Time'].values

    for play in game_json['liveData']['plays']['allPlays']:
        event_type = play['result']['eventTypeId']
        if event_type not in ['FACEOFF', 'HIT', 'BLOCKED_SHOT', 'MISSED_SHOT', 'GIVEAWAY', 'TAKEAWAY', 'GOAL', 'SHOT']:
            continue
        event_index = play['about']['eventIdx']
        game_period = play['about']['period']
        game_seconds = get_sec(play['about']['periodTime'])

        event_description = play['result']['description']
        empty_net = play['result']['emptyNet'] if "emptyNet" in play['result'] else None

        event_detail = play['result'].get('secondaryType')
        event_team = play.get('team', {}).get('triCode')

        event_players = play.get('players', [])
        event_player_1 = event_players[0]['player']['fullName'] if event_players else None
        event_player_2 = event_players[1]['player']['fullName'] if len(event_players) >= 2 else None
        event_player_3 = event_players[2]['player']['fullName'] if len(event_players) >= 3 else None

        coords = play['coordinates']
        coords_x = coords['x'] if 'x' in coords else None
        coords_y = coords['y'] if 'y' in coords else None
        home_goals = play['about']['goals']['home']
        away_goals = play['about']['goals']['away']

        # Use NumPy indexing to retrieve the values from the arrays
        mask = (periods == str(game_period)) & (times == int(game_seconds))
        homeskater_1 = shifts_df[mask]['Home1'].values[0] if not shifts_df[mask].empty else None
        homeskater_2 = shifts_df[mask]['Home2'].values[0] if not shifts_df[mask].empty else None
        homeskater_3 = shifts_df[mask]['Home3'].values[0] if not shifts_df[mask].empty else None
        homeskater_4 = shifts_df[mask]['Home4'].values[0] if not shifts_df[mask].empty else None
        homeskater_5 = shifts_df[mask]['Home5'].values[0] if not shifts_df[mask].empty else None
        homeskater_6 = shifts_df[mask]['Home6'].values[0] if not shifts_df[mask].empty else None

        awayskater_1 = shifts_df[mask]['Away1'].values[0] if not shifts_df[mask].empty else None
        awayskater_2 = shifts_df[mask]['Away2'].values[0] if not shifts_df[mask].empty else None
        awayskater_3 = shifts_df[mask]['Away3'].values[0] if not shifts_df[mask].empty else None
        awayskater_4 = shifts_df[mask]['Away4'].values[0] if not shifts_df[mask].empty else None
        awayskater_5 = shifts_df[mask]['Away5'].values[0] if not shifts_df[mask].empty else None
        awayskater_6 = shifts_df[mask]['Away6'].values[0] if not shifts_df[mask].empty else None

        home_goalie = shifts_df[mask]['HomeGoalie'].values[0] if not shifts_df[mask].empty else None
        away_goalie = shifts_df[mask]['AwayGoalie'].values[0] if not shifts_df[mask].empty else None

        long_description = shifts_df[mask]['Description'].values[0] if not shifts_df[mask].empty else None
        strength = shifts_df[mask]['Strength'].values[0] if not shifts_df[mask].empty else None

        play_by_play_events.append([season, game_id, game_date, event_index, game_period, game_seconds, event_type, event_description, event_detail, event_team, event_player_1, event_player_2,
                                    event_player_3, coords_x, coords_y, home_team, away_team, home_goals, away_goals, empty_net, homeskater_1, homeskater_2, homeskater_3, homeskater_4, homeskater_5,
                                    homeskater_6, awayskater_1, awayskater_2, awayskater_3, awayskater_4, awayskater_5, awayskater_6, home_goalie, away_goalie, long_description])

    return play_by_play_events


def get_game_ids(start_date, end_date):
    # Season year is the first year in the season
    schedule = requests.get(f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={start_date}&endDate={end_date}").json()
    game_ids = [game['gamePk'] for day in schedule['dates'] for game in day['games']]
    return game_ids
    
import requests
import pandas as pd

def get_shift_data(game_id):
    id_str = str(game_id)
    season_start_year = id_str[:4]
    link = f"https://www.nhl.com/scores/htmlreports/{season_start_year}{int(season_start_year) + 1}/PL{id_str[4:]}.HTM"

    session = requests.Session()
    response = session.get(link)
    html = HTMLParser(response.text)

    text_elements = html.css(".bborder")
    data = {'Column': text_elements}
    df = pd.DataFrame(data)

    df = df.groupby(df.index // 8).apply(lambda x: pd.Series(x['Column'].values))
    df.columns = ['Index', 'Period', "Strength", "Time", "Event", "Description", "AwayOnIce", "HomeOnIce"]

    df['Index'] = df['Index'].apply(lambda x: x.text())
    df['Period'] = df['Period'].apply(lambda x: x.text())
    df['Strength'] = df['Strength'].apply(lambda x: x.text())

    time_text = df['Time'].apply(lambda x: x.text(separator='\n').split("\n")[0])
    time_text = time_text[time_text != 'Time:']  # Exclude strings equal to 'Time:'
    df['Time'] = pd.to_numeric(time_text.apply(get_sec), errors='coerce')

    df['Event'] = df['Event'].apply(lambda x: x.text())
    df['Description'] = df['Description'].apply(lambda x: x.text())

    df['HomeOnIce'] = df['HomeOnIce'].apply(lambda x: x.css("font[style='cursor:hand;']"))
    df['AwayOnIce'] = df['AwayOnIce'].apply(lambda x: x.css("font[style='cursor:hand;']"))

    df['Home1'] = [x[0].attributes['title'].split("- ")[-1] if len(x) > 0 and "Goalie" not in x[0].attributes['title'] else None for x in df['HomeOnIce']]
    df['Home2'] = [x[1].attributes['title'].split("- ")[-1] if len(x) > 1 and "Goalie" not in x[1].attributes['title'] else None for x in df['HomeOnIce']]
    df['Home3'] = [x[2].attributes['title'].split("- ")[-1] if len(x) > 2 and "Goalie" not in x[2].attributes['title'] else None for x in df['HomeOnIce']]
    df['Home4'] = [x[3].attributes['title'].split("- ")[-1] if len(x) > 3 and "Goalie" not in x[3].attributes['title'] else None for x in df['HomeOnIce']]
    df['Home5'] = [x[4].attributes['title'].split("- ")[-1] if len(x) > 4 and "Goalie" not in x[4].attributes['title'] else None for x in df['HomeOnIce']]
    df['Home6'] = [x[5].attributes['title'].split("- ")[-1] if len(x) > 5 and "Goalie" not in x[5].attributes['title'] else None for x in df['HomeOnIce']]

    df['Away1'] = [x[0].attributes['title'].split("- ")[-1] if len(x) > 0 and "Goalie" not in x[0].attributes['title'] else None for x in df['AwayOnIce']]
    df['Away2'] = [x[1].attributes['title'].split("- ")[-1] if len(x) > 1 and "Goalie" not in x[1].attributes['title'] else None for x in df['AwayOnIce']]
    df['Away3'] = [x[2].attributes['title'].split("- ")[-1] if len(x) > 2 and "Goalie" not in x[2].attributes['title'] else None for x in df['AwayOnIce']]
    df['Away4'] = [x[3].attributes['title'].split("- ")[-1] if len(x) > 3 and "Goalie" not in x[3].attributes['title'] else None for x in df['AwayOnIce']]
    df['Away5'] = [x[4].attributes['title'].split("- ")[-1] if len(x) > 4 and "Goalie" not in x[4].attributes['title'] else None for x in df['AwayOnIce']]
    df['Away6'] = [x[5].attributes['title'].split("- ")[-1] if len(x) > 5 and "Goalie" not in x[5].attributes['title'] else None for x in df['AwayOnIce']]

    df['HomeGoalie'] = ["".join(player.attributes['title'] if "Goalie" in player.attributes['title'] else "" for player in x).split("- ")[-1] for x in df['HomeOnIce']]
    df['AwayGoalie'] = ["".join(player.attributes['title'] if "Goalie" in player.attributes['title'] else "" for player in x).split("- ")[-1] for x in df['AwayOnIce']]

    filtered_df = df[df["Event"].isin(["FAC", "GIVE", "BLOCK", "GIVE", "HIT", "GOAL", "MISS", "SHOT", "TAKE"])]

    return filtered_df


def main():
    all_games = []
    game_ids = get_game_ids('2021-10-12', '2023-05-28')
    print(len(game_ids))
    start_time = time.time()
    i = 0
    for game in game_ids:
        if i % 164 == 0:
            print(str(i) + " --- %s seconds ---" % (time.time() - start_time))
        i += 1
        if str(game)[4:6] == "02":
            game_json = get_game_json(game)
            shifts_df = get_shift_data(game)
            all_games += json_to_array(shifts_df, game_json)
        else:
            continue
        
    df = pd.DataFrame(all_games, columns=['season', 'game_id', 'game_date', 'event_index', 'game_period', 'game_seconds', 'event_type', 'event_description', 'event_detail', 'event_team', 
                                          'event_player_1', 'event_player_2', 'event_player_3', 'coords_x', 'coords_y', 'home_team', 'away_team', 'home_goals', 'away_goals', 'empty_net', 
                                          'homeskater_1', 'homeskater_2', 'homeskater_3', 'homeskater_4', 'homeskater_5', 'homeskater_6', 'awayskater_1', 'awayskater_2', 'awayskater_3', 
                                          'awayskater_4', 'awayskater_5', 'awayskater_6', 'home_goalie', 'away_goalie', 'long_description'])
    df.to_csv('pbp_data.csv')
if __name__ == "__main__":
    main()
