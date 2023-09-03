import creds
import requests
from datetime import datetime
import pandas as pd
from selectolax.parser import HTMLParser
from sqlalchemy import create_engine

class HockeyScraper:
    def min_to_sec(self, time_str):
        """
        Convert a time string in the format 'MM:SS' to seconds.

        Parameters:
            time_str (str): The time string to convert.

        Returns:
            int: The time in seconds.
        """
        time_obj = datetime.strptime(time_str, '%M:%S')
        return time_obj.minute * 60 + time_obj.second
    
    def get_plays(self, game_id):
        """
        Retrieve and parse game plays data from the NHL API.

        Args:
            game_id (int): The unique identifier for the NHL game.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing parsed game plays data.
        """
        # Send a GET request to the NHL API to retrieve game data
        plays_resp = requests.get('https://statsapi.web.nhl.com/api/v1/game/{}/feed/live?site=en_nhl'.format(game_id))
        plays_json = plays_resp.json()
        # Extract teams in game
        homeTeam = plays_json['gameData']['teams']['home']['triCode']
        awayTeam = plays_json['gameData']['teams']['away']['triCode']
        # Extract plays to itterate over
        plays = plays_json['liveData']['plays']['allPlays']
        # Create list of important plays and empty list to keep future play info
        keyEvents = ['Faceoff','Takeaway','Shot','Goal','Blocked Shot','Hit','Missed Shot','Giveaway']
        allPlays = []
        # Loop over all plays
        for event in plays:
            # Extract Event type and check if it is important, skip if not important
            eventType = event['result']['event']

            if eventType not in keyEvents:
                continue
            # Create eventKey that will be used latter to identify events between play df and shift df
            match eventType:
                case "Faceoff":
                    eventKey = "FAC"
                case "Takeaway":
                    eventKey = "TAKE"
                case "Shot":
                    eventKey = "SHOT"
                case "Goal":
                    eventKey = "GOAL"
                case "Blocked Shot":
                    eventKey = "BLOCK"
                case "Hit":
                    eventKey = "HIT"
                case "Missed Shot":
                    eventKey = "MISS"
                case "Giveaway":
                    eventKey = "GIVE"
            # Extract event info
            eventDescription = event['result']['description']
            shotType = event['result']['secondaryType'] if "secondaryType" in event['result'].keys() else None
            players = event['players'] if 'players' in event.keys() else None
            eventPlayer1 = players[0]['player']['id'] if players != None else None
            eventPlayer2 = players[1]['player']['id'] if players != None and len(players) > 1 else None
            eventPlayer3 = players[2]['player']['id'] if players != None and len(players) > 2 else None
            period = event['about']['period']
            periodTime = self.min_to_sec(event['about']['periodTime'])
            homeGoals = event['about']['goals']['home']
            awayGoals = event['about']['goals']['away']
            coordsX = event['coordinates']['x'] if 'x' in event['coordinates'].keys() else None
            coordsY = event['coordinates']['y'] if 'y' in event['coordinates'].keys() else None
            eventTeam = event['team']['triCode'] if 'team' in event.keys() else None
            playId = eventKey + str(period).zfill(2) + str(periodTime).zfill(4)
            # Append event data to the list
            allPlays.append([period,periodTime,eventType,coordsX,coordsY,eventDescription,shotType,eventPlayer1,eventPlayer2,eventPlayer3,homeGoals,awayGoals,eventTeam,homeTeam,awayTeam,playId])
            # Create a Pandas DataFrame from the collected data
        return pd.DataFrame(allPlays, columns=["period", "periodTime", "eventType", "coordsX", "coordsY", "eventDescription",'shotType', "eventPlayer1", "eventPlayer2", "eventPlayer3", "homeGoals", "awayGoals", "eventTeam", "homeTeam", "awayTeam", "playId"])

    def get_shifts(self, game_id):
        """
        Retrieve and parse player shifts data for an NHL game.

        Args:
            game_id (int): The unique identifier for the NHL game.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing parsed player shifts data.
        """
        # Construct the URL to fetch HTML report
        year = str(game_id)[:4]
        response = requests.get(f"https://www.nhl.com/scores/htmlreports/{year}{int(year)+1}/PL{str(game_id)[4:]}.HTM")
        html = HTMLParser(response.text)
        # Extract relevant elements from the HTML report
        text_elements = html.css(".bborder")
        data = {'Column': text_elements}
        df = pd.DataFrame(data)
        # Organize the extracted data into a DataFrame
        df = df.groupby(df.index // 8).apply(lambda x: pd.Series(x['Column'].values))
        df.columns = ['index', 'period', "strength", "time", "event", "description", "awayOnIce", "homeOnIce"]
        # Get text for simple columns and filter df for important events
        df[['index', 'period', 'strength', 'event', 'description']] = df[['index', 'period', 'strength', 'event', 'description']].applymap(lambda x: x.text())
        df = df[df['event'].isin(['GIVE','FAC','SHOT','HIT','BLOCK','MISS','TAKE','GOAL'])]
        # Get time as int instead of XX:XX string
        time_text = df['time'].apply(lambda x: x.text(separator='\n').split("\n")[0])
        time_text = time_text[time_text != 'Time:']
        df['time'] = pd.to_numeric(time_text.apply(self.min_to_sec), errors='coerce')

        # Get home team from page html
        home_team = html.css_first("div.page:nth-child(3) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(8)").text()[:3]
        away_team = html.css_first("div.page:nth-child(3) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(7)").text()[:3]
        # Get list of players on ice
        df['awayOnIce'] = df['awayOnIce'].apply(lambda x: x.css("font[style='cursor:hand;']"))
        df['homeOnIce'] = df['homeOnIce'].apply(lambda x: x.css("font[style='cursor:hand;']"))
        # Get dict of players on ice and their player_ids
        player_on_ice_json = requests.get(f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}").json()
        player_id_dict = {(idx['teamAbbrev'], f"{idx['firstName']} {idx['lastName']}".lower()): idx['playerId'] for idx in player_on_ice_json['data']}

        # Fix player name issues
        player_id_dict[('MIN', 'jacob middleton')] = 8478136
        player_id_dict[('DAL', 'jani hakanpaa')] = 8475825
        player_id_dict[('NSH', 'thomas novak')] = 8478438
        player_id_dict[('DAL', 'marian studenic')] = 8480226

        home_column_names = ['home1', 'home2', 'home3', 'home4', 'home5', 'home6']
        away_column_names = ['away1', 'away2', 'away3', 'away4', 'away5', 'away6']

        #
        try:
            # Map player names to player IDs
            for idx in range(len(home_column_names)):
                df[home_column_names[idx]] = [str(player_id_dict[(home_team,x[idx].attributes['title'].split("- ")[-1].lower())]) if len(x) > idx and "Goalie" not in x[idx].attributes['title'] else None for x in df['homeOnIce']]

            for idx in range(len(away_column_names)):
                df[away_column_names[idx]] = [str(player_id_dict[(away_team,x[idx].attributes['title'].split("- ")[-1].lower())]) if len(x) > idx and "Goalie" not in x[idx].attributes['title'] else None for x in df['awayOnIce']]
        except KeyError as e:
            print(game_id, repr(e))
        # Extract goalie information
        df['homeGoalie'] = ["".join(player.attributes['title'] if "Goalie" in player.attributes['title'] else "" for player in x).split("- ")[-1].lower() for x in df['homeOnIce']]
        df['awayGoalie'] = ["".join(player.attributes['title'] if "Goalie" in player.attributes['title'] else "" for player in x).split("- ")[-1].lower() for x in df['awayOnIce']]
        # Map goalie names to goalie player IDs
        df['homeGoalie'] = [str(player_id_dict[(home_team,x)]) if x != "" else None for x in df['homeGoalie']]
        df['awayGoalie'] = [str(player_id_dict[(away_team,x)]) if x != "" else None for x in df['awayGoalie']]
        # Create a unique-ish play ID
        df['playId'] = df['event'] + df['period'].str.zfill(2) +  df['time'].astype(str).str.zfill(4)
        # Drop unnecessary columns and reset the index
        return df.drop(columns=['index','awayOnIce', 'homeOnIce'], axis=1).reset_index(drop=True)

    def get_game(self, game_id):
        """
        Retrieve and combine game plays and player shifts data for an NHL game.

        Args:
            game_id (int): The unique identifier for the NHL game.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing combined game data.
        """
       # Retrieve game plays data
        plays = self.get_plays(game_id)
        # Retrieve player shifts data
        shifts = self.get_shifts(game_id)
        # Merge plays and shifts data based on playId
        game_df = pd.merge(plays, shifts, left_on="playId", right_on='playId', how="left")
        
        # Add gameId to the DataFrame
        game_df['gameId'] = game_id
        
        # Drop duplicate entries based on specific columns
        game_df = game_df.drop_duplicates(keep="first", subset=['coordsX', 'coordsY', 'eventDescription', 'playId'])
        
        return game_df
    
    def get_game_ids(self, start_date, end_date):
        """
        Get the game IDs for NHL games within the specified date range.

        Parameters:
            start_date (str): Start date in the format 'YYYY-MM-DD'.
            end_date (str): End date in the format 'YYYY-MM-DD'.

        Returns:
            list: List of game IDs.
        """
        # Retrieve the schedule JSON data from the NHL API
        schedule = requests.get(f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={start_date}&endDate={end_date}&gameType=R").json()
        # Extract the game IDs from the schedule JSON
        game_ids = [game['gamePk'] for day in schedule['dates'] for game in day['games']]
        
        return game_ids

    def scrape_games_to_SQL(self, start_date, end_date):
        """
        Scrape NHL game data for a specified date range and store it in a PostgreSQL database.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
        """
        # Construct the connection string using your credentials
        con_string = f"postgresql+psycopg2://{creds.DB_USER}:{creds.DB_PASS}@{creds.DB_HOST}/{creds.DB_NAME}"

        # Create a database engine and connect to the database
        engine = create_engine(con_string)
        conn = engine.connect()

        # Get a list of game IDs within the specified date range
        game_ids = self.get_game_ids(start_date, end_date)

        try:
            # Try to read existing game IDs from the database
            full_df = pd.read_sql("play_by_play", conn)
            game_ids_parsed = full_df['gameId'].unique()
        except:
            # If there's an exception (e.g., the table doesn't exist yet), initialize an empty list
            game_ids_parsed = []
        
        # Iterate through game IDs and scrape data for new games
        for game in game_ids:
        
            if game not in game_ids_parsed:
                # Get game data
                game_df = self.get_game(game)
                # Append the game data to the 'play_by_play' table in the database
                game_df.to_sql('play_by_play', engine, if_exists='append', index=False)

        # Close the database connection
        conn.close()
    

    def scrape_player_stats(self, player_id):
        pass

    def scrape_videos(self):
        pass




if __name__ == '__main__':
    scraper = HockeyScraper()
    scraper.scrape_games_to_SQL(start_date='2022-10-07', end_date='2023-04-14')




    # links https://statsapi.web.nhl.com/api/v1/game/2022020511/feed/live?site=en_nhl
    # https://www.nhl.com/scores/htmlreports/20222023/PL020511.HTM