import json
import requests
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np

class NoGames(Exception):
    pass


class EspnGames:

    game_scoreboard_url = r"https://www.espn.com/college-football/scoreboard/_/group/{0:s}/year/{1:s}/seasontype/{2:s}/week/{3:s}?xhr=1"
    game_stats_url = r"http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={0:s}"

    def __init__(self, year, week, group='FBS', bowl_week=False):

        self._year = str(year)
        self._week = str(week)

        if bowl_week:
            self._season_type = '3'
        else:
            self._season_type = '2'
            
        if group == 'FBS':
            self._group = '80'
        elif group == 'ACC':
            self._group = '1'
        elif group == 'American':
            self._group = '151'
        elif group == 'Big 12':
            self._group = '4'
        elif group == 'Big Ten':
            self._group = '5'
        elif group == 'Pac-12':
            self._group = '9'
        elif group == 'SEC':
            self._group = '8'
        else:
            raise ValueError(group)

        self.game_ids = []
        self.all_plays = None
        

    def _get_game_ids(self):
        py_obj = json.loads(
                                requests.get(
                                                self.game_scoreboard_url.format(
                                                                                    self._group,
                                                                                    self._year,
                                                                                    self._season_type,
                                                                                    self._week
                                                                                )
                                            ).text
                            )

        events_list = py_obj['content']['sbData']['events']
        self.game_ids = [event['id'] for event in events_list]
    
    def _load_play_by_play(self):

        if self.game_ids:

            plays = []
            for game in self.game_ids:
                py_obj = json.loads(requests.get(self.game_stats_url.format(game)).text)
                drives_list = py_obj['drives']['previous']
                for drive in drives_list:
                    drive_plays = pd.DataFrame(drive['plays'])
                    try:
                        drive_plays['Offense Team'] = drive['team']['displayName']
                    except:
                        drive_plays['Offense Team'] = 'unknown'
                    drive_plays['Game ID'] = game
                    plays.append(drive_plays)

            self.all_plays = pd.concat(plays, sort=False).reset_index(drop=True)

        else:
            return True

    def _format_play_by_play(self):

        if self.all_plays:
        
            self.all_plays['clock'] = self.all_plays['clock'].apply(lambda x: x['displayValue'] if type(x) is dict else x)
            self.all_plays['type'] = self.all_plays['type'].apply(lambda x: x['text'] if type(x) is dict else x)
            self.all_plays['period'] = self.all_plays['period'].apply(lambda x: x['number'] if type(x) is dict else x)
            self.all_plays['scoringType'] = self.all_plays['scoringType'].apply(lambda x: x['name'] if type(x) is dict else x)
        
            temp_df = json_normalize(self.all_plays['start'])
            temp_df.columns = [col+'_start' for col in temp_df.columns]
            self.all_plays = pd.concat([self.all_plays.drop('start',axis=1), temp_df], axis=1)

            temp_df = json_normalize(self.all_plays['end'])
            temp_df.columns = [col+'_end' for col in temp_df.columns]

            self.all_plays = pd.concat([self.all_plays.drop('end',axis=1), temp_df], axis=1)

            del temp_df

        else:
            return True

    def get_plays(self):

        self._get_game_ids()
        self._load_play_by_play()
        self._format_play_by_play()

        

            
            
    
    
week1 = EspnGames(2019,1)

week1.get_plays()

week1.all_plays



