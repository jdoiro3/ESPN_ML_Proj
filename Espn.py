import json
import requests
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
from helpers import *

class NoGames(Exception):
    pass

class ErrorMsg:

    def __init__(self, type, groups, e):
        if type == 'group':
            print('{0:s} is not an allowed group.\nPossible groups are: {1:s}, {2:s}, {3:s}, {4:s}, {5:s}, {6:s}, {7:s}'.format(str(e), *groups))
        else:
            pass




class EspnGames:

    game_scoreboard_url = r"https://www.espn.com/college-football/scoreboard/_/group/{0:s}/year/{1:s}/seasontype/{2:s}/week/{3:s}?xhr=1"
    game_stats_url = r"http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={0:s}"
    groups = {
                    'FBS':'80',
                    'ACC':'1',
                    'American':'151',
                    'Big 12':'4',
                    'Big Ten':'5',
                    'Pac-12':'9',
                    'SEC':'8'
             }

    def __init__(self, year, week, group='FBS', bowl_week=False):

        self._year = str(year)
        self._week = str(week)
        if bowl_week:
            self._season_type = '3'
        else:
            self._season_type = '2'

        try:
            self._group = self.groups[group]
        except KeyError as e:
            ErrorMsg('group', self.groups, e)


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
        self.game_info = _get_game_info(events_list)
    
    def _load_play_by_play(self):

        if self.game_ids:

            week_plays = []
            for game in self.game_ids:
                game_plays = []
                py_obj = json.loads(requests.get(self.game_stats_url.format(game)).text)
                drives_list = py_obj['drives']['previous']
                for drive in drives_list:
                    drive_plays = pd.DataFrame(drive['plays'])

                    try:
                        drive_plays['Offense Team'] = drive['team']['displayName']
                    except KeyError:
                        drive_plays['Offense Team'] = 'unknown'

                    game_plays.append(drive_plays)

                all_game_plays = pd.concat(game_plays)
                all_game_plays['Game ID'] = game
                all_game_plays['Home Team'] = self.game_info[game]['homeTeam']
                all_game_plays['Away Team'] = self.game_info[game]['awayTeam']
                week_plays.append(all_game_plays)

            self.all_plays = pd.concat(week_plays, sort=False).reset_index(drop=True)

        else:
            return True

    def _format_play_by_play(self):

        if ~self.all_plays.empty:
        
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

    def load_plays(self):

        self._get_game_ids()
        self._load_play_by_play()
        self._format_play_by_play()

        

            





