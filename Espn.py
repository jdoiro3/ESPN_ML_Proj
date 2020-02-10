import json
import requests
import pandas as pd
from pandas.io.json import json_normalize

class NoGames(Exception):
    pass

class ErrorMsg:

    def __init__(self, type, groups, e):
        if type == 'group':
            print('{0:s} is not an allowed group.\nPossible groups are: {1:s}, {2:s}, {3:s}, {4:s}, {5:s}, {6:s}, {7:s}'.format(str(e), *groups))
        elif type == 'noArg':
            print("{} ")


class Espn:
    """
    This is an abstract class used by PlayByPlay.

    Attributes:

        Class Attributes
        --------------------------------------------------------------------
        game_scoreboard_url (str) - unformatted string url used to get game ids
        game_stats_url (str) - unformatted string used to get game info
        groups (dic) - dictionary used to map NCAA conferences to their respective ids
        game_ids

        Instance attributes
        --------------------------------------------------------------------
        game_ids (list) - stores the game ids
        game_info (dic) - stores info about each game
        all_plays (DataFrame) - initialized to None but is a DataFrame after child method 'load_plays' is called

    """

    game_scoreboard_url = r"https://www.espn.com/college-football/scoreboard/_/group/{0:s}/year/{1:s}/seasontype/{2:s}/week/{3:s}?xhr=1"
    game_stats_url = r"http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={0:s}"

    groups = {
        'FBS': '80',
        'ACC': '1',
        'American': '151',
        'Big 12': '4',
        'Big Ten': '5',
        'Pac-12': '9',
        'SEC': '8'
    }

    def __init__(self):
        self.game_ids = []
        self.game_info = {}
        self.all_plays = None

    def _get_group(self, group):
        try:
            return self.groups[group]
        except KeyError(group):
            ErrorMsg('group')

    def _homeTeam(self, event):
        team1 = event['competitions'][0]['competitors'][0]
        team2 = event['competitions'][0]['competitors'][1]

        if team1['homeAway'] == 'home':
            return team1['team']['displayName']
        elif team2['homeAway'] == 'home':
            return team2['team']['displayName']
        else:
            return 'unknown'

    def _awayTeam(self, event):
        team1 = event['competitions'][0]['competitors'][0]
        team2 = event['competitions'][0]['competitors'][1]

        if team1['homeAway'] == 'away':
            return team1['team']['displayName']
        elif team2['homeAway'] == 'away':
            return team2['team']['displayName']
        else:
            return 'unknown'

    def _get_game_info(self, events):
        dic = {}
        for event in events:
            dic.update({
                event['id']:
                    {
                        'homeTeam': self._homeTeam(event),
                        'awayTeam': self._awayTeam(event)
                    }
            })
        return dic

    def _get_game_ids(self, group, year, season_type, weeks):
        py_obj = json.loads(
                                requests.get(
                                                self.game_scoreboard_url.format(
                                                                                    group,
                                                                                    year,
                                                                                    season_type,
                                                                                    str(weeks)
                                                                                )
                                            ).text
                            )

        events_list = py_obj['content']['sbData']['events']
        self.game_ids = self.game_ids+[event['id'] for event in events_list]
        self.game_info.update(self._get_game_info(events_list))

    def _get_drive_plays(self, drive):
        drive_plays = pd.DataFrame(drive['plays'])
        try:
            drive_plays['Offense Team'] = drive['team']['displayName']
        except KeyError:
            drive_plays['Offense Team'] = 'unknown'

        return drive_plays

    def _get_game_plays(self, game):
        py_obj = json.loads(requests.get(self.game_stats_url.format(game)).text)
        try:
            drives_list = py_obj['drives']['previous']
            game_all_plays = pd.concat(list(map(self._get_drive_plays, drives_list)), sort=False)
            game_all_plays['homeTeam'] = self.game_info[game]['homeTeam']
            game_all_plays['awayTeam'] = self.game_info[game]['awayTeam']
            return game_all_plays
        except KeyError:
            print('game info not found for {}.'.format(game))

    def _get_plays(self, game_ids):
        plays = list(map(self._get_game_plays, game_ids))
        return pd.concat(plays, sort=False)

    def _format_plays(self):

        self.all_plays['clock'] = self.all_plays['clock'].apply(lambda x: x['displayValue'] if type(x) is dict else x)
        self.all_plays['type'] = self.all_plays['type'].apply(lambda x: x['text'] if type(x) is dict else x)
        self.all_plays['period'] = self.all_plays['period'].apply(lambda x: x['number'] if type(x) is dict else x)
        self.all_plays['scoringType'] = self.all_plays['scoringType'].apply(lambda x: x['name'] if type(x) is dict else x)

        temp_df = json_normalize(self.all_plays['start'])
        temp_df.columns = [col + '_start' for col in temp_df.columns]
        self.all_plays = pd.concat([self.all_plays.drop('start', axis=1).reset_index(drop=True), temp_df], axis=1, sort=False)

        temp_df = json_normalize(self.all_plays['end'])
        temp_df.columns = [col + '_end' for col in temp_df.columns]

        self.all_plays = pd.concat([self.all_plays.drop('end', axis=1).reset_index(drop=True), temp_df], axis=1, sort=False)

        del temp_df




class PlayByPlay(Espn):
    """
    Class that loads College Football play by play data from Espn into a DataFrame.

    Parameters:
        group (str) - default is 'FBS'
        bowl_week (bool) - default is True NEED TO FIX

    """

    weeks_default = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14']

    def __init__(self, group='FBS', bowl_week=False, **kwargs):

        self.options = options = {
            'week':False,
            'weeks':False,
            'year':False,
            'years':False
        }

        self.options.update(kwargs)

        if not (self.options['year'] or self.options['years']):
            raise ValueError("Either a year or years parameter must be passed")
        if self.options['year'] and self.options['years']:
            raise ValueError("A year and years parameter can not be passed together.")
        if self.options['week'] and self.options['weeks']:
            raise ValueError("A week and weeks parameter can not be passed together.")

        if self.options['year']:
            self._year = str(self.options['year'])
        if self.options['week']:
            self._weeks = list(str(self.options['week']))
        if self.options['years']:
            self._years = list(self.options['years'])
        if self.options['weeks']:
            self._weeks = list(self.options['weeks'])

        if bowl_week:
            self._season_type = '3'
        else:
            self._season_type = '2'

        try:
            self._group = self.groups[group]
        except KeyError as e:
            ErrorMsg('group', self.groups, e)

        Espn.__init__(self)

    def __str__(self):
        print("PlaybyPlay instance for week {} of the {} season.".format(self._week, self._year))

    def load_plays(self):
        # multiple years were passed  and we assume they want to load all weeks for the years
        if self.options['years']:
            for year in self._years:
                for week in self.weeks_default:
                    self._get_game_ids(self._group, year, self._season_type, week)

            # get the bowl game plays
            self._get_game_ids(self._group, year, season_type='3', weeks='1')
            print(self.game_ids)
            self.all_plays = self._get_plays(self.game_ids)
            self._format_plays()

        # only a year was passed so we assume they want all plays for the year
        elif self.options['year'] and not self.options['week'] and not self.options['weeks']:
            for week in self.weeks_default:
                self._get_game_ids(self._group, self._year, self._season_type, week)

            self.all_plays = self._get_plays(self.game_ids)

            # get the bowl game plays
            self._get_game_ids(self._group, self._year, season_type='3', weeks='1')
            print(self.game_ids)
            self.all_plays = self._get_plays(self.game_ids)
            self._format_plays()

        # a year was passed with specific weeks
        elif self.options['year'] and (self.options['weeks'] or self.options['week']):

            for week in self._weeks:
                print(week)
                self._get_game_ids(self._group, self._year, self._season_type, week)
            if self.bowl_week:
                # get the bowl game plays
                print("getting bowl game ids...")
                self._get_game_ids(self._group, self._year, season_type='3', weeks='1')

                self.all_plays = self._get_plays(self.game_ids)
                self._format_plays()

        

            





