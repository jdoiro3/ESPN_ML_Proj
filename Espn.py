import pandas as pd
import requests
import json
from pandas.io.json import json_normalize
import asyncio
import concurrent.futures
import itertools
from IPython.display import clear_output
import nest_asyncio
nest_asyncio.apply()


class EspnDataCollector:
    
    weeks_default = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14']
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
        self.plays = pd.DataFrame()

    def _get_group(self, group):
        try:
            return self.groups[group]
        except KeyError(group):
            print('Possible groups are: {1:s}, {2:s}, {3:s}, {4:s}, {5:s}, {6:s}, {7:s}'.format(*groups))

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
    
    def _get_url_params(self, years, weeks=None):
        if not isinstance(years,list):
            raise ValueError("years must be list")

        if weeks:
            if type(weeks) is not list:
                raise ValueError("weeks must be list")
            params = []
            for year in years:
                for week in weeks:
                    params.append((year, week, '2'))
            return params
        else:
            params = [[(year, str(i + 1), '2') for i in range(len(self.weeks_default))] for year in years]
            bowl_params = [(year, '1', '3') for year in years]
            params.append(bowl_params)
            return list(itertools.chain.from_iterable(params))

        
    def _get_drive_plays(self, drive):
        drive_plays = pd.DataFrame(drive['plays'])
        try:
            drive_plays['Offense Team'] = drive['team']['displayName']
        except KeyError:
            drive_plays['Offense Team'] = 'unknown'
            
        return drive_plays
        
    
    async def _get_game_ids_and_info(self, group, years, weeks, url):

        url_params = self._get_url_params(years, weeks)

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                        loop.run_in_executor(
                                                executor,
                                                requests.get,
                                                url.format(group, params[0], params[2], params[1])
                                            ) 
                        for params in url_params
                      ]
            for response in await asyncio.gather(*futures):
                clear_output(wait=True)
                print("getting game ids...")
                events_list = json.loads(response.text)['content']['sbData']['events']
                self.game_ids = self.game_ids+[event['id'] for event in events_list]
                self.game_info.update(self._get_game_info(events_list))
                
    async def _get_plays(self, url):
         with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                        loop.run_in_executor(
                                                executor,
                                                requests.get,
                                                url.format(game)
                                            ) 
                        for game in self.game_ids
                      ]
            counter = 0
            for response in await asyncio.gather(*futures):
                clear_output(wait=True)
                print("total games fetched {}".format(counter+1))
                try:
                    drives_list = json.loads(response.text)['drives']['previous']
                    game_all_plays = pd.concat(list(map(self._get_drive_plays, drives_list)), sort=False)
                    game_all_plays['homeTeam'] = self.game_info[self.game_ids[counter]]['homeTeam']
                    game_all_plays['awayTeam'] = self.game_info[self.game_ids[counter]]['awayTeam']
                    self.plays = self.plays.append(game_all_plays, sort=False)
                except KeyError:
                    print('game info not found for {}.'.format(self.game_ids[counter]))
                counter += 1
        
    def get_game_ids_and_info(self, group, years, weeks):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._get_game_ids_and_info(group, years, weeks, self.game_scoreboard_url))
        
    def get_plays(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._get_plays(self.game_stats_url))  
        
    def format_plays(self):

        self.plays['clock'] = self.plays['clock'].apply(lambda x: x['displayValue'] if type(x) is dict else x)
        self.plays['type'] = self.plays['type'].apply(lambda x: x['text'] if type(x) is dict else x)
        self.plays['period'] = self.plays['period'].apply(lambda x: x['number'] if type(x) is dict else x)
        self.plays['scoringType'] = self.plays['scoringType'].apply(lambda x: x['name'] if type(x) is dict else x)

        temp_df = json_normalize(self.plays['start'])
        temp_df.columns = [col + '_start' for col in temp_df.columns]
        self.plays = pd.concat([self.plays.drop('start', axis=1).reset_index(drop=True), temp_df], axis=1, sort=False)

        temp_df = json_normalize(self.plays['end'])
        temp_df.columns = [col + '_end' for col in temp_df.columns]
        self.plays = pd.concat([self.plays.drop('end', axis=1).reset_index(drop=True), temp_df], axis=1, sort=False)

        del temp_df
                
    
# -

class PlayByPlay(EspnDataCollector):
    """
    Class that loads College Football play by play data from Espn into a DataFrame.
    Parameters:
        group (str) - default is 'FBS'
    """

    def __init__(self, group='FBS', **kwargs):
        
        super().__init__()

        self.options = {
            'weeks':False,
            'years':False,
            'year':False,
            'week':False
        }

        self.options.update(kwargs)

        # year(s) must be passed
        if not (self.options['years'] or self.options['year']):
            raise ValueError("A year(s) parameter must be passed")
        # year(s) must be a list or a tuple    
        if not (isinstance(self.options['years'], (list,bool)) and isinstance(self.options['year'], (list,bool))):
            raise TypeError("years must be a list object")
        # week(s) must be either a list or a tuple    
        if not (isinstance(self.options['weeks'], (list,bool)) and isinstance(self.options['week'], (list,bool))):
            raise TypeError("weeks must be either a list object")
        # year and years params can't be passed together   
        if self.options['year'] and self.options['years']:
            raise ValueError("A year and years parameter can not be passed together.")
        # week and weeks params can't be passed together
        if self.options['week'] and self.options['weeks']:
            raise ValueError("A week and weeks parameter can not be passed together.")


        if self.options['years']:
            self._years = [str(yr) for yr in self.options['years']]
        if self.options['weeks']:
            self._weeks = [str(wk) for wk in self.options['weeks']]
        else:
            self._weeks = self.weeks_default

        try:
            self._group = self.groups[group]
        except KeyError as e:
            print("{} is not a group".format(e))

    def __str__(self):
        print("PlaybyPlay instance for week(s) {} and year(s) {}.".format(self._weeks, self._years))

    def load_plays(self):
        self.get_game_ids_and_info(
                                        self._group, 
                                        self._years, 
                                        self._weeks
                                        )

        self.get_plays()
        self.format_plays()
        
        print("Number of plays: {}".format(self.plays.shape[0]))
        

            





