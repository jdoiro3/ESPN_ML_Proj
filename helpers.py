def _homeTeam(event):
    team1 = event['competitions'][0]['competitors'][0]
    team2 = event['competitions'][0]['competitors'][1]

    if team1['homeAway'] == 'home':
        return team1['team']['displayName']
    elif team2['homeAway'] == 'home':
        return team2['team']['displayName']
    else:
        return 'unknown'


def _awayTeam(event):
    team1 = event['competitions'][0]['competitors'][0]
    team2 = event['competitions'][0]['competitors'][1]

    if team1['homeAway'] == 'away':
        return team1['team']['displayName']
    elif team2['homeAway'] == 'away':
        return team2['team']['displayName']
    else:
        return 'unknown'

def _get_game_info(events):
    dic = {}
    for event in events:
        dic.update({
                        event['id']:
                                    {
                                        'homeTeam': _homeTeam(event),
                                        'awayTeam': _awayTeam(event)
                                    }
                    })
    return dic