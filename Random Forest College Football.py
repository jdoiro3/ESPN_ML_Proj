# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import os
import pandas as pd
pd.options.display.max_columns = 30
import numpy as np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split

def get_sec(time_str):
    """Get Seconds from time."""
    m, s = time_str.split(':')
    return int(m)*60+int(s)


# +
files = os.listdir(r"N:\ue\EntFin\Audit\EA Analytics\Training LunchLearn\Power BI\CF-Data\CSV")

df_list = []
for file in files:
    df = pd.read_csv(r"N:\ue\EntFin\Audit\EA Analytics\Training LunchLearn\Power BI\CF-Data\CSV"+"\\"+file)
    df_list.append(df)
    
data_df = pd.concat(df_list)

print(data_df.shape)
# -

bowl_games_2007 = pd.read_csv(r"N:\ue\EntFin\Audit\EA Analytics\Training LunchLearn\Power BI\CF-Data\PBP - 2007 - Bowl Week.csv")
lsu_game_all_cols = bowl_games_2007[bowl_games_2007['gameId']==280070194].reset_index(drop=True)

# +
data_df['clock'] = data_df['clock'].apply(get_sec)
lsu_game_all_cols['clock'] = lsu_game_all_cols['clock'].apply(get_sec)

data_df['total time remaining'] = ((4-data_df['quarter'])*900)+data_df['clock']
lsu_game_all_cols['total time remaining'] = ((4-lsu_game_all_cols['quarter'])*900)+lsu_game_all_cols['clock']
# -

ending_scores = data_df.sort_values('total time remaining').groupby('gameId', as_index=False).first().sort_values('gameId')[['gameId','homeScore','awayScore']]
ending_score_lsu = lsu_game_all_cols.sort_values('total time remaining').groupby('gameId', as_index=False).first().sort_values('gameId')[['gameId','homeScore','awayScore']]

# +
training_df = data_df[['gameId','offenseId','offenseTeam','homeId','homeScore',
                       'awayScore','down','distance','yardLine','total time remaining']]

lsu_game = lsu_game_all_cols[['gameId','offenseId','offenseTeam','homeId','homeScore',
                       'awayScore','down','distance','yardLine','total time remaining']]

print(training_df.shape)

training_df = training_df.merge(ending_scores, on='gameId',how='left')
training_df = training_df.rename(columns={'homeScore_y':'end home score', 'awayScore_y':'end away score',
                                         'homeScore_x':'homeScore','awayScore_x':'awayScore'})


lsu_game = lsu_game.merge(ending_score_lsu, on='gameId',how='left')
lsu_game = lsu_game.rename(columns={'homeScore_y':'end home score', 'awayScore_y':'end away score',
                                         'homeScore_x':'homeScore','awayScore_x':'awayScore'})

print(training_df.shape)

training_df = training_df[training_df['total time remaining']>=0].reset_index(drop=True)

lsu_game = lsu_game[lsu_game['total time remaining']>=0].reset_index(drop=True)

print(training_df.shape)

# +
training_df['outcome'] = np.where(
                                      training_df['offenseId']==training_df['homeId'], 
                                      training_df['end home score']>training_df['end away score'],
                                      training_df['end home score']<training_df['end away score']
    
                                 ).astype(int)

lsu_game['outcome'] = np.where(
                                      lsu_game['offenseId']==lsu_game['homeId'], 
                                      lsu_game['end home score']>lsu_game['end away score'],
                                      lsu_game['end home score']<lsu_game['end away score']
    
                                 ).astype(int)


training_df = training_df.drop(['end away score','end home score'], axis=1)
lsu_game = lsu_game.drop(['end away score','end home score'], axis=1)

training_df['total points'] = training_df['homeScore']+training_df['awayScore']
training_df['is home team'] = np.where(training_df['offenseId']==training_df['homeId'], 1,0)
training_df['score diff'] = np.where(
                                        training_df['offenseId']==training_df['homeId'],
                                        training_df['homeScore']-training_df['awayScore'],
                                        training_df['awayScore']-training_df['homeScore']
                                        )

training_df['score adjusted'] = training_df['score diff']/np.sqrt(training_df['total time remaining']+1)


lsu_game['total points'] = lsu_game['homeScore']+lsu_game['awayScore']
lsu_game['is home team'] = np.where(lsu_game['offenseId']==lsu_game['homeId'], 1,0)
lsu_game['score diff'] = np.where(
                                        lsu_game['offenseId']==lsu_game['homeId'],
                                        lsu_game['homeScore']-lsu_game['awayScore'],
                                        lsu_game['awayScore']-lsu_game['homeScore']
                                        )
lsu_game['score adjusted'] = lsu_game['score diff']/np.sqrt(lsu_game['total time remaining']+1)
# -

training_df.describe()

print(training_df.shape)
training_df.head()

# +
X = training_df[['down','distance','yardLine','total time remaining', 
                 'total points', 'is home team', 'score diff', 'score adjusted']]

cols = list(X.columns)

X = np.array(X)
Y = np.array(training_df['outcome'])

print(X.shape)
print(Y.shape)
# -

train_X, test_X, train_Y, test_Y = train_test_split(X, Y, test_size = 0.25, random_state = 42)

rf = RandomForestRegressor(n_estimators = 200, random_state = 42)

rf.fit(train_X, train_Y)

predictions = rf.predict(test_X)

pd.DataFrame({'feature':cols, 'value':rf.feature_importances_}).set_index('feature').plot.barh()

# +
X_lsu = lsu_game[['down','distance','yardLine','total time remaining', 
                 'total points', 'is home team', 'score diff', 'score adjusted']]

Y_lsu = np.array(lsu_game['outcome'])
# -

lsu_predictions = rf.predict(X_lsu)

lsu_game.head()

lsu_game_all_cols['prob of win'] = lsu_predictions

lsu_game_all_cols['prob of win'] = np.where(lsu_game_all_cols['offenseTeam']=='LSU', lsu_game_all_cols['prob of win'], 1-lsu_game_all_cols['prob of win'])

lsu_game_all_cols['prob of win'].plot()

lsu_game_all_cols = lsu_game_all_cols.reset_index(drop=True)

lsu_game_all_cols['homeScore diff'] = lsu_game_all_cols['homeScore'].diff()
lsu_game_all_cols['awayScore diff'] = lsu_game_all_cols['awayScore'].diff()
lsu_game_all_cols['event'] = np.where(lsu_game_all_cols['homeScore diff']==6.0,"Touchdown Ohio",
                                         np.where(lsu_game_all_cols['awayScore diff']==6.0, "Touchdown LSU", 
                                                 np.where(lsu_game_all_cols['homeScore diff']==3.0,"Fieldgoal Ohio",
                                                         np.where(lsu_game_all_cols['awayScore diff']==3.0, "Fieldgoal LSU",""))))

lsu_game_all_cols.iloc[30:100]

lsu_game_all_cols.to_csv(r"N:\ue\EntFin\Audit\EA Analytics\Training LunchLearn\Power BI\CF-Data\LSU_Ohio_RandomForest.csv")
