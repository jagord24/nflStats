import sys
import pandas as pd
from datetime import datetime

args = sys.argv
n = len(sys.argv)

print("- - - - - -\nTotal arguments passed:", n)
print("Name of Python script:", args[0])
print("\nArguments passed:", end = " ")
for i in range(1, n):
    print('\n ' + sys.argv[i], end = " ")
print("\n- - - - - -")

file_path = args[1]
print(f'File Path: {file_path}')

team = args[2]

# columns to keep
cols_to_keep = [
    'play_id',
    'game_id',
    'game_date',
    'home_team',
    'away_team',
    'posteam',
    'defteam',
    'season',
    'game_half',
    'week',
    'score_differential',
    'score_differential_post',
    'qtr',
    'game_seconds_remaining',
    'sp',
    'total_home_score',
    'total_away_score',
    'posteam_score',
    'defteam_score',
    'posteam_score_post',
    'defteam_score_post'
]


# start time for reading csv
startTime = datetime.now()

df = pd.read_csv(file_path, usecols=cols_to_keep) # Read the Parquet file into a dataframe

endtime = datetime.now()

print(f'Time to read csv: {endtime - startTime}')
df_shape = df.shape
print(f'File Shape:\n\tRows: {df_shape[0]:,}\n\tColumns: {df_shape[1]:,}')

# filter out overtime from the data
df_no_overtime = df[df['game_half'] != 'Overtime']

# print size of data before and after filtering overtime
print(f'Before Removing Overtime: {df_shape[0]:,} rows')
print(f'After Removing Overtime: {df_no_overtime.shape[0]:,} rows')


def get_team_data(team, df):
    df_team = df[(df['home_team'] == team) | (df['away_team'] == team)]
    # print number of records before and after filtering for team
    # print a confirmation that each record contains the team in either home or away
    print(f'After Filtering for {team}: {df_team.shape[0]:,} rows')

    return df_team



df_team = get_team_data(team, df_no_overtime)
df_team = df_team.copy()

# add column to name the opponent team
df_team.loc[:,'opponent'] = df_team.apply(lambda row: row['away_team'] if row['home_team'] == team else row['home_team'], axis=1)

# add column for team
df_team.loc[:,'team'] = team

df_team.loc[:,'game_seconds_elapsed'] = df_team.apply(lambda row: 3600 - row['game_seconds_remaining'], axis=1)
df_team.loc[:,'season_seconds_elapsed'] = df_team.apply(lambda row: (row['week'] - 1) * 3600 + row['game_seconds_elapsed'], axis = 1)

# get team score and opponent score after the play
df_team.loc[:,'team_score_after'] = df_team.apply(lambda row: row['posteam_score_post'] if row['posteam'] == team else row['defteam_score_post'], axis=1)
df_team.loc[:,'opp_score_after'] = df_team.apply(lambda row: row['posteam_score_post'] if row['posteam'] != team else row['defteam_score_post'], axis=1)

# get team score and opponent score before the play
df_team.loc[:,'team_score_before'] = df_team.apply(lambda row: row['posteam_score'] if row['posteam'] == team else row['defteam_score'], axis=1)
df_team.loc[:,'opp_score_before'] = df_team.apply(lambda row: row['posteam_score'] if row['posteam'] != team else row['defteam_score'], axis=1)

cumulative_team_score = 0
cumulative_opp_score = 0

def points_scored(row, which_team):
    if which_team == 'team':
        return row['team_score_after'] - row['team_score_before']
    elif which_team == 'opp':
        return row['opp_score_after'] - row['opp_score_before']
    else:
        print('Error')
        return 0
    

df_team.loc[:,'points_scored'] = df_team.apply(lambda row: points_scored(row, 'team'), axis=1)
df_team.loc[:,'points_allowed'] = df_team.apply(lambda row: points_scored(row, 'opp'), axis=1)

df_team.loc[:,'play_differential'] = df_team.apply(lambda row: row['points_scored'] - row['points_allowed'], axis=1)

# calculate current game score differential by subtracting opp_score_after from team_score_after
df_team.loc[:,'score_differential'] = df_team['team_score_after'] - df_team['opp_score_after']

# cumulative sum column for play_differential by season
df_team.loc[:,'cumulative_play_differential'] = df_team['play_differential'].cumsum()

'''
def cumulative_score_calculator(row, which_team, cumulative_team_score, cumulative_opp_score):
    score_change_team = row['team_score_after'] - row['team_score_before']
    score_change_opp = row['opp_score_after'] - row['opp_score_before']
    
    print(cumulative_team_score, cumulative_opp_score,'\n\t' , cumulative_team_score - cumulative_opp_score)
    
    if score_change_team > 0 and which_team == 'team':
        cumulative_team_score += score_change_team
        return cumulative_team_score
    elif score_change_opp > 0 and which_team == 'opp':
        cumulative_opp_score += score_change_opp
        return cumulative_opp_score
    elif which_team == 'team':
        return cumulative_team_score
    elif which_team == 'opp':
        return cumulative_opp_score
    else:
        print('Error')
        return 0
    

df_team.loc[:,'cumulative_team_score'] = df_team.apply(lambda row: cumulative_score_calculator(row, 'team', cumulative_team_score, cumulative_opp_score), axis=1)
df_team.loc[:,'cumulative_opp_score'] = df_team.apply(lambda row: cumulative_score_calculator(row, 'opp', cumulative_team_score, cumulative_opp_score), axis=1)
'''
# sort df_team by game_date then play_id
df_team = df_team.sort_values(by=['game_date', 'play_id'], ascending=True)

cols_for_writing = [
    'play_id',
    'game_id',
    'game_date',
    'season',
    'week',
    'game_seconds_elapsed',
    'season_seconds_elapsed',
    'home_team',
    'away_team',
    'posteam',
    'defteam',
    'sp',
    'team_score_before',
    'opp_score_before',
    'team_score_after',
    'opp_score_after',
    'points_scored',
    'points_allowed',
    'play_differential',
    'cumulative_play_differential',
    'score_differential',
    'team',
    'opponent'
]

df_team[cols_for_writing].to_csv(f'data/working/{team}_pbp.csv', index=False)