import pandas as pd
from tabulate import tabulate
import numpy as np
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import backoff 
import os
import seaborn as sns
from time import sleep
import matplotlib.pyplot as plt
import urllib.error
import concurrent.futures


@backoff.on_exception(backoff.expo, (urllib.error.URLError), max_tries=3)
def read_html(url):
    return pd.read_html(url)

def print_tables(cls):
    for key in cls.__dict__:
        if isinstance(cls.__dict__[key], pd.DataFrame):
            print(key)
            print(tabulate(cls.__dict__[key], headers='keys', tablefmt='psql'))

def parse_table(tbl, index_idx, start_idx=None, end_idx=None, ignore_prev_idx=False):
    tbl.fillna("", inplace=True)
    tbl = tbl.T.drop_duplicates().T # fixed columns that have double width in some tables
    columns = get_columns(tbl, index_idx, ignore_prev_idx)
    end_idx = end_idx if end_idx else len(tbl)
    start_idx = start_idx if start_idx else index_idx+1
    df = tbl[start_idx:end_idx]
    df.columns = columns
    df.set_index(columns[0], inplace=True)
    for c in columns:
        if '/' in c:
            if c == '+/-':
                continue
            pref, suf = c.split('_')
            df[pref.split('/')[0] + "_" + suf] = df[c].str.split('/').str[0]
            df[pref.split('/')[1] + "_" + suf] = df[c].str.split('/').str[1]
            del df[c]    
    return df.apply(pd.to_numeric, errors='ignore')

def get_columns(tbl, index_idx, ignore_prev_idx):
    """
    Columns may be 2 rows width, for exaplem 2PT and M/A, % etc
    I group these together to one index     
    """
    if index_idx < 2 or ignore_prev_idx:
        columns = tbl.iloc[index_idx]
    else:
        columns = (tbl.iloc[index_idx] + "_" + tbl.iloc[index_idx-1]).str.strip("_")
    return columns.astype(str).str.lower()

def read_html_with_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table')

    dataframes = []
    link_maps = []

    for table in tables:
        df = read_html(str(table))[0]
        dataframes.append(df)

        link_map = {}
        for i, a in enumerate(table.find_all('a', href=True)):
            if a.text:
                link_map[f"{a.text}_{i}"] = a['href']
        link_maps.append(link_map)

    return dataframes, link_maps

class GameScraper:
    def __init__(self, url, name=None, round=None, game_idx=None):
        self.url = url
        self.name = name
        self.round = int(round)
        self.game_idx = int(game_idx)
        tables = read_html(self.url)
        self.quaters = parse_table(tables[3],0)
        self.metadata = {i: tables[i].iloc[0,0] for i in range(3, len(tables)) }
        self.team1 = parse_table(tables[4], 2)
        self.team2 = parse_table(tables[5], 2)
        self.advanced = parse_table(tables[6], 1)
        self.team1_bench = parse_table(tables[7], 2, end_idx=5, ignore_prev_idx=False)
        self.team2_bench = parse_table(tables[8], 2, end_idx=5, ignore_prev_idx=False)
        self.team1_locals = parse_table(tables[7], 2, start_idx=6, ignore_prev_idx=False)
        self.team2_locals = parse_table(tables[8], 2, start_idx=6, ignore_prev_idx=False)


class TeamScraper:
    def __init__(self, url):
        self.url = url
        tables, self.links = read_html_with_links(team_url)
        self.metadata = {i: tables[i].iloc[0,0] for i in range(0, len(tables)) }
        self.name = self.metadata[0].split('-')[0].strip()
        self.games = parse_table(tables[0], 1)
        self.stats_regular_season = parse_table(tables[2], 2)
        self.stats_advanced = parse_table(tables[3], 1)
        self.stats_per_game = parse_table(tables[4], 2)
        released_idx = np.where(tables[5].iloc[:,0] == 'Released Players')[0][0]
        self.stats_players = parse_table(tables[5], 2)


    def read_games(self, max_games=None, sleep_time=0.1):
        all_games_links = self.links[4]
        url_prefix = "https://basket.co.il/"
        self.all_games = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures= []
            for i, (round, row) in tqdm(enumerate(self.stats_per_game.iterrows())):
                future = executor.submit(GameScraper, url_prefix + all_games_links[f"{row['game']}_{i}"], name=row['game'], round=round, game_idx=i+1)
                futures.append(future)
                if max_games and len(self.all_games) > 4:
                    break
            for future in tqdm(concurrent.futures.as_completed(futures)):
                game = future.result()
                self.all_games[f"{game.name}_{game.round}"] = game
        self.per_game_player_stats = self.creat_per_game_player_stats()
    
    def creat_per_game_player_stats(self):
        player_stats_list = []
        for key in self.all_games:
            team, loc = key.split("\xa0")
            loc = loc.strip('(').strip(')')[:1]
            if loc == "H":
                player_stats = self.all_games[key].team1  
                team_coach = self.all_games[key].metadata[4].split('Coach: ')[-1][:-1]
            else:
                player_stats = self.all_games[key].team2
                team_coach = self.all_games[key].metadata[5].split('Coach: ')[-1][:-1]
            player_stats.loc[:, 'opponent'] = team
            player_stats.loc[:, 'loc'] = loc
            player_stats.loc[:, 'round'] = self.all_games[key].round
            player_stats.loc[:, 'game_idx'] = self.all_games[key].game_idx
            player_stats.loc[:, 'coach'] = team_coach
            player_stats_list.append(player_stats)
        return pd.concat(player_stats_list)
    

def plot_property(df, x, y,  hue='loc'):
    f, ax = plt.subplots()
    sns.despine(bottom=True, left=True)

    # Show each observation with a scatterplot
    sns.stripplot(
        data=df, x=x, y=y, hue=hue, dodge=True, alpha=.5, zorder=1, legend=False)

    # Show the conditional means in the center of the strips
    sns.pointplot(
        data=df, x=x, y=y, hue=hue, dodge=.8 - .8 / 3, palette="dark", markers="d", markersize=6, linestyle="none", alpha=0.6)
    sns.pointplot(
        data=df, x=x, y=y, color='g', markers="+", markersize=6, linestyle="none",)
    plt.xticks(rotation=90)  # Rotate x ticks 45 degrees
    plt.tight_layout()  # Adjust the layout to prevent overlapping labels
    plt.grid()
    ax.set_title(f"{y} per {x}")

if __name__ == "__main__":
    team_url = "https://basket.co.il/team.asp?TeamId=1054&lang=en"
    team = TeamScraper(team_url)
    team_csv_path = "data/players/" + team.name + ".csv"
    if os.path.exists(team_csv_path):
        team.per_game_player_stats = pd.read_csv(team_csv_path)
        df = team.per_game_player_stats
    else:
        team.read_games()
        df = team.per_game_player_stats
        df.to_csv(team_csv_path)
    players_filter =  (df.groupby('player name')['min'].sum() > 50) & (df.groupby('player name')['min'].count() > 3)
    players = df.groupby('player name').mean()[players_filter]

    filtered_df = df[df['player name'].isin(players_filter[players_filter].index) & (df.game_idx > 2)]
    for player in filtered_df['player name'].unique():
        if 'zach' in player.lower() or 'brynton' in player.lower() or "Gabriel" in player.lower():
            continue
        print(player)

    
    players_to_skip = ['Zach Hankins', 'Brynton Lemar', 'Gabriel Chachashvili']
    filtered_df = filtered_df[~filtered_df['player name'].isin(players_to_skip)]
    mean_coach = filtered_df.groupby(['player name', 'coach']).mean()
    sum_coach = filtered_df.groupby(['player name', 'coach']).sum()
    mean_coach['games'] = filtered_df.groupby(['player name', 'coach']).size()
    mean_coach['%3'] = sum_coach['m_3pt'] / sum_coach['a_3pt'] * 100
    mean_coach['%2'] = sum_coach['m_2pt'] / sum_coach['a_2pt'] * 100
    print(mean_coach[['games', 'min', 'pts', '%2', '%3', 'dr_rebounds', 'or_rebounds', 'tr_rebounds', 'st', 'to', 'as', 'val']].round(1))

    plot_property(filtered_df[filtered_df['player name'] != "Total"], 'player name', 'pts', hue='coach')
    plot_property(filtered_df[filtered_df['player name'] != "Total"], 'player name', 'pts')
    plot_property(filtered_df[filtered_df['player name'] != "Total"], 'player name', 'val')
    a = 3 



    # create a plot of the players pts per game with color as loc
    # sns.set(rc={'figure.figsize':(15,10)})
    # yz = df[df['player name'] == "Yovel Zoosman"]
    # speedy = df[df['player name'] == "Speedy Smith"]
    # blayzer = df[df['player name'] == "Oz Blayzer"]
    # filtered_df = df[df['player name'].isin(["Yovel Zoosman", "Speedy Smith", "Oz Blayzer"])]
    # sns.scatterplot(data=filtered_df, y='pts', x='game_idx', hue='player name')
    # sns.lineplot(data=filtered_df, y='pts', x='game_idx', hue='player name')
    # plt.show()