import pandas as pd
from tabulate import tabulate
import numpy as np
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import backoff 
import os

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
        df = pd.read_html(str(table))[0]
        dataframes.append(df)

        link_map = {}
        for a in table.find_all('a', href=True):
            if a.text:
                link_map[a.text] = a['href']
        link_maps.append(link_map)

    return dataframes, link_maps

class GameScraper:
    def __init__(self, url):
        self.url = url
        tables = pd.read_html(self.url)
        self.quaters = parse_table(tables[3],0)
        self.metadata = {i: tables[i].iloc[0,0] for i in range(3, len(tables)) }
        self.team1 = parse_table(tables[4], 2)
        self.team2 = parse_table(tables[5], 2)
        self.advanced = parse_table(tables[6], 1)
        self.team1_bench = parse_table(tables[7], 2, end_idx=5, ignore_prev_idx=False)
        self.team2_bench = parse_table(tables[8], 2, end_idx=5, ignore_prev_idx=False)
        self.team1_locals = parse_table(tables[7], 2, start_idx=6, ignore_prev_idx=False)
        self.team2_locals = parse_table(tables[8], 2, start_idx=6, ignore_prev_idx=False)
    
    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException), max_tries=3)
    def read_html(self):
        return pd.read_html(self.url)


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

    def read_games(self, max_games=None):
        all_games_links = self.links[4]
        url_prefix = "https://basket.co.il/"
        self.all_games = {}
        from time import sleep
        for index, row in tqdm(self.stats_per_game.iterrows()):
            sleep(1)
            game = GameScraper(url_prefix + all_games_links[row['game']])
            self.all_games[row['game']] = game
            if max_games and len(self.all_games) > 4:
                break
        self.per_game_player_stats = self.creat_per_game_player_stats()
    
    def creat_per_game_player_stats(self):
        player_stats_list = []
        for key in self.all_games:
            team, loc = key.split("\xa0")
            loc = loc.strip('(').strip(')')
            player_stats = self.all_games[key].team1 if loc == "H" else self.all_games[key].team2
            player_stats.loc[:, 'opponent'] = team
            player_stats.loc[:, 'loc'] = loc
            player_stats_list.append(player_stats)
        return pd.concat(player_stats_list)
    

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
    players_filter =  (df.groupby('player name')['min'].sum() > 100) & (df.groupby('player name')['min'].count() > 3)
    players = df.groupby('player name').mean()[players_filter]
    # game_url = "https://basket.co.il/game-zone.asp?GameId=25036&lang=en"
    # game = GameScraper(game_url)
    # # game.print_tables()
    a = 3 