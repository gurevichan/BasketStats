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
import consts
from io import StringIO

from bs4 import MarkupResemblesLocatorWarning
import warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

@backoff.on_exception(backoff.expo, (urllib.error.URLError), max_tries=3)
def read_html(url):
    return pd.read_html(url)

def print_tables(cls):
    for key in cls.__dict__:
        if isinstance(cls.__dict__[key], pd.DataFrame):
            print(key)
            print(tabulate(cls.__dict__[key], headers='keys', tablefmt='psql'))

def _to_numeric(df):
    for col in df.columns:
        try:
            df.loc[:, col] = pd.to_numeric(df[col])
        except Exception:
            pass  # Leave column as is if conversion fails
    return df


def parse_table(tbl, index_idx, start_idx=None, end_idx=None, ignore_prev_idx=False):
    # add docstring with parameters and return value
    """    Parses a table from a DataFrame, extracting columns and rows based on the provided indices.
    Args:
        tbl (pd.DataFrame): The DataFrame containing the table to parse.
        index_idx (int): The index of the row to use as column headers.
        start_idx (int, optional): The starting index for the data rows. Defaults to index_idx + 1.
        end_idx (int, optional): The ending index for the data rows. Defaults to None, which means all rows after start_idx.
        ignore_prev_idx (bool, optional): If True, ignores the previous row when creating column names. Defaults to False.
    Returns:
        pd.DataFrame: A DataFrame with the parsed data, where the first column is set as the index.
    """
    tbl.fillna("", inplace=True)
    tbl = tbl.T.drop_duplicates().T # fixed columns that have double width in some tables
    tbl = tbl.dropna()  # drop rows that are completely empty
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
            df = df.copy()
            df.loc[:, pref.split('/')[0] + "_" + suf] = df[c].str.split('/').str[0]
            df.loc[:, pref.split('/')[1] + "_" + suf] = df[c].str.split('/').str[1]
            del df[c]    
    return _to_numeric(df)

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
        df = read_html(StringIO(str(table)))[0]
        dataframes.append(df)

        link_map = {}
        for i, a in enumerate(table.find_all('a', href=True)):
            if a.text:
                link_map[f"{a.text}_{i}"] = a['href']
        link_maps.append(link_map)

    return dataframes, link_maps

def cleanup_team_name(name):
    """
    Cleans up the team name by removing unwanted characters and formatting.
    """
    name = name.replace('/', ' ')
    name = name.replace('  ', ' ')
    name = name.replace(' ', '_')
    return name.strip()

class GameScraper:
    def __init__(self, url, name=None, round=None, game_idx=None):
        self.url = url
        self.name = name
        self.round = int(round)        # round is the official round by the league, it may be moved due to scheduling e.g. 1,5,2,3,8,... 
        self.game_idx = int(game_idx)  # game idx is the order in which the games where player e.g. 1,2,3,4,5,...
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
    def __init__(self, url, year=None):
        self.year = year
        self.url = url
        tables, self.links = read_html_with_links(url)
        self.metadata = {i: tables[i].iloc[0,0] for i in range(0, len(tables)) }
        self.name = cleanup_team_name(self.metadata[0].split('-')[0].strip())
        self.games = parse_table(tables[0], 1)
        self.stats_regular_season = parse_table(tables[2], 2)
        self.stats_advanced = parse_table(tables[3], 1)
        self.stats_per_game = parse_table(tables[4], 2)
        released_idx = np.where(tables[5].iloc[:,0] == 'Released Players')[0][0]
        self.stats_players = parse_table(tables[5], 2)

    def read_games(self, max_games=None, sleep_time=0.1, multithreaded=True):
        all_games_links = self.links[consts.TEAM_LINKS_ALL_GAMES_IDX]
        self.all_games = {}
        if multithreaded:
            self.read_games_multithreaded(max_games, all_games_links)
        else:
            for i, (round, row) in tqdm(enumerate(self.stats_per_game.iterrows())):
                if max_games and i > max_games:
                    break
                game = GameScraper(consts.base_url + all_games_links[f"{row['game']}_{i}"], name=row['game'], round=round, game_idx=i+1)
                self.all_games[f"{game.name}_{game.round}"] = game
                sleep(sleep_time)
        self.per_game_player_stats = self.creat_per_game_player_stats()

    def read_games_multithreaded(self, max_games, all_games_links):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures= []
            for i, (round, row) in tqdm(enumerate(self.stats_per_game.iterrows())):
                if max_games and i > max_games:
                    break
                future = executor.submit(GameScraper, consts.base_url + all_games_links[f"{row['game']}_{i}"], name=row['game'], round=round, game_idx=i+1)
                futures.append(future)
            for future in tqdm(concurrent.futures.as_completed(futures)):
                game = future.result()
                self.all_games[f"{game.name}_{game.round}"] = game
    
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
            player_stats.loc[:, 'opponent'] = cleanup_team_name(team)
            player_stats.loc[:, 'loc'] = loc
            player_stats.loc[:, 'round'] = self.all_games[key].round
            player_stats.loc[:, 'game_idx'] = self.all_games[key].game_idx
            player_stats.loc[:, 'coach'] = team_coach
            player_stats_list.append(player_stats)
        return pd.concat(player_stats_list)
    
    @property
    def player_stats_path(self):
        return os.path.join(consts.team_save_path.format(year=self.year), f"{self.name}_per_game.csv")
    
    def save_to_csv(self, verbose=True):
        os.makedirs(os.path.dirname(self.player_stats_path), exist_ok=True)
        self.per_game_player_stats.to_csv(self.player_stats_path, index=False)
        if verbose:
            print(f"Data for {self.name} saved to csv {self.player_stats_path}.")

        
class SeasonTableScraper:
    teams_dict: dict[str, TeamScraper]
    
    def __init__(self, year):
        self.year = year
        self.url = consts.season_table.format(year=year)
        self.season_table, self._teams_urls_dict = read_html_with_links(self.url)
        self._teams_urls_dict = self._teams_urls_dict[0]  # the first table contains the teams urls
        self.season_table = parse_table(self.season_table[0], 3, ignore_prev_idx=True)
        self.season_table['team'] = self.season_table.index
        self.season_table.reset_index(drop=True, inplace=True)
        self.season_table['year'] = year
        self.teams_dict = {}
        self.team2pos = {}  
        for k, suffix in self._teams_urls_dict.items():
            name, position = k.split('_')
            name = cleanup_team_name(name)
            self.team2pos[name] = position
            self.teams_dict[name] = TeamScraper(consts.base_url + suffix, year=self.year)
    
    def read_teams_data(self, force_read=False):
        for team_name, team in self.teams_dict.items():
            print(f"Reading {self.year} data for {team_name}...")
            if not force_read and os.path.exists(team.player_stats_path):
                print(f"Data for {team_name} already exists. Skipping...")
                team.per_game_player_stats = pd.read_csv(team.player_stats_path)
                continue
            team.read_games()
            team.save_to_csv()
        
 
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

def df_for_print_groupby(filtered_df, by=['player name', 'coach'], sort_by=None):
    get_numeric_columns = lambda df: df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_columns = get_numeric_columns(filtered_df)
    mean_coach = filtered_df[numeric_columns+by].groupby(by).mean()
    std_coach = filtered_df[numeric_columns+by].groupby(by).std()
    sum_coach = filtered_df[numeric_columns+by].groupby(by).sum()
    mean_coach['games'] = filtered_df.groupby(by).size()
    mean_coach['%3'] = sum_coach['m_3pt'] / sum_coach['a_3pt'] * 100
    mean_coach['%2'] = sum_coach['m_2pt'] / sum_coach['a_2pt'] * 100
    mean_coach['2m'] = (sum_coach['m_2pt']/mean_coach['games']).round(1)
    mean_coach['3m'] = (sum_coach['m_3pt']/mean_coach['games']).round(1)
    cols_pre = ['games', 'min', 'pts', '%2', '2m', '%3', '3m', 'dr_rebounds', 'or_rebounds', 'tr_rebounds', 'st', 'to', 'as', 'val']
    cols_final = ['games', 'min', 'pts', '%2', '2m', '%3', '3m', 'def_r', 'off_r', 'tot_r', 'st', 'to', 'as', 'val']
    df_to_print = mean_coach[cols_pre]
    # rename columns. removing the suffix _rebounds
    df_to_print.columns = cols_final

    if sort_by:
        df_to_print = df_to_print.sort_values(sort_by, ascending=False)
    return df_to_print.round(1)

def filter_df(df):
    players_filter =  (df.groupby('player name')['min'].sum() > 50) & (df.groupby('player name')['min'].count() > 3)

    filtered_df = df[df['player name'].isin(players_filter[players_filter].index) & (df.game_idx > 2)]

    players_to_skip = ['Zach Hankins', 'Brynton Lemar', 'Gabriel Chachashvili']
    filtered_df = filtered_df[~filtered_df['player name'].isin(players_to_skip)]
    return filtered_df

def get_team_data(team_url=None, team_scraper=False):
    if team_url is None and not team_scraper:
        raise ValueError("Either team_url or team_scraper must be provided")
    if team_scraper:
        team = team_scraper
    else:
        team = TeamScraper(team_url)
    team_csv_path = "data/players/" + team.name + f'{len(team.stats_per_game)}rounds' + ".csv"
    if os.path.exists(team_csv_path):
        team.per_game_player_stats = pd.read_csv(team_csv_path)
        df = team.per_game_player_stats
    else:
        team.read_games()
        df = team.per_game_player_stats
        df.to_csv(team_csv_path)
    return df

if __name__ == "__main__":
    team_url = "https://basket.co.il/team.asp?TeamId=1054&lang=en"
    hapoel_scraper = TeamScraper(team_url, year=2024)
    df = hapoel_scraper.read_games(max_games=5, multithreaded=False)    
    
    season = SeasonTableScraper(2025)
    season.read_teams_data()
    a = 3

    # print(df_for_print_groupby(filtered_df, sort_by=['player name', 'games']))
    # print("#"*15)
    # print("Upper house")
    # print("#"*15)
    # print(df_for_print_groupby(filtered_df[filtered_df["game_idx"] > 24], sort_by='min')) # we played 24 games but there are 26 rounds... odd number of teams...)

    # print("#"*15)
    # print("2nd round jonathan alon")
    # print("#"*15)
    # # min_game_idx_number_when_jonathan_alon_is_coach
    # min_game_idx = filtered_df[filtered_df['coach'] == 'Jonathan Alon']['game_idx'].min()
    # print(df_for_print_groupby(filtered_df[(filtered_df["game_idx"] <= 24) & (filtered_df["game_idx"] >= min_game_idx)], sort_by='min')) # we played 24 games but there are 26 rounds... odd number of teams...)

    a = 3 



    # print(std_coach[['min', 'pts', 'dr_rebounds', 'or_rebounds', 'tr_rebounds', 'st', 'to', 'as', 'val']].round(1))
    # plot_property(filtered_df[filtered_df['player name'] != "Total"], 'player name', 'pts', hue='coach')
    # plot_property(filtered_df[filtered_df['player name'] != "Total"], 'player name', 'pts')
    # plot_property(filtered_df[filtered_df['player name'] != "Total"], 'player name', 'val')



    # create a plot of the players pts per game with color as loc
    # sns.set(rc={'figure.figsize':(15,10)})
    # yz = df[df['player name'] == "Yovel Zoosman"]
    # speedy = df[df['player name'] == "Speedy Smith"]
    # blayzer = df[df['player name'] == "Oz Blayzer"]
    # filtered_df = df[df['player name'].isin(["Yovel Zoosman", "Speedy Smith", "Oz Blayzer"])]
    # sns.scatterplot(data=filtered_df, y='pts', x='game_idx', hue='player name')
    # sns.lineplot(data=filtered_df, y='pts', x='game_idx', hue='player name')
    # plt.show()