from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pprint import pprint

years = ["2022-2023", "2021-2022", "2020-2021", "2019-2020", "2019-2020", "2018-2019",
         "2017-2018", "2016-2017", "2015-2016", "2014-2015", "2013-2014", "2012-2013",
         "2011-2012", "2010-2011", "2009-2010", "2008-2009", "2007-2008", "2006-2007",
         "2005-2006", "2004-2005", "2003-2004", "2002-2003", "2001-2002", "2000-2001"]


url = "https://fbref.com/en/comps/12/La-Liga-Stats"

def get_score_links():
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    league_table = soup.select("table.stats_table")[0]
    squad_links = league_table.find_all('a')
    squad_links = [l.get("href") for l in squad_links]
    squad_links = [l for l in squad_links if "/en/squads" in l]
    team_urls = [f"https://fbref.com{l}" for l in squad_links]
    team_ids = [l.rsplit("/", 1)[0] for l in team_urls]
    score_stats = [f"{l}/{year}/" for l in team_ids for year in years]
    return score_stats

defense_act = []
def get_defense_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    def_links = soup.find_all('a')
    def_links = [l.get("href") for l in def_links]
    def_links = [l for l in def_links if l and "en/squads" in l]
    def_links = [l for l in def_links if l and "matchlogs/all_comps/defense" in l]
    def_urls = [f"https://fbref.com{l}" for l in def_links]
    def_urls = [*set(def_urls)]
    def_urls_clean = [l for l in def_urls if l != []]
    for url in def_urls:
        data = requests.get(url)
        team_name = url.split("/")[-1].replace("-Match-Logs-All-Competitions", "").replace("-", " ")
        defense = pd.read_html(data.text, match="Defensive Actions")[0]
        # Pandas concat function does not like duplicate column names
        # To resolve this issue, we had to transform the multi-index level columns that were not for the team to append
        # the first level multindex title, i.e Performance, to the column name, then drop an index level, and update those column names we need
        cols = defense.columns
        cols_to_update = ['_'.join(w) for w in cols[10:]]
        defense.columns = defense.columns.droplevel()
        defense.columns.values[10:] = cols_to_update
        defense["Team"] = team_name
        # Remove last row, not needed. Shows team record which we do not need
        defense = defense.iloc[:-1]
        defense_act.append(defense)
    print(".", end="", flush=True)
    return

if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor() as executor:
        executor.map(get_defense_data, score_links)
    print(len(defense_act))
    shooting_df = pd.concat(defense_act, ignore_index=True)
    shooting_df.to_csv(os.path.join(os.path.realpath("./data/fbref_data/"), r"defense.csv"))
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")