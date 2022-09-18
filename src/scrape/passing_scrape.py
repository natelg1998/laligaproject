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

# pprint(get_score_links())

pass_l = []
def get_passing_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    pass_links = soup.find_all('a')
    pass_links = [l.get("href") for l in pass_links]
    pass_links = [l for l in pass_links if l and "en/squads" in l]
    pass_links = [l for l in pass_links if l and "matchlogs/all_comps/passing" in l]
    pass_urls = [f"https://fbref.com{l}" for l in pass_links]
    pass_urls = [*set(pass_urls)]
    for url in pass_urls:
        data = requests.get(url)
        team_name = url.split("/")[-1].replace("-Match-Logs-All-Competitions", "").replace("-", " ")
        passing = pd.read_html(data.text, match="Passing")[0]
        # Pandas concat function does not like duplicate column names
        # To resolve this issue, we had to transform the multi-index level columns that were not for the team to append
        # the first level multindex title, i.e Performance, to the column name, then drop an index level, and update those column names we need
        cols = passing.columns
        cols_to_update = ['_'.join(w) for w in cols[10:]]
        passing.columns = passing.columns.droplevel()
        passing.columns.values[10:] = cols_to_update
        passing["Team"] = team_name
        # Remove last row, not needed. Shows team record which we do not need
        passing = passing.iloc[:-1]
        pass_l.append(passing)
    print(".", end="", flush=True)
    return



if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor(max_workers = 7) as executor:
        executor.map(get_passing_data, score_links)
    print(len(pass_l))
    shooting_df = pd.concat(pass_l, ignore_index=True)
    shooting_df.to_csv(os.path.join(os.path.realpath("./data/"), r"passing.csv"))
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")