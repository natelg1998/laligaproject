from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pprint import pprint

years = ["2021-2022", "2020-2021", "2019-2020", "2019-2020", "2018-2019",
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
    squad_links_year = [f"{l.rsplit('/',1)[0]}/{year}" for l in squad_links for year in years]
    team_urls = [f"https://fbref.com{l}" for l in squad_links_year]
    # team_ids = [l.rsplit("/", 1)[0] for l in team_urls]
    # team_stats = [t.rsplit("/", 1)[-1] for t in team_urls]
    # score_stats = []
    # for team in team_ids:
    #     for year in years:
    #         score_stats.append(f"{team}/{year}")
    return team_urls


shoot_l = []
def get_shooting_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    shoot_links = soup.find_all('a')
    shoot_links = [l.get("href") for l in shoot_links]
    shoot_links = [l for l in shoot_links if l and "all_comps/shooting" in l]
    shoot_urls = [f"https://fbref.com{l}" for l in shoot_links]
    shoot_urls = [*set(shoot_urls)]
    for url in shoot_urls:
        data = requests.get(url)
        shooting = pd.read_html(data.text, match="Shooting")[0]
        shooting.columns = shooting.columns.droplevel()
        team_name = url.split("/")[-1].replace("-Match-Logs-All-Competitions", "").replace("-", " ")
        shooting["Team"] = team_name
        shooting = shooting.iloc[:-1]
        shoot_l.append(shooting)
    print(".", end="", flush=True)
    return shoot_urls


if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor() as executor:
        executor.map(get_shooting_data, score_links)
    print(len(shoot_l))
    shooting_df = pd.concat(shoot_l, ignore_index = True)
    shooting_df.to_csv(os.path.join(os.path.realpath("./data/fbref_data/"), r"shooting.csv"))
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")