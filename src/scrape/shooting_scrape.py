from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pprint import pprint

# years = ["2022-2023", "2021-2022", "2020-2021", "2019-2020", "2019-2020", "2018-2019",
#          "2017-2018", "2016-2017", "2015-2016", "2014-2015", "2013-2014", "2012-2013",
#          "2011-2012", "2010-2011", "2009-2010", "2008-2009", "2007-2008", "2006-2007",
#          "2005-2006", "2004-2005", "2003-2004", "2002-2003", "2001-2002", "2000-2001"]

years = ["2022-2023", "2021-2022", "2020-2021"]
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

pprint(len(get_score_links()))

start = time.time()
for team_url in get_score_links():
    data = requests.get(team_url)
    soup = BeautifulSoup(data.text, "lxml")
    shooting_links = soup.find_all("a")
    shooting_links = [l.get("href") for l in shooting_links]
    shooting_links = [l for l in shooting_links if l and "all_comps/shooting/" in l]
    print(".", end="", flush=True)
end = time.time()
print(f"Time taken to run: {end - start} seconds")
pprint(shooting_links)