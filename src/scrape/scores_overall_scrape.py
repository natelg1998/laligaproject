from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

years = ["2022-2023", "2021-2022", "2020-2021", "2019-2020", "2019-2020", "2018-2019",
         "2017-2018", "2016-2017", "2015-2016", "2014-2015", "2013-2014", "2012-2013",
         "2011-2012", "2010-2011", "2009-2010", "2008-2009", "2007-2008", "2006-2007",
         "2005-2006", "2004-2005", "2003-2004", "2002-2003", "2001-2002", "2000-2001"]

url = "https://fbref.com/en/comps/12/La-Liga-Stats"


def get_score_links():
    global url
    seasons = []
    for year in years:
        data = requests.get(url)
        soup = BeautifulSoup(data.text, "lxml")
        # https://stackoverflow.com/questions/42038130/beautifulsoup-nested-class-selector
        scores_sel = soup.find(class_ = "inactive").find(class_ = "hoversmooth").find_all('a')
        # scores_sel = scores_sel.find_all('a')
        scores_links = [l.get("href") for l in scores_sel]
        scores_links = [l for l in scores_links if "schedule" in l]
        scores_links = [*set(scores_links)]
        # league_table = soup.select("table.stats_table")[0]
        # squad_links = league_table.find_all('a')
        # squad_links = [l.get("href") for l in squad_links]
        # squad_links = [l for l in squad_links if "/en/squads" in l]
        # # squad_links_year = [f"{l.rsplit('/',1)[0]}/{year}/{l.rsplit('/',1)[-1]}" for l in squad_links for year in years]
        team_urls = [f"https://fbref.com{l}" for l in scores_links]
        previous_season = soup.select("a.prev")[0].get("href")
        url = f"https://fbref.com{previous_season}"
        seasons.extend(team_urls)
    return seasons
# pprint(get_score_links())

scores = []
def get_scores_fixtures(url):
    data = requests.get(url)
    matches = pd.read_html(data.text, match="Scores & Fixtures")[0]
    # team_name = url.split("/")[-1].replace("-Stats", "").replace("-", " ")
    # matches["Team"] = team_name
    scores.append(matches)
    print(".", end="", flush=True)
    return

if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor() as executor:
        executor.map(get_scores_fixtures, score_links)
    print(len(scores))
    scores_df = pd.concat(scores, ignore_index = True)
    scores_df.to_csv(os.path.join(os.path.realpath("./data/fbref_data/"), r"scores_overall.csv"))
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")