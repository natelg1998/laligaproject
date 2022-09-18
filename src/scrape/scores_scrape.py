from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


# years = ["2022-2023", "2021-2022", "2020-2021"]
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
    team_stats = [t.rsplit("/", 1)[-1] for t in team_urls]
    score_stats = [f"{l}/{year}/{d}" for l in team_ids for year in years for d in team_stats]
    return score_stats

scores = []
def get_scores_fixtures(url):
    data = requests.get(url)
    matches = pd.read_html(data.text, match="Scores & Fixtures")[0]
    team_name = url.split("/")[-1].replace("-Stats", "").replace("-", " ")
    matches["Team"] = team_name
    scores.append(matches)
    print(".", end="", flush=True)
    return


if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor(max_workers = 7) as executor:
        executor.map(get_scores_fixtures, score_links)
    print(len(scores))
    scores_df = pd.concat(scores)
    scores_df.to_csv(os.path.join(os.path.realpath("./data/"), r"scores.csv"))
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
# Time taken to run: 2173.9899361133575 seconds - 25 workers
# Time taken to run: 1972.1386992931366 seconds - 6 workers
# Time taken to run: 5107.039206266403 seconds - 5 workers
# Time taken to run: 1563.7288415431976 seconds - 7 workers
# Time taken to run: 3590.750078201294 seconds - 8 workers
# Time taken to run: 1881.5751566886902 seconds - Default 12 workers


