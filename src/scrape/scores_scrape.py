from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from database.database_config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST
import psycopg2
from sqlalchemy import create_engine, types

years = [
    "2022-2023",
    "2021-2022",
    "2020-2021",
    "2019-2020",
    "2019-2020",
    "2018-2019",
    "2017-2018",
    "2016-2017",
    "2015-2016",
    "2014-2015"
]

# years = list(range(2022, , -1))
url = "https://fbref.com/en/comps/12/La-Liga-Stats"
# conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
db = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
conn = db.connect()


def get_score_links():
    global url
    teams = []
    for year in years:
        data = requests.get(url)
        soup = BeautifulSoup(data.text, "lxml")
        league_table = soup.select("table.stats_table")[0]
        squad_links = league_table.find_all("a")
        squad_links = [l.get("href") for l in squad_links]
        squad_links = [l for l in squad_links if "/en/squads" in l]
        # squad_links_year = [f"{l.rsplit('/',1)[0]}/{year}/{l.rsplit('/',1)[-1]}" for l in squad_links for year in years]
        team_urls = [f"https://fbref.com{l}" for l in squad_links]
        previous_season = soup.select("a.prev")[0].get("href")
        url = f"https://fbref.com{previous_season}"
        teams.extend(team_urls)
        time.sleep(1)
    return teams


pprint(get_score_links())
scores = []


def get_scores_fixtures(url):
    data = requests.get(url)
    matches = pd.read_html(data.text, match="Scores & Fixtures")[0]
    team_name = url.split("/")[-1].replace("-Stats", "").replace("-", " ")
    matches["Team"] = team_name
    scores.append(matches)
    time.sleep(1)
    print(".", end="", flush=True)
    return


if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(get_scores_fixtures, score_links)
    print(len(scores))
    scores_df = pd.concat(scores, ignore_index=True)
    scores_df.drop(["Match Report", "Notes"], axis=1, inplace=True)  # Do not need these
    scores_df.columns = [
        "_date_",
        "_time_",
        "comp",
        "round",
        "_day_",
        "venue",
        "_result_",
        "gf",
        "ga",
        "opponent",
        "poss",
        "attendance",
        "captain",
        "formation",
        "referee",
        "team",
    ]
    scores_df = scores_df[scores_df["_time_"].notna()]
    scores_df = scores_df[scores_df["comp"] == "La Liga"]
    scores_df.to_csv(
        os.path.join(os.path.realpath("./data/fbref_data/"), r"scores.csv")
    )

    sql_data_types = {'_date_': types.DATE,
                      '_time_': types.TIME,
                      'comp': types.VARCHAR(50),
                      'round': types.VARCHAR(50),
                      '_day_': types.VARCHAR(30),
                      'venue': types.VARCHAR(100),
                      '_result_': types.VARCHAR(20),
                      'gf': types.INT,
                      'ga': types.INT,
                      'opponent': types.VARCHAR(60),
                      'xg': types.FLOAT,
                      'xga': types.FLOAT,
                      'poss': types.INT,
                      'attendance': types.INT,
                      'captain' : types.VARCHAR(80),
                      'formation' : types.VARCHAR(50),
                      'referee' : types.VARCHAR(90),
                      'team': types.VARCHAR(60)}

    scores_df.to_sql(
        "scores_by_team", con=conn, schema="laliga", if_exists="replace", index=False, dtype= sql_data_types
    )
    conn.execute(
        """ALTER TABLE laliga.scores_by_team
                    ADD PRIMARY KEY (_date_, _time_, _day_, team);"""
    )
    conn.close()
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
# # Time taken to run: 2173.9899361133575 seconds - 25 workers
# # Time taken to run: 1972.1386992931366 seconds - 6 workers
# # Time taken to run: 5107.039206266403 seconds - 5 workers
# # Time taken to run: 1563.7288415431976 seconds - 7 workers
# # Time taken to run: 3590.750078201294 seconds - 8 workers
# # Time taken to run: 1881.5751566886902 seconds - Default 12 workers