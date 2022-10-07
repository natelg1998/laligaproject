from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pprint import pprint
from database.database_config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST
import psycopg2
from sqlalchemy import create_engine

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
    "2014-2015",
    "2013-2014",
    "2012-2013",
    "2011-2012",
    "2010-2011",
    "2009-2010",
    "2008-2009",
    "2007-2008",
    "2006-2007",
    "2005-2006",
    "2004-2005",
    "2003-2004",
    "2002-2003",
    "2001-2002",
    "2000-2001",
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
    return teams


shoot_l = []


def get_shooting_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    shoot_links = soup.find_all("a")
    shoot_links = [l.get("href") for l in shoot_links]
    shoot_links = [l for l in shoot_links if l and "all_comps/shooting" in l]
    shoot_urls = [f"https://fbref.com{l}" for l in shoot_links]
    shoot_urls = [*set(shoot_urls)]
    for url in shoot_urls:
        data = requests.get(url)
        shooting = pd.read_html(data.text, match="Shooting")[0]
        shooting.columns = shooting.columns.droplevel()
        team_name = (
            url.split("/")[-1]
            .replace("-Match-Logs-All-Competitions", "")
            .replace("-", " ")
        )
        shooting["Team"] = team_name
        shooting = shooting.iloc[:-1]
        shoot_l.append(shooting)
    print(".", end="", flush=True)
    return shoot_urls


if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(get_shooting_data, score_links)
    print(len(shoot_l))
    shooting_df = pd.concat(shoot_l, ignore_index=True)
    shooting_df.drop(["Match Report"], axis=1, inplace=True)  # Do not need these
    shooting_df.columns = [
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
        "goals",
        "shots",
        "shots_on_target",
        "shots_on_target_percent",
        "goals_per_shot",
        "goals_per_shot_on_target",
        "distance",
        "free_kicks",
        "penalty_kicks",
        "penalty_kicks_attempt",
        "xg",
        "nonpenalty_xg",
        "nonpenalty_xg_per_shot",
        "goals_minus_xg",
        "nonpenalty_goals_minus_xg",
        "team",
    ]
    shooting_df = shooting_df[shooting_df["_time_"].notna()]
    shooting_df.to_csv(
        os.path.join(os.path.realpath("./data/fbref_data/"), r"shooting.csv")
    )
    shooting_df.to_sql(
        "shooting", con=conn, schema="laliga", if_exists="replace", index=False
    )
    conn.execute(
        """ALTER TABLE laliga.shooting 
                        ADD PRIMARY KEY (_date_, _time_, _day_, team);"""
    )
    conn.close()

    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
