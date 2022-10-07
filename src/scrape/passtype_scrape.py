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


# pprint(get_score_links())

pass_l = []


def get_passing_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    pass_links = soup.find_all("a")
    pass_links = [l.get("href") for l in pass_links]
    pass_links = [l for l in pass_links if l and "en/squads" in l]
    pass_links = [
        l for l in pass_links if l and "matchlogs/all_comps/passing_types" in l
    ]
    pass_urls = [f"https://fbref.com{l}" for l in pass_links]
    pass_urls = [*set(pass_urls)]
    for url in pass_urls:
        data = requests.get(url)
        team_name = (
            url.split("/")[-1]
            .replace("-Match-Logs-All-Competitions", "")
            .replace("-", " ")
        )
        passing = pd.read_html(data.text, match="Pass Types")[0]
        # Pandas concat function does not like duplicate column names
        # To resolve this issue, we had to transform the multi-index level columns that were not for the team to append
        # the first level multindex title, i.e Performance, to the column name, then drop an index level, and update those column names we need
        cols = passing.columns
        cols_to_update = ["_".join(w) for w in cols[10:]]
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
    with ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(get_passing_data, score_links)
    print(len(pass_l))
    passing_df = pd.concat(pass_l, ignore_index=True)
    passing_df.drop("Unnamed: 35_level_0_Match Report", axis=1, inplace=True)
    passing_df.columns = [
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
        "att",
        "passtypes_live",
        "passtypes_dead",
        "passtypes_fk",
        "passtypes_tb",
        "passtypes_press",
        "passtypes_sw",
        "passtypes_crs",
        "passtypes_ck",
        "cornerkicks_in",
        "cornerkicks_out",
        "cornerkicks_str",
        "height_ground",
        "height_low",
        "height_high",
        "bodyparts_left",
        "bodyparts_right",
        "bodyparts_head",
        "bodyparts_ti",
        "bodyparts_other",
        "outcomes_cmp",
        "outcomes_off",
        "outcomes_out",
        "outcomes_int",
        "outcomes_blocks",
        "team",
    ]
    passing_df = passing_df[passing_df["_time_"].notna()]
    passing_df.to_csv(
        os.path.join(os.path.realpath("./data/fbref_data/"), r"passing_types.csv")
    )
    passing_df.to_sql(
        "passing_types", con=conn, schema="laliga", if_exists="replace", index=False
    )
    conn.execute(
        """ALTER TABLE laliga.passing_types
                                    ADD PRIMARY KEY (_date_, _time_, _day_, team);"""
    )
    conn.close()
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
