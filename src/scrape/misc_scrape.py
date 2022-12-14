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

url = "https://fbref.com/en/comps/12/La-Liga-Stats"
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


miscellaneous_act = []


def get_miscellaneous_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    misc_links = soup.find_all("a")
    misc_links = [l.get("href") for l in misc_links]
    misc_links = [l for l in misc_links if l and "en/squads" in l]
    misc_links = [l for l in misc_links if l and "matchlogs/all_comps/misc" in l]
    misc_urls = [f"https://fbref.com{l}" for l in misc_links]
    misc_urls = [*set(misc_urls)]
    misc_urls_clean = [l for l in misc_urls if l != []]
    for url in misc_urls:
        data = requests.get(url)
        team_name = (
            url.split("/")[-1]
            .replace("-Match-Logs-All-Competitions", "")
            .replace("-", " ")
        )
        miscellaneous = pd.read_html(data.text, match="Miscellaneous Stats")[0]
        # Pandas concat function does not like duplicate column names
        # To resolve this issue, we had to transform the multi-index level columns that were not for the team to append
        # the first level multindex title, i.e Performance, to the column name, then drop an index level, and update those column names we need
        cols = miscellaneous.columns
        cols_to_update = ["_".join(w) for w in cols[10:]]
        miscellaneous.columns = miscellaneous.columns.droplevel()
        miscellaneous.columns.values[10:] = cols_to_update
        miscellaneous["Team"] = team_name
        # Remove last row, not needed. Shows team record which we do not need
        miscellaneous = miscellaneous.iloc[:-1]
        miscellaneous_act.append(miscellaneous)
    print(".", end="", flush=True)
    return


if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor() as executor:
        executor.map(get_miscellaneous_data, score_links)
    print(len(miscellaneous_act))
    misc_df = pd.concat(miscellaneous_act, ignore_index=True)
    misc_df.drop(
        ["Unnamed: 26_level_0_Match Report", "Unnamed: 22_level_0_Match Report"],
        axis=1,
        inplace=True,
    )
    misc_df.columns = [
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
        "performance_crdy",
        "performance_crdr",
        "performance_2crdy",
        "performance_fls",
        "performance_fld",
        "performance_off",
        "performance_crs",
        "performance_int",
        "performance_tklw",
        "performance_pkwon",
        "performance_pkcon",
        "performance_og",
        "performance_recov",
        "aerial_duels_won",
        "aerial_duels_lost",
        "aerial_duels_won_perc",
        "team",
    ]
    misc_df = misc_df[misc_df["_time_"].notna()]
    misc_df.to_csv(
        os.path.join(os.path.realpath("./data/fbref_data/"), r"miscellaneous.csv")
    )
    misc_df.to_sql(
        "miscellaneous_stats",
        con=conn,
        schema="laliga",
        if_exists="replace",
        index=False,
    )
    conn.execute(
        """ALTER TABLE laliga.miscellaneous_stats
                                            ADD PRIMARY KEY (_date_, _time_, _day_, team);"""
    )
    conn.close()
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
