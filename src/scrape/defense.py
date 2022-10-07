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


defense_act = []


def get_defense_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    def_links = soup.find_all("a")
    def_links = [l.get("href") for l in def_links]
    def_links = [l for l in def_links if l and "en/squads" in l]
    def_links = [l for l in def_links if l and "matchlogs/all_comps/defense" in l]
    def_urls = [f"https://fbref.com{l}" for l in def_links]
    def_urls = [*set(def_urls)]
    def_urls_clean = [l for l in def_urls if l != []]
    for url in def_urls:
        data = requests.get(url)
        team_name = (
            url.split("/")[-1]
            .replace("-Match-Logs-All-Competitions", "")
            .replace("-", " ")
        )
        defense = pd.read_html(data.text, match="Defensive Actions")[0]
        # Pandas concat function does not like duplicate column names
        # To resolve this issue, we had to transform the multi-index level columns that were not for the team to append
        # the first level multindex title, i.e Performance, to the column name, then drop an index level, and update those column names we need
        cols = defense.columns
        cols_to_update = ["_".join(w) for w in cols[10:]]
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
    defense_df = pd.concat(defense_act, ignore_index=True)
    defense_df.drop("Unnamed: 33_level_0_Match Report", axis=1, inplace=True)
    defense_df.columns = [
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
        "tackles_tkl",
        "tackles_tklw",
        "tackles_def_third",
        "tackles_mid_third",
        "tackles_att_third",
        "vs_dribbles_tkl",
        "vs_dribbles_att",
        "vs_dribbles_tkl_perc",
        "vs_dribbles_past",
        "pressures_press",
        "pressures_succ",
        "pressures_perc",
        "pressures_def_third",
        "pressures_mid_third",
        "pressures_att_third",
        "blocks_blocks",
        "blocks_sh",
        "blocks_shsv",
        "blocks_pass",
        "int",
        "tkl_plus_int",
        "clr",
        "err",
        "team",
    ]
    defense_df = defense_df[defense_df["_time_"].notna()]
    defense_df.to_csv(
        os.path.join(os.path.realpath("./data/fbref_data/"), r"defense.csv")
    )
    defense_df.to_sql(
        "defense", con=conn, schema="laliga", if_exists="replace", index=False
    )
    conn.execute(
        """ALTER TABLE laliga.defense
                                    ADD PRIMARY KEY (_date_, _time_, _day_, team);"""
    )
    conn.close()
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
