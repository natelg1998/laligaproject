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


gk_l = []


def get_gk_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    gk_links = soup.find_all("a")
    gk_links = [l.get("href") for l in gk_links]
    gk_links = [l for l in gk_links if l and "en/squads" in l]
    gk_links = [l for l in gk_links if l and "matchlogs/all_comps/keeper" in l]
    gk_urls = [f"https://fbref.com{l}" for l in gk_links]
    gk_urls = [*set(gk_urls)]
    gk_urls_clean = [l for l in gk_urls if l != []]
    for url in gk_urls:
        data = requests.get(url)
        team_name = (
            url.split("/")[-1]
            .replace("-Match-Logs-All-Competitions", "")
            .replace("-", " ")
        )
        goalkeep = pd.read_html(data.text, match="Goalkeeping")[0]
        # Pandas concat function does not like duplicate column names
        # To resolve this issue, we had to transform the multi-index level columns that were not for the team to append
        # the first level multindex title, i.e Performance, to the column name, then drop an index level, and update those column names we need
        cols = goalkeep.columns
        cols_to_update = ["_".join(w) for w in cols[10:]]
        goalkeep.columns = goalkeep.columns.droplevel()
        goalkeep.columns.values[10:] = cols_to_update
        goalkeep["Team"] = team_name
        # Remove last row, not needed. Shows team record which we do not need
        goalkeep = goalkeep.iloc[:-1]
        gk_l.append(goalkeep)
    print(".", end="", flush=True)
    return


if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(get_gk_data, score_links)
    print(len(gk_l))
    gk_df = pd.concat(gk_l, ignore_index=True)
    gk_df.drop(
        ["Unnamed: 36_level_0_Match Report", "Unnamed: 19_level_0_Match Report"],
        axis=1,
        inplace=True,
    )  # Do not need these
    gk_df.columns = [
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
        "performance_shots_on_target",
        "performance_ga",
        "performance_saves",
        "performance_saves_perc",
        "performance_cs",
        "performance_ps_xg",
        "performance_ps_xg_plus_minus",
        "penalty_kicks_pkatt",
        "penalty_kicks_pka",
        "penalty_kicks_pksv",
        "penalty_kicks_pkm",
        "launched_cmp",
        "launched_att",
        "launched_cmp_perc",
        "passes_att",
        "passes_thr",
        "passes_launch_perc",
        "passes_avglen",
        "goalkicks_att",
        "goalkicks_launch_perc",
        "goalkicks_avglen",
        "crosses_opp",
        "crosses_stp",
        "crosses_stp_perc",
        "sweeper_num_opa",
        "sweeper_avgdist",
        "team",
    ]
    gk_df = gk_df[gk_df["_time_"].notna()]
    gk_df.to_csv(
        os.path.join(os.path.realpath("./data/fbref_data/"), r"goalkeeping.csv")
    )
    gk_df.to_sql(
        "goalkeeping", con=conn, schema="laliga", if_exists="replace", index=False
    )
    conn.execute(
        """ALTER TABLE laliga.goalkeeping
                            ADD PRIMARY KEY (_date_, _time_, _day_, team);"""
    )
    conn.close()
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
