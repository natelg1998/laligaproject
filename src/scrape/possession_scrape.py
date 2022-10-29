from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pprint import pprint
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


poss_l = []


def get_defense_data(url):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, "lxml")
    poss_links = soup.find_all("a")
    poss_links = [l.get("href") for l in poss_links]
    poss_links = [l for l in poss_links if l and "en/squads" in l]
    poss_links = [l for l in poss_links if l and "matchlogs/all_comps/possession" in l]
    poss_urls = [f"https://fbref.com{l}" for l in poss_links]
    poss_urls = [*set(poss_urls)]
    poss_urls_clean = [l for l in poss_urls if l != []]
    for url in poss_urls:
        data = requests.get(url)
        team_name = (
            url.split("/")[-1]
            .replace("-Match-Logs-All-Competitions", "")
            .replace("-", " ")
        )
        possession = pd.read_html(data.text, match="Possession")[0]
        # Pandas concat function does not like duplicate column names
        # To resolve this issue, we had to transform the multi-index level columns that were not for the team to append
        # the first level multindex title, i.e Performance, to the column name, then drop an index level, and update those column names we need
        cols = possession.columns
        cols_to_update = ["_".join(w) for w in cols[11:]]
        possession.columns = possession.columns.droplevel()
        possession.columns.values[11:] = cols_to_update
        possession["Team"] = team_name
        # Remove last row, not needed. Shows team record which we do not need
        possession = possession.iloc[:-1]
        poss_l.append(possession)
    print(".", end="", flush=True)
    return


if __name__ == "__main__":
    start = time.time()
    score_links = get_score_links()
    with ThreadPoolExecutor(max_workers=7) as executor:
        executor.map(get_defense_data, score_links)
    print(len(poss_l))
    poss_df = pd.concat(poss_l, ignore_index=True)
    poss_df.drop(
        ["Unnamed: 25_level_0_Match Report"], axis=1, inplace=True
    )  # Do not need these
    poss_df.columns = [
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
        "touches",
        "touches_def_pen",
        "touches_def_third",
        "touches_mid_third",
        "touches_att_third",
        "touches_att_pen",
        "touches_live",
        "dribbles_succ",
        "dribbles_att",
        "dribbles_succ_percent",
        "dribbles_mis",
        "dribbles_dis",
        "receiving_rec",
        "receiving_prog",
        "team",
    ]
    poss_df = poss_df[poss_df["_time_"].notna()]
    poss_df = poss_df[poss_df["comp"] == "La Liga"]
    poss_df.to_csv(
        os.path.join(os.path.realpath("./data/fbref_data/"), r"possession.csv")
    )
    sql_data_types = {'_date_' : types.DATE,
                      '_time_' : types.TIME,
                      'comp' : types.VARCHAR(50),
                      'round' : types.VARCHAR(50),
                      '_day_' : types.VARCHAR(30),
                      'venue' : types.VARCHAR(100),
                      '_result_' : types.VARCHAR(20),
                      'gf' : types.INT,
                      'ga' : types.INT,
                      'opponent' : types.VARCHAR(60),
                      'poss' : types.INT,
                      'touches': types.INT,
                      'touches_def_pen' : types.INT,
                      'touches_def_third' : types.INT,
                      'touches_mid_third' : types.INT,
                      'touches_att_third' : types.INT,
                      'touches_att_pen' : types.INT,
                      'touches_live' : types.INT,
                      'dribbles_succ' : types.INT,
                      'dribbles_att' : types.INT,
                      'dribbles_succ_percent' : types.FLOAT,
                      'dribbles_mis' : types.INT,
                      'dribbles_dis' : types.INT,
                      'receiving_rec' : types.INT,
                      'receiving_prog' : types.INT,
                      'team' : types.VARCHAR(60)}
    poss_df.to_sql(
        "possession", con=conn, schema="laliga", if_exists="replace", index=False, dtype=sql_data_types
    )
    conn.execute(
        """ALTER TABLE laliga.possession
                        ADD PRIMARY KEY (_date_, _time_, _day_, team);"""
    )
    conn.close()
    end = time.time()
    print(f"Time taken to run: {end - start} seconds")
