from database_config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST
import psycopg2

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

cur = conn.cursor()

SQL = """
DROP TABLE IF EXISTS laliga.scores_by_team;
CREATE TABLE laliga.scores_by_team(
	date_ DATE,
	time_ TIME,
	comp TEXT,
	round_ TEXT,
	day_ TEXT,
	venue TEXT,
	result_ CHAR(1),
	gf INTEGER,
	ga INTEGER,
	opponent TEXT,
	xg DECIMAL,
	xga DECIMAL,
	poss INTEGER,
	captain TEXT,
	formation TEXT,
	referee TEXT,
	team TEXT
);
DROP TABLE IF EXISTS laliga.shooting;
CREATE TABLE laliga.shooting(
	date_ DATE,
	time_ TIME,
	comp TEXT,
	round_ TEXT,
	day_ TEXT,
	venue TEXT,
	result_ CHAR(1),
	gf INTEGER,
	ga INTEGER,
	opponent TEXT,
	goals INTEGER,
	shots INTEGER,
	shots_on_target INTEGER,
	shots_on_target_percent DECIMAL,
	goals_per_shot DECIMAL,
	goals_per_shots_on_target DECIMAL,
	avg_distanace DECIMAL,
	free_kicks INTEGER,
	penalty_kicks INTEGER,
	penalty_kick_attempts INTEGER,
	xg DECIMAL,
	non_penalty_xg DECIMAL,
	non_penalty_xg_per_shot DECIMAL,
	goals_minus_xg DECIMAL,
	non_penalty_goals_minus_xg DECIMAL,
	team TEXT
);
DROP TABLE IF EXISTS laliga.scores_overall;
CREATE TABLE laliga.scores_overall(
	week INTEGER,
	day_ TEXT,
	date_ DATE,
	time_ TIME,
	home TEXT,
	score TEXT,
	away TEXT,
	attendance INTEGER,
	venue TEXT,
	referee TEXT,
	xg_home DECIMAL,
	xg_away DECIMAL
);
DROP TABLE IF EXISTS laliga.possession;
CREATE TABLE laliga.possession(
	date_ DATE,
	time_ TIME,
	comp TEXT,
	round_ TEXT,
	day_ TEXT,
	venue TEXT,
	result_ CHAR(1),
	gf INTEGER,
	ga INTEGER,
	opponent TEXT,
	touches INTEGER,
	touches_def_pen INTEGER,
	touches_def_third INTEGER,
	touches_mid_third INTEGER,
	touches_attacking_third INTEGER,
	touches_attacking_penalty INTEGER,
	touches_live INTEGER,
	dribbles_succuss INTEGER,
	dribbles_success_percent DECIMAL,
	dribbles_num_pi INTEGER,
	dribbles_megs INTEGER,
	carries INTEGER,
	carries_tot_distance INTEGER,
	carries_prog INTEGER,
	carries_one_third INTEGER,
	carries_prog_CPA INTEGER,
	carries_mis INTEGER,
	carries_dis INTEGER,
	receiving_targ INTEGER,
	receiving_rec INTEGER,
	receiving_rec_percent DECIMAL,
	receiving_prog INTEGER,
	team TEXT
);
"""

cur.execute(SQL)
conn.commit()
conn.close()

# import os
# print(os.path.realpath('..'))