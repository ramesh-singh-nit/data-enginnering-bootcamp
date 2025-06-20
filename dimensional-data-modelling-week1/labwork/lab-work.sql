 CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );
 CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');


 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );



 WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 2021

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 2022
)
INSERT INTO players
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.height, ts.height) as height,
        COALESCE(ls.college, ts.college) as college,
        COALESCE(ls.country, ts.country) as country,
        COALESCE(ls.draft_year, ts.draft_year) as draft_year,
        COALESCE(ls.draft_round, ts.draft_round) as draft_round,
        COALESCE(ls.draft_number, ts.draft_number)
            as draft_number,
        COALESCE(ls.seasons,
            ARRAY[]::season_stats[]
            ) || CASE WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                ts.season,
                ts.pts,
                ts.ast,
                ts.reb, ts.weight)::season_stats]
                ELSE ARRAY[]::season_stats[] END
            as seasons,
         CASE
             WHEN ts.season IS NOT NULL THEN
                 (CASE WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad' END)::scoring_class
             ELSE ls.scoring_class
         END as scoring_class,
        CASE
        	WHEN ts.season IS NOT NULL THEN 0
        	ELSE ls.years_since_last_active + 1
        END AS years_since_last_active,
         ts.season IS NOT NULL as is_active,
         2022 AS current_season

    FROM last_season ls
    FULL OUTER JOIN this_season ts
    ON ls.player_name = ts.player_name;



 -- DROP TABLE players_scd;

    CREATE TABLE players_scd (
   player_name TEXT,
   scoring_class scoring_class,
   is_active BOOLEAN,
   start_season INTEGER,
   end_season INTEGER,
   current_season INTEGER,
   PRIMARY KEY (player_name, start_season)

 );


 INSERT INTO players_scd (
 WITH with_previous AS (
 SELECT player_name,
 scoring_class,
 is_active,
 LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_scoring_class,
 LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_is_active,
 current_season
 FROM players
 WHERE current_season <= 2021) ,

 with_indicators AS (

 SELECT
 *,
 CASE
 	WHEN scoring_class <> prev_scoring_class THEN 1
 	WHEN is_active <> prev_is_active THEN 1
 	ELSE 0
 END AS change_indicator
 FROM with_previous)

 --with_streak_indicator AS (

 SELECT *,
 SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_indicator
 FROM with_indicators
 --)


SELECT
  player_name,
  scoring_class,
  is_active,
  MIN(current_season) AS start_season,
  MAX(current_season) AS end_season,
  2021 AS current_season
  FROM with_streak_indicator
  GROUP BY player_name, streak_indicator, is_active, scoring_class
  ORDER BY player_name
  )

  SELECT * FROM players_scd;

  SELECT * FROM players
  WHERE player_name = 'Aaron Brooks'
  ORDER BY player_name;