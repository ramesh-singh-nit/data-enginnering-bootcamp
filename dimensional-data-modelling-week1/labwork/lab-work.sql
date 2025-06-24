-------------------- DAY 1 -----------------------------

--select * from actor_films WHERE YEAR = 1971;

--select max(year) from actor_films;

CREATE TYPE film_stats AS (
  film TEXT,
  votes INTEGER,
  rating REAL,
  filmid TEXT
)

--DROP TYPE film_stats;

CREATE TYPE quality_class AS
ENUM('star', 'good', 'average', 'bad');

--DROP TABLE actors;

CREATE TABLE actors (
  actor TEXT,
  actorid TEXT,
  films film_stats[],
  quality_class quality_class,
  is_active BOOLEAN,
  current_year INTEGER,
  PRIMARY KEY (actor, current_year)
);

--SELECT * FROM actors WHERE actor = 'Agnes Moorehead';

INSERT INTO actors (
WITH last_year AS (
SELECT * FROM actors
WHERE current_year = 1974),

current_year AS (
SELECT
actor,
actorid,
ARRAY_AGG(ROW(film,votes,rating,filmid)::film_stats) AS films,
AVG(rating) AS avg_rating,
year
FROM actor_films
WHERE YEAR = 1975
GROUP BY actor, actorid, year
)

SELECT
COALESCE(c.actor, l.actor) AS actor,
COALESCE(c.actorid, l.actorid) AS actorid,
COALESCE(l.films, ARRAY[]::film_stats[]) || CASE WHEN c.films IS NOT NULL THEN
                c.films
                ELSE
                    ARRAY[]::film_stats[] END AS films,
CASE WHEN c.avg_rating IS NOT NULL THEN
   (CASE WHEN (c.avg_rating) > 8 THEN 'star'
   WHEN (c.avg_rating) > 7 THEN 'good'
   WHEN (c.avg_rating) > 6 THEN 'average'
   ELSE 'bad' END) :: quality_class
  ELSE
     l.quality_class
  END AS quality_class,
 c.year IS NOT NULL AS is_active,
 1975 AS current_year
FROM last_year l
FULL OUTER JOIN current_year c
ON l.actor = c.actor );


-------------------- DAY 2 -----------------------------
--DROP TABLE actors_history_scd;

--SELECT * FROM actors WHERE actor = 'Aamir Khan';

CREATE TABLE actors_history_scd (
  actor TEXT,
  actorid TEXT,
  quality_class quality_class,
  is_active BOOLEAN,
  start_year INTEGER,
  end_year INTEGER,
  PRIMARY KEY (actor, start_year)
)

CREATE TYPE scd_type AS (
 quality_class quality_class,
 is_active BOOLEAN,
 start_year INTEGER,
 end_year INTEGER
)

INSERT INTO actors_history_scd (
WITH previous_scd AS (
 SELECT actor,
 actorid,
 quality_class,
 is_active,
 LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS prev_quality_class,
 LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS prev_is_active,
 current_year
 FROM actors WHERE current_year <= 2020
),

with_indicators AS (
SELECT *,
CASE 
	WHEN quality_class <> prev_quality_class THEN 1
	WHEN is_active <> prev_is_active THEN 1
	ELSE 0
END AS change_indicator
FROM  previous_scd
),

streak_indicator AS (
SELECT 
*,
SUM(change_indicator) OVER (PARTITION BY actor ORDER BY current_year) AS streak_indicator
FROM with_indicators
)



SELECT 
actor,
actorid,
quality_class,
is_active,
min(current_year) AS start_year,
max(current_year) AS end_year
FROM streak_indicator
GROUP BY actor, actorid, quality_class, is_active, streak_indicator
)

--SELECT * FROM actors_history_scd;

-------------------- DAY 3 -----------------------------

CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');

CREATE TABLE vertices (
 identifier TEXT,
 type vertex_type,
 properties JSON,
 PRIMARY KEY (identifier, type)
);

DROP TABLE vertices;

CREATE TYPE edge_type AS 
 ENUM ('plays_against', 'shares_team', 'plays_in', 'plays_on')
 
 CREATE TABLE edges (
  subject_identifier TEXT,
  subject_type vertex_type,
  object_identifier TEXT,
  object_type vertex_type,
  edge_type edge_type,
  properties JSON,
  PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
 );
 

INSERT INTO vertices ( 
SELECT
 game_id AS identfier,
 'game'::vertex_type AS type,
 json_build_object(
   'pts_home', pts_home,
   'pts_away', pts_away,
   'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
 ) AS properties

FROM games );

INSERT INTO vertices (
WITH players_agg AS (
SELECT 
   player_id AS identifier,
   MAX(player_name) AS player_name,
   count(1) AS number_of_games,
   sum(pts) AS total_points,
   ARRAY_AGG (DISTINCT team_id) AS teams
FROM game_details
GROUP BY player_id
)

SELECT identifier,
       'player'::vertex_type AS type, 
       json_build_object(
         'player_name', player_name,
         'number_of_games', number_of_games,
         'total_points', total_points,
         'teams', teams
       ) AS properties
FROM players_agg
)
;

INSERT INTO vertices (
WITH team_deduped AS (
SELECT *,
ROW_NUMBER () OVER (PARTITION BY team_id) AS row_num
FROM teams
 
)
SELECT 
     team_id AS identifier,
     'team'::vertex_type AS type,
     json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
     )
     FROM team_deduped WHERE row_num =1 )
;

INSERT INTO edges(

WITH deduped AS ( 
SELECT *, ROW_NUMBER () OVER (PARTITION BY player_id, game_id) AS row_num
FROM game_details
)
SELECT 
  player_id AS subject_identifier,
  'player'::vertex_type AS subject_type,
  game_id AS object_identfier,
  'game'::vertex_type AS object_type,
  'plays_in'::edge_type AS edge_type,
  json_build_object(
   'start_position', start_position,
   'pts', pts,
   'team_id', team_id,
   'team_abbreviation', team_abbreviation
  ) AS properties
  
  FROM deduped WHERE row_num = 1
  )
  ;


SELECT TYPE, count(1)
FROM vertices GROUP BY TYPE;

