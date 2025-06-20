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
WHERE current_year = 1973),

current_year AS (
SELECT
actor,
actorid,
ARRAY_AGG(ROW(film,votes,rating,filmid)::film_stats) AS films,
AVG(rating) AS avg_rating,
year
FROM actor_films
WHERE YEAR = 1974
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
 1974 AS current_year
FROM last_year l
FULL OUTER JOIN current_year c
ON l.actor = c.actor );

--DROP TABLE actors_history_scd;

SELECT * FROM actors WHERE actor = 'Aamir Khan';

CREATE TABLE actors_history_scd (
  actor TEXT,
  actorid TEXT,
  quality_class quality_class,
  is_active BOOLEAN,
  start_year INTEGER,
  end_year INTEGER
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