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

