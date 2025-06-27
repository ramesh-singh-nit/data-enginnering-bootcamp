--- FACT DATA MODELLING LAB DAY 1--------

-- SELECT * FROM game_details;
INSERT INTO fct_game_details (
WITH deduped AS (
  SELECT g.game_date_est,
  g.season,
  g.home_team_id,
  gd.*,
  row_number() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id
  ORDER BY g.game_date_est) AS row_num
  FROM game_details gd
     JOIN games g ON gd.game_id = g.game_id
  )


SELECT
   game_date_est AS dim_game_date,
   season AS dim_season,
   team_id AS dim_team_id,
   player_id AS dim_player_id,
   player_name AS dim_player_name,
   start_position AS dim_start_position,
   team_id = home_team_id AS dim_is_playing_at_home,
   COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
   COALESCE(POSITION('DNd' IN comment), 0) > 0 AS dim_did_not_dress,
   COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
   CAST(SPLIT_PART(min,':', 1) AS REAL) +
   CAST(SPLIT_PART(min,':', 2) AS REAL)/60 AS m_minutes,
   fgm AS m_fgm,
   fga AS m_fga,
   fg3m AS m_fg3m,
   fg3a AS m_fg3a,
   ftm AS m_ftm,
   fta AS m_fta,
   oreb AS m_oreb,
   dreb AS m_dreb,
   reb AS m_reb,
   ast AS m_ast,
   stl AS m_stl,
   blk AS m_blk,
   "TO" AS m_turnovers,
   pf AS m_pf,
   pts AS m_pts,
   plus_minus AS m_plus_minus
   FROM deduped
WHERE row_num = 1);


--DROP TABLE fct_game_details;

CREATE TABLE fct_game_details (
  dim_game_date DATE,
  dim_season INTEGER,
  dim_team_id INTEGER,
  dim_player_id INTEGER,
  dim_player_name TEXT,
  dim_start_position TEXT,
  dim_is_playing_at_home BOOLEAN,
  dim_did_not_play BOOLEAN,
  dim_did_not_dress BOOLEAN,
  dim_not_with_team BOOLEAN,
  m_minutes REAL,
  m_fgm INTEGER,
  m_fga INTEGER,
  m_fg3m INTEGER,
  m_fg3a INTEGER,
  m_ftm INTEGER,
  m_fta INTEGER,
  m_oreb INTEGER,
  m_dreb INTEGER,
  m_reb INTEGER,
  m_ast INTEGER,
  m_stl INTEGER,
  m_blk INTEGER,
  m_turnovers INTEGER,
  m_pf INTEGER,
  m_pts INTEGER,
  m_plus_minus INTEGER,
  PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
)

--SELECT * FROM fct_game_details;


--- FACT DATA MODELLING LAB DAY 2--------

INSERT INTO users_cumulated (
WITH yesterday AS (
select * from users_cumulated

  WHERE date = DATE('2023-01-30')),


today AS (
select
 CAST(user_id AS TEXT) AS user_id,
date(CAST(event_time AS timestamp)) AS date_active
from events
WHERE date(CAST(event_time AS timestamp)) = DATE('2023-01-31')
AND user_id  IS NOT null
GROUP BY user_id, date(CAST(event_time AS timestamp)))

SELECT COALESCE(y.user_id, t.user_id) AS user_id,
CASE WHEN y.dates_active IS NULL
  THEN ARRAY[t.date_active]
  WHEN t.date_active IS NULL THEN y.dates_active
  ELSE ARRAY[t.date_active] || y.dates_active
END AS date_active,
COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date

FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
)



SELECT * FROM users_cumulated WHERE date= '2023-01-31';


WITH users AS (
 SELECT * FROM users_cumulated
 WHERE date = DATE('2023-01-31')

),

series AS (
 SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') AS series_date
),

placeholder_int AS (

SELECT
CASE WHEN dates_active @> ARRAY [date(series_date)]
 THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS bigint)
 ELSE 0
 END  AS placeholder_int_value,
* FROM users_cumulated CROSS JOIN series
)

SELECT user_id,
CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32)),
bit_count(CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32))) > 0 AS dim_monthly_active

FROM placeholder_int
GROUP BY user_id

--DROP TABLE users_cumulated;

CREATE TABLE users_cumulated (
 user_id TEXT,
 -- The list in the past where user is active
 dates_active DATE[],
 -- the current date for the user
 date DATE,
 PRIMARY KEY (user_id, date)

)

--- FACT DATA MODELLING LAB DAY 3--------

CREATE TABLE array_metrics (
 user_id NUMERIC,
 month_start date,
 metric_name text,
 metric_array real[],
 PRIMARY KEY (user_id, month_start, metric_name)

)

delete FROM array_metrics;

INSERT INTO array_metrics
WITH daily_aggregate AS (
    -- Aggregate daily site hits per user
    SELECT
        user_id,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-03')
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
yesterday_array AS (
    -- Retrieve existing metrics for the month starting from '2023-01-01'
    SELECT *
    FROM array_metrics
    WHERE month_start = DATE('2023-01-01')
)
SELECT
    -- Select user_id from either daily_aggregate or yesterday_array
    COALESCE( da.user_id, ya.user_id) AS user_id,
    -- Determine month_start date
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    -- Set metric name to 'site_hits'
    'site_hits' AS metric_name,
    -- Update metric_array based on existing data and new daily aggregates
    CASE
        WHEN ya.metric_array IS NOT NULL THEN
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)]
        WHEN ya.metric_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)])
                || ARRAY[COALESCE(da.num_site_hits,0)]
    END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya
ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO
    UPDATE SET metric_array = EXCLUDED.metric_array;

-- Uncomment and run the following query to verify the cardinality of metric_array
 SELECT cardinality(metric_array), COUNT(1)
 FROM array_metrics
 GROUP BY 1;



-- Aggregate metrics by summing specific elements in the metric_array
WITH agg AS (
    SELECT metric_name, month_start, ARRAY[SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
-- Select and display the metric_name, date (adjusted by index), and summed value
SELECT
    metric_name,
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);
