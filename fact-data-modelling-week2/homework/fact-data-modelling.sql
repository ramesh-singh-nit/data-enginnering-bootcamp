--SELECT * FROM devices WHERE device_id = 8971691997776596000;

--SELECT * FROM events WHERE
--user_id = 11780863980750100000 AND
--date(event_time) = '2023-01-01';

--SELECT * FROM events;


WITH game_details_with_rn AS (
SELECT *,
ROW_NUMBER () OVER (PARTITION BY game_id, team_id, player_id) AS row_num
FROM game_details )

SELECT * FROM game_details_with_rn
WHERE row_num = 1;

--DROP TABLE user_devices_cumulated;

CREATE TABLE user_devices_cumulated (
 user_id NUMERIC,
 browser_type TEXT,
 device_activity_datelist DATE[],
 date DATE,
 PRIMARY KEY (user_id, browser_type, date)
)


INSERT INTO user_devices_cumulated (
WITH events_devices_deduped AS (
WITH events_deduped AS (
WITH events_with_rn AS (
SELECT *,
ROW_NUMBER () OVER (PARTITION BY user_id, device_id, event_time ORDER BY event_time) AS row_num
FROM events)

SELECT * FROM events_with_rn WHERE row_num = 1
),

devices_deduped AS (
WITH devices_with_rn AS (
SELECT *,
ROW_NUMBER () OVER (PARTITION BY device_id, browser_type, browser_version_major, browser_version_minor,
browser_version_patch) AS row_num
FROM devices)

SELECT * FROM devices_with_rn WHERE row_num = 1
)


SELECT e.user_id, d.browser_type,
DATE(CAST(e.event_time AS timestamp)) AS date_active
FROM events_deduped e INNER JOIN devices_deduped d
ON e.device_id = d.device_id
AND e.user_id IS NOT NULL
GROUP BY e.user_id, d.browser_type, date_active
),

yesterday AS (
SELECT * FROM user_devices_cumulated WHERE date = DATE('2023-01-06')
),

today AS (
SELECT CAST(user_id AS NUMERIC) AS user_id,
browser_type,
date_active
FROM events_devices_deduped
WHERE date_active = '2023-01-07'
)

SELECT
COALESCE(y.user_id, t.user_id) AS user_id,
COALESCE(y.browser_type, t.browser_type) AS browser_type,
CASE
	WHEN y.device_activity_datelist IS NULL
	THEN ARRAY[t.date_active]
	WHEN t.date_active IS NULL THEN y.device_activity_datelist
	ELSE ARRAY[t.date_active] || y.device_activity_datelist
END AS device_activity_datelist,
COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id AND t.browser_type = y.browser_type
)

--DELETE FROM user_devices_cumulated;

-- SELECT * FROM user_devices_cumulated WHERE date = '2023-01-05';


WITH browser_active_data AS (
 SELECT * FROM user_devices_cumulated WHERE date = '2023-01-31'
),

series AS (
SELECT * FROM generate_series(date('2023-01-01'), date('2023-01-31'), INTERVAL '1 day') AS series_date
),

placeholder_int AS (
 SELECT CASE
 	  WHEN device_activity_datelist @> ARRAY[date(series_date)]
 	  THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS bigint)
 	  ELSE 0
 END AS palceholder_int_value,
 * FROM user_devices_cumulated CROSS JOIN series

)

SELECT user_id, browser_type,
CAST(CAST(sum(palceholder_int_value) AS bigint) AS bit(32)) AS datelist_int
FROM placeholder_int
GROUP BY user_id, browser_type;


CREATE TABLE host_activity_datelist (
 host TEXT,
 host_activity_datelist date[],
 date DATE,
 PRIMARY KEY (host, date)
)



INSERT INTO host_activity_datelist(
WITH events_deduped AS (
WITH events_with_rn AS (
SELECT *,
ROW_NUMBER () OVER (PARTITION BY user_id, device_id, event_time ORDER BY event_time) AS row_num
FROM events)
SELECT * FROM events_with_rn WHERE row_num = 1
),


 yesterday AS (
 SELECT * FROM host_array_metrics WHERE date = date('2023-01-04')
),

today AS (
 SELECT host,
 date(CAST(event_time AS timestamp)) AS date_active
 FROM events_deduped
 WHERE host IS NOT NULL
 AND date(CAST(event_time AS timestamp)) = '2023-01-05'
 GROUP BY host, date(CAST(event_time AS timestamp))
)

SELECT COALESCE(y.host, t.host) AS host,
CASE WHEN y.host_activity_datelist IS NULL
   THEN ARRAY[t.date_active]
   WHEN t.date_active IS NULL THEN y.host_activity_datelist
   ELSE ARRAY[t.date_active] || y.host_activity_datelist
END AS host_activity_datelist,
coalesce(t.date_active, y.date + interval '1 day') AS date
FROM today t FULL OUTER JOIN yesterday y
ON t.host = y.host
)

-- SELECT * FROM host_array_metrics;

CREATE TABLE host_activity_reduced (
  host text,
  month_start date,
  metric_name text,
  metric_array real[],
  PRIMARY KEY (host, month_start, metric_name)
)

INSERT INTO host_activity_reduced
WITH events_deduped AS (
WITH events_with_rn AS (
SELECT *,
ROW_NUMBER () OVER (PARTITION BY user_id, device_id, event_time ORDER BY event_time) AS row_num
FROM events)
SELECT * FROM events_with_rn WHERE row_num = 1
),

daily_aggregate AS (
  SELECT
    host,
    date(event_time) AS date,
    count(1) AS num_host_hits
    --count (DISTINCT user_id) AS num_vistors
    FROM events_deduped
    WHERE user_id IS NOT NULL
    AND date(event_time) = '2023-01-05'
    GROUP BY host, date(event_time)
),

yesterday_array AS (
   SELECT * FROM host_activity_reduced
   WHERE month_start = '2023-01-01'
   and metric_name = 'num_host_hits'
)

SELECT
  coalesce(da.host, ya.host) AS host,
  coalesce(ya.month_start, date_trunc('month', da.date)) AS month_start,
  'num_host_hits' AS metric_name,
  CASE
  	WHEN ya.metric_array IS NOT NULL then
  	 ya.metric_array || ARRAY[coalesce(da.num_host_hits, 0)]
  	 WHEN ya.metric_array IS NULL THEN
  	  array_fill(0, ARRAY[coalesce(da.date - DATE(date_trunc('month', da.date)), 0)])
  	      || array[coalesce(da.num_host_hits, 0)]
  END AS metric_array
  FROM daily_aggregate da FULL OUTER JOIN yesterday_array ya
  ON da.host = ya.host
  ON CONFLICT (host, month_start, metric_name)
  DO
    UPDATE SET metric_array = EXCLUDED.metric_array;


--SELECT cardinality(metric_array), count(1)
--FROM host_activity_reduced
--GROUP BY 1

--SELECT * FROM host_activity_reduced;