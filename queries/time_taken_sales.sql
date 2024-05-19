WITH time_1 AS
(SELECT make_timestamp(year :: INT, month :: INT, day :: INT, EXTRACT(hour FROM timestamp) :: INT, EXTRACT(minute FROM timestamp) :: INT, EXTRACT(second FROM timestamp) :: INT) AS time_stamp,
 	year
FROM dim_date_times),

time_2 AS
(SELECT year, 
 	time_stamp, 
 	LEAD(time_stamp) OVER (ORDER BY time_stamp DESC) AS lead
FROM time_1),

time_3 AS
(SELECT year,
	AVG(time_stamp - lead) AS avg_times
FROM time_2
WHERE lead IS NOT NULL
GROUP BY year
ORDER BY year DESC)

SELECT year, 
	'"hours": ' || EXTRACT (hour FROM avg_times) || ', "minutes": ' || EXTRACT (minute FROM avg_times) || ', "seconds": ' || ROUND(EXTRACT (second FROM avg_times)) AS actual_time_taken
FROM time_3
ORDER BY avg_times DESC
LIMIT 5;