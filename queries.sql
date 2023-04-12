--roll up
--This query retrieves the total sum of values for each chemical measured
SELECT chemical_name, SUM(value),measurement_unit
FROM measurements
JOIN time ON measurements.time_id = time.time_id
JOIN chemical_type ON measurements.chemical_id = chemical_type.chemical_id
JOIN stations ON measurements.station_id = stations.station_id
GROUP BY chemical_name,measurement_unit



--Slice
--This SQL query selects the name of the chemical, its values in the records, 
--and its measurement unit from the measurements table based on a specific chemical name 
SELECT c.chemical_name, m.value, c.measurement_unit
FROM measurements m
JOIN chemical_type c ON m.chemical_id = c.chemical_id
WHERE c.chemical_name = 'Nitric oxide';

--Dice
--this query selects the date, and the value of a specific chemical, in a specific provice, in a specific year
SELECT t.month,t.day,m.value,c.measurement_unit
FROM measurements m
JOIN chemical_type c ON m.chemical_id = c.chemical_id
JOIN stations s ON m.station_id = s.station_id
JOIN time t ON m.time_id = t.time_id
WHERE c.chemical_name = 'Carbon monoxide'
  AND s.station_province = 'QC'
  AND t.year = '1980';

--Combine
--this query selects the date, and the value of a specific chemical, in a specific provice, 
--for each month of each year. To see the difference over months
SELECT 
	  t.year,
    t.month,
    SUM(m.value) AS total
FROM 
    measurements m
    JOIN stations s ON m.station_id = s.station_id
    JOIN time t ON m.time_id = t.time_id
    JOIN chemical_type c ON m.chemical_id = c.chemical_id
WHERE 
    c.chemical_name = 'Carbon monoxide' AND
    s.station_province = 'QC'
GROUP BY 
    t.year,t.month;



--calculates the total value of a specific chemical across Canada for each day, 
--allowing the observation of the chemical growth trends over time.
SELECT t.year, t.month, t.day, SUM(m.value) AS total
FROM measurements m
JOIN chemical_type c ON m.chemical_id = c.chemical_id
JOIN time t ON m.time_id = t.time_id
WHERE c.chemical_name = 'Carbon monoxide'
GROUP BY t.year, t.month, t.day
ORDER BY t.year ASC, t.month ASC, t.day ASC

--Iceberg
--This query return the 5 records with the highest value for a specific chemical, 
--tt can provide insights into the level of air pollution on those days and 
--help inform decision-making related to public health and environmental
SELECT t.year, t.month, t.day, SUM(m.value) AS total
FROM measurements m
JOIN chemical_type c ON m.chemical_id = c.chemical_id
JOIN time t ON m.time_id = t.time_id
WHERE c.chemical_name = 'Carbon monoxide'
GROUP BY t.year, t.month, t.day
ORDER BY total DESC
LIMIT 5

--This query returns the top 5 stations with the highest total value of a sepcific chemical
--this information can be used to take necessary measures to reduce chemical emissions in those areas and improve air quality.
SELECT s.station_id, s.station_province, s.station_city, SUM(m.value) AS total_value
FROM measurements m
JOIN chemical_type c ON m.chemical_id = c.chemical_id
JOIN stations s ON m.station_id = s.station_id
WHERE c.chemical_name = 'Carbon monoxide'
GROUP BY s.station_id, s.station_province, s.station_city
ORDER BY total_value DESC
LIMIT 5


--Windowing
--This SQL query calculates the total amount of chemical records from each station, and rank them within their respective province and across Canada
--this can help us identify areas that are more likely to have air quality problems
SELECT  s.station_id,s.station_province, c.chemical_name, SUM(m.value) AS total_value,
       RANK() OVER (PARTITION BY s.station_province ORDER BY SUM(m.value) DESC) AS province_rank,
       RANK() OVER (ORDER BY SUM(m.value) DESC) AS overall_rank
FROM measurements m
JOIN chemical_type c ON m.chemical_id = c.chemical_id
JOIN stations s ON m.station_id = s.station_id
WHERE c.chemical_name = 'PM2.5'
GROUP BY s.station_id, s.station_province, c.chemical_name


--Window clause
-- this query calculate the total value of a sepcific in a sepcific province, and compare it to the next and last months
WITH curr_month AS (
  SELECT 
    SUM(m.value) AS curr_month_total
  FROM 
    measurements m
    JOIN chemical_type c ON m.chemical_id = c.chemical_id
    JOIN time t ON m.time_id = t.time_id
	JOIN stations s ON m.station_id=s.station_id
  WHERE 
    c.chemical_name = 'Carbon monoxide'
    AND t.year = 1979
    AND t.month = 2
	AND s.station_province='ON'

),
prev_month AS (
  SELECT 
    SUM(m.value) AS prev_month_total
  FROM 
    measurements m
    JOIN chemical_type c ON m.chemical_id = c.chemical_id
    JOIN time t ON m.time_id = t.time_id
	JOIN stations s ON m.station_id=s.station_id
  WHERE 
    c.chemical_name = 'Carbon monoxide'
    AND t.year = 1979
    AND t.month = 3
	AND s.station_province='ON'
),
next_month AS (
  SELECT 
    SUM(m.value) AS next_month_total
  FROM 
    measurements m
    JOIN chemical_type c ON m.chemical_id = c.chemical_id
    JOIN time t ON m.time_id = t.time_id
	JOIN stations s ON m.station_id=s.station_id
  WHERE 
    c.chemical_name = 'Carbon monoxide'
    AND t.year = 1979
    AND t.month = 4
	AND s.station_province='ON'
)
SELECT 
  c.curr_month_total,
  p.prev_month_total,
  n.next_month_total
FROM 
  curr_month c,
  prev_month p,
  next_month n;
