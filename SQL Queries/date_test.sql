SELECT table_name, column_name, data_type
FROM `weather_db.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('tmmn','tmmx','fm1000','fm100','vpd','pet','pr','rmax','rmin','sph','srad','th','vs')
  AND column_name = 'obs_date'
ORDER BY table_name;

SELECT 'tmmn' as table_name, 
  COUNT(obs_date) as filled_dates, 
  COUNT(*) - COUNT(obs_date) as null_dates
FROM `weather_db.tmmn` WHERE year BETWEEN 2002 AND 2017;

SELECT 'tmmx' as table_name, 
  COUNT(obs_date) as filled_dates, 
  COUNT(*) - COUNT(obs_date) as null_dates
FROM `weather_db.tmmx` WHERE year BETWEEN 2002 AND 2017;

SELECT 'fm1000' as table_name, 
  COUNT(obs_date) as filled_dates, 
  COUNT(*) - COUNT(obs_date) as null_dates
FROM `weather_db.fm1000` WHERE year BETWEEN 2002 AND 2017;

SELECT 'pr' as table_name, 
  COUNT(obs_date) as filled_dates, 
  COUNT(*) - COUNT(obs_date) as null_dates
FROM `weather_db.pr` WHERE year BETWEEN 2002 AND 2017;

SELECT day, year, obs_date, 
  DATE(TIMESTAMP_MICROS(CAST(day / 1000 AS INT64))) as calculated_date
FROM `weather_db.pr` 
WHERE year = 2010 
LIMIT 10;

SELECT MIN(obs_date) as min_date, 
  MAX(obs_date) as max_date,
  COUNT(DISTINCT obs_date) as unique_dates
FROM `weather_db.tmmn` WHERE year BETWEEN 2002 AND 2017;

SELECT COUNT(*) as mismatches
FROM `weather_db.tmmn` 
WHERE year BETWEEN 2002 AND 2017 
  AND year != EXTRACT(YEAR FROM obs_date);

SELECT COUNT(*) as invalid_dates
FROM `weather_db.tmmn` 
WHERE year BETWEEN 2002 AND 2017 
  AND (obs_date < '2002-01-01' OR obs_date > '2017-12-31');

SELECT value, lon, lat, day, year, obs_date
FROM `weather_db.tmmn` WHERE year = 2005 LIMIT 3;

SELECT 
  day,
  TIMESTAMP_MICROS(CAST(day / 1000 AS INT64)) as timestamp_result,
  DATE(TIMESTAMP_MICROS(CAST(day / 1000 AS INT64))) as date_result,
  obs_date
FROM `weather_db.srad` 
WHERE year = 2008 
  AND obs_date IS NOT NULL
LIMIT 5;

SELECT obs_date, 
  COUNT(*) as record_count
FROM `weather_db.tmmx` 
WHERE year = 2010 
GROUP BY obs_date 
ORDER BY obs_date 
LIMIT 10;

SELECT obs_date, COUNT(*) as records
FROM `weather_db.th`
WHERE year IN (2004, 2008, 2012, 2016)
  AND obs_date IN ('2004-02-29', '2008-02-29', '2012-02-29', '2016-02-29')
GROUP BY obs_date;

SELECT 
  MIN(day) as min_day_value,
  MAX(day) as max_day_value,
  MIN(obs_date) as min_converted_date,
  MAX(obs_date) as max_converted_date
FROM `weather_db.vs` 
WHERE year BETWEEN 2002 AND 2017;

SELECT year, 
  COUNT(*) as total_records,
  COUNT(obs_date) as records_with_date,
  ROUND(COUNT(obs_date) / COUNT(*) * 100, 2) as fill_percentage
FROM `weather_db.rmax`
WHERE year BETWEEN 2002 AND 2017
GROUP BY year
ORDER BY year;