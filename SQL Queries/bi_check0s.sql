-- ─────────────────────────────────────────────────────────
-- Check actual BI values in weather_db.bi table
-- ─────────────────────────────────────────────────────────

-- Overall BI distribution in weather data
SELECT 
  COUNT(*) as total_weather_records,
  COUNTIF(value = 0) as zero_values,
  COUNTIF(value > 0 AND value <= 25) as low_danger_1_25,
  COUNTIF(value > 25 AND value <= 50) as moderate_danger_26_50,
  COUNTIF(value > 50 AND value <= 75) as high_danger_51_75,
  COUNTIF(value > 75) as extreme_danger_75_plus,
  MIN(value) as min_bi,
  MAX(value) as max_bi,
  AVG(value) as avg_bi,
  ROUND(COUNTIF(value = 0) * 100.0 / COUNT(*), 2) as percent_zero
FROM `weather_db.bi`
WHERE year = 2003;

-- Check January 1, 2003 - we knows 0s exist here
SELECT 
  value,
  COUNT(*) as record_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `weather_db.bi`
WHERE year = 2003 AND obs_date = '2003-01-01'
GROUP BY value
ORDER BY value
LIMIT 20;

-- Monthly BI patterns (check seasonal variation)
SELECT 
  EXTRACT(MONTH FROM obs_date) as month,
  COUNT(*) as total_records,
  COUNTIF(value = 0) as zero_count,
  ROUND(COUNTIF(value = 0) * 100.0 / COUNT(*), 2) as percent_zero,
  ROUND(AVG(value), 2) as avg_bi,
  MIN(value) as min_bi,
  MAX(value) as max_bi
FROM `weather_db.bi`
WHERE year = 2003
GROUP BY EXTRACT(MONTH FROM obs_date)
ORDER BY month;

-- Sample of zero BI records from weather data
SELECT lon, lat, obs_date, value, year
FROM `weather_db.bi`
WHERE year = 2003 AND value = 0
ORDER BY obs_date
LIMIT 15;