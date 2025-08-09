-- check if fire_date column exists in all emission tables
SELECT table_name, column_name, data_type
FROM `emission_db.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE 'emission_%'
  AND column_name = 'fire_date'
  AND table_name IN (
    'emission_2003','emission_2004','emission_2005','emission_2006','emission_2007',
    'emission_2008','emission_2009','emission_2010','emission_2011','emission_2012',
    'emission_2013','emission_2014','emission_2015'
  )
ORDER BY table_name;

-- count records per table
SELECT 'emission_2003' as table_name,
  COUNT(fire_date) as filled_dates,
  COUNT(*) - COUNT(fire_date) as null_dates
FROM `emission_db.emission_2003`;

-- sample verification
SELECT year, doy, fire_date,
  DATE_ADD(DATE(year, 1, 1), INTERVAL doy - 1 DAY) as calculated_date
FROM `emission_db.emission_2010`
LIMIT 5;

-- date ranges for 2003
SELECT MIN(fire_date) as min_date,
  MAX(fire_date) as max_date,
  COUNT(DISTINCT fire_date) as unique_dates
FROM `emission_db.emission_2003`;

-- year validation
SELECT COUNT(*) as mismatches
FROM `emission_db.emission_2010`
WHERE 2010 != EXTRACT(YEAR FROM fire_date);

-- DOY edge cases
SELECT doy, fire_date, 
  EXTRACT(DAYOFYEAR FROM fire_date) as calculated_doy
FROM `emission_db.emission_2012`
WHERE doy IN (1, 60, 366)
LIMIT 5;

-- Leap year check 2004
SELECT COUNT(*) as leap_day_records
FROM `emission_db.emission_2004`
WHERE doy = 366;

-- non-leap year validation
SELECT COUNT(*) as invalid_doy_count
FROM `emission_db.emission_2003`
WHERE doy = 366;

-- sample records
SELECT year, doy, fire_date
FROM `emission_db.emission_2008` 
LIMIT 3;

-- calc error check
SELECT COUNT(*) as calculation_errors
FROM `emission_db.emission_2007`
WHERE fire_date != DATE_ADD(DATE(year, 1, 1), INTERVAL doy - 1 DAY);

-- DOY range
SELECT MIN(doy) as min_doy, MAX(doy) as max_doy
FROM `emission_db.emission_2012`;

SELECT COUNT(*) as total_records,
  COUNT(fire_date) as records_with_date,
  ROUND(COUNT(fire_date) / COUNT(*) * 100, 2) as fill_percentage
FROM `emission_db.emission_2010`;

-- test date calculation
SELECT DATE_ADD(DATE(2010, 1, 1), INTERVAL 1 - 1 DAY) as jan_1_test;

-- tst Dec 31
SELECT DATE_ADD(DATE(2010, 1, 1), INTERVAL 365 - 1 DAY) as dec_31_test;

-- test leap year Dec 31
SELECT DATE_ADD(DATE(2012, 1, 1), INTERVAL 366 - 1 DAY) as leap_dec_31_test;