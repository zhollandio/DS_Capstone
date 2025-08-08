-- ─────────────────────────────────────────────────────────
-- Add BI column to emission tables 2004-2016
-- ─────────────────────────────────────────────────────────

FOR yr IN (SELECT year FROM UNNEST(GENERATE_ARRAY(2004, 2016)) AS year) DO
  
  -- Add the BI column
  EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `emission_db.emission_%d`
      ADD COLUMN IF NOT EXISTS bi_value FLOAT64
  """, yr.year);
  
  -- Populate BI values handling cross-year dates (doy = 0)
  EXECUTE IMMEDIATE FORMAT("""
      MERGE `emission_db.emission_%d` e
      USING (
        SELECT 
          e_sub.id,
          b.value as bi_value
        FROM `emission_db.emission_%d` e_sub
        JOIN `weather_db.bi` b ON
          ABS(e_sub.longitude - b.lon) < 0.05
          AND ABS(e_sub.latitude - b.lat) < 0.05
          AND e_sub.fire_date = b.obs_date
          AND (b.year = %d OR b.year = %d)  -- Include both current and previous year
        WHERE e_sub.bi_value IS NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY e_sub.id 
          ORDER BY ABS(e_sub.longitude - b.lon) + ABS(e_sub.latitude - b.lat)
        ) = 1
      ) closest_weather
      ON e.id = closest_weather.id
      WHEN MATCHED THEN
        UPDATE SET bi_value = closest_weather.bi_value
  """, yr.year, yr.year, yr.year, yr.year - 1);
  
END FOR;