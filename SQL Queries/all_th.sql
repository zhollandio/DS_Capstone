-- ═══════════════════════════════════════════════════════════════
-- TH
-- ═══════════════════════════════════════════════════════════════

FOR yr IN (SELECT year FROM UNNEST(GENERATE_ARRAY(2003, 2016)) AS year) DO
  EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `emission_db.emission_%d`
      ADD COLUMN IF NOT EXISTS th_value FLOAT64
  """, yr.year);
  
  EXECUTE IMMEDIATE FORMAT("""
      MERGE `emission_db.emission_%d` e
      USING (
        SELECT 
          e_sub.id,
          t.value as th_value
        FROM `emission_db.emission_%d` e_sub
        JOIN `weather_db.th` t ON
          ABS(e_sub.longitude - t.lon) < 0.05
          AND ABS(e_sub.latitude - t.lat) < 0.05
          AND e_sub.fire_date = t.obs_date
          AND (t.year = %d OR t.year = %d)
        WHERE e_sub.th_value IS NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY e_sub.id 
          ORDER BY ABS(e_sub.longitude - t.lon) + ABS(e_sub.latitude - t.lat)
        ) = 1
      ) closest_weather
      ON e.id = closest_weather.id
      WHEN MATCHED THEN
        UPDATE SET th_value = closest_weather.th_value
  """, yr.year, yr.year, yr.year, yr.year - 1);
END FOR;