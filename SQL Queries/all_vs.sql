-- ═══════════════════════════════════════════════════════════════
-- (Wind Speed)
-- ═══════════════════════════════════════════════════════════════

FOR yr IN (SELECT year FROM UNNEST(GENERATE_ARRAY(2003, 2016)) AS year) DO
  EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `emission_db.emission_%d`
      ADD COLUMN IF NOT EXISTS vs_value FLOAT64
  """, yr.year);
  
  EXECUTE IMMEDIATE FORMAT("""
      MERGE `emission_db.emission_%d` e
      USING (
        SELECT 
          e_sub.id,
          v.value as vs_value
        FROM `emission_db.emission_%d` e_sub
        JOIN `weather_db.vs` v ON
          ABS(e_sub.longitude - v.lon) < 0.05
          AND ABS(e_sub.latitude - v.lat) < 0.05
          AND e_sub.fire_date = v.obs_date
          AND (v.year = %d OR v.year = %d)
        WHERE e_sub.vs_value IS NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY e_sub.id 
          ORDER BY ABS(e_sub.longitude - v.lon) + ABS(e_sub.latitude - v.lat)
        ) = 1
      ) closest_weather
      ON e.id = closest_weather.id
      WHEN MATCHED THEN
        UPDATE SET vs_value = closest_weather.vs_value
  """, yr.year, yr.year, yr.year, yr.year - 1);
END FOR;