-- ═══════════════════════════════════════════════════════════════
-- SPH (Specific Humidity)
-- ═══════════════════════════════════════════════════════════════

FOR yr IN (SELECT year FROM UNNEST(GENERATE_ARRAY(2003, 2016)) AS year) DO
  EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `emission_db.emission_%d`
      ADD COLUMN IF NOT EXISTS sph_value FLOAT64
  """, yr.year);
  
  EXECUTE IMMEDIATE FORMAT("""
      MERGE `emission_db.emission_%d` e
      USING (
        SELECT 
          e_sub.id,
          s.value as sph_value
        FROM `emission_db.emission_%d` e_sub
        JOIN `weather_db.sph` s ON
          ABS(e_sub.longitude - s.lon) < 0.05
          AND ABS(e_sub.latitude - s.lat) < 0.05
          AND e_sub.fire_date = s.obs_date
          AND (s.year = %d OR s.year = %d)
        WHERE e_sub.sph_value IS NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY e_sub.id 
          ORDER BY ABS(e_sub.longitude - s.lon) + ABS(e_sub.latitude - s.lat)
        ) = 1
      ) closest_weather
      ON e.id = closest_weather.id
      WHEN MATCHED THEN
        UPDATE SET sph_value = closest_weather.sph_value
  """, yr.year, yr.year, yr.year, yr.year - 1);
END FOR;