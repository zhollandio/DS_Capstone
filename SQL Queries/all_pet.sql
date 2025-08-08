-- ═══════════════════════════════════════════════════════════════
-- PET (Potential Evapotranspiration)
-- ═══════════════════════════════════════════════════════════════

FOR yr IN (SELECT year FROM UNNEST(GENERATE_ARRAY(2003, 2016)) AS year) DO
  EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `emission_db.emission_%d`
      ADD COLUMN IF NOT EXISTS pet_value FLOAT64
  """, yr.year);
  
  EXECUTE IMMEDIATE FORMAT("""
      MERGE `emission_db.emission_%d` e
      USING (
        SELECT 
          e_sub.id,
          p.value as pet_value
        FROM `emission_db.emission_%d` e_sub
        JOIN `weather_db.pet` p ON
          ABS(e_sub.longitude - p.lon) < 0.05
          AND ABS(e_sub.latitude - p.lat) < 0.05
          AND e_sub.fire_date = p.obs_date
          AND (p.year = %d OR p.year = %d)
        WHERE e_sub.pet_value IS NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY e_sub.id 
          ORDER BY ABS(e_sub.longitude - p.lon) + ABS(e_sub.latitude - p.lat)
        ) = 1
      ) closest_weather
      ON e.id = closest_weather.id
      WHEN MATCHED THEN
        UPDATE SET pet_value = closest_weather.pet_value
  """, yr.year, yr.year, yr.year, yr.year - 1);
END FOR;