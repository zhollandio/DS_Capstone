FOR yr IN (SELECT year FROM UNNEST(GENERATE_ARRAY(2003, 2016)) AS year) DO
  EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `emission_db.emission_%d`
      ADD COLUMN IF NOT EXISTS fm100_value FLOAT64
  """, yr.year);
  
  EXECUTE IMMEDIATE FORMAT("""
      MERGE `emission_db.emission_%d` e
      USING (
        SELECT 
          e_sub.id,
          f.value as fm100_value
        FROM `emission_db.emission_%d` e_sub
        JOIN `weather_db.fm100` f ON
          ABS(e_sub.longitude - f.lon) < 0.05
          AND ABS(e_sub.latitude - f.lat) < 0.05
          AND e_sub.fire_date = f.obs_date
          AND (f.year = %d OR f.year = %d)
        WHERE e_sub.fm100_value IS NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY e_sub.id 
          ORDER BY ABS(e_sub.longitude - f.lon) + ABS(e_sub.latitude - f.lat)
        ) = 1
      ) closest_weather
      ON e.id = closest_weather.id
      WHEN MATCHED THEN
        UPDATE SET fm100_value = closest_weather.fm100_value
  """, yr.year, yr.year, yr.year, yr.year - 1);
END FOR;