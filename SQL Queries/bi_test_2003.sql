-- ─────────────────────────────────────────────────────────
-- Update BI - adjusted tolerance to around 5kms + closest weather station
-- ─────────────────────────────────────────────────────────

MERGE `emission_db.emission_2003` e
USING (
  SELECT 
    e_sub.id,
    b.value as bi_value
  FROM `emission_db.emission_2003` e_sub
  JOIN `weather_db.bi` b ON
    ABS(e_sub.longitude - b.lon) < 0.05
    AND ABS(e_sub.latitude - b.lat) < 0.05
    AND e_sub.fire_date = b.obs_date
    AND b.year = 2003
  WHERE e_sub.bi_value IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY e_sub.id 
    ORDER BY ABS(e_sub.longitude - b.lon) + ABS(e_sub.latitude - b.lat)
  ) = 1
) closest_weather
ON e.id = closest_weather.id
WHEN MATCHED THEN
  UPDATE SET bi_value = closest_weather.bi_value;