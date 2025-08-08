-- ─────────────────────────────────────────────────────────────
-- Add / back-fill obs_date in all weather tables 2002-2017
-- ─────────────────────────────────────────────────────────────
DECLARE w_tables ARRAY<STRING> DEFAULT [
  'tmmn', 'tmmx', 'fm1000', 'fm100',
  'vpd', 'pet', 'pr', 'rmax',
  'rmin', 'sph', 'srad', 'th', 'vs'
];

FOR t IN (SELECT * FROM UNNEST(w_tables) AS tbl)
DO
  -- Step 1: add obs_date 
  EXECUTE IMMEDIATE FORMAT("""
    ALTER TABLE `weather_db.%s`
    ADD COLUMN IF NOT EXISTS obs_date DATE
  """, t.tbl);

  EXECUTE IMMEDIATE FORMAT("""
    UPDATE  `weather_db.%s`
    SET     obs_date = DATE(TIMESTAMP_MICROS(CAST(day / 1000 AS INT64)))
    WHERE   obs_date IS NULL               -- skip rows already filled
      AND   year BETWEEN 2002 AND 2017     -- respects partition filter
  """, t.tbl);
END FOR;
