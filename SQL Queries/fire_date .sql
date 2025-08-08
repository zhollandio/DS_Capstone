-- ─────────────────────────────────────────────────────────
-- Add / populate fire_date in emission_2003 - emission_2015
-- ─────────────────────────────────────────────────────────
FOR yr IN (SELECT year FROM UNNEST(GENERATE_ARRAY(2003, 2015)) AS year) DO
  -- add column
  EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `emission_db.emission_%d`
      ADD COLUMN IF NOT EXISTS fire_date DATE
  """, yr.year);
  --  fill column
  EXECUTE IMMEDIATE FORMAT("""
      UPDATE `emission_db.emission_%d`
      SET    fire_date = DATE_ADD(DATE(year, 1, 1), INTERVAL doy - 1 DAY)
      WHERE  fire_date IS NULL
  """, yr.year);
END FOR;