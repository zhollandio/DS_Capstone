DECLARE vars ARRAY<STRING> DEFAULT [
  'bi','etr','fm1000','pet',
  'fm100','pr','rmax','rmin','sph',
  'srad','th','tmmn','tmmx','vpd','vs'
];

DECLARE i INT64 DEFAULT 0;
DECLARE v STRING;

WHILE i < ARRAY_LENGTH(vars) DO
  SET i = i + 1;
  SET v = vars[ORDINAL(i)];
  
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `code-for-planet.weather_db.%s`
    ( year INT64 )
    PARTITION BY RANGE_BUCKET(
      year, GENERATE_ARRAY(2003, 2017, 1)   -- Boundaries: 2003, 2004, ..., 2016, 2017
    )
    OPTIONS(
      description = 'Auto-created range-partitioned table for %s',
      require_partition_filter = TRUE
    );
  """, v, v);
END WHILE;