
DECLARE vars ARRAY<STRING> DEFAULT [
  'fm1000','pet',
  'fm100','pr','rmax','rmin','sph',
  'srad','th','tmmn','tmmx','vpd','vs'
];

DECLARE var_columns ARRAY<STRING> DEFAULT [
  'dead_fuel_moisture_1000hr','potential_evapotranspiration',
  'dead_fuel_moisture_100hr','precipitation_amount','relative_humidity','relative_humidity','specific_humidity',
  'surface_downwelling_shortwave_flux_in_air','wind_from_direction','air_temperature','air_temperature','mean_vapor_pressure_deficit','wind_speed'
];

DECLARE years ARRAY<INT64> DEFAULT [
  2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015
];

DECLARE i INT64 DEFAULT 0;
DECLARE j INT64 DEFAULT 0;
DECLARE v STRING;
DECLARE col_name STRING;
DECLARE y INT64;

WHILE i < ARRAY_LENGTH(vars) DO
  SET i = i + 1;
  SET v = vars[ORDINAL(i)];
  
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `code-for-planet.weather_db.%s`
    (
      value FLOAT64,
      lon FLOAT64,
      lat FLOAT64,
      day INT64,
      crs INT64,
      year INT64
    )
    PARTITION BY RANGE_BUCKET(
      year, GENERATE_ARRAY(2003, 2017, 1)
    )
    OPTIONS(
      description = 'Weather data for %s',
      require_partition_filter = TRUE
    );
  """, v, v);
END WHILE;

SET i = 0;
SET j = 0;

WHILE i < ARRAY_LENGTH(vars) DO
  SET i = i + 1;
  SET v = vars[ORDINAL(i)];
  SET col_name = var_columns[ORDINAL(i)];
  SET j = 0;
  
  WHILE j < ARRAY_LENGTH(years) DO
    SET j = j + 1;
    SET y = years[ORDINAL(j)];
    
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE EXTERNAL TABLE `code-for-planet.weather_db.temp_ext_%s_%d`
      OPTIONS (
        format = 'PARQUET',
        uris = ['gs://data_housee/parquet_weather/%s_%d.parquet']
      );
    """, v, y, v, y);
    
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `code-for-planet.weather_db.%s`
      SELECT 
        %s as value,
        lon,
        lat,
        day,
        crs,
        %d as year
      FROM `code-for-planet.weather_db.temp_ext_%s_%d`;
    """, v, col_name, y, v, y);
    
    EXECUTE IMMEDIATE FORMAT("""
      DROP TABLE `code-for-planet.weather_db.temp_ext_%s_%d`;
    """, v, y);
    
    SELECT FORMAT('Loaded %s_%d.parquet into %s table', v, y, v) AS status;
    
  END WHILE;
END WHILE;

SELECT 'Data loading complete!' AS final_status;