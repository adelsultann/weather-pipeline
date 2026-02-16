-- Staging schema
CREATE SCHEMA IF NOT EXISTS stg;

-- 1) DIMENSIONS STAGING
CREATE TABLE IF NOT EXISTS stg.dim_location (
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  timezone TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.dim_time (
  ts_utc TIMESTAMPTZ NOT NULL,
  date DATE NOT NULL,
  hour SMALLINT NOT NULL,
  day_of_week SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  year SMALLINT NOT NULL
);

-- 2) WEATHER HOURLY STAGING
CREATE TABLE IF NOT EXISTS stg.fact_weather_hourly (
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  timezone TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  temperature_c DOUBLE PRECISION NULL,
  precipitation_mm DOUBLE PRECISION NULL,
  windspeed_kmh DOUBLE PRECISION NULL,
  weathercode INT NULL,
  humidity_pct DOUBLE PRECISION,
  pressure_hpa DOUBLE PRECISION,
  visibility_m DOUBLE PRECISION,
  uv_index DOUBLE PRECISION,
  precip_probability_pct DOUBLE PRECISION,
  fetched_at_utc TIMESTAMPTZ NOT NULL
);

-- 3) WEATHER DAILY STAGING
CREATE TABLE IF NOT EXISTS stg.fact_weather_daily (
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  timezone TEXT NOT NULL,
  date DATE NOT NULL,
  temp_max_c DOUBLE PRECISION,
  temp_min_c DOUBLE PRECISION,
  precip_sum_mm DOUBLE PRECISION,
  weathercode INTEGER,
  uv_index_max DOUBLE PRECISION,
  sunrise_local TIMESTAMPTZ,
  sunset_local TIMESTAMPTZ,
  fetched_at_utc TIMESTAMPTZ NOT NULL
);

-- 4) AIR QUALITY HOURLY STAGING
CREATE TABLE IF NOT EXISTS stg.fact_air_quality_hourly (
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  timezone TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  european_aqi DOUBLE PRECISION,
  us_aqi DOUBLE PRECISION,
  pm10 DOUBLE PRECISION,
  pm2_5 DOUBLE PRECISION,
  no2 DOUBLE PRECISION,
  o3 DOUBLE PRECISION,
  so2 DOUBLE PRECISION,
  co DOUBLE PRECISION,
  fetched_at_utc TIMESTAMPTZ NOT NULL
);

-- MERGE location
MERGE INTO dw.dim_location AS tgt
USING (
  SELECT DISTINCT latitude, longitude, timezone
  FROM stg.dim_location
) AS src
ON (tgt.latitude = src.latitude AND tgt.longitude = src.longitude AND tgt.timezone = src.timezone)
WHEN NOT MATCHED THEN
  INSERT (latitude, longitude, timezone)
  VALUES (src.latitude, src.longitude, src.timezone);

-- MERGE time
MERGE INTO dw.dim_time AS tgt
USING (
  SELECT DISTINCT ts_utc, date, hour, day_of_week, month, year
  FROM stg.dim_time
) AS src
ON (tgt.ts_utc = src.ts_utc)
WHEN NOT MATCHED THEN
  INSERT (ts_utc, date, hour, day_of_week, month, year)
  VALUES (src.ts_utc, src.date, src.hour, src.day_of_week, src.month, src.year);

-- MERGE fact_weather_hourly
MERGE INTO dw.fact_weather_hourly AS tgt
USING (
  SELECT
    loc.location_id,
    t.time_id,
    f.temperature_c,
    f.precipitation_mm,
    f.windspeed_kmh,
    f.weathercode,
    f.humidity_pct,
    f.pressure_hpa,
    f.visibility_m,
    f.uv_index,
    f.precip_probability_pct,
    f.fetched_at_utc
  FROM stg.fact_weather_hourly f
  JOIN dw.dim_location loc
    ON loc.latitude = f.latitude AND loc.longitude = f.longitude AND loc.timezone = f.timezone
  JOIN dw.dim_time t
    ON t.ts_utc = f.ts_utc
) AS src
ON (tgt.location_id = src.location_id AND tgt.time_id = src.time_id)
WHEN MATCHED THEN
  UPDATE SET
    temperature_c = src.temperature_c,
    precipitation_mm = src.precipitation_mm,
    windspeed_kmh = src.windspeed_kmh,
    weathercode = src.weathercode,
    humidity_pct = src.humidity_pct,
    pressure_hpa = src.pressure_hpa,
    visibility_m = src.visibility_m,
    uv_index = src.uv_index,
    precip_probability_pct = src.precip_probability_pct,
    fetched_at_utc = src.fetched_at_utc
WHEN NOT MATCHED THEN
  INSERT (location_id, time_id, temperature_c, precipitation_mm, windspeed_kmh, weathercode, humidity_pct, pressure_hpa, visibility_m, uv_index, precip_probability_pct, fetched_at_utc)
  VALUES (src.location_id, src.time_id, src.temperature_c, src.precipitation_mm, src.windspeed_kmh, src.weathercode, src.humidity_pct, src.pressure_hpa, src.visibility_m, src.uv_index, src.precip_probability_pct, src.fetched_at_utc);

-- MERGE fact_weather_daily
MERGE INTO dw.fact_weather_daily AS tgt
USING (
  SELECT
    loc.location_id,
    f.date,
    f.temp_max_c,
    f.temp_min_c,
    f.precip_sum_mm,
    f.weathercode,
    f.uv_index_max,
    f.sunrise_local,
    f.sunset_local,
    f.fetched_at_utc
  FROM stg.fact_weather_daily f
  JOIN dw.dim_location loc
    ON loc.latitude = f.latitude AND loc.longitude = f.longitude AND loc.timezone = f.timezone
) AS src
ON (tgt.location_id = src.location_id AND tgt.date = src.date)
WHEN MATCHED THEN
  UPDATE SET
    temp_max_c = src.temp_max_c,
    temp_min_c = src.temp_min_c,
    precip_sum_mm = src.precip_sum_mm,
    weathercode = src.weathercode,
    uv_index_max = src.uv_index_max,
    sunrise_local = src.sunrise_local,
    sunset_local = src.sunset_local,
    fetched_at_utc = src.fetched_at_utc
WHEN NOT MATCHED THEN
  INSERT (location_id, date, temp_max_c, temp_min_c, precip_sum_mm, weathercode, uv_index_max, sunrise_local, sunset_local, fetched_at_utc)
  VALUES (src.location_id, src.date, src.temp_max_c, src.temp_min_c, src.precip_sum_mm, src.weathercode, src.uv_index_max, src.sunrise_local, src.sunset_local, src.fetched_at_utc);

-- MERGE fact_air_quality_hourly
MERGE INTO dw.fact_air_quality_hourly AS tgt
USING (
  SELECT
    loc.location_id,
    t.time_id,
    f.european_aqi,
    f.us_aqi,
    f.pm10,
    f.pm2_5,
    f.no2,
    f.o3,
    f.so2,
    f.co,
    f.fetched_at_utc
  FROM stg.fact_air_quality_hourly f
  JOIN dw.dim_location loc
    ON loc.latitude = f.latitude AND loc.longitude = f.longitude AND loc.timezone = f.timezone
  JOIN dw.dim_time t
    ON t.ts_utc = f.ts_utc
) AS src
ON (tgt.location_id = src.location_id AND tgt.time_id = src.time_id)
WHEN MATCHED THEN
  UPDATE SET
    european_aqi = src.european_aqi,
    us_aqi = src.us_aqi,
    pm10 = src.pm10,
    pm2_5 = src.pm2_5,
    no2 = src.no2,
    o3 = src.o3,
    so2 = src.so2,
    co = src.co,
    fetched_at_utc = src.fetched_at_utc
WHEN NOT MATCHED THEN
  INSERT (location_id, time_id, european_aqi, us_aqi, pm10, pm2_5, no2, o3, so2, co, fetched_at_utc)
  VALUES (src.location_id, src.time_id, src.european_aqi, src.us_aqi, src.pm10, src.pm2_5, src.no2, src.o3, src.so2, src.co, src.fetched_at_utc);

-- Clean staging after merge (keep it simple)
TRUNCATE TABLE stg.dim_location;
TRUNCATE TABLE stg.dim_time;
TRUNCATE TABLE stg.fact_weather_hourly;
TRUNCATE TABLE stg.fact_weather_daily;
TRUNCATE TABLE stg.fact_air_quality_hourly;