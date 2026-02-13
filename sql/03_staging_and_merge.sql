-- Staging schema
CREATE SCHEMA IF NOT EXISTS stg;

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

CREATE TABLE IF NOT EXISTS stg.fact_weather_hourly (
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  timezone TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  temperature_c DOUBLE PRECISION NULL,
  precipitation_mm DOUBLE PRECISION NULL,
  windspeed_kmh DOUBLE PRECISION NULL,
  weathercode INT NULL,
  fetched_at_utc TIMESTAMPTZ NOT NULL
);

-- Helpful indexes on staging (optional but good)
CREATE INDEX IF NOT EXISTS idx_stg_time_ts ON stg.dim_time(ts_utc);
CREATE INDEX IF NOT EXISTS idx_stg_fact_ts ON stg.fact_weather_hourly(ts_utc);

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

-- MERGE fact (resolve IDs via joins)
MERGE INTO dw.fact_weather_hourly AS tgt
USING (
  SELECT
    loc.location_id,
    t.time_id,
    f.temperature_c,
    f.precipitation_mm,
    f.windspeed_kmh,
    f.weathercode,
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
    fetched_at_utc = src.fetched_at_utc
WHEN NOT MATCHED THEN
  INSERT (location_id, time_id, temperature_c, precipitation_mm, windspeed_kmh, weathercode, fetched_at_utc)
  VALUES (src.location_id, src.time_id, src.temperature_c, src.precipitation_mm, src.windspeed_kmh, src.weathercode, src.fetched_at_utc);

-- Clean staging after merge (keep it simple)
TRUNCATE TABLE stg.dim_location;
TRUNCATE TABLE stg.dim_time;
TRUNCATE TABLE stg.fact_weather_hourly;