-- Raw layer (keep what the API returned with minimal assumptions)
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.open_meteo_hourly (
  id            BIGSERIAL PRIMARY KEY,
  latitude      DOUBLE PRECISION NOT NULL,
  longitude     DOUBLE PRECISION NOT NULL,
  timezone      TEXT NOT NULL,
  fetched_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload       JSONB NOT NULL
);

-- A small dimensional model (star-ish) for analytics
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_location (
  location_id   BIGSERIAL PRIMARY KEY,
  latitude      DOUBLE PRECISION NOT NULL,
  longitude     DOUBLE PRECISION NOT NULL,
  timezone      TEXT NOT NULL,
  UNIQUE(latitude, longitude, timezone)
);

CREATE TABLE IF NOT EXISTS dw.dim_time (
  time_id       BIGSERIAL PRIMARY KEY,
  ts_utc        TIMESTAMPTZ NOT NULL UNIQUE,
  date          DATE NOT NULL,
  hour          SMALLINT NOT NULL,
  day_of_week   SMALLINT NOT NULL,
  month         SMALLINT NOT NULL,
  year          SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.fact_weather_hourly (
  location_id   BIGINT NOT NULL REFERENCES dw.dim_location(location_id),
  time_id       BIGINT NOT NULL REFERENCES dw.dim_time(time_id),
  temperature_c DOUBLE PRECISION NULL,
  precipitation_mm DOUBLE PRECISION NULL,
  windspeed_kmh DOUBLE PRECISION NULL,
  weathercode   INT NULL,
  fetched_at_utc TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (location_id, time_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_fetched_at ON raw.open_meteo_hourly (fetched_at_utc);
CREATE INDEX IF NOT EXISTS idx_fact_fetched_at ON dw.fact_weather_hourly (fetched_at_utc);