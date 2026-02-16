CREATE TABLE IF NOT EXISTS dw.fact_air_quality_hourly (
  location_id BIGINT NOT NULL REFERENCES dw.dim_location(location_id),
  time_id BIGINT NOT NULL REFERENCES dw.dim_time(time_id),
  european_aqi DOUBLE PRECISION,
  us_aqi DOUBLE PRECISION,
  pm10 DOUBLE PRECISION,
  pm2_5 DOUBLE PRECISION,
  no2 DOUBLE PRECISION,
  o3 DOUBLE PRECISION,
  so2 DOUBLE PRECISION,
  co DOUBLE PRECISION,
  fetched_at_utc TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (location_id, time_id)
);