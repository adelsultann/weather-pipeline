CREATE TABLE IF NOT EXISTS dw.fact_weather_daily (
  location_id BIGINT NOT NULL REFERENCES dw.dim_location(location_id),
  date DATE NOT NULL,
  temp_max_c DOUBLE PRECISION,
  temp_min_c DOUBLE PRECISION,
  precip_sum_mm DOUBLE PRECISION,
  weathercode INTEGER,
  uv_index_max DOUBLE PRECISION,
  sunrise_local TIMESTAMPTZ,
  sunset_local TIMESTAMPTZ,
  fetched_at_utc TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (location_id, date)
);