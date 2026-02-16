-- View for hourly weather facts with dimensions joined
DROP VIEW IF EXISTS dw.v_weather_hourly CASCADE;

CREATE VIEW dw.v_weather_hourly AS
SELECT
  t.ts_utc,
  t.date,
  t.hour,
  l.latitude,
  l.longitude,
  f.temperature_c,
  f.precipitation_mm,
  f.precip_probability_pct,
  f.windspeed_kmh,
  f.weathercode,
  f.humidity_pct,
  f.pressure_hpa,
  f.visibility_m,
  f.uv_index,
  f.fetched_at_utc
FROM dw.fact_weather_hourly f
JOIN dw.dim_time t ON t.time_id = f.time_id
JOIN dw.dim_location l ON l.location_id = f.location_id;
