
-- this table is for the incremental load
-- we get the last raw id from the etl_state table and then we read the raw table from that id to the end
-- and then we update the last raw id to the new last id

CREATE TABLE IF NOT EXISTS dw.etl_state (
  pipeline_name TEXT PRIMARY KEY,
  last_raw_id BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- initialize row if missing
INSERT INTO dw.etl_state (pipeline_name, last_raw_id)
VALUES ('weather_hourly', 0)
ON CONFLICT (pipeline_name) DO NOTHING;