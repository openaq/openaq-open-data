-- create a table that will be used to keep track of
-- what has been upd
CREATE SEQUENCE IF NOT EXISTS open_data_audit_logs_sq START 10;
CREATE TABLE IF NOT EXISTS open_data_audit_logs (
  open_data_audit_logs_id int PRIMARY KEY DEFAULT nextval('open_data_audit_logs_sq')
  , sensor_nodes_id int NOT NULL REFERENCES sensor_nodes ON DELETE CASCADE
  , day date NOT NULL
  , modified_on timestamptz  -- when was this date last modidified
  , queued_on timestamptz    -- when did we last queue up a change
  , exported_on timestamptz  -- and when did we last finish exporting
  , metadata json
  , UNIQUE(sensor_nodes_id, day)
);
