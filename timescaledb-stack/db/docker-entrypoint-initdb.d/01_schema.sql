CREATE EXTENSION postgis;

CREATE EXTENSION timescaledb;

CREATE TABLE eo (
  time   TIMESTAMPTZ                     NOT NULL,
  bbox   GEOGRAPHY(POLYGON, 4326)        NOT NULL,
  image  BYTEA                           NOT NULL
);

SELECT create_hypertable('eo', 'time');

CREATE INDEX IF NOT EXISTS eo_bbox_gix ON eo USING gist (bbox);