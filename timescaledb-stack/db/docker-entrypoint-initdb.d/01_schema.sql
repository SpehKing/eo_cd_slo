-- ==============================================
-- schema.sql   (TimescaleDB + PostGIS, no alg-versions)
-- ==============================================

BEGIN;

------------------------------------------------------------
-- 1. Required extensions
------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis;

------------------------------------------------------------
-- 2. Master imagery table  : eo
------------------------------------------------------------
CREATE TABLE eo (
    id     BIGSERIAL,                           -- surrogate key
    time   TIMESTAMPTZ              NOT NULL,   -- observation time
    bbox   GEOGRAPHY(POLYGON, 4326) NOT NULL,   -- footprint

    -- sensor bands (opaque blobs)
    b01    BYTEA,
    b02    BYTEA,
    b03    BYTEA,
    b04    BYTEA,
    b05    BYTEA,
    b06    BYTEA,
    b07    BYTEA,
    b08    BYTEA,
    b8a    BYTEA,
    b09    BYTEA,
    b10    BYTEA,
    b11    BYTEA,
    b12    BYTEA,
    
    -- Only composite primary key including the partitioning column
    CONSTRAINT eo_pk PRIMARY KEY (id, time)
);

-- Create hypertable AFTER the table is created with proper primary key
SELECT create_hypertable('eo', 'time');

CREATE INDEX IF NOT EXISTS eo_time_idx  ON eo (time);
CREATE INDEX IF NOT EXISTS eo_bbox_gix ON eo USING GIST (bbox);
CREATE INDEX IF NOT EXISTS eo_id_idx ON eo (id); -- Add index for lookups

------------------------------------------------------------
-- 3. Pairwise change-mask table : eo_change
--    (one row per unique (img_a, img_b) pair)
------------------------------------------------------------
CREATE TABLE eo_change (
    img_a_id     BIGINT NOT NULL,
    img_b_id     BIGINT NOT NULL,

    period_start TIMESTAMPTZ NOT NULL,          -- LEAST(time_a,time_b)
    period_end   TIMESTAMPTZ NOT NULL,          -- GREATEST(time_a,time_b)
    bbox         GEOGRAPHY(POLYGON, 4326) NOT NULL,
    mask         BYTEA NOT NULL,                -- neural-net output

    -- Composite primary key including the partitioning column
    CONSTRAINT eo_change_pk       PRIMARY KEY (img_a_id, img_b_id, period_start),
    CONSTRAINT img_a_lt_img_b     CHECK (img_a_id < img_b_id)
    -- Note: Foreign key constraints removed due to TimescaleDB partitioning limitations
);

SELECT create_hypertable('eo_change', 'period_start');

CREATE INDEX IF NOT EXISTS eo_change_bbox_gix       ON eo_change USING GIST (bbox);
CREATE INDEX IF NOT EXISTS eo_change_period_end_idx ON eo_change (period_end);
CREATE INDEX IF NOT EXISTS eo_change_img_a_idx      ON eo_change (img_a_id);
CREATE INDEX IF NOT EXISTS eo_change_img_b_idx      ON eo_change (img_b_id);

------------------------------------------------------------
-- 4. Optional helper: previous / next pointers
------------------------------------------------------------
CREATE MATERIALIZED VIEW eo_prev_next AS
WITH ordered AS (
  SELECT id,
         LAG(id)  OVER (PARTITION BY bbox ORDER BY time) AS prev_id,
         LEAD(id) OVER (PARTITION BY bbox ORDER BY time) AS next_id
  FROM   eo
)
SELECT *
FROM   ordered
WITH NO DATA;      -- populate later via REFRESH MATERIALIZED VIEW

COMMIT;