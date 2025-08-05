-- ==============================================
-- schema_v2.sql   (TimescaleDB + PostGIS with grid partitioning)
-- ==============================================

BEGIN;

------------------------------------------------------------
-- 1. Required extensions
------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis;

------------------------------------------------------------
-- 2. Grid cells reference table (Slovenia 5km x 5km grid)
------------------------------------------------------------
CREATE TABLE grid_cells (
    grid_id     INTEGER PRIMARY KEY,           -- from 'index' field in GeoDataFrame
    index_x     INTEGER NOT NULL,              -- grid x coordinate
    index_y     INTEGER NOT NULL,              -- grid y coordinate
    geom        GEOMETRY(POLYGON, 3857) NOT NULL,  -- assuming Slovenia grid is in EPSG:3857
    bbox_4326   GEOGRAPHY(POLYGON, 4326)       -- converted to WGS84 for matching with eo.bbox
);

-- Spatial indices for fast lookups
CREATE INDEX grid_cells_geom_gix ON grid_cells USING GIST (geom);
CREATE INDEX grid_cells_bbox_4326_gix ON grid_cells USING GIST (bbox_4326);
CREATE INDEX grid_cells_xy_idx ON grid_cells (index_x, index_y);

------------------------------------------------------------
-- 3. Master imagery table : eo (now with grid partitioning)
------------------------------------------------------------
CREATE TABLE eo (
    id          SERIAL,                         -- surrogate key
    time        TIMESTAMPTZ NOT NULL,           -- observation time
    month       DATE NOT NULL,                  -- first day of month (for partitioning)
    grid_id     INTEGER NOT NULL,              -- FK to grid_cells
    bbox        GEOGRAPHY(POLYGON, 4326) NOT NULL,   -- footprint
    
    -- image metadata
    width       INTEGER,                        -- image width in pixels
    height      INTEGER,                        -- image height in pixels
    data_type   TEXT,                          -- data type (e.g., 'uint16', 'float32')
    
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
    
    -- Composite primary key including partitioning columns
    CONSTRAINT eo_pk PRIMARY KEY (id, time, grid_id),
    -- Foreign key to grid
    CONSTRAINT eo_grid_fk FOREIGN KEY (grid_id) REFERENCES grid_cells(grid_id),
    -- Unique constraint for one image per grid cell per month (including time for partitioning)
    CONSTRAINT eo_unique_grid_month UNIQUE (grid_id, month, time)
);

-- Create two-dimensional hypertable
SELECT create_hypertable('eo', 
    by_range('time', INTERVAL '1 month'),
    create_default_indexes => FALSE
);

-- Add space dimension based on grid_id
SELECT add_dimension('eo', 'grid_id', 
    number_partitions => 64  -- adjust based on total grid cells (~600 for Slovenia)
);

-- Create optimized indices
CREATE INDEX eo_time_idx ON eo (time);
CREATE INDEX eo_month_idx ON eo (month);
CREATE INDEX eo_grid_id_idx ON eo (grid_id);
CREATE INDEX eo_bbox_gix ON eo USING GIST (bbox);
CREATE INDEX eo_grid_month_idx ON eo (grid_id, month);

------------------------------------------------------------
-- 4. Function to validate and auto-populate month
------------------------------------------------------------
CREATE OR REPLACE FUNCTION eo_validate_and_populate() 
RETURNS TRIGGER 
LANGUAGE plpgsql AS $$
DECLARE
    grid_geom GEOGRAPHY(POLYGON, 4326);
    image_area NUMERIC;
    grid_area NUMERIC;
    overlap_area NUMERIC;
    overlap_percent NUMERIC;
BEGIN
    -- Auto-populate month from time
    NEW.month := date_trunc('month', NEW.time)::date;
    
    -- Get the grid cell geography
    SELECT bbox_4326 INTO grid_geom
    FROM grid_cells
    WHERE grid_id = NEW.grid_id;
    
    IF grid_geom IS NULL THEN
        RAISE EXCEPTION 'Invalid grid_id: %', NEW.grid_id;
    END IF;
    
    -- Check significant overlap instead of strict containment
    grid_area := ST_Area(grid_geom);
    overlap_area := ST_Area(ST_Intersection(grid_geom, NEW.bbox));
    overlap_percent := (overlap_area / grid_area) * 100;

    -- Require at least 99% overlap with the grid cell
    IF overlap_percent < 99 THEN
        RAISE EXCEPTION 'Image bbox must have at least 99%% overlap with grid cell % (current: %.1f%%)', 
            NEW.grid_id, overlap_percent;
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_eo_validate
BEFORE INSERT OR UPDATE ON eo
FOR EACH ROW EXECUTE FUNCTION eo_validate_and_populate();

------------------------------------------------------------
-- 5. Upsert function for conflict handling
------------------------------------------------------------
CREATE OR REPLACE FUNCTION upsert_eo(
    p_time TIMESTAMPTZ,
    p_grid_id INTEGER,
    p_bbox GEOGRAPHY(POLYGON, 4326),
    p_width INTEGER,
    p_height INTEGER,
    p_data_type TEXT,
    p_b01 BYTEA DEFAULT NULL,
    p_b02 BYTEA DEFAULT NULL,
    p_b03 BYTEA DEFAULT NULL,
    p_b04 BYTEA DEFAULT NULL,
    p_b05 BYTEA DEFAULT NULL,
    p_b06 BYTEA DEFAULT NULL,
    p_b07 BYTEA DEFAULT NULL,
    p_b08 BYTEA DEFAULT NULL,
    p_b8a BYTEA DEFAULT NULL,
    p_b09 BYTEA DEFAULT NULL,
    p_b10 BYTEA DEFAULT NULL,
    p_b11 BYTEA DEFAULT NULL,
    p_b12 BYTEA DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_month DATE;
    v_id INTEGER;
BEGIN
    v_month := date_trunc('month', p_time)::date;
    
    INSERT INTO eo (
        time, month, grid_id, bbox, width, height, data_type,
        b01, b02, b03, b04, b05, b06, b07, b08, b8a, b09, b10, b11, b12
    ) VALUES (
        p_time, v_month, p_grid_id, p_bbox, p_width, p_height, p_data_type,
        p_b01, p_b02, p_b03, p_b04, p_b05, p_b06, p_b07, p_b08, p_b8a, p_b09, p_b10, p_b11, p_b12
    )
    ON CONFLICT (grid_id, month, time) DO UPDATE SET
        bbox = EXCLUDED.bbox,
        width = EXCLUDED.width,
        height = EXCLUDED.height,
        data_type = EXCLUDED.data_type,
        b01 = EXCLUDED.b01,
        b02 = EXCLUDED.b02,
        b03 = EXCLUDED.b03,
        b04 = EXCLUDED.b04,
        b05 = EXCLUDED.b05,
        b06 = EXCLUDED.b06,
        b07 = EXCLUDED.b07,
        b08 = EXCLUDED.b08,
        b8a = EXCLUDED.b8a,
        b09 = EXCLUDED.b09,
        b10 = EXCLUDED.b10,
        b11 = EXCLUDED.b11,
        b12 = EXCLUDED.b12
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- 6. Updated change detection table
------------------------------------------------------------
CREATE TABLE eo_change (
    img_a_id     INTEGER NOT NULL,
    img_b_id     INTEGER NOT NULL,
    grid_id      INTEGER NOT NULL,             -- denormalized for performance
    
    period_start TIMESTAMPTZ NOT NULL,          -- LEAST(time_a,time_b)
    period_end   TIMESTAMPTZ NOT NULL,          -- GREATEST(time_a,time_b)
    bbox         GEOGRAPHY(POLYGON, 4326) NOT NULL,
    
    -- change mask metadata
    width        INTEGER,                       -- mask width in pixels
    height       INTEGER,                       -- mask height in pixels
    data_type    TEXT,                         -- data type (e.g., 'uint8', 'float32')
    
    mask         BYTEA NOT NULL,                -- neural-net output

    -- Composite primary key including the partitioning column
    CONSTRAINT eo_change_pk PRIMARY KEY (img_a_id, img_b_id, period_start),
    CONSTRAINT img_a_lt_img_b CHECK (img_a_id < img_b_id),
    CONSTRAINT eo_change_grid_fk FOREIGN KEY (grid_id) REFERENCES grid_cells(grid_id)
);

SELECT create_hypertable('eo_change', 
    by_range('period_start', INTERVAL '1 month')
);

CREATE INDEX eo_change_grid_id_idx ON eo_change (grid_id);
CREATE INDEX eo_change_bbox_gix ON eo_change USING GIST (bbox);
CREATE INDEX eo_change_period_end_idx ON eo_change (period_end);
CREATE INDEX eo_change_img_a_idx ON eo_change (img_a_id);
CREATE INDEX eo_change_img_b_idx ON eo_change (img_b_id);

------------------------------------------------------------
-- 7. Helper view for temporal navigation within grid cells
------------------------------------------------------------
CREATE MATERIALIZED VIEW eo_prev_next AS
WITH ordered AS (
  SELECT id,
         grid_id,
         time,
         month,
         LAG(id)  OVER (PARTITION BY grid_id ORDER BY time) AS prev_id,
         LEAD(id) OVER (PARTITION BY grid_id ORDER BY time) AS next_id
  FROM   eo
)
SELECT *
FROM   ordered
WITH NO DATA;

CREATE INDEX eo_prev_next_grid_id_idx ON eo_prev_next (grid_id);
CREATE INDEX eo_prev_next_month_idx ON eo_prev_next (month);

------------------------------------------------------------
-- 8. Function to prepopulate grid cells with empty months
------------------------------------------------------------
CREATE OR REPLACE FUNCTION prepopulate_grid_months(
    start_date DATE,
    end_date DATE
) RETURNS TABLE (grid_id INTEGER, month DATE) AS $$
BEGIN
    RETURN QUERY
    SELECT gc.grid_id, 
           generate_series(
               date_trunc('month', start_date::timestamp), 
               date_trunc('month', end_date::timestamp), 
               '1 month'::interval
           )::date AS month
    FROM grid_cells gc
    ORDER BY gc.grid_id, month;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- 9. Query helper functions
------------------------------------------------------------

-- Get all data for a specific month
CREATE OR REPLACE FUNCTION get_month_data(target_month DATE)
RETURNS TABLE (
    grid_id INTEGER,
    "time" TIMESTAMPTZ,
    bbox GEOGRAPHY(POLYGON, 4326),
    has_data BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        gc.grid_id,
        eo.time,
        COALESCE(eo.bbox, gc.bbox_4326) as bbox,
        (eo.id IS NOT NULL) as has_data
    FROM grid_cells gc
    LEFT JOIN eo ON gc.grid_id = eo.grid_id 
                 AND eo.month = target_month
    ORDER BY gc.grid_id;
END;
$$ LANGUAGE plpgsql;

-- Get data coverage statistics by month
CREATE OR REPLACE FUNCTION get_coverage_stats()
RETURNS TABLE (
    month DATE,
    total_cells INTEGER,
    filled_cells INTEGER,
    coverage_percent NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH grid_count AS (
        SELECT COUNT(*) as total FROM grid_cells
    ),
    monthly_coverage AS (
        SELECT 
            month,
            COUNT(DISTINCT grid_id) as filled
        FROM eo
        GROUP BY month
    )
    SELECT 
        mc.month,
        gc.total::INTEGER as total_cells,
        mc.filled::INTEGER as filled_cells,
        ROUND((mc.filled::NUMERIC / gc.total) * 100, 2) as coverage_percent
    FROM monthly_coverage mc
    CROSS JOIN grid_count gc
    ORDER BY mc.month DESC;
END;
$$ LANGUAGE plpgsql;

COMMIT;