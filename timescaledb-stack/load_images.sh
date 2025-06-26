#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# load_images.sh  –  bulk-load Sentinel-2 RGB GeoTIFFs into TimescaleDB
# ---------------------------------------------------------------------------
set -euo pipefail

# --- configuration ----------------------------------------------------------
if [[ -f .env ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs)
fi

DB_USER=${DB_USER:-postgres}
DB_NAME=${DB_NAME:-eo_db}
PROJECT_NAME=${PROJECT_NAME:-timescaledb}
CONTAINER="${PROJECT_NAME}_db"
# Ljubljana area - 3x3 grid of 5x5 km cells centered at 46.0569, 14.5058
ROI="ST_MakeEnvelope(14.4346,45.9856,14.5770,46.1282,4326)::GEOGRAPHY"

echo "Using the following configuration:"
echo "  DB_USER     : $DB_USER"
echo "  DB_NAME     : $DB_NAME"
echo "  CONTAINER   : $CONTAINER"
echo ""

# --- wait until DB is ready --------------------------------------------------
echo -n "Waiting for database to become ready"
until docker exec "$CONTAINER" \
        pg_isready -U "$DB_USER" -d "$DB_NAME" &>/dev/null; do
  printf '.'
  sleep 2
done
echo " done."

# --- copy GeoTIFFs -----------------------------------------------------------
echo "Copying GeoTIFFs into container …"
shopt -s nullglob
FILES=(outputs/geotiffs/sentinel2_rgb_*.tif)
(( ${#FILES[@]} > 0 )) || { echo "❌  No matching GeoTIFFs found"; exit 1; }
for f in "${FILES[@]}"; do
  docker cp "$f" "$CONTAINER":/tmp/
done

# --- insert rows -------------------------------------------------------------
echo "Inserting metadata + imagery …"
docker exec -u "$DB_USER" "$CONTAINER" bash -euo pipefail -c '
for f in /tmp/sentinel2_rgb_*.tif; do
  fn=$(basename "$f")          # sentinel2_rgb_grid_0_0_20180605_100610.tif
  base=${fn%.tif}              # drop extension

  # --- robust timestamp extraction ------------------------------------------
  # Take the *last* two underscore-separated fields (YYYYMMDD and HHMMSS)
  IFS="_" read -r -a parts <<< "$base"
  len=${#parts[@]}
  dp=${parts[len-2]}           # YYYYMMDD
  tp=${parts[len-1]}           # HHMMSS

  # Reformat: 20180605 100610 -> 2018-06-05 10:06:10+00
  ts="${dp:0:4}-${dp:4:2}-${dp:6:2} ${tp:0:2}:${tp:2:2}:${tp:4:2}+00"

  psql -q -d "'"$DB_NAME"'" -c "
    INSERT INTO eo(time, bbox, image)
    VALUES (
      '\''${ts}'\'',
      '"$ROI"',
      pg_read_binary_file('\''${f}'\'')
    );
  "
done
'

echo "✅  Load finished successfully."
