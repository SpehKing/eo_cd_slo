"""
Download cloud-free Sentinel-2 RGB images (July 2022-2024, Ljubljana, 3×3 grid of 5×5 km areas)
and save one GeoTIFF per year per grid cell.

Requires:
  pip install sentinelhub==3.10.1 eolearn-core eolearn-io eolearn-features rasterio numpy
"""

import os
from datetime import timedelta

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from sentinelhub import BBox, CRS, DataCollection, SHConfig
from eolearn.core import EOWorkflow, FeatureType, OutputTask, linearly_connect_tasks
from eolearn.features import SimpleFilterTask
from eolearn.io import SentinelHubInputTask


# ---------------------------------------------------------------------------
# 1.  ▒▓▒  CONFIGURATION  ▒▓▒
# ---------------------------------------------------------------------------

# 1.1  Sentinel Hub credentials ------------------------------------------------
SH_CLIENT_ID = "7a94e1d7-08c0-41ca-90ea-9d6f3e82fec9"
SH_CLIENT_SECRET = "RcFpwPk0gArvUa08hrQZ2KixatclzogR"

config = SHConfig()
config.sh_client_id = SH_CLIENT_ID
config.sh_client_secret = SH_CLIENT_SECRET
config.save()  # ~/.sentinelhub/config.json

# 1.2  Area Of Interest (Ljubljana centre, 3×3 grid of 5×5 km squares) --------
# One degree ≈ 111 km lat;  cos(lat) * 111 km lon
LAT_CENTRE, LON_CENTRE = 46.0569, 14.5058
cell_size_km = 5  # Each grid cell is 5×5 km
dx_deg_cell = (
    cell_size_km * 1000 / (111_000 * np.cos(np.deg2rad(LAT_CENTRE)))
)  # cell width in degrees
dy_deg_cell = cell_size_km * 1000 / 111_000  # cell height in degrees

# Create 3×3 grid centered at Ljubljana
grid_bboxes = []
grid_names = []

for row in range(3):
    for col in range(3):
        # Calculate offset from center (center is at row=1, col=1)
        lat_offset = (1 - row) * dy_deg_cell  # row 0 is north, row 2 is south
        lon_offset = (col - 1) * dx_deg_cell  # col 0 is west, col 2 is east

        # Calculate cell center
        cell_lat = LAT_CENTRE + lat_offset
        cell_lon = LON_CENTRE + lon_offset

        # Create bounding box for this cell
        bbox = BBox(
            [
                cell_lon - dx_deg_cell / 2,
                cell_lat - dy_deg_cell / 2,
                cell_lon + dx_deg_cell / 2,
                cell_lat + dy_deg_cell / 2,
            ],
            crs=CRS.WGS84,
        )

        grid_bboxes.append(bbox)
        grid_names.append(f"grid_{row}_{col}")

# 1.3  Output directory --------------------------------------------------------
OUTPUT_DIR = os.path.join(".", "outputs", "geotiffs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# 2.  ▒▓▒  EO-LEARN WORKFLOW  ▒▓▒
# ---------------------------------------------------------------------------


def build_workflow():
    add_data_task = SentinelHubInputTask(
        data_collection=DataCollection.SENTINEL2_L2A,
        bands_feature=(FeatureType.DATA, "RGB"),
        bands=["B04", "B03", "B02"],
        time_difference=timedelta(hours=2),
        resolution=10,
        additional_data=[(FeatureType.MASK, "CLM")],
    )

    class AllClearPredicate:
        """Accept the acquisition only if every pixel in the CLM mask is 0 (cloud-free)."""

        def __call__(self, clm):
            return np.all(clm == 0)

    filter_task = SimpleFilterTask((FeatureType.MASK, "CLM"), AllClearPredicate())
    workflow_nodes = linearly_connect_tasks(
        add_data_task, filter_task, OutputTask("eopatch")
    )
    return EOWorkflow(workflow_nodes), workflow_nodes[0]


workflow, input_node = build_workflow()


# ---------------------------------------------------------------------------
# 3.  ▒▓▒  RUN FOR EACH YEAR AND GRID CELL  ▒▓▒
# ---------------------------------------------------------------------------

for year in (2018, 2019, 2020, 2021, 2022, 2023, 2024):
    time_interval = (f"{year}-06-01", f"{year}-08-31")

    for grid_idx, (bbox, grid_name) in enumerate(zip(grid_bboxes, grid_names)):
        print(f"[{year}] Processing {grid_name} (cell {grid_idx + 1}/9)")

        # 3.1  Execute workflow ----------------------------------------------------
        result = workflow.execute(
            {input_node: {"bbox": bbox, "time_interval": time_interval}}
        )
        eopatch = result.outputs["eopatch"]

        if not eopatch.timestamp:
            print(
                f"[{year}] {grid_name}: No cloud-free acquisitions found in June-August."
            )
            continue

        # 3.2  Take the first cloud-free scene (or change to median/mean if desired)
        rgb = eopatch.data["RGB"][0]  # shape (h, w, 3)
        ts = eopatch.timestamp[0]  # datetime object

        # 3.3  Prepare GeoTIFF -----------------------------------------------------
        rgb_8bit = np.clip(rgb * 2.5 * 255, 0, 255).astype(np.uint8)  # stretch + cast
        rgb_8bit = np.transpose(rgb_8bit, (2, 0, 1))  # (bands, h, w)

        transform = from_bounds(
            bbox.min_x,
            bbox.min_y,
            bbox.max_x,
            bbox.max_y,
            rgb_8bit.shape[2],
            rgb_8bit.shape[1],
        )

        fname = f"sentinel2_rgb_{grid_name}_{ts.strftime('%Y%m%d_%H%M%S')}.tif"
        path = os.path.join(OUTPUT_DIR, fname)

        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=rgb_8bit.shape[2],
            height=rgb_8bit.shape[1],
            count=3,
            dtype=rgb_8bit.dtype,
            crs=bbox.crs.pyproj_crs(),
            transform=transform,
        ) as dst:
            dst.write(rgb_8bit)

        print(f"[{year}] {grid_name}: Saved {path}")

print("Done.")
