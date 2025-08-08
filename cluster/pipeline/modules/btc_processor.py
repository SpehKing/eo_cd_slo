#!/usr/bin/env python3
"""
BTC Processing Module for Pipeline

Combines functionality from Analyze_tiff.ipynb and BTC_pipeline_v2.ipynb
into an integrated pipeline module that generates change detection masks
using the BTC model.
"""

import asyncio
import logging
import sys
import os
from pathlib import Path
import numpy as np
import torch
import matplotlib.pyplot as plt
from PIL import Image
import rasterio
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
import json
import psycopg2

# Clean path resolution for BTC imports - exactly like in the working Jupyter notebook
current_file = Path(__file__).resolve()
pipeline_root = current_file.parent.parent  # Go up to pipeline/
cluster_root = pipeline_root.parent  # Go up to cluster/

# Add cluster directory to path for BTC imports (same as notebook)
if str(cluster_root) not in sys.path:
    sys.path.insert(0, str(cluster_root))

# Import BTC modules directly (same as notebook)
from configs.config_parser import get_parser
from models.finetune_framework import FinetuneFramework
from ml_dependencies.transforms import build_transforms

from ..config.settings import config, ProcessingMode
from ..utils.state_manager import state_manager, TaskStatus


class BTCProcessorV5:
    """BTC change detection processor with state management"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.BTCProcessorV5")
        self.model = None
        self.transforms = None
        self.device = None
        self.btc_config = None
        self.current_year = None

    def get_mask_output_path(
        self, img_a_path: Path, img_b_path: Path, year: int
    ) -> Path:
        """Build the destination path for the generated mask.
        Uses naming convention:
        - data/masks/{year}/change_mask_grid_{grid_id}_{year}.png
        Falls back to a generic name if parsing fails.
        """
        try:
            parts = img_a_path.stem.split("_")
            # Expecting stem like: sentinel2_grid_{grid}_{year}_08
            grid_id = int(parts[2]) if len(parts) >= 4 else None
        except Exception:
            grid_id = None

        masks_dir = config.get_year_masks_dir(year)
        if grid_id is not None:
            filename = f"change_mask_grid_{grid_id}_{year}.png"
        else:
            # Fallback, include stems to avoid collisions
            filename = f"change_mask_{year}_{img_a_path.stem}_to_{img_b_path.stem}.png"
        return masks_dir / filename

    async def initialize(self) -> bool:
        """Initialize BTC model and transforms"""
        try:
            self.logger.info("Initializing BTC model...")

            # Load BTC configuration
            parser = get_parser()
            self.btc_config = parser.parse_args(["--config", config.btc_config_path])

            # Override some settings for inference
            self.btc_config.eval_only = True
            self.btc_config.ckpt_path = config.btc_model_checkpoint
            self.btc_config.devices = "auto"
            self.btc_config.dev = False
            self.btc_config.wandb_proj = "None"
            self.btc_config.vis_path = False

            self.logger.info(f"BTC Config loaded: {config.btc_config_path}")
            self.logger.info(f"Model checkpoint: {config.btc_model_checkpoint}")
            self.logger.info(f"Image size: {config.btc_image_size}")

            # Build transforms
            self.transforms = build_transforms(
                self.btc_config, pretrain=False, test=True, has_mask=False
            )

            # Log the transforms to verify normalization is included
            self.logger.info("BTC transforms built successfully")
            self.logger.info(f"Transform pipeline: {self.transforms.transforms}")

            # Extract and log normalization parameters
            normalize_transform = None
            for transform in self.btc_config.train.transforms:
                if "Normalize" in transform:
                    normalize_transform = transform["Normalize"]
                    break

            if normalize_transform:
                self.logger.info(f"✓ Normalization found in config:")
                self.logger.info(f"  Mean: {normalize_transform['mean']}")
                self.logger.info(f"  Std: {normalize_transform['std']}")
            else:
                self.logger.warning("⚠️ No normalization transform found in BTC config!")
                self.logger.warning("This could cause inference differences!")

            # Set device
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.logger.info(f"Using device: {self.device}")

            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize BTC processor: {e}")
            return False

    async def load_model(self) -> bool:
        """Load the BTC model"""
        try:
            self.logger.info("Loading BTC model...")

            # Create metrics collection (required for model loading)
            from torchmetrics import MetricCollection
            from torchmetrics.classification import (
                BinaryF1Score,
                BinaryRecall,
                BinaryPrecision,
                BinaryJaccardIndex,
            )

            metrics = MetricCollection(
                {
                    "F1": BinaryF1Score(),
                    "Recall": BinaryRecall(),
                    "Precision": BinaryPrecision(),
                    "cIoU": BinaryJaccardIndex(),
                }
            )

            # Load model from HuggingFace
            self.logger.info(
                f"Loading model from HuggingFace: {config.btc_model_checkpoint}"
            )

            self.model = FinetuneFramework.from_pretrained(
                config.btc_model_checkpoint,
                metrics=metrics,
                logger=None,
                config_namespace=self.btc_config,
            )

            # Set device and evaluation mode
            self.model = self.model.to(self.device)
            self.model.eval()

            # Print model info
            total_params = sum(p.numel() for p in self.model.parameters())
            self.logger.info(f"Model loaded successfully!")
            self.logger.info(f"Total parameters: {total_params:,}")
            self.logger.info(f"Model set to evaluation mode on {self.device}")

            return True

        except Exception as e:
            self.logger.error(f"Failed to load BTC model: {e}")
            return False

    def find_image_pairs_for_year(self, year: int) -> List[Tuple[Path, Path]]:
        """Find consecutive image pairs for a specific year"""
        pairs = []

        try:
            # Get all years to find consecutive pairs
            all_years = sorted(config.years)

            if config.mode == ProcessingMode.LOCAL_ONLY:
                current_year_dir = config.get_year_images_dir(year)

                # Find next year for pairing
                current_index = all_years.index(year)
                if current_index < len(all_years) - 1:
                    next_year = all_years[current_index + 1]
                    next_year_dir = config.get_year_images_dir(next_year)

                    # Find image pairs between consecutive years
                    for grid_id in config.grid_ids:
                        current_pattern = f"sentinel2_grid_{grid_id}_{year}_08.*"
                        next_pattern = f"sentinel2_grid_{grid_id}_{next_year}_08.*"

                        current_files = list(current_year_dir.glob(current_pattern))
                        next_files = list(next_year_dir.glob(next_pattern))

                        if current_files and next_files:
                            # Take the first match for each pattern
                            current_file = current_files[0]
                            next_file = next_files[0]
                            pairs.append((current_file, next_file))

            else:
                # For database mode, look in temp directory
                temp_dir = config.images_dir / "temp"
                if temp_dir.exists():
                    for grid_id in config.grid_ids:
                        # Look for consecutive year pairs
                        current_index = all_years.index(year)
                        if current_index < len(all_years) - 1:
                            next_year = all_years[current_index + 1]

                            current_pattern = f"sentinel2_grid_{grid_id}_{year}_08.*"
                            next_pattern = f"sentinel2_grid_{grid_id}_{next_year}_08.*"

                            current_files = list(temp_dir.glob(current_pattern))
                            next_files = list(temp_dir.glob(next_pattern))

                            if current_files and next_files:
                                pairs.append((current_files[0], next_files[0]))

            self.logger.info(f"Found {len(pairs)} image pairs for year {year}")
            return pairs

        except Exception as e:
            self.logger.error(f"Error finding image pairs for year {year}: {e}")
            return []

    def convert_tiff_to_png(
        self, tiff_path: Path, target_size: int = 256
    ) -> Tuple[Optional[np.ndarray], Optional[Dict]]:
        """Convert TIFF to PNG format and resize
        Deterministically map Sentinel-2 bands B02,B03,B04 -> RGB (R,G,B) = [B04,B03,B02].
        This matches the insert/schema where band order is B02,B03,B04 in the file.
        """
        try:
            with rasterio.open(tiff_path) as src:
                # Read first 3 bands (assumed B02,B03,B04 = Blue,Green,Red)
                if src.count >= 3:
                    # Read as (B,G,R)
                    img_data = src.read([1, 2, 3])  # (3, H, W)
                    # Reorder deterministically to (R,G,B)
                    img_data = img_data[[2, 1, 0], :, :]
                    self.logger.debug(
                        f"Reordered bands (B02,B03,B04)->(R,G,B) for {tiff_path.name}"
                    )
                else:
                    # Single-band fallback -> gray to 3 channels
                    band1 = src.read(1)
                    img_data = np.stack([band1, band1, band1], axis=0)

                # Handle no-data values
                if src.nodata is not None:
                    img_data = np.where(img_data == src.nodata, 0, img_data)

                # Transpose to (H, W, C)
                img_array = np.transpose(img_data, (1, 2, 0))

                # Normalize to 0-255 uint8 (keep existing logic)
                if img_array.dtype != np.uint8:
                    max_val = float(img_array.max()) if img_array.size else 0.0
                    min_val = float(img_array.min()) if img_array.size else 0.0
                    if max_val <= 1.0:
                        img_array = (img_array * 255.0).clip(0, 255).astype(np.uint8)
                    elif max_val <= 255.0:
                        img_array = np.clip(img_array, 0, 255).astype(np.uint8)
                    else:
                        if max_val > min_val:
                            img_array = (
                                ((img_array - min_val) / (max_val - min_val) * 255.0)
                                .clip(0, 255)
                                .astype(np.uint8)
                            )
                        else:
                            img_array = np.zeros_like(img_array, dtype=np.uint8)

                # Resize to model input size
                img = Image.fromarray(img_array)
                if img.size != (target_size, target_size):
                    img = img.resize((target_size, target_size), Image.LANCZOS)

                final_array = np.array(img)

                metadata = {
                    "original_size": f"{src.width}x{src.height}",
                    "final_size": f"{target_size}x{target_size}",
                    "bands": int(src.count),
                    "data_type": str(src.dtypes[0]),
                    "reordered_to_rgb": bool(src.count >= 3),
                }

                return final_array, metadata

        except Exception as e:
            self.logger.error(f"Error converting TIFF {tiff_path}: {e}")
            return None, None

    def preprocess_with_btc_transforms(
        self, img_a: np.ndarray, img_b: np.ndarray
    ) -> Optional[Dict]:
        """Preprocess images using BTC transforms"""
        try:
            # Prepare data dictionary
            data = {"imageA": img_a, "imageB": img_b}

            self.logger.debug(f"Input shapes - A: {img_a.shape}, B: {img_b.shape}")
            self.logger.debug(
                f"Input ranges - A: [{img_a.min()}, {img_a.max()}], B: [{img_b.min()}, {img_b.max()}]"
            )

            # Apply BTC transforms
            transformed = self.transforms(data)

            self.logger.debug(
                f"Transformed shapes - A: {transformed['imageA'].shape}, B: {transformed['imageB'].shape}"
            )
            self.logger.debug(
                f"Transformed ranges - A: [{transformed['imageA'].min():.3f}, {transformed['imageA'].max():.3f}]"
            )
            self.logger.debug(
                f"Transformed ranges - B: [{transformed['imageB'].min():.3f}, {transformed['imageB'].max():.3f}]"
            )

            # Add batch dimension
            batch = {
                "imageA": transformed["imageA"].unsqueeze(0),
                "imageB": transformed["imageB"].unsqueeze(0),
            }

            # Verify normalization was applied by checking the value range
            # After ImageNet normalization, values should typically be in range [-2.5, 2.5]
            img_a_tensor = transformed["imageA"]
            img_b_tensor = transformed["imageB"]

            expected_mean_range = (-1.0, 1.0)  # Rough range after normalization

            if (
                img_a_tensor.min() < -3.0
                or img_a_tensor.max() > 3.0
                or img_b_tensor.min() < -3.0
                or img_b_tensor.max() > 3.0
            ):
                self.logger.warning(
                    "⚠️ Unusual tensor ranges after transforms - normalization may not be applied correctly!"
                )
                self.logger.warning(
                    f"A range: [{img_a_tensor.min():.3f}, {img_a_tensor.max():.3f}]"
                )
                self.logger.warning(
                    f"B range: [{img_b_tensor.min():.3f}, {img_b_tensor.max():.3f}]"
                )
            else:
                self.logger.debug(
                    "✓ Tensor ranges suggest normalization was applied correctly"
                )

            return batch

        except Exception as e:
            self.logger.error(f"Error in BTC preprocessing: {e}")
            return None

    async def generate_change_mask(
        self, img_a_path: Path, img_b_path: Path
    ) -> Tuple[Optional[np.ndarray], Optional[Dict]]:
        """Generate change detection mask for two images"""
        try:
            self.logger.debug(
                f"Generating mask for {img_a_path.name} -> {img_b_path.name}"
            )

            # Convert TIFFs to arrays
            img_a_array, meta_a = self.convert_tiff_to_png(
                img_a_path, config.btc_image_size
            )
            img_b_array, meta_b = self.convert_tiff_to_png(
                img_b_path, config.btc_image_size
            )

            if img_a_array is None or img_b_array is None:
                raise Exception("Failed to convert TIFF images")

            # Preprocess with BTC transforms
            batch = self.preprocess_with_btc_transforms(img_a_array, img_b_array)
            if batch is None:
                raise Exception("Failed to preprocess images")

            # Move to device
            batch_device = {
                "imageA": batch["imageA"].to(self.device),
                "imageB": batch["imageB"].to(self.device),
            }

            # Run inference
            with torch.no_grad():
                output = self.model(batch_device)

                # Apply sigmoid to get probabilities
                probabilities = torch.sigmoid(output)

                # Create binary mask with threshold
                binary_mask = (probabilities > config.btc_threshold).float()

                # Move to CPU
                prob_cpu = probabilities.cpu().squeeze().numpy()
                mask_cpu = binary_mask.cpu().squeeze().numpy()

            # Create output metadata with normalization info
            result_metadata = {
                "input_images": [str(img_a_path), str(img_b_path)],
                "image_size": config.btc_image_size,
                "threshold": config.btc_threshold,
                "model_checkpoint": config.btc_model_checkpoint,
                "generated_at": datetime.now().isoformat(),
                "preprocessing": {
                    "transforms_applied": str(self.transforms.transforms),
                    "input_tensor_ranges": {
                        "imageA": [
                            float(batch["imageA"].min()),
                            float(batch["imageA"].max()),
                        ],
                        "imageB": [
                            float(batch["imageB"].min()),
                            float(batch["imageB"].max()),
                        ],
                    },
                },
                "probability_stats": {
                    "min": float(prob_cpu.min()),
                    "max": float(prob_cpu.max()),
                    "mean": float(prob_cpu.mean()),
                    "std": float(prob_cpu.std()),
                },
                "mask_stats": {
                    "total_pixels": int(mask_cpu.size),
                    "changed_pixels": int(np.sum(mask_cpu)),
                    "change_percentage": float(
                        (np.sum(mask_cpu) / mask_cpu.size) * 100
                    ),
                },
            }

            # Convert mask to uint8 for storage
            mask_uint8 = (mask_cpu * 255).astype(np.uint8)

            return mask_uint8, result_metadata

        except Exception as e:
            self.logger.error(f"Error generating change mask: {e}")
            return None, None

    def save_mask_locally(
        self, mask: np.ndarray, metadata: Dict, output_path: Path
    ) -> bool:
        """Save mask and metadata locally"""
        try:
            # Save mask as PNG
            mask_image = Image.fromarray(mask, mode="L")
            mask_image.save(output_path)

            # Save metadata
            metadata_path = output_path.with_suffix(".json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            self.logger.debug(f"Saved mask to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving mask locally: {e}")
            return False

    def read_change_mask_from_memory(
        self, mask: np.ndarray
    ) -> Tuple[bytes, Dict[str, any]]:
        """
        Convert in-memory mask to raw bytes (consistent with insert script format)

        Args:
            mask: numpy array mask (uint8, 256x256)

        Returns:
            Tuple of (mask data as bytes, metadata dictionary)
        """
        try:
            # Ensure it's 256x256 using PIL for consistency
            if mask.shape != (256, 256):
                # Use PIL for resizing (already imported)
                mask_image = Image.fromarray(mask, mode="L")
                mask_image = mask_image.resize((256, 256), Image.NEAREST)
                mask = np.array(mask_image)

            # Ensure it's uint8 format
            mask = mask.astype(np.uint8)

            # Create metadata (same format as insert script)
            metadata = {"width": 256, "height": 256, "data_type": "uint8"}

            # Return raw bytes (consistent with insert script approach)
            return mask.tobytes(), metadata

        except Exception as e:
            self.logger.error(f"Error converting mask to raw bytes: {e}")
            raise

    def insert_change_mask(
        self,
        grid_id: int,
        img_a_id: int,
        img_b_id: int,
        timestamp_a: datetime,
        timestamp_b: datetime,
        bbox_wkt: str,
        mask: np.ndarray,
    ) -> bool:
        """
        Insert a change detection mask for two images (exact same approach as original script)

        Args:
            grid_id: Grid cell ID
            img_a_id: ID of first image (earlier)
            img_b_id: ID of second image (later)
            timestamp_a: First timestamp
            timestamp_b: Second timestamp
            bbox_wkt: PostGIS geography polygon string
            mask: numpy array mask to insert

        Returns:
            True if successful, False otherwise
        """
        try:
            # Canonicalize to satisfy schema:
            # - Primary key: (img_a_id, img_b_id, period_start)
            # - Check: img_a_id < img_b_id
            # - Periods: period_start = LEAST(time_a, time_b), period_end = GREATEST(time_a, time_b)
            id_min, id_max = (
                (img_a_id, img_b_id) if img_a_id < img_b_id else (img_b_id, img_a_id)
            )
            period_start = timestamp_a if timestamp_a <= timestamp_b else timestamp_b
            period_end = timestamp_b if timestamp_b >= timestamp_a else timestamp_a

            # Read change mask (convert to raw bytes)
            mask_data, mask_metadata = self.read_change_mask_from_memory(mask)

            # Pre-existence check to avoid unnecessary INSERTs
            precheck_sql = "SELECT 1 FROM eo_change WHERE img_a_id=%s AND img_b_id=%s AND period_start=%s LIMIT 1"

            insert_sql = """
                INSERT INTO eo_change (
                    img_a_id, img_b_id, grid_id,
                    period_start, period_end, bbox,
                    width, height, data_type, mask
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s, ST_GeogFromText(%s),
                    %s, %s, %s, %s
                )
                ON CONFLICT ON CONSTRAINT eo_change_pk DO NOTHING
                """

            values = (
                id_min,
                id_max,
                grid_id,
                period_start,
                period_end,
                bbox_wkt,
                mask_metadata["width"],
                mask_metadata["height"],
                mask_metadata["data_type"],
                mask_data,
            )

            # Connect to DB
            conn = psycopg2.connect(**config.db_config)
            try:
                with conn.cursor() as cur:
                    # Pre-check
                    cur.execute(precheck_sql, (id_min, id_max, period_start))
                    if cur.fetchone():
                        self.logger.info(
                            f"Mask already exists for grid {grid_id}: ids=({id_min},{id_max}) period_start={period_start:%Y-%m} — skipping"
                        )
                        return True

                    # Insert
                    cur.execute(insert_sql, values)
                    conn.commit()

                self.logger.info(
                    f"✓ Inserted change mask for grid {grid_id}: {period_start.strftime('%Y-%m')} -> {period_end.strftime('%Y-%m')} "
                    f"({mask_metadata['width']}x{mask_metadata['height']}, {mask_metadata['data_type']})"
                )
                return True
            finally:
                conn.close()

        except psycopg2.Error as e:
            try:
                err_detail = getattr(e, "pgerror", str(e))
                constraint = getattr(getattr(e, "diag", None), "constraint_name", None)
            except Exception:
                err_detail = str(e)
                constraint = None
            self.logger.error(
                f"✗ Failed to insert change mask for grid {grid_id}: {err_detail}"
                + (f" (constraint: {constraint})" if constraint else "")
            )
            return False
        except Exception as e:
            self.logger.error(f"✗ Failed to insert change mask for grid {grid_id}: {e}")
            return False

    async def save_mask_to_database(
        self, mask: np.ndarray, metadata: Dict, img_a_path: Path, img_b_path: Path
    ) -> bool:
        """Save mask to database using the exact same approach as original script"""
        try:
            if config.mode == ProcessingMode.LOCAL_ONLY:
                self.logger.info("Local mode: skipping database storage for masks")
                return False

            # Parse grid_id and years from filenames: sentinel2_grid_{grid}_{year}_08.*
            def parse_info(p: Path) -> Tuple[int, int]:
                parts = p.stem.split("_")
                grid_id = int(parts[2])
                year = int(parts[3])
                return grid_id, year

            grid_a, year_a = parse_info(img_a_path)
            grid_b, year_b = parse_info(img_b_path)
            if grid_a != grid_b:
                self.logger.error("Image pair grid_id mismatch, cannot insert mask")
                return False
            grid_id = grid_a

            # Build representative dates for month equality (15th of August)
            date_a = datetime(year_a, 8, 15)
            date_b = datetime(year_b, 8, 15)

            # Connect to DB to find the eo records and exact grid bbox
            conn = psycopg2.connect(**config.db_config)
            try:
                with conn.cursor() as cur:
                    # Prefer matching via month column for determinism
                    cur.execute(
                        """
                        SELECT id, time
                        FROM eo
                        WHERE grid_id = %s
                          AND month = date_trunc('month', %s::timestamp)::date
                        ORDER BY time
                        LIMIT 1
                        """,
                        (grid_id, date_a),
                    )
                    row_a = cur.fetchone()

                    cur.execute(
                        """
                        SELECT id, time
                        FROM eo
                        WHERE grid_id = %s
                          AND month = date_trunc('month', %s::timestamp)::date
                        ORDER BY time
                        LIMIT 1
                        """,
                        (grid_id, date_b),
                    )
                    row_b = cur.fetchone()

                    if not row_a or not row_b:
                        self.logger.error(
                            f"EO records not found for grid {grid_id} years {year_a} and/or {year_b}"
                        )
                        return False

                    img_a_id, time_a = row_a
                    img_b_id, time_b = row_b

                    # Fetch exact grid bbox from grid_cells to match schema
                    cur.execute(
                        "SELECT ST_AsText(bbox_4326) FROM grid_cells WHERE grid_id = %s",
                        (grid_id,),
                    )
                    bbox_row = cur.fetchone()
                    if not bbox_row or not bbox_row[0]:
                        self.logger.error(
                            f"Grid bbox not found for grid_id {grid_id} in grid_cells"
                        )
                        return False
                    bbox_wkt = bbox_row[0]

            finally:
                conn.close()

            # Insert the change mask (insert_change_mask will canonicalize id/time order)
            return self.insert_change_mask(
                grid_id, img_a_id, img_b_id, time_a, time_b, bbox_wkt, mask
            )

        except Exception as e:
            self.logger.error(f"Error saving mask to database: {e}")
            return False

    async def process_image_pair(
        self, img_a_path: Path, img_b_path: Path, year: int
    ) -> bool:
        """Process a single image pair to generate change mask"""
        try:
            # Generate mask
            mask, metadata = await self.generate_change_mask(img_a_path, img_b_path)
            if mask is None:
                return False

            # Get output path for local storage
            output_path = self.get_mask_output_path(img_a_path, img_b_path, year)

            # Always save locally for the current year (for clarity/debugging)
            local_success = self.save_mask_locally(mask, metadata, output_path)
            if local_success:
                self.logger.info(f"✓ Saved local binary mask: {output_path}")

            # Save to database if not in local-only mode
            db_success = True
            if config.mode != ProcessingMode.LOCAL_ONLY:
                db_success = await self.save_mask_to_database(
                    mask, metadata, img_a_path, img_b_path
                )

            # Consider successful if either local or db save worked
            success = local_success or db_success

            if success:
                change_pct = metadata["mask_stats"]["change_percentage"]
                self.logger.info(
                    f"Generated mask: {output_path.name} ({change_pct:.2f}% change) "
                    f"[Local: {'✓' if local_success else '✗'}, DB: {'✓' if db_success else '✗'}]"
                )

            return success

        except Exception as e:
            self.logger.error(f"Error processing image pair: {e}")
            return False

    async def process_year(self, year: int) -> bool:
        """Process BTC generation for a specific year"""
        self.logger.info(f"Processing BTC masks for year {year}")
        self.current_year = year

        # Find image pairs for this year
        image_pairs = self.find_image_pairs_for_year(year)
        if not image_pairs:
            self.logger.warning(f"No image pairs found for year {year}")
            return True

        # Generate task IDs
        task_ids = []
        for i, (img_a, img_b) in enumerate(image_pairs):
            task_id = f"btc_{year}_{i}"
            task_ids.append(task_id)

        # Load or create checkpoint
        checkpoint = state_manager.load_checkpoint("btc_process", year)
        if not checkpoint:
            checkpoint = state_manager.create_stage_checkpoint(
                "btc_process", year, task_ids
            )
        else:
            # If the task set has changed (e.g., new pairs), recreate checkpoint
            existing_ids = set(checkpoint.tasks.keys())
            desired_ids = set(task_ids)
            if existing_ids != desired_ids:
                self.logger.warning(
                    "BTC task list changed since last run; recreating checkpoint"
                )
                checkpoint = state_manager.create_stage_checkpoint(
                    "btc_process", year, task_ids
                )

        # Get pending tasks
        pending_task_ids = state_manager.get_pending_tasks("btc_process", year)
        # If none pending but there are failed tasks, reset them
        if not pending_task_ids:
            failed_ids = state_manager.get_failed_tasks("btc_process", year)
            if failed_ids:
                self.logger.info(
                    f"No pending tasks but {len(failed_ids)} failed tasks found; resetting failed tasks"
                )
                state_manager.reset_failed_tasks("btc_process", year)
                pending_task_ids = state_manager.get_pending_tasks("btc_process", year)

        pending_pairs = []
        for i, pair in enumerate(image_pairs):
            task_id = f"btc_{year}_{i}"
            if task_id in pending_task_ids:
                pending_pairs.append((pair, task_id))

        self.logger.info(f"Found {len(pending_pairs)} pending BTC tasks for {year}")

        if not pending_pairs:
            # Provide more context about checkpoint state
            self.logger.info(
                f"No pending pairs. Completed: {checkpoint.completed_tasks}, Failed: {checkpoint.failed_tasks}, Skipped: {checkpoint.skipped_tasks}, Total: {checkpoint.total_tasks}"
            )
            self.logger.info(f"All BTC processing for {year} already completed")
            return True

        # Process pairs
        success_count = 0
        for (img_a_path, img_b_path), task_id in pending_pairs:
            # Update status to running
            state_manager.update_task_status(
                "btc_process", year, task_id, TaskStatus.RUNNING
            )

            try:
                success = await self.process_image_pair(img_a_path, img_b_path, year)

                if success:
                    # Update status to completed
                    metadata = {
                        "img_a": str(img_a_path),
                        "img_b": str(img_b_path),
                        "output": str(
                            self.get_mask_output_path(img_a_path, img_b_path, year)
                        ),
                    }
                    state_manager.update_task_status(
                        "btc_process",
                        year,
                        task_id,
                        TaskStatus.COMPLETED,
                        metadata=metadata,
                    )
                    success_count += 1
                else:
                    # Update status to failed
                    error_msg = (
                        f"Failed to process pair {img_a_path.name} -> {img_b_path.name}"
                    )
                    state_manager.update_task_status(
                        "btc_process",
                        year,
                        task_id,
                        TaskStatus.FAILED,
                        error_message=error_msg,
                    )

            except Exception as e:
                error_msg = f"Unexpected error processing pair: {e}"
                self.logger.error(error_msg)
                state_manager.update_task_status(
                    "btc_process",
                    year,
                    task_id,
                    TaskStatus.FAILED,
                    error_message=error_msg,
                )

        self.logger.info(
            f"Completed BTC processing for {year}: {success_count}/{len(pending_pairs)} successful"
        )
        return success_count == len(pending_pairs)

    async def run_btc_processing(self) -> bool:
        """Execute BTC processing for all years"""
        if not await self.initialize():
            self.logger.error("Failed to initialize BTC processor")
            return False

        if not await self.load_model():
            self.logger.error("Failed to load BTC model")
            return False

        self.logger.info(f"Starting BTC processing for years: {config.years}")
        self.logger.info(f"Storage mode: {config.mode.value}")
        self.logger.info(f"Model: {config.btc_model_checkpoint}")
        self.logger.info(f"Threshold: {config.btc_threshold}")

        # Process each year sequentially
        overall_success = True
        for year in config.years[:-1]:  # Skip last year (no next year to pair with)
            try:
                year_success = await self.process_year(year)
                if not year_success:
                    overall_success = False
                    self.logger.warning(f"Some BTC processing failed for year {year}")
            except Exception as e:
                self.logger.error(f"Failed to process year {year}: {e}")
                overall_success = False

        return overall_success


# Export the processor class
__all__ = ["BTCProcessorV5"]
