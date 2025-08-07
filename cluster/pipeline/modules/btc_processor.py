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

            self.logger.info("BTC transforms built successfully")

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
        """Convert TIFF to PNG format and resize"""
        try:
            with rasterio.open(tiff_path) as src:
                # Read RGB bands
                if src.count >= 3:
                    img_data = src.read([1, 2, 3])  # Shape: (3, height, width)
                else:
                    band1 = src.read(1)
                    img_data = np.stack([band1, band1, band1], axis=0)

                # Handle no-data values
                if src.nodata is not None:
                    img_data = np.where(img_data == src.nodata, 0, img_data)

                # Transpose to (height, width, channels)
                img_array = np.transpose(img_data, (1, 2, 0))

                # Normalize to 0-255 range
                if img_array.dtype != np.uint8:
                    if img_array.max() <= 1.0:
                        img_array = (img_array * 255).astype(np.uint8)
                    elif img_array.max() <= 255:
                        img_array = np.clip(img_array, 0, 255).astype(np.uint8)
                    else:
                        img_min, img_max = img_array.min(), img_array.max()
                        if img_max > img_min:
                            img_array = (
                                (img_array - img_min) / (img_max - img_min) * 255
                            ).astype(np.uint8)
                        else:
                            img_array = np.zeros_like(img_array, dtype=np.uint8)

                # Convert to PIL and resize
                img = Image.fromarray(img_array)
                if img.size != (target_size, target_size):
                    img = img.resize((target_size, target_size), Image.LANCZOS)

                # Convert back to numpy
                final_array = np.array(img)

                metadata = {
                    "original_size": f"{src.width}x{src.height}",
                    "final_size": f"{target_size}x{target_size}",
                    "bands": src.count,
                    "data_type": str(src.dtypes[0]),
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

            # Apply BTC transforms
            transformed = self.transforms(data)

            # Add batch dimension
            batch = {
                "imageA": transformed["imageA"].unsqueeze(0),
                "imageB": transformed["imageB"].unsqueeze(0),
            }

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

            # Create output metadata
            result_metadata = {
                "input_images": [str(img_a_path), str(img_b_path)],
                "image_size": config.btc_image_size,
                "threshold": config.btc_threshold,
                "model_checkpoint": config.btc_model_checkpoint,
                "generated_at": datetime.now().isoformat(),
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

    async def save_mask_to_database(
        self, mask: np.ndarray, metadata: Dict, img_a_path: Path, img_b_path: Path
    ) -> bool:
        """Save mask to database (placeholder for now)"""
        # TODO: Implement database insertion for masks
        self.logger.warning("Database storage for masks not yet implemented")
        return False

    def get_mask_output_path(
        self, img_a_path: Path, img_b_path: Path, year: int
    ) -> Path:
        """Get output path for change mask"""
        # Parse grid ID from filename
        grid_id = None
        try:
            parts = img_a_path.stem.split("_")
            grid_id = int(parts[2])
        except:
            grid_id = "unknown"

        if config.mode == ProcessingMode.LOCAL_ONLY:
            year_masks_dir = config.get_year_masks_dir(year)
        else:
            year_masks_dir = config.masks_dir / "temp" / str(year)
            year_masks_dir.mkdir(parents=True, exist_ok=True)

        # Create filename
        filename = f"change_mask_grid_{grid_id}_{year}.png"
        return year_masks_dir / filename

    async def process_image_pair(
        self, img_a_path: Path, img_b_path: Path, year: int
    ) -> bool:
        """Process a single image pair to generate change mask"""
        try:
            # Generate mask
            mask, metadata = await self.generate_change_mask(img_a_path, img_b_path)
            if mask is None:
                return False

            # Save mask
            output_path = self.get_mask_output_path(img_a_path, img_b_path, year)

            if config.mode == ProcessingMode.LOCAL_ONLY:
                success = self.save_mask_locally(mask, metadata, output_path)
            else:
                # Try database first, fallback to local
                success = await self.save_mask_to_database(
                    mask, metadata, img_a_path, img_b_path
                )
                if not success:
                    success = self.save_mask_locally(mask, metadata, output_path)

            if success:
                change_pct = metadata["mask_stats"]["change_percentage"]
                self.logger.info(
                    f"Generated mask: {output_path.name} ({change_pct:.2f}% change)"
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

        # Get pending tasks
        pending_task_ids = state_manager.get_pending_tasks("btc_process", year)
        pending_pairs = []

        for i, pair in enumerate(image_pairs):
            task_id = f"btc_{year}_{i}"
            if task_id in pending_task_ids:
                pending_pairs.append((pair, task_id))

        self.logger.info(f"Found {len(pending_pairs)} pending BTC tasks for {year}")

        if not pending_pairs:
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
