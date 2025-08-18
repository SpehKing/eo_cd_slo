#!/usr/bin/env python3
"""
Main Pipeline Controller

Orchestrates the entire EO Change Detection pipeline including:
1. Sentinel-2 image downloads
2. Database insertions (or local storage)
3. BTC change detection processing

Supports resumable execution, real-time monitoring, and both local and database modes.
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
import os

# Setup logging first
from .config.settings import config, LogLevel, ProcessingMode

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.log_level.value),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config.get_log_file("pipeline")),
        logging.StreamHandler(),
    ],
)

# Import pipeline modules
from .modules.download import SentinelDownloaderV5
from .modules.insert import SentinelInserterV5
from .modules.btc_processor import BTCProcessorV5
from .utils.state_manager import state_manager
from .utils.monitor import monitor


class PipelineController:
    """Main pipeline controller orchestrating all stages"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.PipelineController")
        self.downloader = SentinelDownloaderV5()
        self.inserter = SentinelInserterV5()
        self.btc_processor = BTCProcessorV5()

        self.is_running = False
        self.is_paused = False
        self.should_stop = False

        # Register with monitor for control
        monitor.register_pipeline_controller(self)

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.should_stop = True

    async def _prefetch_hf_model(self) -> bool:
        """Pre-download the BTC model snapshot from Hugging Face and show progress.
        Caches under DATA_DIR/hf_cache to persist across runs.
        """
        try:
            from huggingface_hub import snapshot_download
            from huggingface_hub.utils import logging as hf_logging

            cache_dir = config.base_data_dir / "hf_cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            # Ensure hub uses our persistent cache
            os.environ.setdefault("HF_HOME", str(cache_dir))
            # Use standard downloader to avoid missing hf_transfer package issues
            os.environ.setdefault("HF_HUB_ENABLE_HF_TRANSFER", "0")

            self.logger.info(
                f"Prefetching Hugging Face model: {config.btc_model_checkpoint}"
            )
            self.logger.info(f"HF cache: {os.environ['HF_HOME']}")

            # Show per-file download progress
            hf_logging.set_verbosity_info()

            def _download():
                return snapshot_download(
                    repo_id=config.btc_model_checkpoint,
                    cache_dir=str(cache_dir),
                )

            # Run the blocking download in a thread and wait until it finishes
            path = await asyncio.to_thread(_download)
            self.logger.info(f"Model snapshot available at: {path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to prefetch Hugging Face model: {e}")
            return False

    async def run_pipeline(
        self, resume: bool = True, wait_for_start: bool = False
    ) -> bool:
        """Run the complete pipeline"""
        try:
            self.is_running = True
            self.should_stop = False

            self.logger.info("=" * 80)
            self.logger.info("STARTING EO CHANGE DETECTION PIPELINE")
            self.logger.info("=" * 80)
            self.logger.info(f"Configuration:")
            self.logger.info(f"  Mode: {config.mode.value}")
            self.logger.info(f"  Years: {config.years}")
            self.logger.info(f"  Grid IDs: {config.grid_ids}")
            self.logger.info(f"  BTC Model: {config.btc_model_checkpoint}")
            self.logger.info(f"  Resume: {resume}")
            self.logger.info(f"  Wait for start: {wait_for_start}")

            # Start monitoring server if enabled
            if config.enable_real_time_monitoring:
                await self._start_monitoring()

            # Wait for start command from web interface if requested
            if wait_for_start:
                await monitor.wait_for_start_command()

            # Update monitoring status
            monitor.update_pipeline_status("running")

            # Initialize pipeline modules
            self.logger.info("Initializing pipeline modules...")

            # Initialize downloader (loads grid data)
            if not await self.downloader.initialize():
                self.logger.error("Failed to initialize downloader")
                return False

            # Initialize inserter (loads grid data and DB connection)
            if not await self.inserter.initialize():
                self.logger.error("Failed to initialize inserter")
                return False

            # Initialize BTC processor (builds transforms, sets device)
            if not await self.btc_processor.initialize():
                self.logger.error("Failed to initialize BTC processor")
                return False

            # Prefetch HF model so it's cached before loading
            if not await self._prefetch_hf_model():
                self.logger.error("Failed to prefetch BTC model")
                return False

            # Ensure BTC model is loaded once up front
            if not await self.btc_processor.load_model():
                self.logger.error("Failed to load BTC model")
                return False

            self.logger.info("✓ All modules initialized successfully")

            overall_success = True

            # NEW: Immediate insertion workflow - download and insert each grid immediately
            # This prevents re-downloading existing data and provides faster feedback
            for year in config.years:
                if self.should_stop:
                    self.logger.info("Pipeline stopped by user request")
                    break

                # Check for control commands
                await self._handle_control_commands()

                monitor.update_pipeline_status("running", "download_and_insert", year)
                year_success = await self._run_combined_download_insert_stage(
                    year, resume
                )
                if not year_success:
                    overall_success = False
                    if not resume:
                        break

            if self.should_stop:
                if overall_success:
                    monitor.update_pipeline_status("stopped")
                return overall_success

            # Stage 3: BTC processing on year pairs (e.g., 2023->2024)
            # Only run for years that have a subsequent year
            for year in config.years:
                if year >= max(config.years):
                    continue
                if self.should_stop:
                    self.logger.info("Pipeline stopped by user request")
                    break
                monitor.update_pipeline_status("running", "btc_process", year)

                # Check for control commands
                await self._handle_control_commands()

                year_success = await self._run_btc_stage(year, resume)
                if not year_success:
                    overall_success = False
                    if not resume:
                        break

            # Final status update
            if self.should_stop:
                monitor.update_pipeline_status("stopped")
                self.logger.info("Pipeline execution stopped by user")
            elif overall_success:
                monitor.update_pipeline_status("completed")
                self.logger.info("Pipeline completed successfully!")
            else:
                monitor.update_pipeline_status("error")
                self.logger.error("Pipeline completed with errors")

            return overall_success

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            monitor.update_pipeline_status("error")
            return False
        finally:
            self.is_running = False

    async def _handle_control_commands(self):
        """Handle control commands during pipeline execution"""
        command = await monitor.check_control_commands()

        if command == "stop":
            self.logger.info("Stop command received")
            self.should_stop = True
            monitor.update_pipeline_status("stopping")

        elif command == "pause":
            self.logger.info("Pause command received")
            self.is_paused = True
            monitor.update_pipeline_status("paused")

            # Wait until resumed or stopped
            while self.is_paused and not self.should_stop:
                await asyncio.sleep(1)
                resume_command = await monitor.check_control_commands()
                if resume_command == "resume":
                    self.logger.info("Resume command received")
                    self.is_paused = False
                    monitor.update_pipeline_status("running")
                elif resume_command == "stop":
                    self.logger.info("Stop command received while paused")
                    self.should_stop = True
                    monitor.update_pipeline_status("stopping")

    async def _start_monitoring(self):
        """Start the monitoring server"""
        try:
            await monitor.start_server()
            # Start background update task
            asyncio.create_task(monitor.start_background_updates())
            self.logger.info(
                f"Monitoring dashboard available at: http://localhost:{config.monitoring_port}"
            )
        except Exception as e:
            self.logger.warning(f"Failed to start monitoring server: {e}")

    async def _run_download_stage(self, year: int, resume: bool) -> bool:
        """Run download stage for a specific year"""
        try:
            self.logger.info(f"Stage 1: Downloading Sentinel-2 images for {year}")

            # Check if already completed
            if resume and state_manager.is_stage_completed("download", year):
                self.logger.info(
                    f"Download stage for {year} already completed, skipping"
                )
                return True

            # Run downloads
            success = await self.downloader.process_year(year)

            if success:
                self.logger.info(f"✓ Download stage completed for {year}")
            else:
                self.logger.error(f"✗ Download stage failed for {year}")

            return success

        except Exception as e:
            self.logger.error(f"Download stage error for {year}: {e}")
            return False

    async def _run_insert_stage(self, year: int, resume: bool) -> bool:
        """Run insert/storage stage for a specific year"""
        try:
            self.logger.info(f"Stage 2: Inserting/storing images for {year}")

            # Check if already completed
            if resume and state_manager.is_stage_completed("insert", year):
                self.logger.info(f"Insert stage for {year} already completed, skipping")
                return True

            # Run insertions
            success = await self.inserter.process_year(year)

            if success:
                self.logger.info(f"✓ Insert stage completed for {year}")
            else:
                self.logger.error(f"✗ Insert stage failed for {year}")

            return success

        except Exception as e:
            self.logger.error(f"Insert stage error for {year}: {e}")
            return False

    async def _run_btc_stage(self, year: int, resume: bool) -> bool:
        """Run BTC change detection stage for a specific year"""
        try:
            self.logger.info(f"Stage 3: Generating change masks for {year}")

            # Check if already completed
            if resume and state_manager.is_stage_completed("btc_process", year):
                self.logger.info(f"BTC stage for {year} already completed, skipping")
                return True

            # Run BTC processing
            success = await self.btc_processor.process_year(year)

            if success:
                self.logger.info(f"✓ BTC stage completed for {year}")
            else:
                self.logger.error(f"✗ BTC stage failed for {year}")

            return success

        except Exception as e:
            self.logger.error(f"BTC stage error for {year}: {e}")
            return False

    async def _run_combined_download_insert_stage(
        self, year: int, resume: bool
    ) -> bool:
        """Run combined download and insert stage for immediate insertion workflow"""
        try:
            self.logger.info(
                f"Stage 1+2: Download and insert images for {year} (immediate insertion)"
            )

            # Check if already completed
            if resume and state_manager.is_stage_completed("download_and_insert", year):
                self.logger.info(
                    f"Download+Insert stage for {year} already completed, skipping"
                )
                return True

            # Initialize both downloader and inserter
            if not await self.downloader.initialize():
                self.logger.error("Failed to initialize downloader")
                return False

            if not await self.inserter.initialize():
                self.logger.error("Failed to initialize inserter")
                return False

            # Connect OpenEO for downloads
            if not await self.downloader.connect_openeo():
                self.logger.error("Failed to connect to OpenEO")
                return False

            # Generate download tasks for this year
            tasks = self.downloader.generate_download_tasks_for_year(year)
            self.logger.info(
                f"Generated {len(tasks)} download+insert tasks for year {year}"
            )

            if len(tasks) == 0:
                self.logger.warning(f"No tasks generated for year {year}")
                return True  # Not an error, just no work to do

            # Process each grid cell: download then immediately insert
            success_count = 0
            total_tasks = len(tasks)

            for i, task in enumerate(tasks, 1):
                try:
                    grid_id = task["grid_id"]

                    # Check for control commands
                    await self._handle_control_commands()

                    if self.should_stop:
                        self.logger.info("Pipeline stopped during combined stage")
                        break

                    self.logger.info(
                        f"Processing grid {grid_id} ({i}/{total_tasks}) for {year}"
                    )

                    # Step 1: Check if data already exists in database/filesystem
                    if await self._check_grid_exists(grid_id, year):
                        self.logger.info(
                            f"Grid {grid_id} for {year} already exists, skipping"
                        )
                        success_count += 1
                        continue

                    # Step 2: Download the image
                    download_success, download_message, filepath = (
                        await self.downloader.download_with_retry(task)
                    )

                    if not download_success:
                        self.logger.error(
                            f"Failed to download grid {grid_id}: {download_message}"
                        )
                        continue

                    self.logger.info(f"✓ Downloaded grid {grid_id}: {download_message}")

                    # Step 3: Immediately insert the downloaded image
                    if filepath and filepath.exists():
                        insert_success = await self.inserter.process_single_image(
                            filepath
                        )

                        if insert_success:
                            self.logger.info(
                                f"✓ Inserted grid {grid_id} into database/storage"
                            )
                            success_count += 1

                            # Clean up temporary file if in database mode
                            if config.mode != ProcessingMode.LOCAL_ONLY:
                                try:
                                    filepath.unlink()
                                    self.logger.debug(
                                        f"Cleaned up temporary file: {filepath}"
                                    )
                                except Exception as cleanup_error:
                                    self.logger.warning(
                                        f"Failed to cleanup {filepath}: {cleanup_error}"
                                    )
                        else:
                            self.logger.error(f"Failed to insert grid {grid_id}")
                    else:
                        self.logger.error(
                            f"Downloaded file not found for grid {grid_id}"
                        )

                    # Rate limiting between downloads
                    await asyncio.sleep(config.openeo_rate_limit)

                except Exception as e:
                    self.logger.error(
                        f"Failed to process grid {grid_id} for {year}: {e}"
                    )

            self.logger.info(
                f"Completed {success_count}/{total_tasks} download+insert tasks for year {year}"
            )

            # Mark stage as completed if all successful
            if success_count == total_tasks:
                state_manager.mark_stage_completed("download_and_insert", year)

            return success_count > 0

        except Exception as e:
            self.logger.error(f"Combined download+insert stage error for {year}: {e}")
            return False

    async def _check_grid_exists(self, grid_id: int, year: int) -> bool:
        """Check if grid data already exists to avoid re-downloading"""
        try:
            # For local mode, check if file exists
            if config.mode == ProcessingMode.LOCAL_ONLY:
                year_dir = config.get_year_images_dir(year)
                filename = f"sentinel2_grid_{grid_id}_{year}_08.tiff"
                return (year_dir / filename).exists()

            # For database mode, check database
            else:
                # Use the inserter's existing check method
                from datetime import datetime

                test_date = datetime(
                    year, 8, 15
                )  # Use August 15th as representative date
                return self.inserter.check_existing_record(grid_id, test_date)

        except Exception as e:
            self.logger.error(
                f"Failed to check if grid {grid_id} exists for {year}: {e}"
            )
            return False

    async def retry_failed_tasks(
        self, stage: Optional[str] = None, year: Optional[int] = None
    ):
        """Retry failed tasks for a specific stage/year or all"""
        self.logger.info("Retrying failed tasks...")

        if stage and year:
            # Retry specific stage/year
            state_manager.reset_failed_tasks(stage, year)
            self.logger.info(f"Reset failed tasks for {stage}_{year}")
        else:
            # Retry all failed tasks
            for checkpoint_key in state_manager.checkpoints.keys():
                parts = checkpoint_key.split("_")
                if len(parts) == 2:
                    stage_name, year_str = parts
                    state_manager.reset_failed_tasks(stage_name, int(year_str))
                else:
                    state_manager.reset_failed_tasks(parts[0], None)
            self.logger.info("Reset all failed tasks")

    def get_pipeline_status(self) -> dict:
        """Get current pipeline status"""
        return {
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "should_stop": self.should_stop,
            "config": {
                "mode": config.mode.value,
                "years": config.years,
                "grid_ids": config.grid_ids,
                "btc_model": config.btc_model_checkpoint,
            },
            "progress": state_manager.get_all_progress(),
        }

    async def stop_pipeline(self):
        """Stop the pipeline gracefully"""
        self.logger.info("Stopping pipeline...")
        self.should_stop = True
        monitor.update_pipeline_status("stopping")

    async def pause_pipeline(self):
        """Pause the pipeline"""
        self.logger.info("Pausing pipeline...")
        self.is_paused = True
        monitor.update_pipeline_status("paused")

    async def resume_pipeline(self):
        """Resume the pipeline"""
        self.logger.info("Resuming pipeline...")
        self.is_paused = False
        monitor.update_pipeline_status("running")


# Main entry point functions
async def run_pipeline_async(resume: bool = True, wait_for_start: bool = False) -> bool:
    """Run the pipeline asynchronously"""
    controller = PipelineController()
    return await controller.run_pipeline(resume=resume, wait_for_start=wait_for_start)


def run_pipeline_sync(resume: bool = True, wait_for_start: bool = False) -> bool:
    """Run the pipeline synchronously"""
    return asyncio.run(run_pipeline_async(resume=resume, wait_for_start=wait_for_start))


async def main():
    """Main entry point for CLI usage"""
    import argparse

    parser = argparse.ArgumentParser(description="EO Change Detection Pipeline")
    parser.add_argument(
        "--no-resume", action="store_true", help="Start fresh (ignore checkpoints)"
    )
    parser.add_argument(
        "--retry-failed", action="store_true", help="Retry only failed tasks"
    )
    parser.add_argument(
        "--status", action="store_true", help="Show pipeline status and exit"
    )
    parser.add_argument(
        "--monitor-only", action="store_true", help="Start monitoring server only"
    )
    parser.add_argument(
        "--wait-for-start",
        action="store_true",
        help="Wait for start command from web interface",
    )

    args = parser.parse_args()

    if args.status:
        # Show status and exit
        progress = state_manager.get_all_progress()
        print("\nPipeline Status:")
        print("=" * 50)
        for key, stage_info in progress.items():
            print(
                f"{key}: {stage_info['progress']:.1f}% "
                f"({stage_info['completed']}/{stage_info['total']} completed, "
                f"{stage_info['failed']} failed)"
            )
        return

    if args.monitor_only:
        # Start monitoring server and wait
        print(f"Starting monitoring server on port {config.monitoring_port}")
        await monitor.start_server()
        await monitor.start_background_updates()
        print(f"Dashboard available at: http://localhost:{config.monitoring_port}")
        print("Press Ctrl+C to stop")
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("\nMonitoring stopped")
        return

    if args.retry_failed:
        # Retry failed tasks
        controller = PipelineController()
        await controller.retry_failed_tasks()
        print("Failed tasks reset. Run pipeline again to retry.")
        return

    # Run the pipeline
    resume = not args.no_resume
    wait_for_start = args.wait_for_start

    if wait_for_start:
        print(
            f"Starting pipeline in wait mode. Access dashboard at: http://localhost:{config.monitoring_port}"
        )
        print("Pipeline will wait for start command from web interface...")

    success = await run_pipeline_async(resume=resume, wait_for_start=wait_for_start)

    if success:
        print("\n🎉 Pipeline completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Pipeline completed with errors")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
