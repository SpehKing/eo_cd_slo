#!/usr/bin/env python3
"""
Simple test script to validate the pipeline setup and configuration.
Run this script to check if all components are properly configured.
"""

import asyncio
import os
import sys
import json
import tempfile
from pathlib import Path

# Add cluster directory to path
cluster_dir = Path(__file__).parent
sys.path.insert(0, str(cluster_dir))


async def test_imports():
    """Test if all pipeline modules can be imported."""
    print("🔍 Testing imports...")

    try:
        from pipeline.config.settings import get_pipeline_config

        print("  ✅ Config module imported successfully")
    except Exception as e:
        print(f"  ❌ Config import failed: {e}")
        return False

    try:
        from pipeline.utils.state_manager import StateManager

        print("  ✅ State manager imported successfully")
    except Exception as e:
        print(f"  ❌ State manager import failed: {e}")
        return False

    try:
        from pipeline.utils.monitor import PipelineMonitor

        print("  ✅ Monitor imported successfully")
    except Exception as e:
        print(f"  ❌ Monitor import failed: {e}")
        return False

    try:
        from pipeline.modules.download import SentinelDownloaderV5

        print("  ✅ Download module imported successfully")
    except Exception as e:
        print(f"  ❌ Download module import failed: {e}")
        return False

    try:
        from pipeline.modules.insert import SentinelInserterV5

        print("  ✅ Insert module imported successfully")
    except Exception as e:
        print(f"  ❌ Insert module import failed: {e}")
        return False

    try:
        from pipeline.modules.btc_processor import BTCProcessorV5

        print("  ✅ BTC processor imported successfully")
    except Exception as e:
        print(f"  ❌ BTC processor import failed: {e}")
        return False

    try:
        from pipeline.controller import PipelineController

        print("  ✅ Controller imported successfully")
    except Exception as e:
        print(f"  ❌ Controller import failed: {e}")
        return False

    return True


async def test_configuration():
    """Test configuration loading."""
    print("\n⚙️  Testing configuration...")

    try:
        from pipeline.config.settings import get_pipeline_config

        config = get_pipeline_config()

        print(f"  ✅ Configuration loaded successfully")
        print(f"  📁 Data directory: {config.data_dir}")
        print(f"  🔄 Processing mode: {config.processing_mode}")
        print(f"  👥 Max workers: {config.max_workers}")
        print(
            f"  📊 Grid IDs: {config.grid_ids[:3]}..."
            if len(config.grid_ids) > 3
            else f"  📊 Grid IDs: {config.grid_ids}"
        )
        print(f"  📅 Years: {config.years}")

        return True
    except Exception as e:
        print(f"  ❌ Configuration test failed: {e}")
        return False


async def test_directories():
    """Test directory creation and permissions."""
    print("\n📁 Testing directory structure...")

    try:
        from pipeline.config.settings import get_pipeline_config

        config = get_pipeline_config()

        # Test creating directories
        test_dirs = [
            config.data_dir / "images",
            config.data_dir / "masks",
            config.data_dir / "checkpoints",
            config.data_dir / "logs",
        ]

        for test_dir in test_dirs:
            test_dir.mkdir(parents=True, exist_ok=True)
            print(f"  ✅ Directory created/verified: {test_dir}")

            # Test write permissions
            test_file = test_dir / "test_write.tmp"
            with open(test_file, "w") as f:
                f.write("test")
            test_file.unlink()
            print(f"  ✅ Write permission verified: {test_dir}")

        return True
    except Exception as e:
        print(f"  ❌ Directory test failed: {e}")
        return False


async def test_state_manager():
    """Test state management functionality."""
    print("\n💾 Testing state manager...")

    try:
        from pipeline.utils.state_manager import StateManager
        from pipeline.config.settings import get_pipeline_config

        config = get_pipeline_config()
        state_manager = StateManager()

        # Test checkpoint creation
        test_checkpoint = {
            "stage_name": "test",
            "year": 2023,
            "total_tasks": 2,
            "completed_tasks": 1,
            "failed_tasks": 0,
            "skipped_tasks": 0,
            "tasks": {
                "test_task_1": {
                    "status": "completed",
                    "started_at": "2024-01-01T10:00:00",
                    "completed_at": "2024-01-01T10:15:00",
                },
                "test_task_2": {"status": "pending"},
            },
        }

        # Save checkpoint
        checkpoint_file = config.data_dir / "checkpoints" / "test_2023.json"
        with open(checkpoint_file, "w") as f:
            json.dump(test_checkpoint, f, indent=2)

        # Load checkpoint
        loaded_checkpoint = state_manager.load_checkpoint("test", 2023)

        if loaded_checkpoint:
            print("  ✅ Checkpoint save/load working")
            print(
                f"  📊 Loaded checkpoint: {loaded_checkpoint['stage_name']} {loaded_checkpoint['year']}"
            )
        else:
            print("  ❌ Checkpoint load failed")
            return False

        # Clean up
        checkpoint_file.unlink()

        return True
    except Exception as e:
        print(f"  ❌ State manager test failed: {e}")
        return False


async def test_module_initialization():
    """Test module initialization without actual processing."""
    print("\n🔧 Testing module initialization...")

    try:
        from pipeline.modules.download import SentinelDownloaderV5
        from pipeline.modules.insert import SentinelInserterV5
        from pipeline.modules.btc_processor import BTCProcessorV5

        # Test downloader initialization
        downloader = SentinelDownloaderV5()
        print("  ✅ Downloader initialized")

        # Test inserter initialization
        inserter = SentinelInserterV5()
        print("  ✅ Inserter initialized")

        # Test BTC processor initialization (without loading the model)
        processor = BTCProcessorV5(load_model=False)
        print("  ✅ BTC processor initialized (without model)")

        return True
    except Exception as e:
        print(f"  ❌ Module initialization failed: {e}")
        return False


async def test_controller_initialization():
    """Test controller initialization."""
    print("\n🎛️  Testing controller initialization...")

    try:
        from pipeline.controller import PipelineController

        controller = PipelineController()
        print("  ✅ Controller initialized")

        # Test status check without starting
        await controller._update_status("Testing pipeline setup")
        print("  ✅ Status update working")

        return True
    except Exception as e:
        print(f"  ❌ Controller test failed: {e}")
        return False


async def check_dependencies():
    """Check if optional dependencies are available."""
    print("\n📦 Checking optional dependencies...")

    dependencies = [
        ("torch", "PyTorch (for BTC processing)"),
        ("openeo", "OpenEO (for Sentinel downloads)"),
        ("psycopg2", "PostgreSQL driver"),
        ("aiohttp", "Async HTTP server"),
        ("rasterio", "Raster data processing"),
        ("geopandas", "Geospatial data processing"),
    ]

    available = []
    missing = []

    for dep, description in dependencies:
        try:
            __import__(dep)
            available.append((dep, description))
            print(f"  ✅ {dep}: {description}")
        except ImportError:
            missing.append((dep, description))
            print(f"  ⚠️  {dep}: {description} (not available)")

    if missing:
        print(f"\n📝 Missing dependencies ({len(missing)}):")
        for dep, desc in missing:
            print(f"    - {dep}: {desc}")
        print(
            "\nInstall with: pip install torch openeo psycopg2-binary aiohttp rasterio geopandas"
        )

    return len(missing) == 0


async def main():
    """Run all tests."""
    print("🧪 Pipeline Setup Validation")
    print("=" * 40)

    tests = [
        ("Import Test", test_imports),
        ("Configuration Test", test_configuration),
        ("Directory Test", test_directories),
        ("State Manager Test", test_state_manager),
        ("Module Initialization Test", test_module_initialization),
        ("Controller Test", test_controller_initialization),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            result = await test_func()
            if result:
                passed += 1
        except Exception as e:
            print(f"  ❌ {test_name} crashed: {e}")

    # Check dependencies separately (doesn't count towards pass/fail)
    await check_dependencies()

    print(f"\n📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Pipeline setup is ready.")
        print("\n🚀 Next steps:")
        print("  1. Build the Docker image: ./pipeline.sh build")
        print("  2. Start the pipeline: ./pipeline.sh start-local")
        print("  3. Monitor progress: ./pipeline.sh monitor")
    else:
        print("❌ Some tests failed. Please check the output above.")
        print("\n🔧 Troubleshooting:")
        print("  - Make sure you're in the cluster directory")
        print("  - Install missing dependencies")
        print("  - Check file permissions")

    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
