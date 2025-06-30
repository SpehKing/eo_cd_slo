#!/usr/bin/env python3
"""
Script to help set up Logfire for the EO Change Detection API
"""

import os
import sys


def print_setup_instructions():
    """Print instructions for setting up Logfire"""

    print("🛰️  Sentinel-2 API Logfire Setup Instructions")
    print("=" * 50)
    print()
    print("1. Install Logfire CLI (if not already installed):")
    print("   pip install logfire")
    print()
    print("2. Authenticate with Logfire:")
    print("   logfire auth")
    print()
    print("3. Create or select a project:")
    print("   logfire projects use eo-cd-slo")
    print("   (or create a new project if needed)")
    print()
    print("4. Get your write token:")
    print("   logfire projects tokens create")
    print()
    print("5. Set environment variables:")
    print("   - Copy .env.example to .env")
    print("   - Update EO_CD_LOGFIRE_TOKEN with your token")
    print("   - Update other settings as needed")
    print()
    print("6. View logs in Logfire dashboard:")
    print("   https://logfire.pydantic.dev/")
    print()
    print("Docker Setup:")
    print("- The Docker compose file will automatically use the .env file")
    print("- Make sure to set EO_CD_ENABLE_LOGFIRE=true")
    print("- Logs will appear in your Logfire project dashboard")
    print()


def generate_demo_env():
    """Generate a demo .env file for development"""

    env_content = """# EO Change Detection API Environment Variables
# Development/Demo Configuration

# Database Configuration
EO_CD_DB_HOST=localhost
EO_CD_DB_PORT=5432
EO_CD_DB_NAME=eo_db
EO_CD_DB_USER=postgres
EO_CD_DB_PASSWORD=password

# Logfire Configuration (for development - no token needed)
EO_CD_LOGFIRE_PROJECT_NAME=eo-cd-slo-dev
EO_CD_LOGFIRE_ENVIRONMENT=development
EO_CD_ENABLE_LOGFIRE=true
# EO_CD_LOGFIRE_TOKEN=your_token_here  # Uncomment and add your token for production

# Logging Configuration
EO_CD_LOG_LEVEL=INFO

# API Configuration
EO_CD_API_TITLE=Sentinel-2 Image API (Development)
EO_CD_API_VERSION=1.0.0

# Image Processing Configuration
EO_CD_MAX_IMAGE_SIZE=1024
EO_CD_JPEG_QUALITY=85
"""

    with open(".env", "w") as f:
        f.write(env_content)

    print("✅ Created .env file for development")
    print("🔧 For production, add your Logfire token to EO_CD_LOGFIRE_TOKEN")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "generate-env":
        generate_demo_env()
    else:
        print_setup_instructions()
