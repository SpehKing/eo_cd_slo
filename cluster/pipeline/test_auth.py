#!/usr/bin/env python3
"""
Test OpenEO Authentication

Simple script to test if your hardcoded authentication works.
"""

import sys
import os
from pathlib import Path

# Add the pipeline directory to Python path
sys.path.append(str(Path(__file__).parent))

# Import hardcoded credentials
from hardcoded_credentials import apply_credentials

# Apply credentials to environment
apply_credentials()

# Now test the authentication
import openeo


def test_authentication():
    """Test OpenEO authentication with hardcoded credentials"""
    print("=" * 60)
    print("TESTING OPENEO AUTHENTICATION")
    print("=" * 60)

    try:
        # Connect to OpenEO
        print("1. Connecting to OpenEO...")
        connection = openeo.connect("openeo.dataspace.copernicus.eu")
        print("✓ Connected successfully")

        # Get client ID
        client_id = os.getenv("OPENEO_CLIENT_ID")
        client_secret = os.getenv("OPENEO_CLIENT_SECRET")
        refresh_token = os.getenv("OPENEO_REFRESH_TOKEN")

        print(f"\n2. Using credentials:")
        print(f"   Client ID: {client_id}")
        print(f"   Client Secret: {'SET' if client_secret else 'NOT SET'}")
        print(f"   Refresh Token: {'SET' if refresh_token else 'NOT SET'}")

        # Try authentication
        print(f"\n3. Attempting authentication...")

        if client_secret:
            print("   Trying Client Credentials Flow...")
            try:
                # Use the correct client credentials method
                connection = connection.authenticate_oidc_client_credentials(
                    client_id=client_id, client_secret=client_secret
                )
                print("✓ Client Credentials authentication successful!")
            except Exception as e:
                print(f"   Client Credentials failed: {e}")
                # Fall back to other methods
                raise e

        elif refresh_token:
            print("   Trying Refresh Token Flow...")
            try:
                # Use the correct refresh token method
                connection = connection.authenticate_oidc_refresh_token(
                    refresh_token=refresh_token
                )
                print("✓ Refresh Token authentication successful!")
            except Exception as e:
                print(f"   Refresh Token failed: {e}")
                raise e

        else:
            print("   Using Device Flow (interactive)...")
            try:
                # Use the correct device flow method
                connection = connection.authenticate_oidc_device()
                print("✓ Device Flow authentication successful!")
            except Exception as e:
                print(f"   Device Flow failed: {e}")
                print("   Trying default authentication...")
                # Final fallback - let openEO choose the method
                connection = connection.authenticate_oidc()
                print("✓ Default authentication successful!")

        # Test a simple API call
        print(f"\n4. Testing API access...")
        collections = connection.list_collections()
        print(f"✓ API test successful - found {len(collections)} collections")

        print(f"\n🎉 AUTHENTICATION TEST PASSED!")
        print(f"Your pipeline should now work with these credentials.")
        return True

    except Exception as e:
        print(f"\n❌ AUTHENTICATION TEST FAILED!")
        print(f"Error: {e}")
        print(f"\nTroubleshooting:")
        print(f"1. Check your internet connection")
        print(f"2. Verify your Copernicus account is active")
        print(f"3. Make sure openeo.dataspace.copernicus.eu is accessible")
        return False


if __name__ == "__main__":
    success = test_authentication()
    sys.exit(0 if success else 1)
