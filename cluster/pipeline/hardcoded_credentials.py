#!/usr/bin/env python3
"""
Hardcoded OpenEO Credentials

Simple file to store your OpenEO authentication credentials.
Edit the values below with your actual credentials.
"""

# Your hardcoded Client ID from the new OAuth client
OPENEO_CLIENT_ID = "sh-709a7af8-09ba-46e8-bcfc-f4d0d20461d4"

# Your hardcoded Client Secret from the new OAuth client
OPENEO_CLIENT_SECRET = "v9dPYitFLccIhtRohuIrvT0EwnsiJUIJ"  # KEEP THIS SECRET!

# If you want to store a refresh token manually, put it here
OPENEO_REFRESH_TOKEN = None  # Set to your refresh token string if you have one

# Instructions:
# 1. The Client ID is already set from your OpenEO Federation application
# 2. For Client Secret: This might not be available for your application type
# 3. For Refresh Token: You can manually extract it after authentication
# 4. If both are None, the system will use Device Flow (shows URL + code)


def apply_credentials():
    """Apply these credentials to environment variables"""
    import os

    if OPENEO_CLIENT_ID:
        os.environ["OPENEO_CLIENT_ID"] = OPENEO_CLIENT_ID
        print(f"✓ Set OPENEO_CLIENT_ID: {OPENEO_CLIENT_ID}")

    if OPENEO_CLIENT_SECRET:
        os.environ["OPENEO_CLIENT_SECRET"] = OPENEO_CLIENT_SECRET
        print("✓ Set OPENEO_CLIENT_SECRET: [HIDDEN]")

    if OPENEO_REFRESH_TOKEN:
        os.environ["OPENEO_REFRESH_TOKEN"] = OPENEO_REFRESH_TOKEN
        print("✓ Set OPENEO_REFRESH_TOKEN: [HIDDEN]")

    print("🔐 Credentials applied to environment variables")


if __name__ == "__main__":
    apply_credentials()
