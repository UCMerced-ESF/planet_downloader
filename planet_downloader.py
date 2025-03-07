#!/usr/bin/env python3
"""
Planet Labs Satellite Imagery Downloader

This script automates the process of retrieving satellite imagery from Planet Labs
for a specified geographic point and time window. It then converts the downloaded
imagery into a multi-layer Cloud Optimized GeoTIFF (COG) using GDAL.

Default coordinates: 37.355138, -120.411734 (Merced County, California)
"""

import os
import sys
import json
import logging
import argparse
import requests
import datetime
from pathlib import Path
from dotenv import load_dotenv
import subprocess
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("planet_downloader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv('planet_api_key')
if not API_KEY:
    logger.error("Planet API key not found. Please set it in the .env file.")
    sys.exit(1)

# Base URLs for Planet API
BASE_URL = "https://api.planet.com/data/v1"
SEARCH_URL = f"{BASE_URL}/quick-search"

# Default parameters
# Set lat long here
DEFAULT_LATITUDE = 37.355138
DEFAULT_LONGITUDE = -120.411734
DEFAULT_ITEM_TYPE = "PSScene"
# Each product in the array will be activated and downloaded for every data scan/date
# Data options:
# visual optimized: ortho_visual
# 4 Band (RGB+NIR) basic_analytic_4b
# 8 Band (RGB+NIR+MIR) basic_analytic_8b
DEFAULT_ASSET_TYPES = ["basic_analytic_8b", "ortho_visual"]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download Planet Labs satellite imagery")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--latitude",
        type=float,
        default=DEFAULT_LATITUDE,
        help=f"Latitude of the point of interest (default: {DEFAULT_LATITUDE})"
    )
    parser.add_argument(
        "--longitude",
        type=float,
        default=DEFAULT_LONGITUDE,
        help=f"Longitude of the point of interest (default: {DEFAULT_LONGITUDE})"
    )
    parser.add_argument(
        "--item-type",
        default=DEFAULT_ITEM_TYPE,
        help=f"Planet item type (default: {DEFAULT_ITEM_TYPE})"
    )
    parser.add_argument(
        "--asset-types",
        nargs="+",
        default=DEFAULT_ASSET_TYPES,
        help=f"Asset types to download (default: {' and '.join(DEFAULT_ASSET_TYPES)})"
    )
    parser.add_argument(
        "--max-cloud-cover",
        type=float,
        default=30.0,
        help="Maximum cloud cover percentage (default: 20.0)"
    )
    parser.add_argument(
        "--output-dir",
        default="./planet_data",
        help="Output directory for downloaded data (default: ./planet_data)"
    )
    parser.add_argument(
        "--activate-only",
        action="store_true",
        help="Only request activation for assets without waiting or downloading"
    )
    
    return parser.parse_args()


def build_search_request(start_date, end_date, latitude, longitude, item_type, max_cloud_cover):
    """Build the search request for Planet API."""
    # Geometry filter (point)
    geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": {
            "type": "Point",
            "coordinates": [longitude, latitude]
        }
    }
    
    # Date range filter
    date_range_filter = {
        "type": "DateRangeFilter",
        "field_name": "acquired",
        "config": {
            "gte": f"{start_date}T00:00:00.000Z",
            "lte": f"{end_date}T23:59:59.999Z"
        }
    }
    
    # Cloud cover filter
    cloud_cover_filter = {
        "type": "RangeFilter",
        "field_name": "cloud_cover",
        "config": {
            "lte": max_cloud_cover / 100.0  # Convert percentage to decimal
        }
    }
    
    # Combine all filters with AND
    combined_filter = {
        "type": "AndFilter",
        "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }
    
    # Build the search request
    search_request = {
        "item_types": [item_type],
        "filter": combined_filter
    }
    
    return search_request


def search_planet_imagery(search_request):
    """Search for Planet imagery using the search request."""
    logger.info("Searching for Planet imagery...")
    
    # Make the search request
    response = requests.post(
        SEARCH_URL,
        auth=(API_KEY, ""),
        json=search_request
    )
    
    if response.status_code != 200:
        logger.error(f"Error searching for imagery: {response.text}")
        return None
    
    # Parse the response
    search_result = response.json()
    features = search_result.get("features", [])
    logger.info(f"Found {len(features)} scenes matching the criteria")
    
    return features


def get_asset_activation_status(asset_url):
    """Check the activation status of an asset."""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                asset_url,
                auth=(API_KEY, ""),
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            if response.status_code == 429:  # Rate limit
                logger.warning(f"Rate limit hit, waiting {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
                
            logger.error(f"Error checking asset status (attempt {attempt + 1}/{max_retries}): {response.text}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
        
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
    
    return None


def activate_asset(asset_url):
    """Activate an asset for download."""
    max_retries = 3
    retry_delay = 2
    
    logger.info(f"Activating asset with URL: {asset_url}")
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                asset_url,
                auth=(API_KEY, ""),
                timeout=30
            )
            
            if response.status_code in (202, 204):
                logger.info("Asset activation request successful")
                return True
            
            if response.status_code == 429:  # Rate limit
                logger.warning(f"Rate limit hit, waiting {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
                
            logger.error(f"Error activating asset (attempt {attempt + 1}/{max_retries}): {response.text}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
        
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
    
    return False


def wait_for_asset_activation(asset_url, timeout=300, check_interval=5):
    """Wait for an asset to be activated."""
    logger.info("Waiting for asset activation...")
    start_time = time.time()
    
    # Use the provided asset URL directly for status checking
    # This should be the self_link from the asset data
    
    # Extract asset key from the URL to update status
    # URL format: .../item-types/{item_type}/items/{item_id}/assets/{asset_type}
    url_parts = asset_url.split('/')
    if len(url_parts) >= 8:
        item_id = url_parts[-4]
        asset_type = url_parts[-1]
        asset_key = f"{item_id}_{asset_type}"
    else:
        asset_key = None
        logger.warning(f"Could not extract asset key from URL: {asset_url}")
    
    while time.time() - start_time < timeout:
        asset_status = get_asset_activation_status(asset_url)
        
        if not asset_status:
            return False
        
        if asset_status["status"] == "active":
            logger.info("Asset is now active and ready for download")
            
            # Update the status in the status file if we have the asset key
            if asset_key:
                status = load_status()
                if asset_key in status["activated_scenes"]:
                    status["activated_scenes"][asset_key] = "active"
                    save_status(status)
                    logger.info(f"Updated status for {asset_key} to 'active'")
            
            return True
        
        logger.info(f"Asset status: {asset_status['status']}. Waiting...")
        time.sleep(check_interval)
    
    logger.error(f"Asset activation timed out after {timeout} seconds")
    return False


def download_asset(asset_url, output_path):
    """Download an activated asset."""
    logger.info(f"Downloading asset to {output_path}...")
    
    # Get the download URL
    status = get_asset_activation_status(asset_url)
    if not status or status["status"] != "active" or "location" not in status:
        logger.error("Asset is not active or download URL not available")
        return False
    
    download_url = status["location"]
    
    # Download the asset
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = requests.get(download_url, stream=True, timeout=60)
            
            if response.status_code == 200:
                # Save the asset to disk
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Asset downloaded successfully to {output_path}")
                return True
            
            if response.status_code == 429:  # Rate limit
                logger.warning(f"Rate limit hit, waiting {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            
            logger.error(f"Error downloading asset (attempt {attempt + 1}/{max_retries}): Status code {response.status_code}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Download request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
        
        if attempt < max_retries - 1:
            logger.info(f"Retrying download in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
    
    logger.error("All download attempts failed")
    return False




def load_status():
    """Load the status of activated and downloaded scenes from the status file."""
    status_file = "planet_status.json"
    if os.path.exists(status_file):
        with open(status_file, "r") as f:
            return json.load(f)
    return {"activated_scenes": {}, "downloaded_scenes": {}}


def save_status(status):
    """Save the status of activated and downloaded scenes to the status file."""
    status_file = "planet_status.json"
    with open(status_file, "w") as f:
        json.dump(status, f, indent=4)


def save_metadata(scene, output_dir):
    """Save metadata for a scene to a JSON file."""
    scene_id = scene["id"]
    acquired_date = scene["properties"]["acquired"].split("T")[0]
    
    # Extract year from the acquired date
    year = acquired_date.split("-")[0]
    
    # Create output directory structure: /output_dir/YYYY/YYYY-MM-DD/
    year_dir = os.path.join(output_dir, year)
    date_dir = os.path.join(year_dir, acquired_date)
    os.makedirs(date_dir, exist_ok=True)
    
    # Create metadata file path
    metadata_file = os.path.join(date_dir, f"{scene_id}_metadata.json")
    
    # Extract relevant metadata from scene properties
    metadata = {
        "id": scene_id,
        "acquired": scene["properties"]["acquired"],
        "cloud_cover": scene["properties"]["cloud_cover"],
        "sun_azimuth": scene["properties"].get("sun_azimuth"),
        "sun_elevation": scene["properties"].get("sun_elevation"),
        "view_angle": scene["properties"].get("view_angle"),
        "satellite_id": scene["properties"].get("satellite_id"),
        "ground_control": scene["properties"].get("ground_control"),
        "item_type": scene["properties"].get("item_type"),
        "quality_category": scene["properties"].get("quality_category")
    }
    
    # Save metadata to file
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=4)
    
    logger.info(f"Saved metadata to {metadata_file}")
    
    return date_dir  # Return the date directory path for use in process_scene


def process_scene(scene, asset_type, output_dir, activate_only=False):
    """Process a single scene and asset type: activate, download, and convert to COG."""
    scene_id = scene["id"]
    acquired_date = scene["properties"]["acquired"].split("T")[0]
    
    # Save metadata for the scene and get the date directory path
    date_dir = save_metadata(scene, output_dir)
    
    # Load current status
    status = load_status()
    
    # Get the asset URL
    assets_url = f"{BASE_URL}/item-types/{scene['properties']['item_type']}/items/{scene_id}/assets"
    
    # Get available assets
    response = requests.get(
        assets_url,
        auth=(API_KEY, "")
    )
    
    if response.status_code != 200:
        logger.error(f"Error getting assets for scene {scene_id}: {response.text}")
        return False
    
    assets = response.json()
    
    if asset_type not in assets:
        logger.error(f"Asset type '{asset_type}' not available for scene {scene_id}")
        return False
    
    # Get asset data and links
    asset_data = assets[asset_type]
    if "_links" not in asset_data:
        logger.error(f"Asset data for {scene_id} does not contain _links. Asset data: {json.dumps(asset_data)}")
        return False
        
    links = asset_data["_links"]
    if "activate" not in links or "_self" not in links:
        logger.error(f"Asset links for {scene_id} missing required links. Links: {json.dumps(links)}")
        return False
        
    activation_link = links["activate"]
    self_link = links["_self"]
    
    logger.info(f"Found activation link for {asset_type}: {activation_link}")
    
    # Create a unique key for tracking this specific asset
    asset_key = f"{scene_id}_{asset_type}"
    
    # Check if already activated
    if asset_key in status["activated_scenes"]:
        logger.info(f"Asset {asset_key} already activated, skipping activation...")
        if not activate_only:
            if not wait_for_asset_activation(self_link):
                return False
    else:
        # Activate the asset
        if activate_asset(activation_link):
            status["activated_scenes"][asset_key] = "activating"
            save_status(status)
            logger.info(f"Asset {asset_key} activation requested successfully")
        else:
            return False
        
        # Only wait for activation if not in activate-only mode
        if not activate_only:
            if not wait_for_asset_activation(self_link):
                return False
    
    # If in activate-only mode, return here
    if activate_only:
        return True
    
    # Check if already downloaded
    raw_tif_path = os.path.join(date_dir, f"{scene_id}_{asset_type}.tif")
    if asset_key in status["downloaded_scenes"] and os.path.exists(raw_tif_path):
        logger.info(f"Asset {asset_key} already downloaded, skipping download...")
    else:
        # Download the asset
        if download_asset(self_link, raw_tif_path):
            status["downloaded_scenes"][asset_key] = True
            save_status(status)
        else:
            return False
    
    # Convert to COG
    cog_path = os.path.join(date_dir, f"{scene_id}_{asset_type}_cog.tif")
    
    return True


def display_status_summary():
    """Display a summary of activated and downloaded scenes."""
    status = load_status()
    
    logger.info("===== Planet Products Status Summary =====")
    
    # Count activated and downloaded scenes
    activated_count = len(status["activated_scenes"])
    downloaded_count = len(status["downloaded_scenes"])
    
    logger.info(f"Total activated assets: {activated_count}")
    logger.info(f"Total downloaded assets: {downloaded_count}")
    
    # List activated scenes
    if activated_count > 0:
        logger.info("\nActivated assets:")
        for asset_key, activation_status in status["activated_scenes"].items():
            if activation_status == "active":
                status_text = "active and ready for download"
            else:
                status_text = "activation in progress"
            logger.info(f"  - {asset_key}: {status_text}")
    
    # List downloaded scenes
    if downloaded_count > 0:
        logger.info("\nDownloaded assets:")
        for asset_key in status["downloaded_scenes"].keys():
            logger.info(f"  - {asset_key}")
    
    logger.info("=========================================")


def main():
    """Main function to run the script."""
    args = parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Display status summary at the beginning
    display_status_summary()
    
    # Build search request
    search_request = build_search_request(
        args.start_date,
        args.end_date,
        args.latitude,
        args.longitude,
        args.item_type,
        args.max_cloud_cover
    )
    
    # Search for imagery
    features = search_planet_imagery(search_request)
    if not features:
        logger.error("No imagery found matching the criteria")
        return
    
    # Process each scene and each asset type
    successful_assets = 0
    total_assets = len(features) * len(args.asset_types)
    
    for scene in features:
        logger.info(f"Processing scene {scene['id']}...")
        for asset_type in args.asset_types:
            logger.info(f"Processing asset type {asset_type} for scene {scene['id']}...")
            if process_scene(scene, asset_type, str(output_dir), args.activate_only):
                successful_assets += 1
    
    if args.activate_only:
        logger.info("Activation requests completed. Run the script again later without --activate-only to download the assets.")
    
    logger.info(f"Processed {successful_assets} out of {total_assets} assets successfully")
    
    # Display updated status summary at the end
    logger.info("\nUpdated status after processing:")
    display_status_summary()


if __name__ == "__main__":
    main()