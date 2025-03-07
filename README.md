# Planet Labs Satellite Imagery Downloader

This project automates the process of retrieving satellite imagery from [Planet Labs](https://www.planet.com) for a specified geographic point and time window. It then converts the downloaded imagery into a multi-layer **Cloud Optimized GeoTIFF (COG)** using GDAL, storing all outputs locally.

## Features

- Connect to Planet Labs' API and authenticate using your API key
- Query and download imagery for a specific location and date range
- Filter results by cloud cover percentage
- Automatically organize downloaded imagery in a structured format
- Convert downloaded imagery to Cloud Optimized GeoTIFF (COG) format
- Comprehensive logging for tracking operations and troubleshooting

## Requirements

- Python 3.7+
- GDAL (with command-line tools)
- Planet Labs API key

### Python Dependencies

- requests
- python-dotenv
- pathlib

## Installation

1. Clone this repository or download the source code

2. Install the required Python packages:

```bash
pip install requests python-dotenv
```

3. Make sure GDAL is installed on your system with command-line tools available

4. Create a `.env` file in the project directory with your Planet Labs API key:

```
planet_api_key = 'YOUR_API_KEY_HERE'
```

## Usage

The script operates in two modes: activation and download. Due to Planet Labs' API workflow, assets need to be activated before they can be downloaded. Activation typically takes 30 minutes to 1 hour to complete.

Recommended workflow:

1. First, run the script with `--activate-only` to request activation for all matching assets:

```bash
python planet_downloader.py \
  --start-date 2023-01-01 \
  --end-date 2023-01-31 \
  --activate-only
```

2. Wait 30-60 minutes for the activation process to complete

3. Then run the script again without `--activate-only` to download the activated assets:

```bash
python planet_downloader.py \
  --start-date 2023-01-01 \
  --end-date 2023-01-31
```

### Command Line Arguments

- `--start-date`: Start date in YYYY-MM-DD format (required)
- `--end-date`: End date in YYYY-MM-DD format (required)
- `--latitude`: Latitude of the point of interest (default: 37.355138)
- `--longitude`: Longitude of the point of interest (default: -120.411734)
- `--item-type`: Planet item type (default: PSScene)
- `--asset-types`: Asset types to download (default: basic_analytic_8b and ortho_visual)
- `--max-cloud-cover`: Maximum cloud cover percentage (default: 30.0)
- `--output-dir`: Output directory for downloaded data (default: ./planet_data)
- `--activate-only`: Only request activation for assets without downloading

## Output Structure

The downloaded imagery will be organized in the following structure:

```
output_dir/
  ├── YYYY-MM-DD/
  │   ├── scene_id_metadata.json       # Scene metadata
  │   ├── scene_id_asset_type.tif      # Original downloaded asset
  │   └── scene_id_asset_type_cog.tif  # Cloud Optimized GeoTIFF
  ├── planet_downloader.log            # Log file
  └── planet_status.json               # Activation status tracking
```

## Status Tracking

The script maintains a `planet_status.json` file to track the activation status of assets. This helps resume interrupted downloads and avoid re-activating already processed assets.

## Error Handling

The script includes robust error handling for:
- API rate limits
- Network connectivity issues
- Activation timeouts
- Incomplete or corrupted downloads

All operations are logged to both the console and `planet_downloader.log` for troubleshooting.

## Extending the Script

This script can be easily extended to:
- Support different Planet product types
- Add more filtering options
- Implement additional processing steps
- Calculate vegetation indices or other analyses
- Generate thumbnails or previews