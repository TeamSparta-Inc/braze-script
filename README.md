# Braze User Backfill Script

Python script for uploading user data to Braze via API. Supports reading CSV files from both S3 and local filesystem.

## Features

- CSV to Braze format conversion
- S3 integration for cloud storage
- Batch upload with retry logic
- Support for custom user attributes
- Comprehensive logging
- Environment variable configuration

## Prerequisites

- Python 3.7+
- AWS CLI configured with appropriate credentials
- Braze API key with user tracking permissions
- Access to S3 bucket (for S3 mode)

## Installation

1. Install required dependencies:

```bash
pip install -r requirements.txt
```

2. Configure environment variables (copy `.env.example` to `.env` and edit):

```bash
cp .env.example .env
```

Edit `.env` file:
```
BRAZE_API_KEY=your-braze-api-key
S3_CSV_KEY=backfill-csv/your-file.csv
AWS_PROFILE=admin
S3_BUCKET=sparta-braze-currents
```

## Usage

### Using S3 (Recommended)

1. Upload your CSV file to S3:
```bash
aws s3 cp your-file.csv s3://sparta-braze-currents/backfill-csv/
```

2. Set the S3 key in environment variable or modify the script
3. Run the script:

```bash
python user_backfill.py
```

### Using Local Files

Modify the main function to use local files:

```python
success = uploader.upload_from_csv('/path/to/your/file.csv', batch_size=50, from_s3=False)
```

## Configuration

The script can be configured via environment variables:

- `BRAZE_API_KEY`: Your Braze API key
- `S3_CSV_KEY`: S3 key of the CSV file (e.g., `backfill-csv/braze_user.csv`)
- `AWS_PROFILE`: AWS CLI profile name (default: `admin`)
- `S3_BUCKET`: S3 bucket name (default: `sparta-braze-currents`)

## CSV Format

Required columns:
- `email` or `external_id` (at least one required)

Optional columns:
- `first_name`
- `phone`
- `user_type`
- `is_marketing`
- `signup_date`
- `business`
- `applied_business` (JSON array)
- `in_progress_business` (JSON array)
- `completed_business` (JSON array)
- `is_test`
- `kdt_funnel_stage`
- `hh_funnel_stage`
- `has_card`

## SQL Queries

The `sql/` directory contains sample queries for extracting user data from your database.

## Current S3 Files

Files currently stored in S3 (`sparta-braze-currents/backfill-csv/`):
- `braze_user_202510281722.csv` (10.6 KB)
- `braze_user_202510281601.csv` (105.9 MB)

## Error Handling

The script includes:
- Automatic retry logic for failed batches
- Detailed error logging
- Graceful handling of malformed data
