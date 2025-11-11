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

2. Run the script with S3 key as argument:
```bash
python user_backfill.py backfill-csv/braze_user_202510281601.csv
```

3. Optional parameters:
```bash
# Custom batch size
python user_backfill.py backfill-csv/braze_user.csv --batch-size 100

# Show help
python user_backfill.py --help
```

### Using Local Files

Use the `--local` flag to read from local filesystem:

```bash
python user_backfill.py /path/to/your/file.csv --local
```

## Configuration

The script can be configured via environment variables:

- `BRAZE_API_KEY`: Your Braze API key (required)
- `AWS_PROFILE`: AWS CLI profile name (default: `admin`)
- `S3_BUCKET`: S3 bucket name (default: `sparta-braze-currents`)

Command line arguments:

- `s3_key`: S3 CSV 파일 키 또는 로컬 파일 경로 (required, positional argument)
- `--batch-size`: 배치 크기 (optional, default: 50)
- `--local`: 로컬 파일 모드 사용 (optional flag)

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
