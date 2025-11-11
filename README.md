# Braze User Backfill

Python script for uploading user data to Braze via API.

## Features

- CSV to Braze format conversion
- Batch upload with retry logic
- Support for custom user attributes
- Comprehensive logging

## Usage

1. Set your Braze API key in the script
2. Prepare your CSV file with user data
3. Run the script:

```bash
python user_backfill.py
```

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
- `applied_business`
- `in_progress_business`
- `completed_business`
- `is_test`
- `kdt_funnel_stage`
- `hh_funnel_stage`
- `has_card`

## SQL Queries

The `sql/` directory contains sample queries for extracting user data from your database.
