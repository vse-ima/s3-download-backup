# MinIO Backup Downloader

A Python script to automatically download recent database backups from MinIO object storage.

## Features

- **Recursive search** through bucket folders
- **Time-based filtering** (files modified within the last 24 hours)
- **Regex pattern matching** for file names
- **Detailed logging** (console + file output)

## Requirements

- Python 3.8+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Installation

### With uv (recommended)

The script includes inline dependencies. Simply run:

```bash
uv run main.py
```

### With pip

```bash
pip install minio
python main.py
```

## Configuration

Edit the configuration section in the script:

```python
MINIO_ENDPOINT = "minio.example.com:9000"
MINIO_ACCESS_KEY = "your-access-key"
MINIO_SECRET_KEY = "your-secret-key"
BUCKET_NAME = "backups"
BUCKET_PREFIX = "databases/"  # Folder in bucket (empty for root)
DOWNLOAD_DIR = "./downloads"
```

## Usage

### Automated with cron

#### Daily backup download at 2:00 AM

```bash
# Edit crontab
crontab -e

# Add this line (adjust paths accordingly)
0 2 * * * /usr/local/bin/uv run /path/to/main.py >> /var/log/minio_backup.log 2>&1
```

## Logging

The script generates two types of logs:

- **Console output**: Real-time progress, can be disable by commenting out the `StreamHandler` in the logging setup.
- **File**: `minio_download.log` in the script directory

