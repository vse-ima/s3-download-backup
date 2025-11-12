#!/usr/bin/env python3
# /// script
# dependencies = [
#   "minio",
# ]
# ///
"""
Python script that downloads recent backup files from an S3 bucket
based on a regex pattern. It searches recursively in the bucket, filters files
modified within the last 24 hours, and downloads them to a local directory,
preserving the directory structure. Logs are maintained for all operations.

The script can be launch with "uv run main.py"
"""

import os
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path
from minio import Minio
from minio.error import S3Error

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('s3_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class S3BackupDownloader:
    def __init__(self, endpoint, access_key, secret_key, bucket_name, 
                 download_dir, regex_pattern):
        """
        Initialize the S3 client (using the minio library)
        
        Args:
            endpoint: S3/MinIO server URL (e.g. 'minio.example.com:9000')
            access_key: Access key for S3/MinIO
            secret_key: Secret key for S3/MinIO
            bucket_name: Bucket name
            download_dir: Local destination directory
            regex_pattern: Regex pattern to filter files
        """
        self.bucket_name = bucket_name
        self.download_dir = Path(download_dir)
        self.pattern = re.compile(regex_pattern, re.IGNORECASE)

        # Create destination directory if it doesn't exist
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Initialize the S3/MinIO client
        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=True
            )
            logger.info(f"Connected to S3/MinIO endpoint: {endpoint}")
        except Exception as e:
            logger.error(f"Error connecting to S3/MinIO: {e}")
            raise
    
    def is_recent(self, last_modified, hours=24):
        """
        Check if a file was modified within the last `hours` hours.

        Args:
            last_modified: datetime of last modification
            hours: number of hours (24 by default)

        Returns:
            bool: True if the file is recent
        """
        # Make last_modified offset-aware if it is not
        if last_modified.tzinfo is None:
            from datetime import timezone
            last_modified = last_modified.replace(tzinfo=timezone.utc)
        
        cutoff_time = datetime.now(last_modified.tzinfo) - timedelta(hours=hours)
        return last_modified > cutoff_time
    
    def find_recent_backups(self, prefix=''):
        """
        Recursively search for recent backup files in the bucket.

        Args:
            prefix: Prefix/folder within the bucket

        Returns:
            list: List of found objects [{'name', 'modified', 'size'}]
        """
        recent_backups = []

        try:
            # List all objects in the bucket recursively
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=prefix,
                recursive=True
            )

            for obj in objects:
                # Check if the object name matches the pattern
                if self.pattern.match(obj.object_name):
                    # Check if the file is recent
                    if self.is_recent(obj.last_modified):
                        recent_backups.append({
                            'name': obj.object_name,
                            'modified': obj.last_modified,
                            'size': obj.size
                        })
                        logger.info(f"Recent backup found: {obj.object_name} "
                                    f"(modified {obj.last_modified})")

            if not recent_backups:
                logger.warning("No recent backups found")

            return recent_backups

        except S3Error as e:
            logger.error(f"S3 error while searching: {e}")
            return []
        except Exception as e:
            logger.error(f"Error while searching: {e}")
            return []
    
    def download_file(self, object_name):
        """
        Download a file from S3/MinIO.

        Args:
            object_name: Object name in the bucket

        Returns:
            bool: True if download succeeded
        """
        try:
            # Create local path (preserving structure)
            local_path = self.download_dir / Path(object_name).name
            
            logger.info(f"Downloading {object_name}...")
            
            # Download the file
            self.client.fget_object(
                self.bucket_name,
                object_name,
                str(local_path)
            )
            
            file_size = local_path.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"Download succeeded: {local_path} ({file_size:.2f} MB)")
            return True
            
        except S3Error as e:
            logger.error(f"S3 error while downloading {object_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error while downloading {object_name}: {e}")
            return False
    
    def run(self, prefix=''):
        """
        Run the full search-and-download process.

        Args:
            prefix: Folder/prefix in the bucket to search
        """
        logger.info("=" * 60)
        logger.info("Starting backup download")
        logger.info(f"Bucket: {self.bucket_name}")
        logger.info(f"Prefix: {prefix if prefix else '(root)'}")
        logger.info(f"Destination: {self.download_dir}")
        logger.info("=" * 60)

        # Search for recent backups
        backups = self.find_recent_backups(prefix)

        if not backups:
            logger.error("ERROR: No recent backups found!")
            return False

        # Download found files
        success_count = 0
        for backup in backups:
            if self.download_file(backup['name']):
                success_count += 1

        logger.info("=" * 60)
        logger.info(f"Downloads finished: {success_count}/{len(backups)} succeeded")
        logger.info("=" * 60)

        return success_count > 0


def main():

    # ==================== CONFIGURATION ====================
    # Adjust these values for your environment
    MINIO_ENDPOINT = "minio.example.com"
    MINIO_ACCESS_KEY = "your-access-key"
    MINIO_SECRET_KEY = "your-secret-key"
    BUCKET_NAME = "bucket-name"
    BUCKET_PREFIX = "folder/"
    DOWNLOAD_DIR = "./downloads"
    REGEX_PATTERN = r'.*DATA.*\.bak$'  # Regex pattern to filter files
    # =======================================================
    
    try:
        downloader = S3BackupDownloader(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=BUCKET_NAME,
            download_dir=DOWNLOAD_DIR,
            regex_pattern=REGEX_PATTERN
        )
        
        success = downloader.run(prefix=BUCKET_PREFIX)
        
        if not success:
            exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)


if __name__ == "__main__":
    main()