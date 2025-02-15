#!/opt/homebrew/bin/python3
import os
import subprocess
import datetime
import time
import boto3
import logging
import traceback

# Configure logging for runtime logs in DigitalOcean console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def backup_mongodb_and_upload():
    """Backup all MongoDB collections and upload to S3."""
    start_time = time.time()  # Start time for execution duration

    # Get MongoDB settings from environment variables
    mongo_host = os.getenv("MONGO_HOST", "localhost")
    db_name = os.getenv("MONGO_DB_NAME", "your_database")
    
    # Get S3 settings from environment variables
    s3_bucket = os.getenv("S3_BUCKET", "your-s3-bucket")
    s3_folder = os.getenv("S3_FOLDER", "backups/mongo")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        logging.error("AWS credentials are not set.")
        return {"error": "AWS credentials are missing."}

    backup_dir = "/tmp/mongo_backup"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_file = f"{backup_dir}/mongodump_{timestamp}.gz"

    # Ensure backup directory exists
    os.makedirs(backup_dir, exist_ok=True)

    try:
        logging.info(f"Starting MongoDB backup at {timestamp}...")

        # Run mongodump
        dump_cmd = [
            "mongodump",
            "--host", mongo_host,
            "--gzip",
            "--archive=" + backup_file
        ]
        subprocess.run(dump_cmd, check=True)
        logging.info(f"Backup created: {backup_file}")

        # Get file size
        backup_size = os.path.getsize(backup_file) / (1024 * 1024)  # Convert to MB
        logging.info(f"Backup file size: {backup_size:.2f} MB")

        # Connect to S3
        logging.info("Connecting to S3...")
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        # Upload backup to S3
        s3_key = f"{s3_folder}/mongodump_{timestamp}.gz"
        s3_client.upload_file(backup_file, s3_bucket, s3_key)
        logging.info(f"Uploaded to S3: s3://{s3_bucket}/{s3_key}")

        # Cleanup
        os.remove(backup_file)
        logging.info("Local backup removed.")

        execution_time = time.time() - start_time  # Calculate execution duration
        logging.info(f"Backup completed in {execution_time:.2f} seconds.")

        return {"message": "Backup successful", "s3_path": f"s3://{s3_bucket}/{s3_key}", "duration": execution_time}

    except subprocess.CalledProcessError as e:
        logging.error(f"Error during mongodump: {e}")
        logging.error(traceback.format_exc())  # Print full error trace
        return {"error": "mongodump failed", "details": str(e)}

    except Exception as e:
        logging.error(f"Error during S3 upload: {e}")
        logging.error(traceback.format_exc())  # Print full error trace
        return {"error": "S3 upload failed", "details": str(e)}

def main(args=None):
    """Main entry point for cloud functions."""
    return backup_mongodb_and_upload()

def schedule_backup():
    """Run the backup every hour (for local execution)."""
    while True:
        logging.info("Starting scheduled backup process...")
        backup_mongodb_and_upload()
        logging.info("Waiting for the next backup in 1 hour...")
        time.sleep(3600)  # Wait for 1 hour

if __name__ == "__main__":
    logging.info("MongoDB Backup Script Started...")
    schedule_backup()
