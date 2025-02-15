import os
import subprocess
import datetime
import time
import boto3

def backup_mongodb_and_upload():
    """Backup all MongoDB collections and upload to S3."""
    mongo_host = "localhost"  # Change if needed
    db_name = "your_database"  # Change if backing up a specific database
    backup_dir = "/tmp/mongo_backup"
    s3_bucket = "your-s3-bucket"
    s3_folder = "backups/mongo"

    # Timestamp for file naming
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_file = f"{backup_dir}/mongodump_{timestamp}.gz"

    # Ensure backup directory exists
    os.makedirs(backup_dir, exist_ok=True)

    try:
        # Run mongodump
        dump_cmd = [
            "mongodump",
            "--host", mongo_host,
            "--gzip",
            "--archive=" + backup_file
        ]
        subprocess.run(dump_cmd, check=True)
        print(f"Backup created: {backup_file}")

        # Upload to S3
        s3_client = boto3.client("s3")
        s3_key = f"{s3_folder}/mongodump_{timestamp}.gz"
        s3_client.upload_file(backup_file, s3_bucket, s3_key)
        print(f"Uploaded to S3: s3://{s3_bucket}/{s3_key}")

        # Cleanup
        os.remove(backup_file)
        print("Local backup removed.")

    except subprocess.CalledProcessError as e:
        print(f"Error during mongodump: {e}")
    except Exception as e:
        print(f"Error during S3 upload: {e}")

def schedule_backup():
    """Run the backup every hour."""
    while True:
        backup_mongodb_and_upload()
        print("Waiting for the next backup...")
        time.sleep(10)  # Wait for 1 hour

if __name__ == "__main__":
    schedule_backup()
