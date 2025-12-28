import os
import time
import logging

RETENTION_DAYS = 7
SECONDS_IN_DAY = 86400

TARGET_DIRS = [
    "data/raw",
    "data/staging",
    "logs"
]

PRESERVE_KEYWORDS = ["report", "summary"]

logging.basicConfig(
    filename="logs/cleanup_activity.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def should_preserve(filename):
    return any(keyword in filename.lower() for keyword in PRESERVE_KEYWORDS)

def cleanup_directory(directory):
    if not os.path.exists(directory):
        return

    now = time.time()

    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)

        if not os.path.isfile(file_path):
            continue

        if should_preserve(file):
            continue

        file_age_days = (now - os.path.getmtime(file_path)) / SECONDS_IN_DAY

        if file_age_days > RETENTION_DAYS:
            os.remove(file_path)
            logging.info(f"Deleted old file: {file_path}")

def run_cleanup():
    logging.info("Cleanup started")
    for directory in TARGET_DIRS:
        cleanup_directory(directory)
    logging.info("Cleanup completed")

if __name__ == "__main__":
    run_cleanup()
