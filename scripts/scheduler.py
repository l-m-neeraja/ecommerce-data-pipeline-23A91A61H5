import schedule
import subprocess
import time
import logging
import os
from datetime import datetime

LOCK_FILE = "pipeline.lock"

LOG_FILE = "logs/scheduler_activity.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def pipeline_running():
    return os.path.exists(LOCK_FILE)

def run_pipeline():
    if pipeline_running():
        logging.warning("Pipeline already running. Skipping this run.")
        return

    try:
        open(LOCK_FILE, "w").close()
        logging.info("Starting scheduled pipeline execution")

        result = subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logging.info("Pipeline executed successfully")
        else:
            logging.error("Pipeline failed")
            logging.error(result.stderr)

    except Exception as e:
        logging.exception("Scheduler encountered an error")

    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        logging.info("Pipeline execution finished")

schedule.every().day.at("22:30").do(run_pipeline)

logging.info("Scheduler started")

while True:
    schedule.run_pending()
    time.sleep(30)
