import yaml
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "config.yaml"

with open(CONFIG_PATH, "r") as f:
    _config = yaml.safe_load(f)

DB_CONFIG = _config["database"]
