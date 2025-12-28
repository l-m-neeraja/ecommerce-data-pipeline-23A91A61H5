import pytest
import os

@pytest.fixture(scope="session")
def data_paths():
    return {
        "raw": "data/raw",
        "processed": "data/processed"
    }

@pytest.fixture(scope="session")
def analytics_path():
    return "data/processed/analytics"
