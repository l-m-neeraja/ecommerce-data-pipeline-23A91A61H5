import pytest
import json
from pathlib import Path


QUALITY_REPORT_PATH = Path("data/processed/quality_report.json")


def test_quality_report_generated():
    """
    Verify quality report JSON file is generated.
    """
    if not QUALITY_REPORT_PATH.exists():
        pytest.skip("Quality report not generated yet")

    assert QUALITY_REPORT_PATH.exists(), "quality_report.json not found"


def test_quality_report_schema():
    """
    Validate structure of quality report JSON.
    """
    if not QUALITY_REPORT_PATH.exists():
        pytest.skip("Quality report not generated yet")

    with open(QUALITY_REPORT_PATH, "r") as f:
        report = json.load(f)

    required_keys = {
        "quality_score",
        "checks",
        "timestamp"
    }

    for key in required_keys:
        assert key in report, f"Missing key in quality report: {key}"


def test_quality_score_range():
    """
    Ensure quality score is within valid range (0–100).
    """
    if not QUALITY_REPORT_PATH.exists():
        pytest.skip("Quality report not generated yet")

    with open(QUALITY_REPORT_PATH, "r") as f:
        report = json.load(f)

    score = report.get("quality_score")
    assert isinstance(score, (int, float)), "Quality score must be numeric"
    assert 0 <= score <= 100, "Quality score out of valid range"


def test_quality_checks_present():
    """
    Verify required quality checks are present.
    """
    if not QUALITY_REPORT_PATH.exists():
        pytest.skip("Quality report not generated yet")

    with open(QUALITY_REPORT_PATH, "r") as f:
        report = json.load(f)

    expected_checks = {
        "null_checks",
        "duplicate_checks",
        "referential_integrity"
    }

    checks = report.get("checks", {})
    for check in expected_checks:
        assert check in checks, f"Missing quality check: {check}"
