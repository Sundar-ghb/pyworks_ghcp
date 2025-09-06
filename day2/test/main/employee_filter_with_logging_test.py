import json
import csv
import sys
from pathlib import Path
import pytest

from pyworks_ghcp.day2.src.main.employee_filter_with_logging import (
    load_employees,
    filter_by_salary,
    save_to_csv,
    main,
)

def test_save_to_csv_empty_list_logs_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    out = tmp_path / "out.csv"
    caplog.set_level("WARNING")
    save_to_csv([], out)
    assert "No employees to save." in caplog.text
    assert not out.exists()

def test_cli_end_to_end_logs_info(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    data = [
        {"name": "Alice", "role": "Dev", "salary": 120000},
        {"name": "Bob", "role": "QA", "salary": 80000},
        {"name": "Charlie", "role": "Mgr", "salary": 150000},
    ]
    inp = tmp_path / "employees.json"
    inp.write_text(json.dumps(data), encoding="utf-8")
    out = tmp_path / "high_salary.csv"

    monkeypatch.setattr(sys, "argv", [
        "employee_filter_logging",
        "--input", str(inp),
        "--output", str(out),
        "--threshold", "100000",
    ])

    caplog.set_level("INFO")
    main()

    assert "Saved 2 employees" in caplog.text
    assert out.exists()
    with out.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["name"] for r in rows] == ["Alice", "Charlie"]