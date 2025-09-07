import csv
import json
import sys
from pathlib import Path

import pytest

import os
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])
sys.path.insert(0, src_path)  # noqa

from src.main.employee_filter_cli import (
    load_employees,
    filter_by_salary,
    save_to_csv,
    main,
)

def test_load_employees_success(tmp_path: Path):
    data = [
        {"name": "Alice", "role": "Dev", "salary": 120000},
        {"name": "Bob", "role": "QA", "salary": 80000},
    ]
    f = tmp_path / "employees.json"
    f.write_text(json.dumps(data), encoding="utf-8")

    result = load_employees(f)

    assert isinstance(result, list)
    assert result[0]["name"] == "Alice"
    assert result[1]["salary"] == 80000

def test_filter_by_salary_threshold():
    employees = [
        {"name": "A", "salary": 120000},
        {"name": "B", "salary": 90000},
        {"name": "C", "salary": 150000},
        {"name": "D"},  # missing salary → treated as 0
    ]

    result = filter_by_salary(employees, 100_000)

    names = [e["name"] for e in result]
    assert names == ["A", "C"]

def test_save_to_csv_writes_file(tmp_path: Path):
    rows = [
        {"name": "Alice", "role": "Dev", "salary": 120000},
        {"name": "Charlie", "role": "Mgr", "salary": 150000},
    ]
    out = tmp_path / "out.csv"

    save_to_csv(rows, out)

    assert out.exists()
    with out.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        got = list(reader)
    assert got[0]["name"] == "Alice"
    assert got[1]["salary"] == "150000"

def test_save_to_csv_empty_list_warns(tmp_path: Path, capsys: pytest.CaptureFixture):
    out = tmp_path / "out.csv"

    save_to_csv([], out)

    captured = capsys.readouterr()
    assert "[WARN] No employees to save." in captured.out
    assert not out.exists()

def test_cli_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    data = [
        {"name": "Alice", "role": "Dev", "salary": 120000},
        {"name": "Bob", "role": "QA", "salary": 80000},
        {"name": "Charlie", "role": "Mgr", "salary": 150000},
    ]
    inp = tmp_path / "employees.json"
    inp.write_text(json.dumps(data), encoding="utf-8")
    out = tmp_path / "high_salary.csv"

    monkeypatch.setenv("PYTHONWARNINGS", "default")
    monkeypatch.setattr(sys, "argv", [
        "employee_filter_cli",
        "--input", str(inp),
        "--output", str(out),
        "--threshold", "100000",
    ])

    main()

    captured = capsys.readouterr()
    assert "[INFO] Saved 2 employees" in captured.out
    assert out.exists()
    with out.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    names = [r["name"] for r in rows]
    assert names == ["Alice", "Charlie"]

def test_cli_missing_input_raises(monkeypatch: pytest.MonkeyPatch):
    out = Path("should_not_be_created.csv")
    try:
        monkeypatch.setattr(sys, "argv", [
            "employee_filter_cli",
            "--input", "no_such_file.json",
            "--output", str(out),
        ])
        with pytest.raises(FileNotFoundError):
            main()
    finally:
        if out.exists():
            out.unlink()

def test_load_employees_invalid_json_raises(tmp_path: Path):
    bad = tmp_path / "bad.json"
    bad.write_text("{invalid_json:}", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_employees(bad)