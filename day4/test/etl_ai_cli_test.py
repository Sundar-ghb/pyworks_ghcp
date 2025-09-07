import csv
import sys
from pathlib import Path
import json
import pytest

import os
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])
sys.path.insert(0, src_path)  # noqa

import src.main.etl_ai_cli as etl_ai  # Import your Day 4 script

# -----------------------
# Fixtures
# -----------------------

@pytest.fixture
def sample_employee_csv(tmp_path: Path):
    """Create a small CSV for testing."""
    file_path = tmp_path / "employees.csv"
    rows = [
        ["name", "role", "salary", "location", "years_experience"],
        ["Alice", "Developer", 120000, "New York", 5],
        ["Bob", "QA", 80000, "London", 3],
        ["Charlie", "Manager", 150000, "Berlin", 10],
        ["Diana", "Developer", 130000, "Toronto", 7],
    ]
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    return file_path

@pytest.fixture
def sample_etl_output_csv(tmp_path: Path):
    """Create a small ETL output CSV for AI step testing."""
    file_path = tmp_path / "etl_output.csv"
    rows = [
        ["role", "avg_salary"],
        ["Developer", 125000],
        ["Manager", 155000],
        ["QA", 95000],
    ]
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    return file_path

# -----------------------
# ETL Function Tests
# -----------------------

@pytest.mark.parametrize("engine_func", [etl_ai.pandas_etl, etl_ai.polars_etl])
def test_etl_functions(engine_func, sample_employee_csv, tmp_path):
    out_file = tmp_path / "avg_salary_by_role.csv"
    engine_func(sample_employee_csv, out_file, threshold=100_000)
    assert out_file.exists()
    with out_file.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert any(r["role"] == "Developer" and float(r["avg_salary"]) > 120000 for r in rows)

# -----------------------
# AI Inference Tests
# -----------------------

def test_ai_inference_adds_category(sample_etl_output_csv, tmp_path, monkeypatch):
    out_file = tmp_path / "ai_output.csv"

    # Mock the Hugging Face pipeline to avoid downloading/running a model
    class DummyPipeline:
        def __call__(self, texts):
            return [{"label": "POSITIVE", "score": 0.99} for _ in texts]

    monkeypatch.setattr(etl_ai, "pipeline", lambda *a, **k: DummyPipeline())

    etl_ai.ai_inference(sample_etl_output_csv, out_file)
    assert out_file.exists()

    with out_file.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert "salary_category" in rows[0]
    # Check that categories match our rule-based logic
    dev = next(r for r in rows if r["role"] == "Developer")
    mgr = next(r for r in rows if r["role"] == "Manager")
    qa = next(r for r in rows if r["role"] == "QA")
    assert dev["salary_category"] == "POSITIVE"
    assert mgr["salary_category"] == "POSITIVE"
    assert qa["salary_category"] == "POSITIVE"

# -----------------------
# CLI Tests
# -----------------------

def test_cli_etl_only(sample_employee_csv, tmp_path, monkeypatch):
    out_file = tmp_path / "out.csv"
    monkeypatch.setattr(sys, "argv", [
        "etl_ai_cli",
        "--input", str(sample_employee_csv),
        "--output", str(out_file),
        "--threshold", "100000",
        "--engine", "pandas"
    ])
    etl_ai.main()
    assert out_file.exists()

def test_cli_with_ai(sample_employee_csv, tmp_path, monkeypatch):
    out_file = tmp_path / "out_ai.csv"

    # Mock pipeline to avoid model download
    class DummyPipeline:
        def __call__(self, texts):
            return [{"label": "POSITIVE", "score": 0.99} for _ in texts]

    monkeypatch.setattr(etl_ai, "pipeline", lambda *a, **k: DummyPipeline())

    monkeypatch.setattr(sys, "argv", [
        "etl_ai_cli",
        "--input", str(sample_employee_csv),
        "--output", str(out_file),
        "--threshold", "100000",
        "--engine", "polars",
        "--ai"
    ])
    etl_ai.main()
    assert out_file.exists()
    with out_file.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert "salary_category" in rows[0]