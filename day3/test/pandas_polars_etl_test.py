import csv
import sys
from pathlib import Path
import pytest

import os
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])
sys.path.insert(0, src_path)  # noqa

from src.main.pandas_etl import pandas_etl
from src.main.polars_etl import polars_etl

@pytest.fixture
def sample_csv(tmp_path: Path):
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

def test_pandas_etl(sample_csv: Path, tmp_path: Path):
    out_file = tmp_path / "avg_salary_by_role.csv"
    pandas_etl(sample_csv, out_file, threshold=100_000)
    assert out_file.exists()
    with out_file.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert any(r["role"] == "Developer" and float(r["avg_salary"]) > 120000 for r in rows)

def test_polars_etl(sample_csv: Path, tmp_path: Path):
    out_file = tmp_path / "avg_salary_by_role.csv"
    polars_etl(sample_csv, out_file, threshold=100_000)
    assert out_file.exists()
    with out_file.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert any(r["role"] == "Developer" and float(r["avg_salary"]) > 120000 for r in rows)