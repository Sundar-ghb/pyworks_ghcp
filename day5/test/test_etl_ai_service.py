import csv
import pytest
from fastapi.testclient import TestClient
from pathlib import Path
import os, sys
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])
sys.path.insert(0, src_path)  # noqa

import src.main.etl_ai_service as service

@pytest.fixture
def client():
    return TestClient(service.app)

@pytest.fixture
def sample_employee_csv(tmp_path):
    file_path = tmp_path / "employees.csv"
    rows = [
        ["name", "role", "salary", "location", "years_experience"],
        ["Alice", "Developer", 125000, "New York", 5],
        ["Bob", "QA", 80000, "London", 3],
        ["Charlie", "Manager", 155000, "Berlin", 10],
    ]
    with file_path.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)
    return file_path

@pytest.mark.parametrize("engine_func", [service.pandas_etl, service.polars_etl])
def test_etl_functions(engine_func, sample_employee_csv, tmp_path):
    out_file = tmp_path / "avg_salary_by_role.csv"
    engine_func(sample_employee_csv, out_file, threshold=100_000)
    assert out_file.exists()
    with out_file.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert any(r["role"] == "Developer" and float(r["avg_salary"]) > 120000 for r in rows)
    # Clean up
    if out_file.exists():
        out_file.unlink()

def test_process_endpoint_etl_only(client, sample_employee_csv):
    with sample_employee_csv.open("rb") as f:
        response = client.post(
            "/process?threshold=100000&engine=pandas&ai=false&return_format=json",
            files={"file": ("employees.csv", f, "text/csv")}
        )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert "avg_salary" in data[0]
   
def test_process_endpoint_with_ai(client, sample_employee_csv, monkeypatch):
    class DummyPipeline:
        def __call__(self, text, truncation=True):
            return [{"label": "POSITIVE", "score": 0.99}]

    monkeypatch.setattr(service, "pipeline", lambda *a, **k: DummyPipeline())

    with sample_employee_csv.open("rb") as f:
        response = client.post(
            "/process?threshold=100000&engine=polars&ai=true&return_format=json",
            files={"file": ("employees.csv", f, "text/csv")}
        )
    assert response.status_code == 200
    data = response.json()
    assert "predicted_label" in data[0]
    assert data[0]["predicted_label"] == "POSITIVE"
    