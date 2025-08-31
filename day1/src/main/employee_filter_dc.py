#!/usr/bin/env python3
"""
Day 1: Python Starter — Filter employees from JSON and save to CSV.
With logging decorator for function tracing.
"""

import json
import csv
import time
from pathlib import Path
from typing import List, Dict, Callable
from functools import wraps

import os
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])

# --- Config ---
INPUT_FILE = Path(src_path + "/resources/employees.json")
OUTPUT_FILE = Path(src_path + "/resources/high_salary_employees.csv")
SALARY_THRESHOLD = 100_000  # filter condition

# --- Decorator ---
def log_execution(func: Callable) -> Callable:
    """Decorator to log function start, end, and execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        print(f"[LOG] Starting: {func.__name__}")
        result = func(*args, **kwargs)
        elapsed = (time.perf_counter() - start_time) * 1000
        print(f"[LOG] Finished: {func.__name__} in {elapsed:.2f} ms")
        return result
    return wrapper

# --- Core Functions ---
@log_execution
def load_employees(file_path: Path) -> List[Dict]:
    """Load employee data from a JSON file."""
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)

@log_execution
def filter_by_salary(employees: List[Dict], threshold: int) -> List[Dict]:
    """Return employees with salary above the threshold."""
    return [emp for emp in employees if emp.get("salary", 0) > threshold]

@log_execution
def save_to_csv(employees: List[Dict], file_path: Path) -> None:
    """Save employee data to a CSV file."""
    if not employees:
        print("[WARN] No employees to save.")
        return
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=employees[0].keys())
        writer.writeheader()
        writer.writerows(employees)

# --- Main ---
@log_execution
def main():
    employees = load_employees(INPUT_FILE)
    high_salary_emps = filter_by_salary(employees, SALARY_THRESHOLD)
    save_to_csv(high_salary_emps, OUTPUT_FILE)
    print(f"[INFO] Saved {len(high_salary_emps)} employees to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()