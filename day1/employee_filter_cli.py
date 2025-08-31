#!/usr/bin/env python3
"""
Day 2: Employee Filter CLI Tool
Author: Sundarapandiyan — Week 1 Transition Plan
"""

import json
import csv
import time
import argparse
from pathlib import Path
from typing import List, Dict, Callable
from functools import wraps

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

# --- CLI Entry Point ---
@log_execution
def main():
    parser = argparse.ArgumentParser(
        description="Filter employees by salary and save to CSV."
    )
    parser.add_argument(
        "-i", "--input", type=Path, required=True,
        help="Path to input JSON file."
    )
    parser.add_argument(
        "-o", "--output", type=Path, required=True,
        help="Path to output CSV file."
    )
    parser.add_argument(
        "-t", "--threshold", type=int, default=100_000,
        help="Salary threshold (default: 100000)."
    )

    args = parser.parse_args()

    employees = load_employees(args.input)
    high_salary_emps = filter_by_salary(employees, args.threshold)
    save_to_csv(high_salary_emps, args.output)
    print(f"[INFO] Saved {len(high_salary_emps)} employees to {args.output}")

if __name__ == "__main__":
    main()