#!/usr/bin/env python3
"""
Day 1: Python Starter — Filter employees from JSON and save to CSV.
Author: Sundarapandiyan (Python Transition Week 1)
"""

import json
import csv
from pathlib import Path
from typing import List, Dict

# --- Config ---
INPUT_FILE = Path("employees.json")
OUTPUT_FILE = Path("high_salary_employees.csv")
SALARY_THRESHOLD = 100_000  # filter condition

def load_employees(file_path: Path) -> List[Dict]:
    """Load employee data from a JSON file."""
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)

def filter_by_salary(employees: List[Dict], threshold: int) -> List[Dict]:
    """Return employees with salary above the threshold."""
    return [emp for emp in employees if emp.get("salary", 0) > threshold]

def save_to_csv(employees: List[Dict], file_path: Path) -> None:
    """Save employee data to a CSV file."""
    if not employees:
        print("No employees to save.")
        return
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=employees[0].keys())
        writer.writeheader()
        writer.writerows(employees)

def main():
    employees = load_employees(INPUT_FILE)
    high_salary_emps = filter_by_salary(employees, SALARY_THRESHOLD)
    save_to_csv(high_salary_emps, OUTPUT_FILE)
    print(f"Saved {len(high_salary_emps)} employees to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()