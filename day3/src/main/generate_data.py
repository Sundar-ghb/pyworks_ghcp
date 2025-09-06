import csv
import random
from pathlib import Path

import os
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])

def generate_large_employee_csv(file_path: Path, num_rows: int = 100_000):
    roles = ["Developer", "QA", "Manager", "Data Scientist", "DevOps", "Designer"]
    locations = ["New York", "London", "Bangalore", "Berlin", "Toronto", "Sydney"]

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "role", "salary", "location", "years_experience"])
        for i in range(num_rows):
            name = f"Employee_{i+1}"
            role = random.choice(roles)
            salary = random.randint(50_000, 200_000)
            location = random.choice(locations)
            years_exp = random.randint(1, 20)
            writer.writerow([name, role, salary, location, years_exp])

if __name__ == "__main__":
    generate_large_employee_csv(Path(src_path + "/resources/employees.csv"), num_rows=200_000)
    print("✅ employees.csv generated with 200,000 rows")