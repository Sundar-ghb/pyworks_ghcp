import pandas as pd

from pathlib import Path
import os
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])

# Extract
df = pd.read_csv(src_path + "/resources/employees.csv")

# Transform
high_salary = df[df["salary"] > 100_000]
avg_salary_by_role = (
    high_salary.groupby("role")["salary"]
    .mean()
    .reset_index()
    .rename(columns={"salary": "avg_salary"})
)

# Load
avg_salary_by_role.to_csv(src_path + "/resources/avg_salary_by_role.csv", index=False)
print("Pandas ETL complete.")