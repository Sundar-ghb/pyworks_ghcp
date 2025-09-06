import pandas as pd
from pathlib import Path

def pandas_etl(input_csv: Path, output_csv: Path, threshold: int = 100_000):
    df = pd.read_csv(input_csv)
    high_salary = df[df["salary"] > threshold]
    avg_salary_by_role = (
        high_salary.groupby("role")["salary"]
        .mean()
        .reset_index()
        .rename(columns={"salary": "avg_salary"})
    )
    avg_salary_by_role.to_csv(output_csv, index=False)