import polars as pl

from pathlib import Path
import os
main_path = os.path.abspath(os.path.dirname(__file__))
src_path = str(Path(main_path).parents[0])

# Extract
df = pl.read_csv(src_path + "/resources/employees.csv")

# Transform (lazy execution for speed)
result = (
    df.lazy()
    .filter(pl.col("salary") > 100_000)
    .group_by("role")
    .agg(pl.col("salary").mean().alias("avg_salary"))
    .collect()
)

# Load
result.write_csv(src_path + "/resources/avg_salary_by_role.csv")
print("Polars ETL complete.")