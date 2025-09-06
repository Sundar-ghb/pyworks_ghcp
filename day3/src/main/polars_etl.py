import polars as pl
from pathlib import Path

def polars_etl(input_csv: Path, output_csv: Path, threshold: int = 100_000):
    df = pl.read_csv(input_csv)
    result = (
        df.lazy()
        .filter(pl.col("salary") > threshold)
        .group_by("role")
        .agg(pl.col("salary").mean().alias("avg_salary"))
        .collect()
    )
    result.write_csv(output_csv)