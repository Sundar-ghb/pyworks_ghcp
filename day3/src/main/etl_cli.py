#!/usr/bin/env python3
"""
Day 3 (Revised): Unified ETL CLI with Pandas and Polars engines
Includes internal execution time measurement (cross-platform) and Polars API fix.
Author: Sundarapandiyan — Week 1 Transition Plan
"""

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import polars as pl

# --- Logging Config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_cli")

# --- ETL Implementations ---
def pandas_etl(input_csv: Path, output_csv: Path, threshold: int):
    start = time.perf_counter()
    logger.debug("Running Pandas ETL...")
    df = pd.read_csv(input_csv)
    high_salary = df[df["salary"] > threshold]
    avg_salary_by_role = (
        high_salary.groupby("role")["salary"]
        .mean()
        .reset_index()
        .rename(columns={"salary": "avg_salary"})
    )
    avg_salary_by_role.to_csv(output_csv, index=False)
    elapsed = (time.perf_counter() - start) * 1000
    logger.info(f"Pandas ETL complete in {elapsed:.2f} ms. Output saved to {output_csv}")

def polars_etl(input_csv: Path, output_csv: Path, threshold: int):
    start = time.perf_counter()
    logger.debug("Running Polars ETL...")
    df = pl.read_csv(input_csv)
    result = (
        df.lazy()
        .filter(pl.col("salary") > threshold)
        .group_by("role")  # ✅ Updated API
        .agg(pl.col("salary").mean().alias("avg_salary"))
        .collect()
    )
    result.write_csv(output_csv)
    elapsed = (time.perf_counter() - start) * 1000
    logger.info(f"Polars ETL complete in {elapsed:.2f} ms. Output saved to {output_csv}")

# --- CLI Entry Point ---
def main():
    parser = argparse.ArgumentParser(
        description="ETL pipeline to filter employees by salary and compute average salary by role."
    )
    parser.add_argument("-i", "--input", type=Path, required=True, help="Path to input CSV file.")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Path to output CSV file.")
    parser.add_argument("-t", "--threshold", type=int, default=100_000, help="Salary threshold (default: 100000).")
    parser.add_argument("-e", "--engine", type=str, choices=["pandas", "polars"], default="pandas",
                        help="ETL engine to use (default: pandas).")
    parser.add_argument("--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO",
                        help="Set the logging level.")

    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)

    total_start = time.perf_counter()

    if args.engine == "pandas":
        pandas_etl(args.input, args.output, args.threshold)
    elif args.engine == "polars":
        polars_etl(args.input, args.output, args.threshold)
    else:
        logger.error(f"Unknown engine: {args.engine}")
        return

    total_elapsed = (time.perf_counter() - total_start) * 1000
    logger.info(f"Total script execution time: {total_elapsed:.2f} ms")

if __name__ == "__main__":
    main()