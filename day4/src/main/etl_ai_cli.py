#!/usr/bin/env python3
"""
Day 4: ETL + AI Inference CLI
Author: Sundarapandiyan — Week 1 Transition Plan
"""

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import polars as pl
from transformers import pipeline

# --- Logging Config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_ai_cli")

# --- ETL Functions ---
def pandas_etl(input_csv: Path, output_csv: Path, threshold: int):
    start = time.perf_counter()
    df = pd.read_csv(input_csv)
    high_salary = df[df["salary"] > threshold]
    avg_salary_by_role = (
        high_salary.groupby("role")["salary"]
        .mean()
        .reset_index()
        .rename(columns={"salary": "avg_salary"})
    )
    avg_salary_by_role.to_csv(output_csv, index=False)
    logger.info(f"Pandas ETL complete in {(time.perf_counter()-start)*1000:.2f} ms")

def polars_etl(input_csv: Path, output_csv: Path, threshold: int):
    start = time.perf_counter()
    df = pl.read_csv(input_csv)
    result = (
        df.lazy()
        .filter(pl.col("salary") > threshold)
        .group_by("role")  # ✅ Polars API fix
        .agg(pl.col("salary").mean().alias("avg_salary"))
        .collect()
    )
    result.write_csv(output_csv)
    logger.info(f"Polars ETL complete in {(time.perf_counter()-start)*1000:.2f} ms")

# --- AI Step ---
def ai_inference(input_csv: Path, output_csv: Path):
    start = time.perf_counter()
    logger.info("Loading AI model (small, CPU-friendly)...")
    classifier = pipeline("text-classification", model="distilbert-base-uncased-finetuned-sst-2-english")

    df = pd.read_csv(input_csv)
    categories = []
    for _, row in df.iterrows():
        role = row["role"]
        """ avg_salary = row["avg_salary"]
        # Simple rule-based label for demo (replace with model output if desired)
        if avg_salary > 150_000:
            label = "High"
        elif avg_salary >= 100_000:
            label = "Medium"
        else:
            label = "Low"
        categories.append(label) """

        # Actually run the model on the role text
        prediction = classifier(role)[0]  # returns [{'label': 'POSITIVE', 'score': 0.99}]
        categories.append(prediction["label"])


    df["salary_category"] = categories
    df.to_csv(output_csv, index=False)
    logger.info(f"AI inference complete in {(time.perf_counter()-start)*1000:.2f} ms. Output saved to {output_csv}")

# --- CLI Entry Point ---
def main():
    parser = argparse.ArgumentParser(description="ETL pipeline with optional AI inference.")
    parser.add_argument("-i", "--input", type=Path, required=True, help="Path to input CSV file.")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Path to output CSV file.")
    parser.add_argument("-t", "--threshold", type=int, default=100_000, help="Salary threshold.")
    parser.add_argument("-e", "--engine", choices=["pandas", "polars"], default="pandas", help="ETL engine.")
    parser.add_argument("--ai", action="store_true", help="Run AI inference after ETL.")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Logging level.")

    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)

    total_start = time.perf_counter()

    etl_output = args.output if not args.ai else Path("etl_temp.csv")

    if args.engine == "pandas":
        pandas_etl(args.input, etl_output, args.threshold)
    else:
        polars_etl(args.input, etl_output, args.threshold)

    if args.ai:
        ai_inference(etl_output, args.output)
        if etl_output.exists():
            etl_output.unlink()  # cleanup temp file

    logger.info(f"Total pipeline time: {(time.perf_counter()-total_start)*1000:.2f} ms")

if __name__ == "__main__":
    main()