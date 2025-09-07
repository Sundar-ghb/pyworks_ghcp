#!/usr/bin/env python3
"""
Day 5: FastAPI microservice for ETL + AI inference (real model usage)
Author: Sundarapandiyan — Week 1 Transition Plan
"""

import time
import logging
from pathlib import Path

import pandas as pd
import polars as pl
from fastapi import FastAPI, File, UploadFile, Query
from fastapi.responses import FileResponse, JSONResponse
from transformers import pipeline

# --- Logging Config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_ai_service")

app = FastAPI(title="ETL + AI Service", version="1.1")

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

# --- AI Step (real model usage) ---
def ai_inference(input_csv: Path, output_csv: Path):
    start = time.perf_counter()
    logger.info("Loading AI model (small, CPU-friendly)...")
    classifier = pipeline(
        "text-classification",
        model="distilbert-base-uncased-finetuned-sst-2-english",
        device=-1  # CPU; change to 0 for GPU
    )

    df = pd.read_csv(input_csv)
    predicted_labels = []
    prediction_scores = []

    for _, row in df.iterrows():
        role_text = row["role"]
        prediction = classifier(role_text, truncation=True)[0]
        predicted_labels.append(prediction["label"])
        prediction_scores.append(prediction["score"])

    df["predicted_label"] = predicted_labels
    df["prediction_score"] = prediction_scores

    df.to_csv(output_csv, index=False)
    logger.info(f"AI inference complete in {(time.perf_counter()-start)*1000:.2f} ms")

# --- API Endpoint ---
@app.post("/process")
async def process_file(
    file: UploadFile = File(...),
    threshold: int = Query(100_000, description="Salary threshold"),
    engine: str = Query("pandas", enum=["pandas", "polars"]),
    ai: bool = Query(False, description="Run AI inference after ETL"),
    return_format: str = Query("csv", enum=["csv", "json"])
):
    temp_input = Path(f"temp_input_{time.time()}.csv")
    temp_etl_output = Path(f"temp_etl_{time.time()}.csv")
    final_output = Path(f"output_{time.time()}.csv")

    # Save uploaded file
    with temp_input.open("wb") as f:
        f.write(await file.read())

    # Run ETL
    if engine == "pandas":
        pandas_etl(temp_input, temp_etl_output, threshold)
    else:
        polars_etl(temp_input, temp_etl_output, threshold)

    # Optional AI step
    if ai:
        ai_inference(temp_etl_output, final_output)
    else:
        final_output = temp_etl_output

    # Return result
    if return_format == "csv":
        return FileResponse(final_output, filename="result.csv")
    else:
        df = pd.read_csv(final_output)
        return JSONResponse(content=df.to_dict(orient="records"))