from fastapi import FastAPI
from pydantic import BaseModel
from inference import analyze_text
from db import store_result
from cache import get_cached, set_cached
import time

app = FastAPI(title="AI Inference API with Backends")

class TextRequest(BaseModel):
    text: str

@app.get("/health")
async def health():
    return {"status": "ok"}

metrics = {"requests": 0, "avg_latency_ms": 0}

@app.get("/metrics")
async def get_metrics():
    return metrics

@app.post("/analyze")
async def analyze(request: TextRequest):
    start = time.time()
    metrics["requests"] += 1

    cached = get_cached(request.text)
    if cached:
        return {"input": request.text, "result": cached, "cached": True}

    result = analyze_text(request.text)  # Could be async if model supports it
    set_cached(request.text, result[0])
    store_result(request.text, result[0])

    latency = (time.time() - start) * 1000
    metrics["avg_latency_ms"] = (
        (metrics["avg_latency_ms"] * (metrics["requests"] - 1) + latency)
        / metrics["requests"]
    )

    return {"input": request.text, "result": result, "cached": False}