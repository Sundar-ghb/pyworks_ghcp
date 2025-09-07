from fastapi import FastAPI
from pydantic import BaseModel
from inference import analyze_text

app = FastAPI(title="AI Inference API")

class TextRequest(BaseModel):
    text: str

@app.post("/analyze")
def analyze(request: TextRequest):
    result = analyze_text(request.text)
    return {"input": request.text, "result": result}