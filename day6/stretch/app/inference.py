from transformers import pipeline

# Load a small, fast model for demo
# You can swap with a domain-specific model later
model = pipeline("sentiment-analysis")

def analyze_text(text: str):
    return model(text)