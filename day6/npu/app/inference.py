import onnxruntime as ort
import numpy as np

# Choose the right EP for your hardware

# For CPU:
providers = ["CPUExecutionProvider"]
# For DirectML (most Windows NPUs):
#providers = ["DmlExecutionProvider"]

# For Qualcomm QNN:
# providers = ["QNNExecutionProvider"]

session = ort.InferenceSession("model.onnx", providers=providers)

def analyze_text(text: str):
    # Tokenize (same tokenizer as training)
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(
        "distilbert-base-uncased-finetuned-sst-2-english"
    )
    inputs = tokenizer(text, return_tensors="np", padding=True, truncation=True)
    ort_inputs = {k: np.array(v) for k, v in inputs.items()}
    outputs = session.run(None, ort_inputs)
    logits = outputs[0]
    label_id = np.argmax(logits, axis=1)[0]
    return {"label": "POSITIVE" if label_id == 1 else "NEGATIVE", "score": float(np.max(logits))}