from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers.onnx import export
from pathlib import Path

model_name = "distilbert-base-uncased-finetuned-sst-2-english"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

onnx_path = Path("model.onnx")
export(tokenizer, model, onnx_path, opset=13)

from onnxruntime.quantization import quantize_dynamic, QuantType
quantize_dynamic("model.onnx", "model-quant.onnx", weight_type=QuantType.QInt8)