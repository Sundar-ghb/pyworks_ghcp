import redis
import json

r = redis.Redis(host="redis", port=6379, decode_responses=True)

def get_cached(text: str):
    data = r.get(text)
    return json.loads(data) if data else None

def set_cached(text: str, result: dict):
    r.set(text, json.dumps(result), ex=3600)  # 1 hour TTL