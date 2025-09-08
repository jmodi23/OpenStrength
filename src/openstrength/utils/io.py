from pathlib import Path
import json

def read_json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))

def write_json(path: str, obj) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
