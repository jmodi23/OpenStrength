from pathlib import Path
from src.openstrength.utils.io import write_json, read_json

def test_json_roundtrip(tmp_path: Path):
    obj = {"a": 1}
    p = tmp_path / "x.json"
    write_json(p, obj)
    assert read_json(p) == obj
