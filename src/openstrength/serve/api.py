from __future__ import annotations
from fastapi import FastAPI
from pydantic import BaseModel
from pathlib import Path
import json
from src.openstrength.rag.pipeline import plan

app = FastAPI(title="OpenStrength API")

class PlanRequest(BaseModel):
    goal: str
    training_age: str
    frequency: int
    equipment: str
    bodymass_kg: float
    constraints: str | None = None

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/plan")
def make_plan(req: PlanRequest):
    result = plan(req.goal, req.training_age, req.frequency, req.equipment, req.bodymass_kg, req.constraints)
    Path("artifacts").mkdir(parents=True, exist_ok=True)
    Path("artifacts/last_plan.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result
