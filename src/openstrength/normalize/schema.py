from __future__ import annotations
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Literal, Dict

class Citation(BaseModel):
    title: Optional[str] = None
    doi: Optional[str] = None
    chunk_id: Optional[str] = None
    source: Optional[str] = None
    license: Optional[str] = None

class ProtocolExercise(BaseModel):
    exercise: str
    sets: int
    reps: str
    intensity: Optional[str] = None
    rest: Optional[str] = None
    notes: Optional[str] = None

class Document(BaseModel):
    model_config = ConfigDict(extra="ignore")
    doc_id: str
    license: Optional[str] = None
    type: Literal["trial","review","guideline","plan","webpage"] = "trial"
    population: Dict[str, str] = {}
    goal: List[str] = []
    protocol: Dict[str, List[ProtocolExercise]] = {"exercises": []}
    nutrition: Dict[str, str | float | int] = {}
    evidence: Dict[str, str | int | List[str]] = {}
    citations: List[Citation] = []
    text: str
