from __future__ import annotations
import re, json, hashlib
from pathlib import Path
from typing import Dict, List, Optional
import fitz  # PyMuPDF
from bs4 import BeautifulSoup
import trafilatura

OUT_DIR = Path("data/staged")
OUT_DIR.mkdir(parents=True, exist_ok=True)

DOI_RX = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Za-z0-9]+\b")

def pdf_to_text(pdf_path: Path) -> str:
    try:
        doc = fitz.open(pdf_path)
        texts = []
        for page in doc:
            texts.append(page.get_text("text"))
        return "\n".join(texts)
    except Exception:
        return ""

def html_to_text(html_path: Path) -> str:
    raw = Path(html_path).read_text(encoding="utf-8", errors="ignore")
    # Try trafilatura (handles boilerplate removal); fallback to BeautifulSoup if needed
    extracted = trafilatura.extract(raw)
    if extracted:
        return extracted
    return BeautifulSoup(raw, "html.parser").get_text(separator="\n")

def sectionize(text: str) -> Dict[str, str]:
    # Lightweight heuristic section splitter
    # Looks for common headings; extend as you go.
    sections = {"title": "", "abstract": "", "methods": "", "results": "", "conclusion": "", "body": text}
    # Title: first non-empty line under ~120 chars
    for line in text.splitlines():
        line = line.strip()
        if 5 < len(line) < 120:
            sections["title"] = line
            break
    # Abstract segment
    m = re.search(r"(?is)\babstract\b[:\n]*(.+?)(\n\s*\bmethods?\b|\n\s*\bintroduction\b|\n\s*\bbackground\b|$)", text)
    if m:
        sections["abstract"] = m.group(1).strip()
    # Methods
    m = re.search(r"(?is)\bmethods?\b[:\n]*(.+?)(\n\s*\bresults?\b|$)", text)
    if m:
        sections["methods"] = m.group(1).strip()
    # Results
    m = re.search(r"(?is)\bresults?\b[:\n]*(.+?)(\n\s*\bdiscussion\b|\n\s*\bconclusion(s)?\b|$)", text)
    if m:
        sections["results"] = m.group(1).strip()
    # Conclusion
    m = re.search(r"(?is)\bconclusion(s)?\b[:\n]*(.+)$", text)
    if m:
        sections["conclusion"] = m.group(2).strip()
    return sections

def extract_dois(text: str) -> List[str]:
    return sorted(set(DOI_RX.findall(text)))

def process_file(path: Path, source_type: str) -> Optional[dict]:
    if source_type == "pdf":
        text = pdf_to_text(path)
    else:
        text = html_to_text(path)
    text = re.sub(r"[ \t]+\n", "\n", text).strip()
    if not text:
        return None
    sections = sectionize(text)
    dois = extract_dois(text)
    sha = hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else hashlib.sha256(text.encode()).hexdigest()
    return {
        "doc_id": f"{source_type}:{path.name}:{sha[:12]}",
        "source_path": str(path),
        "source_type": source_type,
        "title": sections["title"],
        "abstract": sections["abstract"],
        "methods": sections["methods"],
        "results": sections["results"],
        "conclusion": sections["conclusion"],
        "fulltext": text,
        "dois": dois,
        "license": None,  # fill later if you have it
        "language": "en",
    }

def main():
    cfg = {"pdf_dir": "data/raw/pmc/seed", "html_dir": "data/raw/web/seed"}
    out_jsonl = OUT_DIR / "extracted.jsonl"
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)

    records = []
    pdf_dir = Path(cfg["pdf_dir"]); html_dir = Path(cfg["html_dir"])
    for p in sorted(pdf_dir.glob("*.pdf")):
        rec = process_file(p, "pdf")
        if rec: records.append(rec)
    for h in sorted(html_dir.glob("*.html")) + sorted(html_dir.glob("*.htm")):
        rec = process_file(h, "html")
        if rec: records.append(rec)

    with out_jsonl.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Wrote {len(records)} records to {out_jsonl}")

if __name__ == "__main__":
    main()
