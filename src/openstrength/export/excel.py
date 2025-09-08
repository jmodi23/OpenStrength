from __future__ import annotations
from pathlib import Path
import json
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--inp", default="artifacts/last_plan.json")
parser.add_argument("--out", default="artifacts/exports/plan.xlsx")
args = parser.parse_args()

Path(args.out).parent.mkdir(parents=True, exist_ok=True)

def main():
    plan = json.loads(Path(args.inp).read_text(encoding="utf-8"))
    rows = []
    for day in plan.get("lift_plan", []):
        d = day.get("day")
        for b in day.get("blocks", []):
            rows.append({"Day": d, "Exercise": b["exercise"], "Sets": b["sets"],
                         "Reps": b["reps"], "Intensity": b.get("intensity",""),
                         "Rest": b.get("rest",""), "Notes": b.get("notes","")})
    training_df = pd.DataFrame(rows)
    nutrition_df = pd.DataFrame([plan.get("nutrition", {})])
    cites_df = pd.DataFrame(plan.get("citations", []))
    with pd.ExcelWriter(args.out) as w:
        training_df.to_excel(w, index=False, sheet_name="Training")
        nutrition_df.to_excel(w, index=False, sheet_name="Nutrition")
        cites_df.to_excel(w, index=False, sheet_name="Citations")
    training_df.to_csv(Path(args.out).with_suffix(".csv"), index=False)
    print(f"Wrote {args.out} and {Path(args.out).with_suffix('.csv')}")

if __name__ == "__main__":
    main()
