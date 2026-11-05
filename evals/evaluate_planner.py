"""Evaluate artificial multi-turn planning cases; no bioinformatics computation."""

import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis_planner import plan_request


def evaluate() -> dict:
    dataset = json.loads(Path(__file__).with_name("planning_cases.json").read_text(encoding="utf-8"))
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        paths = [str(root / "demo_R1.fastq.gz"), str(root / "demo_R2.fastq.gz")]
        for path in paths:
            Path(path).touch()
        contigs = root / "demo.fa"
        contigs.write_text(">fixture\nACGT\n")
        for case in dataset["cases"]:
            state = {"paths": paths}
            for text in case["turns"]:
                state = plan_request({"task": case["task"], "user_text": text.format(contigs=contigs), "current_plan": state})
            actual = {**state, "selected": [stage["id"] for stage in state.get("stages", [])]}
            failures = {key: {"expected": value, "actual": actual.get(key)}
                        for key, value in case["expected"].items() if actual.get(key) != value}
            results.append({"id": case["id"], "task": case["task"], "passed": not failures, "failures": failures})
    return {"version": 1, "provenance": dataset["provenance"], "total": len(results),
            "passed": sum(item["passed"] for item in results), "results": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = evaluate()
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    raise SystemExit(0 if result["passed"] == result["total"] else 1)
