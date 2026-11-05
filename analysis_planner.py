"""Read-only planning CLI shared by the Skill, runtime, and evaluations."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from intent_router import resolve_intent
from parameter_rules import RULES
from workflow_catalog import ARTIFACTS, STAGES


def catalog() -> dict:
    return {
        "version": 2,
        "stages": [
            {"id": stage.id, "label": stage.label, "workflow_id": stage.workflow_id,
             "requirements": stage.requirements, "produces": stage.produces}
            for stage in STAGES
        ],
        "artifacts": {key: asdict(value) for key, value in ARTIFACTS.items()},
        "parameters": {key: {**asdict(rule), "unit": None if rule.boolean else
                            ("bp" if "length" in key or "contig_len" in key else "count_or_phred"),
                            "bounds_source": "project_allowlist"}
                       for key, rule in RULES.items()},
    }


def plan_request(request: dict, *, check_files: bool = True) -> dict:
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    if request.get("task", "intent") not in {"intent", "param_suggest"}:
        raise ValueError("only intent and param_suggest tasks are supported")
    text = request.get("user_text")
    previous = request.get("current_plan") or {}
    if not isinstance(text, str) or not text.strip() or not isinstance(previous, dict):
        raise ValueError("user_text must be nonempty and current_plan must be an object")
    revision = previous.get("plan_revision", 0)
    if "base_plan_revision" in request and request["base_plan_revision"] != revision:
        return {
            "version": 2, "handled": True, "accepted": False, "ready_to_execute": False,
            "action": "clarify", "previous_unchanged": True, "parameter_patch": {},
            "scope_patch": {}, "json_patch": [], "questions": ["方案版本已变化，请基于最新方案重新提交修改。"],
            "plan_revision": revision, "base_plan_revision": request["base_plan_revision"],
        }
    if "proposal" in request:
        from plan_proposals import admit_proposal
        return admit_proposal(request["proposal"], previous, text, check_files=check_files)
    return resolve_intent(text, previous, check_files=check_files)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", action="store_true")
    parser.add_argument("--request", type=Path, help="JSON request; otherwise read stdin")
    parser.add_argument("--skip-file-check", action="store_true", help="Planning/evaluation only; execution always checks files")
    args = parser.parse_args()
    try:
        request = None if args.catalog else json.loads(args.request.read_text(encoding="utf-8") if args.request else sys.stdin.read())
        result = catalog() if args.catalog else plan_request(request, check_files=not args.skip_file_check)
    except (ValueError, OSError) as exc:
        print(json.dumps({"accepted": False, "ready_to_execute": False, "questions": [str(exc)]}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
