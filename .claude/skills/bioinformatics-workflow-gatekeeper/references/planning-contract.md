# Planning contract

The maintained catalog and parameter whitelist are `workflow_catalog.py` and `parameter_rules.py` in the Agent directory. Obtain their current machine-readable form using `python analysis_planner.py --catalog`; do not maintain a second copy of those lists in this Skill.

Call `analysis_planner.plan_request(request)` in Python, or send the following JSON to `python analysis_planner.py` through stdin. The CLI performs planning and file validation, without preparing workflows or launching analysis.

```json
{
  "task": "param_suggest",
  "user_text": "fastp最短长度改成八十，其他不变",
  "base_plan_revision": 3,
  "current_plan": {
    "plan_revision": 3,
    "goals": ["read_qc"],
    "paths": ["/data/demo_R1.fastq.gz", "/data/demo_R2.fastq.gz"],
    "parameters": {"fastp.length_required": 50}
  }
}
```

`current_plan` is backend state, not reconstructed chat prose. `base_plan_revision` must equal the backend version. The result includes `accepted`, `ready_to_execute`, `questions`, `goals`, `authorized_stages`, `confirmed_dependencies`, `missing_dependencies`, `scope_patch`, `parameter_patch`, and `json_patch`. Only a ready result contains `execution_spec`. The plan revision increments for a new draft, and rejected parameter changes preserve the previous version.

`scope_patch` is empty for parameter-only requests. Parameter JSON Patch operations first test `/plan_revision`, then test old values for existing keys before replacing them. The flat parameter keys match the backend's `parameters` object, for example `/parameters/fastp.length_required`. A key without an explicit saved override uses `add`; do not invent a default old value.

Dependency dialogue:

1. W2 candidate extraction with only raw paired reads returns missing W1 stages and no executable specification. Display the question and persist that draft.
2. “同意补跑W1” approves only those displayed W1 prerequisites, preserves the W2 candidate extraction endpoint, and returns a new plan. It does not start analysis.
3. “不补跑W1” retains the endpoint but remains blocked. Supplying existing contigs can satisfy the dependency without W1.
4. Execution confirmation applies to the latest ready revision, never to an unresolved draft or to newly inferred dependencies.

`--skip-file-check` is for isolated planning evaluations against synthetic paths. The execution gate always checks actual inputs. It cannot make missing files executable.
