"""Run-scoped software/Skill evidence and an offline, curated bibliography.

Availability is never treated as use. Historical versions are not probed from
today's environment. Model report HTML is validated before this trusted appendix
is added; all strings are escaped and external links are constructed from DOIs.
"""
from __future__ import annotations

import hashlib
import html
import json
import platform
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

REGISTRY_PATH = Path(__file__).with_name("citation_registry.json")
LIBRARY_DOCS = {"Python": "https://www.python.org/", "PyYAML": "https://pyyaml.org/"}


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path: Path, value: dict | str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def capture_runner_environment(state: dict) -> None:
    """Called after preparation actually used yaml in this Python process."""
    import yaml
    state["provenance_capture"] = {
        "version": 1, "route": "compiled_pipeline", "skill_tracking": "not_invoked_by_design",
        "skills": [], "libraries": [
            {"name": "Python", "version": platform.python_version(), "role": "执行器运行时",
             "source": "execution-time platform.python_version", "url": "https://www.python.org/"},
            {"name": "PyYAML", "version": str(yaml.__version__), "role": "读取和写入工作流配置",
             "source": "execution-time yaml.__version__", "url": "https://pyyaml.org/"},
        ],
    }


def logged_commands(text: str, registry: dict) -> list[dict]:
    """Conservative first-token parsing of actual execution log, never dry-run.

    Snakemake logs shell declarations, not an OS process trace. Preserve that
    evidence grade instead of pretending a conditional branch certainly ran.
    """
    aliases = {exe: key for key, tool in registry["tools"].items() for exe in tool["executables"]}
    found: list[dict] = []
    for number, line in enumerate(text.splitlines(), 1):
        try:
            tokens = shlex.split(line.strip(), comments=True)
        except ValueError:
            continue
        if not tokens:
            continue
        executable = tokens[0].rsplit("/", 1)[-1]
        key = aliases.get(executable)
        # A bare tool mentioned in prose is not a command. Inputs/tool output
        # such as "fastp v1.3.2" are version evidence only, not shell commands.
        subcommands = {"stat", "seq", "fx2tab", "grep", "rename", "concat", "rmdup", "subseq",
                       "end_to_end", "createdb", "cluster", "easy-cluster", "easy-linclust", "createtsv",
                       "linclust", "search", "easy-search", "convertalis", "result2repseq", "createsubdb",
                       "view", "sort", "index", "coverage", "depth", "faidx", "flagstat", "idxstats",
                       "annotate", "distill", "filter", "merge_annotations", "intersect", "genomecov", "getfasta"}
        command_like = len(tokens) > 1 and (tokens[1].startswith("-") or tokens[1] in subcommands
                                            or (executable in {"bowtie2-build", "hmmpress"} and "/" in tokens[1]))
        if key and command_like:
            found.append({"tool_id": key, "line": number, "executable": tokens[0],
                          "source": ".bioagent/execution.log"})
    return found


def _source_signature(run_dir: Path, state: dict, metrics: list[dict]) -> tuple[str, list[dict]]:
    sources = [{"path": ".bioagent/execution.log", "sha256": _hash(run_dir / ".bioagent/execution.log")}
               ] if (run_dir / ".bioagent/execution.log").is_file() else []
    if (run_dir / ".bioagent/skill-usage.json").is_file():
        sources.append({"path": ".bioagent/skill-usage.json", "sha256": _hash(run_dir / ".bioagent/skill-usage.json")})
    sources.extend({"path": m["source"], "sha256": m["sha256"]} for m in metrics)
    facts = {"run_id": state.get("run_id", run_dir.name), "sources": sources,
             "capture": state.get("provenance_capture"),
             "phases": state.get("execution", {}).get("phases", []),
             "planner": state.get("confirmed_plan", {}).get("planning", {}).get("planner_trace")}
    return hashlib.sha256(json.dumps(facts, sort_keys=True).encode()).hexdigest(), sources


def collect_provenance(run_dir: Path, state: dict, metrics: list[dict]) -> dict:
    run_dir = Path(run_dir).resolve()
    signature, sources = _source_signature(run_dir, state, metrics)
    snapshot = run_dir / ".bioagent/report-provenance.json"
    if snapshot.is_file():
        stored = json.loads(snapshot.read_text(encoding="utf-8"))
        if stored.get("source_signature") != signature or stored.get("run_id") != state.get("run_id", run_dir.name):
            raise ValueError("报告引用来源已变化，请核查本次运行的证据；不能沿用旧引用快照。")
        return stored
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    log_path = run_dir / ".bioagent/execution.log"
    log = log_path.read_text(encoding="utf-8", errors="replace") if log_path.is_file() else ""
    tools: dict[str, dict] = {}

    def tool(key: str) -> dict:
        return tools.setdefault(key, {"id": key, "name": registry["tools"][key]["name"],
                                     "versions": [], "evidence": [], "usage": "command_logged"})

    for command in logged_commands(log, registry):
        entry = tool(command["tool_id"])
        if len(entry["evidence"]) < 20:
            entry["evidence"].append(command)
    for metric in metrics:
        entry = tool("fastp")
        entry["usage"] = "artifact_verified"
        entry["evidence"].append({"source": metric["source"], "sha256": metric["sha256"]})
        if metric.get("version") and str(metric["version"]) not in entry["versions"]:
            entry["versions"].append(str(metric["version"]))
            entry.setdefault("version_sources", []).append(metric["source"] + "#/summary/fastp_version")
    # The completed runner phase and Snakemake's own completion log identify
    # the orchestrator, even if the command itself wasn't echoed by Python.
    if state.get("execution", {}).get("phases") and re.search(r"Finished job \d+\.", log):
        entry = tool("snakemake")
        entry["usage"] = "runner_verified"
        entry["evidence"].append({"source": ".bioagent/state.json#/execution/phases"})
    capture = state.get("provenance_capture", {})
    skill_path = run_dir / ".bioagent/skill-usage.json"
    if skill_path.is_file():
        skill_audit = json.loads(skill_path.read_text(encoding="utf-8"))
    else:
        skill_audit = {"tracking": capture.get("skill_tracking", "not_recorded"), "skills": capture.get("skills", [])}
    references: list[dict] = []
    by_doi: dict[str, int] = {}

    def add_reference(key: str) -> int | None:
        record = registry["references"].get(key)
        if not record:
            return None
        doi = record["doi"].lower()
        if doi not in by_doi:
            by_doi[doi] = len(references) + 1
            references.append({**record, "id": key, "number": by_doi[doi]})
        return by_doi[doi]

    for key, entry in tools.items():
        entry["reference_numbers"] = [add_reference(registry["tools"][key]["reference"])]
    for skill in skill_audit.get("skills", []):
        skill["reference_numbers"] = [n for key in skill.get("reference_ids", []) if (n := add_reference(key))]
    # Only backend-recorded, curated IDs are accepted. Free-form model DOIs
    # and metadata never become bibliography records.
    literature = []
    for use in capture.get("reference_uses", []):
        number = add_reference(use.get("reference_id", ""))
        literature.append({**use, "reference_number": number,
                           "validation": "verified_registry" if number else "unregistered"})
    planner = state.get("confirmed_plan", {}).get("planning", {}).get("planner_trace", {}) or {}
    return {
        "version": 1, "run_id": state.get("run_id", run_dir.name),
        "generated_at": datetime.now(timezone.utc).isoformat(), "source_signature": signature,
        "registry_sha256": _hash(REGISTRY_PATH), "sources": sources,
        "tools": list(tools.values()), "libraries": capture.get("libraries", []),
        "skills": skill_audit, "literature_uses": literature, "references": references,
        "planner": {key: planner[key] for key in ("source", "model") if key in planner},
        "limitations": ["工具覆盖限于已登记工具在执行日志中的独立命令和已支持的产物；条件分支、容器内或嵌套脚本依赖不由此确认。",
                        "未记录的历史版本和依赖不会从当前安装环境回填。"],
    }


def _bib_escape(value: Any) -> str:
    # Braces must remain balanced even for untrusted metadata.
    return str(value).replace("\\", r"\textbackslash{}").replace("{", r"\{").replace("}", r"\}").replace("%", r"\%").replace("&", r"\&")


def bibliography(manifest: dict) -> str:
    entries = []
    for ref in manifest["references"]:
        fields = {"author": ref.get("authors"), "title": re.sub(r"<[^>]*>", "", ref["title"]),
                  "journal": ref.get("journal"), "year": ref.get("year"), "volume": ref.get("volume"),
                  "number": ref.get("issue"), "pages": ref.get("pages"), "doi": ref["doi"], "note": ref.get("note")}
        if fields["pages"]:
            fields["pages"] = re.sub(r"(?<=[A-Za-z0-9])-(?=[A-Za-z0-9])", "--", str(fields["pages"]))
        entries.append("@article{" + ref["id"] + ",\n" + ",\n".join(
            "  " + key + " = {" + _bib_escape(value) + "}" for key, value in fields.items() if value
        ) + "\n}")
    return "\n\n".join(entries) + "\n"


def save_provenance(run_dir: Path, manifest: dict) -> None:
    for path in (run_dir / ".bioagent/report-provenance.json", run_dir / "analysis-provenance.json"):
        _write(path, manifest)
    _write(run_dir / "references.bib", bibliography(manifest))


def render_appendix(manifest: dict) -> str:
    esc = lambda value: html.escape(str(value), quote=True)
    marks = lambda numbers: " ".join(f'<a href="#reference-{int(n)}">[{int(n)}]</a>' for n in numbers if n)
    out = ['<section class="report-provenance"><h2>方法、软件与引用</h2>',
           '<p>编号对应下方参考文献。版本以本次运行记录为准。</p>',
           '<h3>工具与软件库</h3><table><thead><tr><th>名称 / 引用</th><th>版本</th><th>用途或使用证据</th></tr></thead><tbody>']
    statuses = {"artifact_verified": "结果文件确认", "runner_verified": "执行器阶段记录确认", "command_logged": "执行日志记录命令（条件分支未单独追踪）"}
    for tool in manifest["tools"]:
        evidence = "；".join(dict.fromkeys(e["source"] for e in tool["evidence"]))
        out.append('<tr><td>' + esc(tool["name"]) + ' ' + marks(tool["reference_numbers"]) + '</td><td>' + esc(
            ", ".join(tool["versions"]) or "未记录") + '</td><td>' + esc(statuses[tool["usage"]]) + '<br><code>' + esc(evidence) + '</code></td></tr>')
    for library in manifest["libraries"]:
        name = esc(library["name"])
        if library["name"] in LIBRARY_DOCS:
            name = '<a href="' + LIBRARY_DOCS[library["name"]] + '">' + name + '</a>'
        out.append('<tr><td>' + name + '</td><td>' + esc(library.get("version") or "未记录") + '</td><td>' + esc(library["role"]) + '；' + esc(library["source"]) + '</td></tr>')
    if not manifest["tools"] and not manifest["libraries"]:
        out.append('<tr><td>未记录</td><td>未记录</td><td>缺少可追溯的工具使用证据。</td></tr>')
    out.append('</tbody></table>')
    if not manifest["libraries"]:
        out.append('<p>软件库及运行时版本：该历史运行未记录。</p>')
    if manifest["planner"]:
        out.append('<p>确认方案记录的规划模型：' + esc(manifest["planner"].get("model", "未记录")) + '；来源：' + esc(manifest["planner"].get("source", "未记录")) + '</p>')
    out.append('<h3>Skill 调用记录</h3>')
    audit = manifest["skills"]
    if audit.get("skills"):
        out.append('<ul>')
        for skill in audit["skills"]:
            status = {"requested": "已请求，结果未记录", "loaded": "加载成功（不代表任务全部完成）", "tool_error": "工具调用错误"}.get(skill["status"], skill["status"])
            out.append('<li>' + esc(skill["name"]) + ' ' + marks(skill.get("reference_numbers", [])) + '；状态：' + esc(status) + '；来源：<code>' + esc(skill.get("source") or "未记录") + '</code>；版本：' + esc(skill.get("version") or "未记录") + '；源码 SHA256：<code>' + esc(skill.get("sha256") or "未记录") + '</code></li>')
        out.append('</ul>')
    elif audit.get("tracking") == "not_invoked_by_design":
        out.append('<p>本次由模型规划接口和确定性执行器完成，未调用 Skill。已安装或可用的 Skill 不计入调用。</p>')
    elif audit.get("tracking") == "sdk_tool_messages" and audit.get("turn_completed"):
        out.append('<p>本次执行的 SDK 工具消息中未出现 Skill 调用。</p>')
    else:
        out.append('<p>该运行未完整记录 Skill 调用，无法追溯；不能据此断言调用或未调用。</p>')
    if manifest["literature_uses"]:
        out.append('<h3>其他文献使用记录</h3><ul>')
        for use in manifest["literature_uses"]:
            out.append('<li>' + esc(use.get("context", use["reference_id"])) + ' ' + marks([use["reference_number"]]) + '；' + esc(use["validation"]) + '</li>')
        out.append('</ul>')
    out.append('<h3>参考文献</h3><ol>')
    for ref in manifest["references"]:
        doi = str(ref["doi"])
        link = "https://doi.org/" + quote(doi, safe="/")
        citation = f'{ref.get("authors", "")} ({ref.get("year", "")}). {re.sub(r"<[^>]*>", "", ref["title"])}. {ref.get("journal", "")}, {ref.get("volume", "")}, {ref.get("pages", "")}.'
        out.append('<li id="reference-' + str(int(ref["number"])) + '">' + esc(citation) + ' <a href="' + esc(link) + '">' + esc(doi) + '</a></li>')
    out.append('</ol>')
    if not manifest["references"]:
        out.append('<p>当前证据未关联到已核验文献；没有自动补造参考文献。</p>')
    out.append('<p>配套文件：<code>analysis-provenance.json</code>（引用与证据清单）、<code>references.bib</code>（文献导出）。</p><ul>')
    out.extend('<li>' + esc(note) + '</li>' for note in manifest["limitations"])
    out.append('</ul></section>')
    return "".join(out)


class SkillUsageAudit:
    """Audit actual SDK tool-use/result messages, not the model's prose claims."""
    def __init__(self, run_dir: Path, skill_root: Path):
        self.path = run_dir / ".bioagent/skill-usage.json"
        self.skill_root = skill_root.resolve()
        self.document = {"tracking": "sdk_tool_messages", "turn_completed": False, "skills": []}
        _write(self.path, self.document)

    def observe(self, message: Any) -> None:
        changed = False
        for block in getattr(message, "content", []) or []:
            get = lambda key, default=None: block.get(key, default) if isinstance(block, dict) else getattr(block, key, default)
            if get("name") == "Skill" and isinstance(get("input"), dict):
                call_id = str(get("id", ""))
                if any(s["call_id"] == call_id for s in self.document["skills"]):
                    continue
                name = str(get("input").get("skill", "unknown"))
                entry = {"name": name, "call_id": call_id, "status": "requested", "sha256": None}
                if re.fullmatch(r"[A-Za-z0-9_-]+", name):
                    path = (self.skill_root / name / "SKILL.md").resolve()
                    if path.is_relative_to(self.skill_root) and path.is_file():
                        entry["sha256"] = _hash(path)
                        entry["source"] = ".claude/skills/" + name + "/SKILL.md"
                        content = path.read_text(encoding="utf-8")
                        if content.startswith("---\n"):
                            import yaml
                            try:
                                metadata = yaml.safe_load(content.split("---", 2)[1]) or {}
                                details = metadata.get("metadata", {})
                                entry["version"] = str(details["version"]) if "version" in details else None
                                refs = details.get("citation_ids", [])
                                entry["reference_ids"] = refs if isinstance(refs, list) and all(isinstance(r, str) for r in refs) else []
                            except (yaml.YAMLError, TypeError, AttributeError):
                                pass
                self.document["skills"].append(entry)
                changed = True
            elif get("tool_use_id"):
                for entry in self.document["skills"]:
                    if entry["call_id"] == get("tool_use_id"):
                        # A successful Skill tool result confirms loading, not
                        # that every instruction in the Skill was completed.
                        entry["status"] = "tool_error" if get("is_error") else "loaded"
                        changed = True
        if changed:
            _write(self.path, self.document)

    def finish(self) -> None:
        self.document["turn_completed"] = True
        _write(self.path, self.document)
