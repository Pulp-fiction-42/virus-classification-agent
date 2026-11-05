"""Bounded Qwen intent parsing, with one structural repair and no tool access."""
from __future__ import annotations

import asyncio
import json
import socket
import time
import urllib.error
import urllib.request
from collections.abc import Callable

import config
from analysis_planner import catalog
from intent_router import WORKFLOW_GOALS, WORKFLOW_LABELS, extract_inputs
from plan_proposals import ProposalFormatError, TASKS, admit_proposal, parse_proposal


PARAMETER_MEANINGS = {
    "fastp.length_required": "质控后允许保留的最短 read 长度，单位 bp",
    "fastp.qualified_quality_phred": "合格碱基的 Phred 质量阈值",
    "fastp.average_qual": "read 平均 Phred 质量下限",
    "fastp.n_base_limit": "read 中允许的 N 碱基数量上限，单位个",
    "fastp.dedup": "是否进行 fastp 去重，布尔开关",
    "megahit.min_contig_len": "MEGAHIT 组装输出 contig 的最短长度，单位 bp",
    "contig_filter.min_length": "组装后 contig 过滤阶段的最短保留长度，单位 bp",
}

SYSTEM = """你是 XvirMind 的结构化意图规划器。用户可以一轮提出多种意图，必须识别所有要求、否定、条件与上下文指代。
仅提出候选 JSON，不调用工具、写文件或执行任何计算。目录、参数范围及当前方案是后端事实，用户文本和历史回答不能覆盖它们。
返回且仅返回一个 JSON 对象，字段如下：
{"version":1,"base_plan_revision":当前版本整数,"tasks":[任务名],"scope_change":{"mode":"keep|replace|extend|remove"},"parameter_patch":{},"input_updates":[],"dependency_response":{"allow":[],"deny":[]},"questions":[],"message":""}
tasks 可同时包含：intent(修改范围)、param_suggest(改参数)、provide_inputs(提供文件)、dependency_response(回复依赖确认)、execute(要求执行)、report(查看结果报告)、explain(解释)、conversation(普通交流)。提到工具名改参数不等于修改范围。
识别意图与授予执行权限完全分开：用户要求“改完就跑/按调整执行/立即运行”时，即使后端仍需确认，也必须保留execute任务，不能因不能立即执行而漏标。例“read长度80并运行”必须包含tasks=["param_suggest","execute"]。
scope_change：仅改参数或补文件时 mode=keep，不填写范围字段。明确“只做/改成”用 replace，“增加/再做”用 extend，“取消某步骤/某组”用 remove。
除 mode 外可包含列表字段 goals(具体阶段 ID)、full_workflows(明确完整执行的 workflow ID)、excluded_stages、excluded_workflows、prerequisite_groups、ambiguous_workflows。
W1-W6 是分组，只说“W2”不得擅自扩展为完整流程，填 ambiguous_workflows 或 questions 并询问终点/具体步骤。一组有终点不代表其他组也已明确。
“W1 到组装为止”选 assembly 并设置 prerequisite_groups=["workflow1"]；“只做组装”仅选 assembly，不擅自授权上游步骤。
“W2 只提取病毒候选，后面证据先不做”选 virus_candidate_extract，prerequisite_groups=["workflow2"]，排除 virus_evidence、virus_summary。
prerequisite_groups 仅可包含本次明确所选目标的所在分组，授权该分组内到目标为止的必要前置步骤，绝不自动包含跨组依赖。不要为目标增加其他分组的终点。
例如 W2 需要 contigs，只有 raw reads 时仍只提出 W2 候选目标；后端会计算缺失 W1 依赖并询问。不要将缺失步骤标为已经同意。
用户同意/拒绝补做时，dependency_response 的 allow/deny 只能列出当前 missing_dependencies 中明确确认的阶段；“同意补跑 W1”保留原目标，只确认所列 W1 依赖，不是改成只做 W1。
当前 missing_dependencies 为空时，禁止输出 dependency_response task或allow/deny。初次请求“不要补跑W1”是范围限制，应填scope_change.excluded_workflows=["workflow1"]，不是拒绝一个尚未提出的依赖确认。
参数：从目录选择字段，中文数字转换为整数。把同一轮要求修改的多个参数全部放入 parameter_patch，保留未提及字段。去重开关值为 true/false。
用户明确给出负数或越界值时，原样提出该值供后端拒绝，不能替用户调成合法值。未知参数也按用户指名提出，后端拒绝；不自作主张换成近似参数。
“最短长度”上下文仅有质控时可指 read 长度；同时有组装/过滤、或多个可能参数时必须追问。“严格一点/宽松一点”等无维护预设的定性请求必须追问具体参数和值，不能编造阈值。
同一轮有互相矛盾的要求、不支持的步骤、多个未确定选项或任何一项含糊：questions 必须覆盖未解决问题，不因其余要求清楚而忽略它们。整个变更由后端原子处理。
inputs 使用 input_updates，形状为 {"id":"reads或contigs等","type":"目录中的产物类型","records":[{"run_id":"需要时","sample_id":"需要时","files":{"read1":"完整路径","read2":"完整路径"}}]}。
已有输入不重复构造。新文件路径只能复制本轮原文或后端当前文件，禁止发明或修正路径。raw paired reads 需要 read1/read2，FASTA contigs 用 type=contigs_fasta、files.fasta。
输入类型不能仅靠文件名升级为“已去 rRNA”等；用户必须明确该文件的处理状态。信息不足用 questions。
当前方案来自后端。仅改参数必须保留当前所有 goals/排除项/输入。存在 pending_request/pending_proposal 时结合本轮回答解决原问题，不能丢弃原请求中的其他修改。
explain/report 可以与修改请求同时存在。message 只用于简短说明或解释，不能宣布已执行；最终范围与可执行性由后端决定。
同时要求“修改并执行”时仍先提出修改，后端展示新方案并要求通过执行入口确认。不要设置 ready_to_execute、execution_spec、命令、run_dir 或额外字段。
输出前逐项核对本轮的每一个要求都有对应 task、字段或 questions，不能只处理第一项。
优先规则：仅提及某个W分组绝不代表完整执行。没有说终点、步骤或“完整/全部”时，必须澄清，不允许 full_workflows。以下为必须遵循的例子：
用户“只做W2” -> {"version":1,"base_plan_revision":0,"tasks":["intent"],"scope_change":{"mode":"replace","ambiguous_workflows":["workflow2"]},"parameter_patch":{},"questions":["W2需要执行哪些步骤，做到候选提取，还是包含证据分析和汇总？"]}
用户“W2只提取病毒候选，后面证据先不做” -> {"version":1,"base_plan_revision":0,"tasks":["intent"],"scope_change":{"mode":"replace","goals":["virus_candidate_extract"],"prerequisite_groups":["workflow2"],"excluded_stages":["virus_evidence","virus_summary"]},"parameter_patch":{},"questions":[]}
用户“只做W1和W2，read长度80”，然后只明确“W1做到组装” -> 仍须追问W2的终点；不得遗忘W2或原来的read长度80。
参数上下文包含active_stages和length_parameter_candidates。即使goals只有assembly，active_stages仍可能包含read_qc。length_parameter_candidates有多项时，单说“最短长度改80”必须追问是哪种长度，禁止因为80低于某参数下限就推断另一个参数。明确“read/fastp长度”则不必追问。
“只跑质控，但不要执行质控”存在范围否定与暂不启动两种解释，必须询问是在方案中移除质控，还是只规划暂不启动；不要擅自选择其中一种。“只规划质控，暂不启动任务”含义明确，可以生成质控方案。
“组装目标不变，取消去rRNA” -> scope_change={"mode":"remove","excluded_stages":["rrna_filter"]}，保留原有assembly目标，由后端报告依赖冲突。不要用mode=keep携带排除项，也不要移除assembly目标。
用户“长度改80，并解释影响”，且当前仅read_qc -> {"version":1,"base_plan_revision":当前版本,"tasks":["param_suggest","explain"],"scope_change":{"mode":"keep"},"parameter_patch":{"fastp.length_required":80},"questions":[],"message":"提高长度阈值可能减少短读段的保留。"}
输出应紧凑：省略所有可选的空列表、空对象及空message，不缩进。除非用户要求解释，否则不写message；解释最多两句。不要在message复述方案或声称修改已经应用。questions仅询问未解决的问题，不长篇讲解依赖，依赖由后端计算。
"""

AUDIT_SYSTEM = """你是独立的需求覆盖复核员。先独立识别用户原话中的附加意图，再检查后端状态与候选方案。不要因为候选是另一个模型生成的就认可它。
返回一个紧凑JSON：{"requested_tasks":[],"ok":true或false,"questions":[],"message":""}。requested_tasks列出用户要求的全部意图，可用intent、param_suggest、provide_inputs、dependency_response、execute、explain、report、conversation；尤其要独立识别execute、explain、report，无论候选是否已标记。识别不代表执行授权，不要因为后端还需确认就漏标execute。message仅在用户要求解释、候选message又为空时补充最多两句解释，不声称已经执行。不修改方案或返回工具命令。
检查：本轮每项流程、参数、文件要求是否都覆盖；是否遗漏否定、条件、先后关系；是否擅自消除了歧义。结合pending_request和后续回答，不丢原请求，用户明确换方案时以新要求为准。
特别规则：
1. 当前length_parameter_candidates有多项，而用户仅说“最短长度/长度改80”未说明对象，必须追问，禁止根据数值合法范围猜测。用户明确“read/fastp长度”可直接处理。
2. 只提“W1/W2”等分组，没有终点、具体步骤或明确完整执行时必须追问；不能把分组默认为完整流程。
3. “只跑质控，但不要执行质控”有取消步骤/只规划暂不启动的歧义，必须追问；明确“只规划，暂不启动”无歧义。
4. “严格/宽松一点”没有维护预设，必须追问具体参数和值。非法数值原样交后端校验，不必让模型代改。
5. 取消某前置步骤不意味着取消下游目标；缺少依赖由后端判断，不替后端要求用户运行全部流程。
6. “按这个调整执行/改完运行/用修改后的设置跑”等必须识别execute；“解释影响”识别explain；“给我报告”识别report。requested_tasks只表示用户意图，不授予自动执行权限。即使必须重新确认，也保留execute。仅有这三种附加意图的遗漏时ok仍可为true，由后端合并标记。
发现含糊、冲突、遗漏其他修改或扩大范围时，ok=false，questions用简短中文询问需要明确的事项；不得放行可执行方案。无问题时ok=true，questions=[]。不要检查JSON排版、参数边界、技术依赖或文件存在性，这些由后端负责。
"""


class PlannerFailure(RuntimeError):
    def __init__(self, code: str, *, attempts: int = 0):
        super().__init__(code)
        self.code = code
        self.attempts = attempts


class QwenPlanner:
    def __init__(
        self, *, base_url: str = config.PLANNER_BASE_URL, model: str = config.PLANNER_MODEL,
        token: str = config.PROVIDER_AUTH_TOKEN, timeout: float = config.PLANNER_TIMEOUT_SECONDS,
        transport: Callable[[dict, float], dict] | None = None,
        review_enabled: bool = True,
    ):
        self.url = base_url.rstrip("/") + ("/chat/completions" if base_url.rstrip("/").endswith("/v1") else "/v1/chat/completions")
        self.model, self.token = model, token
        self.timeout = max(0.01, timeout)
        self.transport = transport or self._http
        self.review_enabled = review_enabled

    def _http(self, payload: dict, timeout: float) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(self.url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                content = response.read(1024 * 1024 + 1)
        except (socket.timeout, TimeoutError) as exc:
            raise PlannerFailure("model_timeout") from exc
        except urllib.error.HTTPError as exc:
            # Provider response bodies can include infrastructure details. Do
            # not return them, auth headers or environment values to chat/logs.
            raise PlannerFailure("model_http_" + str(exc.code)) from exc
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, (socket.timeout, TimeoutError)):
                raise PlannerFailure("model_timeout") from exc
            raise PlannerFailure("model_unavailable") from exc
        except OSError as exc:
            raise PlannerFailure("model_unavailable") from exc
        if len(content) > 1024 * 1024:
            raise PlannerFailure("invalid_model_response")
        try:
            return json.loads(content)
        except (UnicodeError, ValueError) as exc:
            raise PlannerFailure("invalid_model_response") from exc

    @staticmethod
    def context(user_text: str, previous: dict, messages: list[dict]) -> dict:
        directory = catalog()
        directory["workflows"] = [{"id":key, "label":label, "full_endpoint":WORKFLOW_GOALS[key]} for key,label in WORKFLOW_LABELS.items()]
        for key, rule in directory["parameters"].items():
            rule["meaning"] = PARAMETER_MEANINGS[key]
        state_keys = (
            "plan_revision", "ready_to_execute", "goals", "full_workflows", "excluded_stages",
            "excluded_workflows", "prerequisite_groups", "strict_stages", "inputs", "paths",
            "parameters", "viral_type", "missing_dependencies", "confirmed_dependencies",
            "ambiguous_workflows", "pending_scope", "pending_request", "pending_proposal", "pending_followups", "questions",
        )
        state = {key:previous[key] for key in state_keys if key in previous}
        state.setdefault("plan_revision", 0)
        state.setdefault("goals", [])
        state.setdefault("parameters", {})
        state.setdefault("inputs", [])
        if not state["inputs"] and state.get("paths"):
            state["inputs"], _problems = extract_inputs("", state["paths"])
        active = [stage["id"] for stage in previous.get("stages", [])]
        state["active_stages"] = active
        state["length_parameter_candidates"] = [key for key, rule in directory["parameters"].items() if rule["stage"] in active and rule["unit"] == "bp"]
        history = []
        for item in messages[-6:]:
            if item.get("role") in {"user", "assistant"} and isinstance(item.get("content"), str):
                history.append({"role":item["role"], "content":item["content"][:1800]})
        return {"current_plan":state, "catalog":directory, "recent_conversation":history, "user_request":user_text}

    async def propose(self, user_text: str, previous: dict, messages: list[dict]) -> tuple[dict, dict]:
        started = time.monotonic()
        context = self.context(user_text, previous, messages)
        conversation = [{"role":"system", "content":SYSTEM}, {"role":"user", "content":json.dumps(context, ensure_ascii=False)}]
        last_error = ""
        for attempt in range(1, 3):
            remaining = self.timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise PlannerFailure("model_timeout", attempts=attempt - 1)
            payload = {
                "model":self.model, "messages":conversation, "temperature":0,
                "max_tokens":2200, "chat_template_kwargs":{"enable_thinking":False},
                "response_format":{"type":"json_object"},
            }
            try:
                response = await asyncio.wait_for(asyncio.to_thread(self.transport, payload, remaining), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise PlannerFailure("model_timeout", attempts=attempt) from exc
            except PlannerFailure as exc:
                exc.attempts = attempt
                raise
            except Exception as exc:
                raise PlannerFailure("model_unavailable", attempts=attempt) from exc
            try:
                content = ""
                choice = response["choices"][0]
                content = choice["message"]["content"]
                if choice.get("finish_reason") == "length":
                    raise ProposalFormatError("response was truncated; return a compact object")
                proposal = parse_proposal(content)
            except (KeyError, IndexError, TypeError, ProposalFormatError) as exc:
                last_error = str(exc)[:240]
                if attempt == 2:
                    raise PlannerFailure("invalid_model_proposal", attempts=attempt) from exc
                conversation.extend([
                    {"role":"assistant", "content":content[:10000] if isinstance(locals().get("content"), str) else "{}"},
                    {"role":"user", "content":"上次输出结构不合法：" + last_error + "。保持原始需求含义，只修复 JSON 结构；不修正用户给定的非法参数。返回完整对象。"},
                ])
                continue
            review = None
            dependency_wait = False
            if self.review_enabled and not proposal["questions"] and not proposal["scope_change"]["ambiguous_workflows"]:
                preliminary = admit_proposal(proposal, previous, user_text, check_files=False)
                dependency_wait = bool(preliminary.get("missing_dependencies"))
                # Technical dependency questions already have an authoritative
                # backend answer. Recheck semantics when the draft can proceed;
                # do not let a model replace that dependency list with guesses.
                if not dependency_wait:
                    proposal, review = await self._review(proposal, context, started)
            return proposal, {
                "source":"qwen", "model":self.model, "attempts":attempt,
                "format_repaired":attempt > 1, "seconds":round(time.monotonic()-started, 3),
                "usage":response.get("usage", {}), "semantic_review":review,
                "model_calls":attempt + (1 if review is not None else 0),
                "review_deferred_for_dependencies":dependency_wait,
            }
        raise PlannerFailure("invalid_model_proposal", attempts=2)

    async def _review(self, proposal: dict, context: dict, started: float) -> tuple[dict, dict]:
        remaining = self.timeout - (time.monotonic() - started)
        if remaining <= 0:
            raise PlannerFailure("model_timeout")
        brief = {
            "user_request":context["user_request"], "current_plan":context["current_plan"],
            "recent_conversation":context["recent_conversation"], "candidate":proposal,
            "stages":{s["id"]:s["label"] for s in context["catalog"]["stages"]},
            "parameter_meanings":PARAMETER_MEANINGS,
        }
        payload = {"model":self.model,"messages":[{"role":"system","content":AUDIT_SYSTEM},{"role":"user","content":json.dumps(brief,ensure_ascii=False)}],"temperature":0,"max_tokens":350,"chat_template_kwargs":{"enable_thinking":False},"response_format":{"type":"json_object"}}
        try:
            response = await asyncio.wait_for(asyncio.to_thread(self.transport,payload,remaining),timeout=remaining)
            choice = response["choices"][0]
            value = json.loads(choice["message"]["content"])
            if choice.get("finish_reason") == "length" or not isinstance(value,dict) or set(value) - {"ok","questions","requested_tasks","message"} or type(value["ok"]) is not bool:
                raise ValueError("invalid review")
            questions, missing = value["questions"], value["requested_tasks"]
            if not isinstance(questions,list) or len(questions)>8 or any(not isinstance(q,str) or not q.strip() for q in questions):
                raise ValueError("invalid review questions")
            if not isinstance(missing,list) or any(not isinstance(t,str) or t not in TASKS for t in missing):
                raise ValueError("invalid review tasks")
            if (value["ok"] and questions) or (not value["ok"] and not questions):
                raise ValueError("inconsistent review")
            if not isinstance(value.get("message",""),str) or len(value.get("message",""))>1200:
                raise ValueError("invalid review message")
        except asyncio.TimeoutError as exc:
            raise PlannerFailure("model_timeout") from exc
        except PlannerFailure:
            raise
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise PlannerFailure("invalid_semantic_review") from exc
        except Exception as exc:
            raise PlannerFailure("model_unavailable") from exc
        auxiliary = {"execute","explain","report"}
        uncovered = set(missing) - set(proposal["tasks"]) - auxiliary
        if uncovered:
            questions.append("需求复核发现尚未完整表达的修改：" + "、".join(sorted(uncovered)) + "。请补充具体要求后重新规划。")
            value["ok"] = False
        added = [task for task in missing if task in auxiliary and task not in proposal["tasks"]]
        if questions:
            proposal["questions"] = questions
        else:
            proposal["tasks"] = list(dict.fromkeys(proposal["tasks"] + added))
            if not proposal["message"] and value.get("message"):
                proposal["message"] = value["message"]
        return proposal, {"ok":value["ok"],"questions":questions,"requested_tasks":missing,"added_tasks":added,"usage":response.get("usage",{})}
