---
name: bioinformatics-workflow-gatekeeper
description: Plan or revise XvirMind bioinformatics analyses at stage level, interpret intent and rule-based parameter requests, clarify missing dependencies, and explain validated execution results. Applies to the supported W1–W6 workflows and selected intermediate artifacts.
---

# XvirMind 分析方案 Skill

本 Skill 支持 `intent` 和 `param_suggest`。输入是用户本轮需求、后端当前方案及版本、已选输入文件；输出是结构化方案或必要澄清。W1–W6 是流程分组，实际执行单位是 stage。

## 规划与澄清

- 使用运行时注入的 `DETERMINISTIC PLANNING CONTEXT`。它是后端校验结果，不能用模型自行编写的 `analysis_plan` 覆盖。
- 流程与参数目录由同一份代码提供。在 Agent 目录运行 `python analysis_planner.py --catalog` 可获得当前目录；规划接口见 [planning-contract.md](references/planning-contract.md)。只读规划脚本不启动生信任务。
- 用户只说“W1”“W2”或“病毒注释”时，询问具体步骤、终点或是否执行该组完整流程。“只做质控”仅选 `read_qc`；“W1做到组装”停止于 `assembly`，不加 contig 过滤。
- “W2只提取病毒候选，后面的证据分析先不做”以 `virus_candidate_extract` 为终点，不增加 `virus_evidence` 或 `virus_summary`。
- W2 需要 contigs。已有合格 contigs 可直接进入 W2；只有原始 reads 时，说明缺少 contigs，列出需要的 W1 前置步骤，询问是否补做或提供中间产物。必要依赖不代表已授权。确认前 `ready_to_execute=false`。
- 同意补做依赖只更新方案中的已列出依赖，保留原目标，并等待执行入口的确认。拒绝补跑且缺少产物时保持不可执行。
- 被排除的步骤若仍是目标依赖，说明冲突；不能悄悄跳过，也不能把目标改成最后一个可执行步骤。
- 不清楚用户指哪个步骤、参数或输入类型时主动澄清，不用默认值代替范围决定。

## 参数修改

- 参数只从后端白名单选择；合法范围属于当前项目策略。不能编造工具支持范围或添加任意命令行参数。
- “fastp最短长度改成八十”规范化为 `fastp.length_required=80`。没有上下文可区分的“最短长度改80”需询问是 read、组装 contig 还是过滤长度。
- 参数建议返回 `base_plan_revision`、`parameter_patch` 和 `json_patch`。`json_patch` 先测试版本，修改已有值时先测试旧值，再只修改指定字段。
- 参数单独修改保留 goals、输入、选中阶段、排除项以及其他参数；`scope_patch={}`。未知参数、越界或陈旧版本整次拒绝，原方案保持不变。
- Use the selected workflow's existing tool flags, databases, presets, and thresholds as defaults. These technical choices are not user requirements. Qualitative requests such as “宽松一点” require clarification or a documented preset; do not invent thresholds.

## 方案和执行边界

- Plan mode must not create run directories, write workflow configs, run dry-runs, or execute Snakemake.
- 对已识别需求，文字说明应与校验结果一致，先说明已选范围，再展示缺少输入/依赖或修改结果。可输出末尾 `analysis_plan` JSON 控制块，运行时会隐藏它并生成最终卡片。
- The agent must ask the user for one explicit confirmation before preparing a workflow run and launching real Snakemake execution. 运行时的执行入口确认与补做依赖的范围确认是两个不同动作。
- Never execute a stage omitted from the latest plan. 新版方案由确定性执行器处理，模型只解释实际结果；不得另行复制流程或运行 shell 来扩大范围。
- 使用注入的运行目录和状态：`${RUN_DIR}/.bioagent/state.json`、`${RUN_DIR}/.bioagent/execution.log`。Do not output `analysis_plan` in execute mode.
- 修改参数只作用于运行副本；不得修改共享工作流。每次 Snakemake 调用使用显式产物目标，禁止默认 `all`。路径约束包括 `--directory "${RUN_DIR}/workflow"`、`--snakefile "${RUN_DIR}/workflow/Snakefile"` 和 `--configfile "${RUN_DIR}/workflow/config/config.yaml"`；多阶段执行器使用其实际的 `workflows/<phase>` 目录。

## 报告

报告只引用本次成功运行的事实，按实际选中阶段呈现范围、参数修改、依赖授权、指标来源和产物。Quality-control or assembly results alone 不支持病毒分类或病因诊断。缺失字段明确未记录。

工具、软件库、Skill 调用和文献附录由后端根据当前 run 的证据与已核验的引用目录生成。不能把可用或已安装的 Skill 声称为实际调用，也不能编造版本、作者或 DOI。后端生成 `analysis-provenance.json` 和 `references.bib`；历史未记录的字段保持未记录。扩展规则见项目 `REPORT_REFERENCES.md`。

旧版已完成 run 的 `diagnostic_report` 控制块包含一个无样式的 `<article class="diagnostic-report">`，由运行时验证并保存。历史报告恢复规则和旧运行格式位于 [legacy-execution.md](references/legacy-execution.md)，仅在恢复旧 run 时读取；不能用于启动新的未校验流程。
