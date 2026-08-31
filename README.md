# BioAgent Runtime

BioAgent 是 Open WebUI 与 Claude Agent SDK 之间的 OpenAI-compatible runtime，面向 workflow 1–6 的固定 Snakemake 生物信息学分析。

## 本次改进

- 新增无模型 `/v1/analysis/plans/compile`，由六类目标、病毒类型和 Standard/Emergency 控制直接编译 `analysis_pipeline` v2。
- 新增 `/v1/analysis/runs/start`，执行前重编 persisted plan、检查 canonical dataset fingerprint，并以固定 run ID 幂等启动 runner。
- deterministic runtime 独立初始化；MiniMax、Claude transcript 或 TaxonAgent 不可用时仍可规划和启动 runner。
- MiniMax 对话必须返回 `respond | clarify | propose_patch` action；patch 仅允许 goals 与 viral type，且必须由用户确认。
- 多目标共享上游，中间 artifact 选择最短依赖闭包，额外 dataset artifact 不进入 execution spec。
- `standard` 直接执行；`emergency` 在适用的双端 reads 上执行 5% Kraken2 初筛并停止，纯中间产物输入自动降级为标准模式。
- 集成 TaxonAgent MCP Server；遇到 ICTV 病毒分类、划界标准、marker gene 或 genome type 查询时，runtime 可自动调用 `ictv_family_info`。

## 结构

```text
agent/
├── adapter.py       # HTTP/OpenAI SSE 协议适配
├── runtime.py       # SDK 配置、会话恢复、结果流与 MCP 注册
├── deterministic_runtime.py # 不导入模型 client 的 runner admission
├── conversation_contract.py # action、patch、citation 与 retrieval 型别
├── conversation_context.py # 模型可见上下文的唯一安全白名单
├── conversation_events.py # adapter 与 provider 共用的事件／错误
├── taxonagent_adapter.py # legacy TaxonAgent 至 typed citation 的边界
├── config.py        # 模型、密钥、路径与 MCP endpoint 等静态配置
├── workflow_adapters.py # typed input、phase 准备与 targets
├── workflow_catalog.py  # artifact 契约与公开 stage DAG
├── pipeline_spec.py     # analysis_pipeline v2 验证与最小依赖闭包编译
├── analysis_runner.py   # 多 phase Snakemake 执行与 telemetry
├── .claude/
│   └── sessions/    # chat-map 与 SDK JSONL transcript，不提交 Git
├── tests/
├── tools/watch.py   # 可选的消息监控脚本
├── taxonagent_mcp_integration.md
└── e2e_test_result_20260719.md
```

TaxonAgent MCP Server 的实现位于 5090 上独立的 `taxonagent` 项目，不属于本目录。

## 会话模型

Open WebUI 向 adapter 转发 `X-OpenWebUI-Chat-Id`。runtime 用轻量 chat-map 保存对应的 Claude session ID，不修改 Open WebUI 数据库：

本机启动前先复制 `.env.example` 为 `.env` 并填写 `MINIMAX_API_KEY`；`start.sh` 与本机 8002 包装脚本都会在 uvicorn 启动前载入该文件，`.env` 不提交 Git。

1. adapter 为每个 `chat_id` 保留一个常驻 `ClaudeSDKClient`。
2. 同一 `chat_id` 的请求复用同一个 client，并通过会话锁串行写入 SDK stream。
3. 新建 client 固定使用 `dontAsk`，不加载 Claude Code preset、Skills 或文件工具；仅允许 TaxonAgent MCP。
4. 恢复时在 `ClaudeAgentOptions(resume=claude_session_id)` 中加载 transcript，每轮只调用 `client.query(prompt)`。
5. 新会话第一轮完成后保存 SDK 返回的真实 session ID；后续只发送最新用户消息。
6. SDK transcript 保存在 `.claude/sessions/projects/<project>/<claude_session_id>.jsonl`。
7. adapter 重启后从 chat-map 读取 session ID，并通过 `resume` 继续原 SDK session。

空闲 client 会在超时后断开；FastAPI shutdown 时会断开所有 client。当前部署仍假设单个 uvicorn worker。

## Agent 能力

- 对话 session 使用 `dontAsk` permission mode，只提供解释、追问、受控 patch 和科研问答；planner 与 runner 不经过 Claude session。
- 成功执行由 runner 写入 state v3、telemetry v3 与 `analysis_pipeline` schema v3 报告数据；Open WebUI 根据 run 状态提供任务区下载入口，并由后端按需确定性生成自包含 HTML。模型不撰写报告。
- ICTV 相关问题通过 MCP Streamable HTTP 调用 TaxonAgent，工具权限为 `mcp__taxonagent__*`。

## TaxonAgent MCP 集成

```text
用户问题（Open WebUI）
  ↓
BioAgent Adapter（cyclone:8001）
  ↓  识别 ICTV 意图并调用 MCP tool
TaxonAgent MCP Server（5090:8765/mcp）
  ↓  ictv_family_info(family_name)
TaxonAgent 核心（ICTV knowledge graph + Ollama glm4）
```

`runtime.py` 通过 `ClaudeAgentOptions.mcp_servers` 注册名为 `taxonagent` 的 HTTP server；endpoint 由 `config.TAXONAGENT_MCP_URL` 定义为 `http://172.24.202.246:8765/mcp`。System Prompt 指示模型在查询 ICTV taxonomy、demarcation criteria、classification rules、marker genes、genome type 或特定病毒科时调用 `ictv_family_info(family_name)`。

初始实现细节与既有端到端结果分别见 [TaxonAgent MCP 初始集成记录](taxonagent_mcp_integration.md) 和 [2026-07-19 测试报告](e2e_test_result_20260719.md)。集成记录中的旧部署路径仅供追溯，当前启动方式以本 README 为准。

### 启动 TaxonAgent MCP Server

在 5090 的专用 terminal 中执行：

```bash
ssh 5090
conda activate taxonagent
python ~/projects/taxonagent/mcp/taxonagent_mcp.py
```

Server 启动后监听 `0.0.0.0:8765`，MCP endpoint 为 `/mcp`。BioAgent adapter 必须能从 cyclone 访问 `172.24.202.246:8765`。

### 已知限制

- 当前只暴露 `ictv_family_info` 一个工具。
- 首次调用通常需要 150–250 秒，因为 5090 上的 Ollama glm4 推理没有缓存。
- MCP Server 目前需手动启动，尚未配置 systemd 守护。

## cyclone 部署

```text
Agent:          /data/bioagent/agent
Open WebUI:     /data/bioagent/open-webui
Sessions:       /data/bioagent/agent/.claude/sessions
Workflows:      /data/user/guojindan/Projects/Workflows
Runs:           ${BIOAGENT_RUN_ROOT:-/data/runs}
TaxonAgent MCP: http://172.24.202.246:8765/mcp
```

BioAgent 与 Open WebUI 服务由用户级 systemd 管理：

```bash
systemctl --user restart bioagent-adapter open-webui-backend
systemctl --user status bioagent-adapter open-webui-backend
```

Open WebUI 必须启用：

```text
ENABLE_FORWARD_USER_INFO_HEADERS=True
```

## 验证

```bash
python -m unittest discover -s tests -v
curl http://127.0.0.1:8001/v1/models
```

`BIOAGENT_RUN_ROOT` 可覆盖 run 根目录；未设置时默认为 `/data/runs`。开发脚本默认写入项目内 `runs/`。
