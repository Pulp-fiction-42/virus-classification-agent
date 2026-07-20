# BioAgent Runtime

BioAgent 是 Open WebUI 与 Claude Agent SDK 之间的 OpenAI-compatible runtime，面向病毒分类和固定 Snakemake 生物信息学工作流。

## 最新变更：TaxonAgent MCP 集成（2026-07-19）

将 TaxonAgent 以 MCP Sub-agent 的方式接入 BioAgent。用户在对话中提问 ICTV 病毒分类问题时，BioAgent 会自动识别意图并通过 MCP 调用 TaxonAgent，返回 ICTV 划界标准、基因组信息、公共卫生背景等结构化结果。

### 架构

```
用户问题（Open WebUI）
  ↓
BioAgent Adapter（cyclone, 172.24.195.66:8001）
  MiniMax M3 LLM，识别 ICTV 意图 → MCP tool call
  ↓  MCP Streamable HTTP（MCP spec 2025-03-26）
TaxonAgent MCP Server（5090, 172.24.202.246:8765）
  FastMCP，工具：ictv_family_info(family_name)
  ↓
TaxonAgent 核心（ICTV KG 38,228 节点 / 32 科 / 834 属 + Ollama glm4）
  ↓
返回 ICTV 分类规则、划界标准、属列表等
```

协议：MCP Streamable HTTP（2025-03-26 规范），单一端点 `/mcp`，非旧版 SSE。

### 代码改动

**新增文件**（5090 | taxonagent 项目）：

- `/home/bgi-chao/taxonagent_mcp.py`：将 `run_query()` 包装为 MCP tool，监听 `0.0.0.0:8765`

**修改文件**（cyclone | bioagent 项目）：

- `config.py`：新增 `TAXONAGENT_MCP_URL` 常量；`SYSTEM_PROMPT` 末尾追加 ICTV 工具调用指引
- `runtime.py`：`_build_options()` 加入 `mcp_servers` 注册和 `mcp__taxonagent__*` 工具权限

```python
# runtime.py _build_options() 改动
allowed_tools=["Read", "Glob", "Grep", "mcp__taxonagent__*"],
mcp_servers={
    "taxonagent": {
        "type": "http",
        "url": config.TAXONAGENT_MCP_URL,   # http://172.24.202.246:8765/mcp
    },
},
```

### 端到端测试结果

**测试时间**：2026-07-19 23:59:49

| 测试层 | 内容 | 结果 | 耗时 |
|--------|------|------|------|
| 网络层 | cyclone → 5090:8765 TCP | ✅ | <1s |
| 协议层 | MCP 握手 + 工具发现 | ✅ | <1s |
| 应用层 | `ictv_family_info("Coronaviridae")` | ✅ | 154.8s |
| 端到端 Case 1 | 英文问 Flaviviridae | ✅ | 167.5s |
| 端到端 Case 2 | 中文问 Filoviridae | ✅ | 251.1s |

协议层原始输出：

```
✅ MCP initialize: HTTP 200
   server: TaxonAgent v1.28.1
   protocolVersion: 2025-03-26
   session-id: 3c40ff05139344a7ae37c98ba7dc5a26
✅ tools/list: 1 tool(s) found
   tool `ictv_family_info`: Query ICTV classification rules and taxonomy info for a virus family.
```

端到端原始输出（中文问 Filoviridae，节选）：

```
# Filoviridae（丝状病毒科）ICTV 分类规则

定量阈值
- 编码完整基因组差异度 ≥ 55%（PASC 方法）

公共卫生背景
- WHO 状态：优先病原体 | 病死率：25%–90% | BSL-4 | HHS Select Agent
```

完整测试报告见 `e2e_test_result_20260719.md`。

### 已知限制

| 问题 | 说明 |
|------|------|
| 响应慢 | 首次调用 150–250s，Ollama glm4 推理无缓存 |
| 无进程守护 | MCP server 用 nohup 启动，重启后需手动拉起 |
| 单工具 | 目前只暴露 `ictv_family_info`，后续可扩展序列分类、多科比较等 |
| venv 权限 | taxonagent venv 属于 aakash，bgi-chao 用 sys.path 绕过 |

### 运维

启动 TaxonAgent MCP Server（5090）：

```bash
nohup /home/bgi-chao/miniconda3/bin/python3.13 /home/bgi-chao/taxonagent_mcp.py \
  > /home/bgi-chao/taxonagent_mcp.log 2>&1 &
```

重启 BioAgent Adapter（cyclone）：

```bash
pkill -f "uvicorn adapter:app"
cd /data/bioagent/agent
BIOAGENT_RUN_ROOT=/data/runs \
nohup /data/user/liufangzhou/tools/miniconda3/bin/conda run -n adapter \
  uvicorn adapter:app --host 127.0.0.1 --port 8001 > /tmp/bioagent_adapter.log 2>&1 &
```

---

## 历史功能

### plan / execute 模式

- 引入 `/plan` 和 `/execute` 模式切换，并为 Open WebUI `chat_id` 保存持久 Claude SDK session 映射。
- `plan` mode 使用 `dontAsk` permission mode，注入 BioAgent 方案协议，避免进入 Claude Code Plan/ExitPlanMode。
- `execute` mode 在用户请求前追加执行提示，写入标准 `.bioagent/state.json`，成功后生成 `diagnostic-opinion.html`。
- 固定 Snakemake workflow gatekeeper 改为一次确认后直接执行，取消 dry-run 和第二次确认。

## 结构

```text
agent/
├── adapter.py            # HTTP/OpenAI SSE 协议适配
├── runtime.py            # SDK 配置、会话恢复和结果流（含 MCP 注册）
├── config.py             # 模型、密钥、路径、MCP 地址等静态配置
├── taxonagent_mcp.py     # TaxonAgent MCP Server（运行于 5090）
├── .claude/
│   ├── skills/           # 项目级 Agent Skills
│   └── sessions/         # chat-map 与 SDK JSONL transcript，不提交 Git
├── e2e_test_result_20260719.md   # 端到端测试完整报告
├── taxonagent_mcp_integration.md # MCP 集成技术文档
├── tests/
└── tools/watch.py        # 可选的消息监控脚本
```

## 会话模型

Open WebUI 向 adapter 转发 `X-OpenWebUI-Chat-Id`。runtime 用轻量 chat-map 保存对应的 Claude session ID，不修改 Open WebUI 数据库：

1. adapter 为每个 `chat_id` 保留一个常驻 `ClaudeSDKClient`。
2. 同一 `chat_id` 的请求复用同一个 client，并通过会话锁串行写入 SDK stream。
3. 新建 client 时以 `bypassPermissions` 能力启动 SDK subprocess，随后立即调用 `set_permission_mode("dontAsk")`；方案阶段只预先允许 `Read`、`Glob`、`Grep`。
4. 恢复时在 `ClaudeAgentOptions(resume=claude_session_id)` 中加载 transcript，每轮只调用 `client.query(prompt)`。
5. 新会话第一轮完成后保存 SDK 返回的真实 session ID；后续只发送最新用户消息。

## 用户命令

```text
/plan
/execute
```

## cyclone 部署

```text
Agent:       /data/bioagent/agent
Open WebUI:  /data/bioagent/open-webui
Sessions:    /data/bioagent/agent/.claude/sessions
Workflows:   /data/user/guojindan/Projects/Workflows
Runs:        ${BIOAGENT_RUN_ROOT:-/data/runs}
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
