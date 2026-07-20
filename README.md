# BioAgent Runtime

BioAgent 是 Open WebUI 与 Claude Agent SDK 之间的 OpenAI-compatible runtime，面向病毒分类和固定 Snakemake 生物信息学工作流。

---

## 最新变更：TaxonAgent MCP 集成（2026-07-19）

将 TaxonAgent 以 MCP Sub-agent 的方式接入 BioAgent。用户提问 ICTV 病毒分类问题时，BioAgent 自动识别意图并调用 TaxonAgent，返回 ICTV 划界标准、基因组信息、公共卫生背景等结构化结果。

### 架构

```
用户问题（Open WebUI）
  ↓
BioAgent（cyclone:8001）— MiniMax M3，识别 ICTV 意图 → MCP tool call
  ↓  Streamable HTTP (MCP spec 2025-03-26)  POST /mcp
TaxonAgent MCP Server（5090:8765）— ictv_family_info(family_name)
  ↓
TaxonAgent 核心（ICTV KG 38,228 节点 / 32 科 / 834 属 + Ollama glm4）
```

### 代码改动

| 文件 | 位置 | 改动 |
|------|------|------|
| `mcp/taxonagent_mcp.py` | 5090, taxonagent | 新增：FastMCP server，暴露 `ictv_family_info` 工具 |
| `config.py` | cyclone, bioagent | 新增 `TAXONAGENT_MCP_URL`；System Prompt 追加 ICTV 调用指引 |
| `runtime.py` | cyclone, bioagent | `_build_options()` 注册 `mcp_servers` 和 `mcp__taxonagent__*` 权限 |

完整技术文档见 `taxonagent_mcp_integration.md`，测试报告见 `e2e_test_result_20260719.md`。

---

## 启动

**TaxonAgent MCP Server（5090）**：

```bash
conda activate taxonagent
python ~/projects/taxonagent/mcp/taxonagent_mcp.py
```

**BioAgent Adapter（cyclone）**：

```bash
cd /data/bioagent/agent
BIOAGENT_RUN_ROOT=/data/runs conda run -n adapter \
  uvicorn adapter:app --host 127.0.0.1 --port 8001
```
