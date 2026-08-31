# TaxonAgent MCP 集成文档

**目标**：将 TaxonAgent（运行于 5090）作为 MCP Sub-agent 接入 BioAgent（运行于 cyclone），当用户询问 ICTV 病毒分类相关问题时，BioAgent 能自动识别意图并调用 TaxonAgent。

**完成日期**：2026-07-19
**状态**：✅ 端到端测试通过

> [!IMPORTANT]
> 本文记录 2026-07-19 的初始部署，其中部分绝对路径和运维命令已经过时。当前服务入口为 `~/projects/taxonagent/mcp/taxonagent_mcp.py`，请使用项目 README 中的 `taxonagent` conda 环境启动命令。

---

## 1. 架构概览

```
用户 (Open WebUI)
  │
  ▼
BioAgent Adapter  [cyclone, 172.24.195.66:8001]
  │  Claude Agent SDK (MiniMax M3 LLM)
  │  识别 ICTV 意图 → MCP tool call
  │
  │  Streamable HTTP (MCP spec 2025-03-26)
  │  POST http://172.24.202.246:8765/mcp
  │
  ▼
TaxonAgent MCP Server  [5090, 172.24.202.246:8765]
  │  FastMCP, transport="streamable-http"
  │  Tool: ictv_family_info(family_name)
  │
  ▼
TaxonAgent 核心
  │  LangGraph 状态机
  │  ICTV 知识图谱 (.pkl, 38228 节点, 32 科)
  │  Ollama glm4 (localhost:11434)
  │
  ▼
返回 ICTV 分类规则、划界标准、属列表等
```

**传输协议**：MCP Streamable HTTP（2025-03-26 规范），非旧版 SSE。单一端点 `/mcp`，不再分离 `/sse` 和 `/messages`。

---

## 2. 服务器信息

| 角色 | 主机 | IP | 关键端口 |
|------|------|----|---------|
| BioAgent (cyclone) | cyclone | 172.24.195.66 | 8001 (adapter), 8080 (Open WebUI) |
| TaxonAgent (5090) | 5090 | 172.24.202.246 | 8765 (MCP server), 11434 (Ollama, 仅本机) |

网络：两台服务器内网互通（172.24.x.x 子网），端口 22 双向 OPEN，应用端口单向（cyclone → 5090:8765）。

---

## 3. 新增文件

### 3.1 `taxonagent_mcp.py`（集群：5090 | 项目：taxonagent）

**路径**：`/home/bgi-chao/taxonagent_mcp.py`

> 注：文件写在 `bgi-chao` 的 home 目录而非 taxonagent 项目目录（`/home/aakash/projects/taxonagent/`），原因是该目录属于 `aakash` 用户，`bgi-chao` 无写权限。

```python
#!/usr/bin/env python3
"""
TaxonAgent MCP Server — Streamable HTTP transport (MCP spec 2025-03-26)

Start:
    /home/bgi-chao/miniconda3/bin/python3.13 /home/bgi-chao/taxonagent_mcp.py
    # Endpoint: http://0.0.0.0:8765/mcp
"""

import sys
import os

TAXON_ROOT = "/home/aakash/projects/taxonagent"
sys.path.insert(0, os.path.join(TAXON_ROOT, "src"))
sys.path.insert(0, os.path.join(TAXON_ROOT, "venv/lib/python3.13/site-packages"))

os.environ.setdefault("OLLAMA_MODEL", "glm4")
os.environ.setdefault("OLLAMA_BASE_URL", "http://localhost:11434")

from mcp.server.fastmcp import FastMCP
from taxonagent.agent.classify_agent import run_query

# host/port 在构造函数里指定（不是 run() 的参数）
mcp = FastMCP("TaxonAgent", host="0.0.0.0", port=8765)


@mcp.tool()
def ictv_family_info(family_name: str) -> str:
    """
    Query ICTV classification rules and taxonomy info for a virus family.

    Returns genome type, genome size, marker genes, genera list, and ICTV
    demarcation criteria (quantitative thresholds + qualitative rules).

    Args:
        family_name: Virus family name ending in -viridae, e.g. "Coronaviridae",
                     "Flaviviridae", "Filoviridae", "Picornaviridae".
    Returns:
        Structured text with ICTV taxonomy and demarcation rules.
    """
    query = "What are the ICTV classification rules and demarcation criteria for {}?".format(family_name)
    return run_query(query)


if __name__ == "__main__":
    print("TaxonAgent MCP Server starting on http://0.0.0.0:8765/mcp")
    mcp.run(transport="streamable-http")
```

**启动命令**：
```bash
nohup /home/bgi-chao/miniconda3/bin/python3.13 /home/bgi-chao/taxonagent_mcp.py \
  > /home/bgi-chao/taxonagent_mcp.log 2>&1 &
```

**依赖说明**：
- Python 3.13 来自 `/home/bgi-chao/miniconda3/bin/python3.13`（系统无 3.13，用 miniconda）
- `mcp` 包安装到 miniconda：`/home/bgi-chao/miniconda3/bin/pip install "mcp[cli]"`（版本 1.28.1）
- taxonagent 核心包通过 `sys.path` 从 `/home/aakash/projects/taxonagent` 加载
- taxonagent 的 venv 中 python3.11 为断链（从 macOS 复制，指向 `/opt/homebrew`），但 python3.13 site-packages 仍可用

---

## 4. 修改的文件

### 4.1 `config.py`（集群：cyclone | 项目：bioagent）

**路径**：`/data/bioagent/agent/config.py`

**改动一**：在 `SYSTEM_PROMPT` 末尾（结束三引号前）追加 ICTV 调用指引：

```python
# 在 SYSTEM_PROMPT 字符串末尾、结束 """ 之前添加：

When the user asks about ICTV virus taxonomy, demarcation criteria, classification rules,
marker genes, genome type, or queries information about a specific virus family
(e.g. Coronaviridae, Flaviviridae, Filoviridae), use the ictv_family_info tool
from the taxonagent MCP server. Pass the virus family name (ending in -viridae)
as the family_name argument.
```

**改动二**：文件末尾添加常量：

```python
TAXONAGENT_MCP_URL = "http://172.24.202.246:8765/mcp"
```

### 4.2 `runtime.py`（集群：cyclone | 项目：bioagent）

**路径**：`/data/bioagent/agent/runtime.py`

**当前改动**：`SessionManager._build_options()` 仅注册只读 TaxonAgent 边界，不再加载 Claude Code preset 或文件工具：

```python
def _build_options(self, *, permission_mode: str, resume: str | None = None) -> ClaudeAgentOptions:
    return ClaudeAgentOptions(
        system_prompt=config.SYSTEM_PROMPT,
        tools=[],
        allowed_tools=["mcp__taxonagent__*"],
        mcp_servers={
            "taxonagent": {
                "type": "http",
                "url": config.TAXONAGENT_MCP_URL,
            },
        },
        permission_mode=permission_mode,
        setting_sources=[],
        ...
    )
```

TaxonAgent 返回值会先转换成 typed citation；缺少 `source_version` 或 `locator` 时只标记为
`incomplete`，统一知识库上线前不启用正式 citation enforcement。知识查询轮次不能同时形成 plan patch。

**设计说明**：
- `mcp_servers` 在 `ClaudeAgentOptions` 的构造函数中传入，而不是读取文件系统配置（`.mcp.json`），确保每个 session 都能正确加载
- `allowed_tools` 中的 `"mcp__taxonagent__*"` 是通配符，自动允许该 MCP server 下所有工具，无需逐一列出
- 工具命名规则：`mcp__<server_name>__<tool_name>`，此处 server_name 是 `taxonagent`，tool_name 是 `ictv_family_info`

---

## 5. 关键技术决策

### 5.1 传输方式选 Streamable HTTP 而非 SSE

旧的 HTTP+SSE（分离 `GET /sse` + `POST /messages`）在 MCP spec 2025-03-26 中已废弃。新项目直接用 Streamable HTTP：
- FastMCP：`mcp.run(transport="streamable-http")`（`"http"` 是别名）
- 客户端：`{"type": "http", "url": "..."}`
- 统一端点：`/mcp`

### 5.2 FastMCP API 细节

`host` 和 `port` 是 `FastMCP.__init__()` 的参数，不是 `run()` 的参数：

```python
# ✅ 正确
mcp = FastMCP("TaxonAgent", host="0.0.0.0", port=8765)
mcp.run(transport="streamable-http")

# ❌ 错误（会报 TypeError）
mcp.run(transport="http", host="0.0.0.0", port=8765)
```

### 5.3 Python 环境问题

5090 上 taxonagent 的 venv 是从 macOS 直接复制的，python3.11 符号链接指向 `/opt/homebrew/`（macOS Homebrew 路径），在 Linux 上断链。解决方案：
- 不重建 venv（无写权限，venv 属于 `aakash` 用户，当前用户是 `bgi-chao`）
- 使用 `bgi-chao` 自己的 miniconda Python 3.13（`/home/bgi-chao/miniconda3/bin/python3.13`）
- 通过 `sys.path.insert` 手动加载 venv 中的 site-packages（`venv/lib/python3.13/site-packages`，3.13 兼容）
- `mcp` 包单独安装到 miniconda 环境

### 5.4 MCP 握手流程

Streamable HTTP 需要完整的三步握手（tools/list 在 initialized 之后才可调用）：

```
1. POST /mcp  {"method": "initialize", ...}
   → 返回 session-id（在响应头 mcp-session-id 中）

2. POST /mcp  {"method": "notifications/initialized"}  + mcp-session-id header
   → 确认握手完成

3. POST /mcp  {"method": "tools/list" or "tools/call", ...}  + mcp-session-id header
   → 正常调用
```

跳过第 2 步直接调用 `tools/list` 会返回 HTTP 400。

---

## 6. 验证结果

### 6.1 分层验证

| 层 | 测试内容 | 结果 |
|---|---|---|
| 网络层 | cyclone → 5090:8765 TCP 连通 | ✅ OPEN (errno=0) |
| 协议层 | MCP initialize 握手 | ✅ HTTP 200, session-id 正常 |
| 工具层 | tools/list | ✅ 返回 ictv_family_info |
| 应用层 | tools/call ictv_family_info("Coronaviridae") | ✅ 返回完整 ICTV 数据，耗时 202s |
| 端到端 | BioAgent → ICTV 问题 → MCP 调用 → 返回 | ✅ 耗时 207s |

### 6.2 端到端测试结果

**测试输入**：`"What are the ICTV demarcation criteria for Flaviviridae?"`

**BioAgent 返回**（节选）：
```
## ICTV Demarcation Criteria for Flaviviridae

### Species Demarcation
- Quantitative: AA identity < 84.0% in RdRp (NS5)
- Phylogenetic: distinct monophyletic clades in RdRp phylogeny

### Genus Demarcation
- NS5/RdRp phylogeny + genome organization
- Bootstrap ≥ 70%, ML tree
```

---

## 7. 运维说明

### 启动 TaxonAgent MCP Server（5090）

```bash
# 启动
ssh 5090
nohup /home/bgi-chao/miniconda3/bin/python3.13 /home/bgi-chao/taxonagent_mcp.py \
  > /home/bgi-chao/taxonagent_mcp.log 2>&1 &

# 查看日志
tail -f /home/bgi-chao/taxonagent_mcp.log

# 检查是否在运行
ss -tlnp | grep 8765
ps aux | grep taxonagent_mcp

# 停止
pkill -f taxonagent_mcp.py
```

### 重启 BioAgent Adapter（cyclone）

```bash
ssh cyclone
pkill -f "uvicorn adapter:app"
cd /data/bioagent/agent
BIOAGENT_RUN_ROOT=/data/runs \
nohup /data/user/liufangzhou/tools/miniconda3/bin/conda run -n adapter \
  uvicorn adapter:app --host 127.0.0.1 --port 8001 \
  > /tmp/bioagent_adapter.log 2>&1 &
```

### 快速健康检查

```bash
# 检查 MCP server
python3 - << 'EOF'
import socket
s = socket.socket(); s.settimeout(3)
print("MCP server:", "UP" if s.connect_ex(("172.24.202.246", 8765)) == 0 else "DOWN")
s.close()
EOF

# 检查 BioAgent adapter
curl -s http://127.0.0.1:8001/v1/models | python3 -m json.tool
```

---

## 8. 已知限制与后续扩展

### 当前限制

| 限制 | 说明 |
|------|------|
| 响应慢 | 首次调用 200s+，因为 Ollama glm4 推理较慢，无缓存 |
| Ollama 单机 | 5090 Ollama 仅监听 127.0.0.1，只能本机访问 |
| 单工具 | 当前只暴露 `ictv_family_info` 一个工具 |
| 无持久化进程管理 | MCP server 用 nohup 启动，无 systemd 守护 |
| venv 权限问题 | taxonagent venv 属于 aakash，bgi-chao 无法重建 |

### 可扩展的工具（对应之前分析的 TaxonAgent 功能）

```python
@mcp.tool()
def ictv_list_families() -> str:
    """列出知识图谱中所有病毒科"""
    return run_query("List all families in the knowledge graph")

@mcp.tool()
def ictv_compare_families(family1: str, family2: str) -> str:
    """比较两个病毒科的分类标准差异"""
    return run_query("Compare {} and {}".format(family1, family2))

@mcp.tool()
def ictv_classify_text(query: str) -> str:
    """通用 ICTV 分类问答"""
    return run_query(query)

@mcp.tool()
def ictv_sequence_classify(fasta: str) -> str:
    """对 FASTA 序列做全自动 ICTV 分类（需序列分析流水线）"""
    return run_query(fasta)
```
