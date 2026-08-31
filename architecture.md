# BioAgent Runtime Architecture

## 请求链路

```text
Open WebUI
  | OpenAI chat request + X-OpenWebUI-Chat-Id
  v
adapter.py
  | protocol conversion
  v
SessionManager (runtime.py)
  | ClaudeAgentOptions(resume=<claude_session_id>)
  v
Claude Agent SDK client.query(prompt)
  | only TaxonAgent MCP; structured conversation_action output
  v
MiniMax Anthropic-compatible API
```

## 模块职责

### `adapter.py`

- 暴露 `/v1/models`、`/v1/chat/completions`、确定性 plan compile 和 run start API。
- 读取 Open WebUI 的 chat/task header。
- 普通 chat 永远是咨询对话；规划和执行请求不进入模型链路。
- 将 SDK 文本增量转换为 OpenAI SSE，或生成非流式 completion。
- 不保存数据库会话，不访问 Open WebUI 存储。

### `runtime.py`

- `SessionManager` 通过 chat-map 保存 Open WebUI chat ID 对应的 Claude session ID。
- 为每个 chat ID 创建并复用一个常驻 `ClaudeSDKClient`。
- 用每个 session 自己的 `asyncio.Lock` 串行同一聊天的请求。
- 新 session 固定使用 `dontAsk`，不加载 Claude Code preset、project Skills、文件工具或额外目录，只保留 TaxonAgent MCP。
- 对话输出经过 `ConversationAction` schema 验证；模型只能回答、追问或提出 goals/viral_type patch，不能直接修改方案或启动分析。
- `DeterministicRuntime` 独立拥有资源估算与 runner admission；`DeterministicPlanner` 复用 catalog/compiler，执行前重编并把 backend plan reference 写入 state。
- 成功 run 由固定 runner 写入版本化交付指标与报告可用状态；runtime 不发送报告 output item，Open WebUI 从 run 状态提供下载入口，后端按需确定性渲染分析报告，不接收模型生成的 HTML。
- 方案中的资源数字不由模型生成。runtime 根据已验证的 execution spec、输入序列规模与 SQLite 历史索引生成 P50/P90、峰值内存、安全余量和置信度。
- 首次建立 SDK context 时兼容性引导 Open WebUI 已有历史；已有 transcript 或已引导后只发送最新用户消息。
- SDK 不设置 thinking budget；执行稽核以 Open WebUI 保存的 deterministic decision events 为准。
- 校验 transcript、SDK 执行结果和返回的 session ID。

`SessionManager` 持有内存 client registry 和文件式 chat-map，但不访问 `webui.db`。

### `config.py`

模型、API 地址、API 密钥、项目路径和系统提示词集中定义在该文件中。运行目录和 SDK 临时目录允许通过环境变量覆盖。

### `workflow_adapters.py`

- 唯一外部 adapter 是 `analysis_pipeline` v2；旧 adapter 不再接受。
- `workflow_catalog.py` 定义 artifact 文件角色、记录粒度和公开 stage DAG，`pipeline_spec.py` 从输入与交付物编译最小依赖闭包。
- workflow 1–6 各自最多生成一个 run-local phase；同一 source workflow 的 targets 合并，跨 workflow 按拓扑顺序复用上游产物。
- 中间入口只物化完整 typed artifact bundle；占位仅用于 Snakemake 必需但无生物学意义的伴随文件。

### `analysis_runner.py`

- 固定 runner 只接受 state 路径，按编译后的 phase 顺序执行 Snakemake；任一 phase 失败或取消后立即停止。
- state v3 与 telemetry v3 汇总 phase、job、资源、命令、manifest，以及上游、病毒识别、宿主、微生物谱、定量和注释指标；交付报告 schema v4 另外保存确定性 `conclusions` 与 `evidence`。
- 每条结果结论至少引用一个数值证据。证据记录报告字段、精确值、单位和 run-local 来源定位；后端下载报告时重新读取来源并计算允许的聚合公式，任何缺失、越界或不一致都拒绝出具报告。
- runner 每秒采样 Snakemake process tree RSS、整机内存和并行 analysis runner 数，并在终态更新资源历史索引；失败或取消记录只供稽核，不进入分位数。
- workflow 1–6 的 run-local 副本增加 benchmark；workflow2 另增加机器可读分类 CSV。源 workflow 不被修改。

## 持久化

Claude SDK 将 transcript 写入：

```text
/data/bioagent/agent/.claude/sessions/projects/-data-bioagent-agent/<claude_session_id>.jsonl
```

Open WebUI 继续独立保存 UI 消息和账号数据。runtime 保存 chat UUID 到 Claude session ID 的映射及版本化 Agent 审计 JSONL，不读取或修改 Open WebUI 数据库。

资源校准记录保存在 `.claude/sessions/bioagent/resource-history.sqlite3`。agent 启动时按 telemetry mtime 增量补漏，规划时只查询索引；输入统计按文件身份缓存且不保存原始路径。

## 并发与进程

当前部署使用单个 uvicorn worker。多个 worker 会各自持有不同的内存 client registry，不适合作为第一版部署方式。同一聊天内部已通过 session lock 串行写入 SDK stream。
