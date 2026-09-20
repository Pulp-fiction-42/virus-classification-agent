# BioAgent Runtime Architecture

## 请求链路

```text
Open WebUI
  | OpenAI chat request + optional X-OpenWebUI-Chat-Id
  v
adapter.py
  | protocol conversion
  v
SessionManager (runtime.py)
  | bounded ProviderRequest / strict action validation
  v
MiniMaxProvider (provider.py)
  | one SDK query, structured output, no business tools
  v
MiniMax Anthropic-compatible API (explicit opt-in)
```

## 模块职责

### `adapter.py`

- 暴露 `/v1/models`、`/v1/chat/completions`、确定性 plan compile 和 run start API。
- 读取 Open WebUI 的 chat/task header。
- 普通 chat 永远是咨询对话；规划和执行请求不进入模型链路。
- 将 SDK 文本增量转换为 OpenAI SSE，或生成非流式 completion。
- 不保存数据库会话，不访问 Open WebUI 存储。

### `runtime.py` 与 `provider.py`

- `runtime.py` 构造受控历史与确定性业务摘要，验证 `ConversationAction`，发送现有文本、trace 与 proposal 事件；规划和执行仍由确定性模块拥有。
- `provider.py` 接收四类结构化任务、schema 与预算，统一返回模型身份、耗时、结构化输出或错误；默认禁用云端，不持有 MCP、文件或计算工具。
- MiniMax 适配器固定使用 `claude-agent-sdk==0.2.103`（Claude Code 2.1.179）、中国大陆 Anthropic-compatible 端点及 `MiniMax-M3[1m]` SDK 模型映射；不向 Claude CLI 传递对外名称 `MiniMax-M3`。
- 有效 Open WebUI UUID 将 session／resume 映射及 transcript 保存在 `.claude/sessions/providers/minimax-m0`。缺失、`local:` 或其他非 UUID chat ID 使用宿主传入的有限历史开启单次 SDK session，不保存 resume 映射。累计上下文超过预算时持久会话也开启新 SDK session，旧 transcript 保留作审计。
- SDK 限制单轮生成；若它在提交 `StructuredOutput` 后报告轮次上限，只接受完整且通过 schema 校验的该载荷，不追加确认或修复调用。
- 模型不能启动计算、改写证据或批准资料。普通聊天暂不加载 M0 待审核知识包，项目授权检索留到 B2。
- `evidence_replay.py`、`knowledge.py`、`evidence_contract.py` 提供供应方无关的候选、资产、规则与引用模块。根仓库 CLI 连接这些模块与后端纯报告函数，不读取业务数据库。

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
agent/.claude/sessions/providers/minimax-m0/sdk/projects/<project>/<sdk_session_id>.jsonl
```

Open WebUI 继续独立保存 UI 消息和账号数据。MiniMax provider 只为有效 chat UUID 保存到内部 SDK session ID 的映射；临时 chat ID 不持久化映射。provider 不读取或修改 Open WebUI 数据库，公共结果不包含 SDK session ID。

资源校准记录保存在 `.claude/sessions/bioagent/resource-history.sqlite3`。agent 启动时按 telemetry mtime 增量补漏，规划时只查询索引；输入统计按文件身份缓存且不保存原始路径。

## 并发与进程

当前部署使用单个 uvicorn worker。同一聊天通过 provider 内的锁串行生成；不同进程的锁不共享，暂不支持多个 worker 同时写入相同的会话映射。
