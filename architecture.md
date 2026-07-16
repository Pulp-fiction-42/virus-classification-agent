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
  | project Skills + all Claude Code tools
  v
MiniMax Anthropic-compatible API
```

## 模块职责

### `adapter.py`

- 暴露 `/v1/models` 和 `/v1/chat/completions`。
- 读取 Open WebUI 的 chat/task header。
- 在普通 chat 流程前拦截 `/plan` 和 `/execute`。
- 将 SDK 文本增量转换为 OpenAI SSE，或生成非流式 completion。
- 不保存数据库会话，不访问 Open WebUI 存储。

### `runtime.py`

- `SessionManager` 通过 chat-map 保存 Open WebUI chat ID 对应的 Claude session ID。
- 为每个 chat ID 创建并复用一个常驻 `ClaudeSDKClient`。
- 用每个 session 自己的 `asyncio.Lock` 串行同一聊天的请求。
- 新 session 以 `bypassPermissions` 能力启动 SDK subprocess，随后立即调用 `set_permission_mode("dontAsk")`；方案阶段只预先允许只读的 `Read`、`Glob`、`Grep`。
- `/plan` 调用 `set_permission_mode("dontAsk")`，并注入 BioAgent `analysis_plan` 协议以避开 Claude Code Plan/ExitPlanMode；`/execute` 调用 `set_permission_mode("bypassPermissions")`，并额外拼接执行提示和 runtime 分配的 run reservation，要求按既有方案执行、保持 Markdown 输出、更新 `.bioagent/state.json` 和 `.bioagent/execution.log`。
- 成功的 execute 回合会拦截末尾 `diagnostic_report` HTML fragment，验证语义标签后由 runtime 统一补全样式和文档结构，写入 run 目录并发送持久化报告 output item。
- 首次建立 SDK context 时兼容性引导 Open WebUI 已有历史；已有 transcript 或已引导后只发送最新用户消息。
- 构建统一的 `ClaudeAgentOptions`，加载项目 Skills 和 Claude Code preset tools。
- 校验 transcript、SDK 执行结果和返回的 session ID。

`SessionManager` 持有内存 client registry 和文件式 chat-map，但不访问 `webui.db`。

### `config.py`

模型、API 地址、API 密钥、项目路径、额外目录和系统提示词集中定义在该文件中。运行目录和 SDK 临时目录允许通过环境变量覆盖。

### `.claude/skills/`

Skill 是 SDK 从项目文件系统发现的运行规则。Snakemake gatekeeper Skill 负责 workflow1-workflow6 的保守匹配、plan 模式 `analysis_plan` 内部控制块、一次用户确认后的直接执行、诊断意见书内容边界以及 Bash 执行边界。

## 持久化

Claude SDK 将 transcript 写入：

```text
/data/bioagent/agent/.claude/sessions/projects/-data-bioagent-agent/<claude_session_id>.jsonl
```

Open WebUI 继续独立保存 UI 消息和账号数据。runtime 只保存 chat UUID 到 Claude session ID 的映射，不读取或修改 Open WebUI 数据库。

## 并发与进程

当前部署使用单个 uvicorn worker。多个 worker 会各自持有不同的内存 client registry，不适合作为第一版部署方式。同一聊天内部已通过 session lock 串行写入 SDK stream。
