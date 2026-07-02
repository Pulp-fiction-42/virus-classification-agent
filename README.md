# BioAgent Runtime

BioAgent 是 Open WebUI 与 Claude Agent SDK 之间的 OpenAI-compatible runtime，面向病毒分类和固定 Snakemake 生物信息学工作流。

## 本次改进

- 引入 `/plan` 和 `/execute` 模式切换，并将 Open WebUI `chat_id` 直接映射到持久 Claude SDK session。
- `plan` mode 默认只保留 permission mode，不额外注入 runtime prompt；`execute` mode 在用户请求前追加 `遵循之前计划模式下生成的计划执行`。
- 固定 Snakemake workflow gatekeeper 改为一次确认后直接执行，取消 dry-run 和第二次确认。

## 结构

```text
agent_sdk/
├── adapter.py       # HTTP/OpenAI SSE 协议适配
├── runtime.py       # SDK 配置、会话恢复和结果流
├── config.py        # 模型、密钥、路径等静态配置
├── .claude/
│   ├── skills/      # 项目级 Agent Skills
│   └── sessions/    # SDK JSONL transcript，不提交 Git
├── tests/
└── tools/watch.py   # 可选的消息监控脚本
```

## 会话模型

Open WebUI 向 adapter 转发 `X-OpenWebUI-Chat-Id`。该 ID 直接作为 Claude SDK 的 session ID，不维护额外数据库映射：

1. adapter 为每个 `chat_id` 保留一个常驻 `ClaudeSDKClient`。
2. 同一 `chat_id` 的请求复用同一个 client，并通过会话锁串行写入 SDK stream。
3. 新建 client 时以 `bypassPermissions` 能力启动 SDK subprocess，随后立即调用 `set_permission_mode("plan")`，默认活动模式仍是 `plan`。
4. 每轮用 `client.query(prompt, session_id=chat_id)` 绑定 SDK 会话。
5. transcript 存在时只发送最新用户消息；不存在时用 Open WebUI 请求历史引导一次旧聊天。
6. SDK transcript 保存在 `.claude/sessions/projects/<project>/<chat_id>.jsonl`。
7. adapter 重启后内存 client 丢失，下一轮会重新创建 client，并用相同 `chat_id` 继续 SDK session。

空闲 client 会在超时后断开；FastAPI shutdown 时会断开所有 client。当前部署仍假设单个 uvicorn worker。

## Agent 能力

- 会话开始时通过 `setting_sources=["project"]`、`skills="all"` 加载 `.claude/skills/`。
- 默认 `/plan`：活动 SDK permission mode 为 `plan`，不额外注入 runtime prompt。
- `/execute`：调用 `set_permission_mode("bypassPermissions")`，并在用户消息前额外注入 `遵循之前计划模式下生成的计划执行`。
- `/plan` 和 `/execute` 命令由 adapter 拦截，不发送给 Claude，不污染对话上下文。
- 固定 Snakemake 工作流的识别和一次确认后直接执行规则位于 `bioinformatics-workflow-gatekeeper` Skill。

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
Runs:        /data/runs
```

服务由用户级 systemd 管理：

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

`config.py` 是唯一配置源，包括 API 密钥。SDK subprocess 所需变量由 runtime 从这些 Python 常量构造，不从 shell 环境读取配置。
