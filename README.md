# BioAgent Runtime

BioAgent 是 Open WebUI 与 MiniMax/Claude Agent SDK 之间的 OpenAI-compatible adapter，并拥有固定 Workflow 1–6 的 deterministic planner。macOS 模式下，对话 Agent 在本机运行，所有数据访问和 Snakemake 执行通过 SSH worker 留在 Cyclone。

## 主要接口

- `/v1/models`、`/v1/chat/completions`：OpenAI-compatible 对话。
- `/v1/analysis/plans/compile`：确定性编译分析方案。
- `/v1/analysis/runs/start`：验证 dataset revision 后排队远端 run。
- `/v1/analysis/files`：列举 Cyclone data root 的 logical 文件树。
- `/v1/analysis/datasets/resolve|preflight|fingerprints`：symlink-aware 解析、FASTQ quick/full check 与二次指纹验证。
- `/v1/analysis/runs/snapshot`：state、队列/tmux、progress、telemetry、trace 与报告资料。
- `/v1/analysis/resources`：Cyclone CPU、内存、run-root storage 和 active/queued 数量。
- `/v1/analysis/evidence/verify`：在 Cyclone 对报告数值做原始产物复算。

## Cyclone transport

设置 `BIOAGENT_SSH_HOST=Cyclone` 后使用 `RemoteDeterministicRuntime`。`remote_client.py` 负责：

- 强制 `ClearAllForwardings=yes` 并复用 SSH control socket。
- 对最小 worker 文件集计算 SHA-256，原子发布到 `BIOAGENT_REMOTE_WORKER_ROOT/releases/<sha256>`。
- 使用指定的远端 Python；worker 和 RPC 仅依赖标准库，runner 依赖随发布同步的本项目模块。

`remote_worker.py` 负责 data allowlist、双层指纹、FASTQ 检查、资源采集、tmux session 和 reconciliation。每个远端 supervisor 以 `fcntl.flock` 等待全局分析锁，并在独立 `supervisor.json` 写 heartbeat、PID、boot ID 与 worker version，避免与 runner 的 state 原子写互相覆盖。

## 配置

完整的新机器部署、Cyclone 路径和 `.env` 配置见根目录 [Developer Guide](../README.md#developer-guide)。本机 `agent/.env` 被 Git 忽略，tracked `agent/.env.example` 是唯一配置模板。

RPC 控制进程、tmux supervisor 与 analysis runner 使用同一套 home Pixi Python；Snakemake 与 SeqKit 使用 Pixi 的全局入口。这样可避开高负载 `/data` 上大量小文件读取，也无需维护第二套 adapter。缺少这些路径时 toolchain 会拒绝新任务，不会回退旧环境；新增命令行工具时使用 `pixi global install -c bioconda <tool>`。

Workflow 1 在 run root 建立副本时会把 run-local `config.yaml` 的 `software.seqkit` 改为上述 Pixi 入口；Cyclone 上的 Workflow 源码保持不变。

远端 fingerprint 验证暂时不可用与指纹真实变化是不同状态：前者可重试且不会把 dataset 标记 stale，后者才会阻止规划或执行。

不要把 `MINIMAX_API_KEY` 写入 `.env`；`start.sh` 由 login zsh 启动并从现有 shell 环境继承。

## 测试

```bash
PYTHONPATH=/Users/lfz/miniconda3/envs/open-webui/lib/python3.11/site-packages \
  /Users/lfz/miniconda3/envs/adapter/bin/python -m pytest -q
```

原始 dataset 始终只读；测试产物只能写入远端 run root。
