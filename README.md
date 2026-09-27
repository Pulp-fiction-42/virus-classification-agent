# BioAgent Runtime

BioAgent 为 Open WebUI 提供 OpenAI-compatible adapter、可替换的结构化生成 provider 和固定 Workflow 1–6 的 deterministic planner。macOS 模式下，编排在本机运行，所有数据访问和 Snakemake 执行通过 SSH worker 留在 Cyclone。

## 主要接口

- `/v1/models`、`/v1/chat/completions`：OpenAI-compatible 对话。
- `/v1/analysis/plans/compile`：确定性编译分析方案。
- `/v1/analysis/runs/start`：验证 dataset revision 后排队远端 run。
- `/v1/analysis/runs/resume`：仅对具备冻结恢复契约的 `interrupted` run 重新校验并追加 attempt；旧 run 或依赖身份不完整时拒绝原地恢复。
- `/v1/analysis/files`：列举 Cyclone data root 的 logical 文件树。
- `/v1/analysis/datasets/resolve|preflight|fingerprints`：symlink-aware 解析、FASTQ quick/full check 与二次指纹验证。
- `/v1/analysis/runs/snapshot`：state、队列/tmux、progress、telemetry、trace 与报告资料；传 `report_kind=preliminary|final` 选择对应 telemetry。
- `/v1/analysis/runs/delivery-report?format=html|pdf`：校验 run-local 登记、大小与 SHA-256 后传输标准交付报告。
- `/v1/analysis/resources`：Cyclone CPU、内存、run-root storage 和 active/queued 数量。
- `/v1/analysis/evidence/verify`：在 Cyclone 对报告数值做原始产物复算。

## Cyclone transport

设置 `BIOAGENT_SSH_HOST=Cyclone` 后使用 `RemoteDeterministicRuntime`。`remote_client.py` 负责：

- 强制 `ClearAllForwardings=yes` 并复用 SSH control socket。
- 对最小 worker 文件集计算 SHA-256，原子发布到 `BIOAGENT_REMOTE_WORKER_ROOT/releases/<sha256>`。
- 使用指定的远端 Python；worker 和 RPC 仅依赖标准库，runner 依赖随发布同步的本项目模块。

`remote_worker.py` 负责 data allowlist、双层指纹、FASTQ 检查、资源采集、单一持久 scheduler 和 reconciliation。scheduler 以 `fcntl.flock` 保证最终互斥，并在独立 `supervisor.json` 写 heartbeat、PID、boot ID 与 worker version，避免与 runner 的 state 原子写互相覆盖。

新 run 在 state v3 中冻结 recovery contract v2，并把每次小型控制日志、telemetry 与 checkpoint 登记在 `.bioagent/attempts/<序号>/`；大型计算结果继续共用 run root。恢复只跳过 checkpoint 已验证的完整 phase，中断 phase 使用 Snakemake `--rerun-incomplete` 并强制重跑受控 rules。新建的 Workflow 1 run 会按所选阶段冻结 workflow 源码闭包、软件及 rRNA 索引身份；旧恢复契约与 Workflow 2–6 会明确返回不可恢复，不会原地补写身份。当前只完成全阶段 dry-run，尚未完成真实 Workflow 1 中断恢复验收。

## 配置

完整的新机器部署、Cyclone 路径和 `.env` 配置见根目录 [Developer Guide](../README.md#developer-guide)。本机 `agent/.env` 被 Git 忽略，tracked `agent/.env.example` 是唯一配置模板。

RPC 控制进程、tmux supervisor 与 analysis runner 使用同一套 home Pixi Python；Snakemake 与 SeqKit 使用 Pixi 的全局入口。这样可避开高负载 `/data` 上大量小文件读取，也无需维护第二套 adapter。缺少这些路径时 toolchain 会拒绝新任务，不会回退旧环境；新增命令行工具时使用 `pixi global install -c bioconda <tool>`。

标准交付报告使用独立的 `BIOAGENT_REPORT_PYTHON`、`BIOAGENT_RSCRIPT` 与 `BIOAGENT_WEASYPRINT`；正常 run 完成后自动生成，也可用 `standard_report.py --state <run>/.bioagent/state.json` 单独重建。

Workflow 1 在 run root 建立副本时会把 run-local `config.yaml` 的 `software.seqkit` 改为上述 Pixi 入口；Cyclone 上的 Workflow 源码保持不变。

远端 fingerprint 验证暂时不可用与指纹真实变化是不同状态：前者可重试且不会把 dataset 标记 stale，后者才会阻止规划或执行。

不要把 `MINIMAX_API_KEY` 写入 `.env`；`start.sh` 由 login zsh 启动并从现有 shell 环境继承。依赖通过 `requirements.txt` 安装，其中固定 `claude-agent-sdk==0.2.103`（Claude Code 2.1.179）。MiniMax 使用中国大陆 `https://api.minimax.cn/anthropic` 端点，SDK 内部默认模型映射为 `MiniMax-M3[1m]`，对外 provider 契约仍回报 `MiniMax-M3`。

`BIOAGENT_MODEL_POLICY` 默认 `local_only`，目前尚未安装本地后端，模型入口会明确不可用。当前开发实例获得授权后使用 `minimax_explicit`；缺少密钥不阻止确定性服务启动。

## M0 开发回放

- `provider.py` 定义四类任务的统一请求／结果；MiniMax SDK 的会话与审计文件使用独立 `minimax-m0` 命名空间，旧会话文件保留。有效 Open WebUI UUID 使用既有 SDK session/resume；缺失、`local:` 或其他非 UUID chat ID 以宿主传入的有限历史执行单次临时 SDK session。宿主管理受控历史、动作校验和执行权限，provider 不持有业务工具。
- `evidence_replay.py` 将真实初筛统计复算为候选证据；数值占位符、引用身份和规则发现由宿主校验。源码中既有 `reads` 字段对应的配对分类计数，在新证据中显式记为 `read_pairs`。
- `assets/m0` 是约 16 KiB 的开发资料与规则包。`knowledge.py` 验证哈希后构建内存 SQLite 关系表和 FTS5 索引；索引可从冻结 JSON 重建。所有片段仍待审核，默认检索不返回它们。
- 根仓库 `m0-replay.py` 用 `--include-unreviewed` 开启开发草稿，用 `--allow-cloud` 显式授权本次 MiniMax 调用。这些开关不启用多人知识服务或正式报告发布。

完整复现与限制见 [M0 实施记录](../docs/m0-implementation-record.md)。本次没有本地模型生成、专家审核或生物学性能验收。

## M2 工程研判检查

`candidate_assessment.py` 只读解析已完成的 Workflow 2 run，并用 `assets/m2` 中逐档校验的 capability／工程规则 release 生成聚合检查：

```bash
python candidate_assessment.py --state <run>/.bioagent/state.json --summary
```

命令不接受输出路径，也不写回 run。当前没有核准的领域规则、知识或正式 reference release，因此每个候选都保持 `Assessment(status="not_generated")`；输出只有来源哈希与候选、观测、finding、assessment 聚合计数，不包含候选 ID 或序列。NT／NR 的来源 checksum 清单只能登记为观察，不能证明已安装数据库的完整内容身份，也不会提升 Workflow 2–6 的恢复资格。

## 测试

```bash
PYTHONPATH=/Users/lfz/miniconda3/envs/open-webui/lib/python3.11/site-packages \
  /Users/lfz/miniconda3/envs/adapter/bin/python -m pytest -q
```

原始 dataset 始终只读；测试产物只能写入远端 run root。
