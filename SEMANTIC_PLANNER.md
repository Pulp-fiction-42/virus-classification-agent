# Qwen 多意图规划入口

正式 `/analysis` 会话默认通过 Qwen 解析自然语言。Python 校验候选，确定性执行器只执行通过校验并由用户确认的方案。没有微调；Skill 不是当前入口的必要条件。

## 请求到执行

1. 后端读取当前方案、版本、已选文件和待解决问题。
2. **发起模型请求前先持久化不可执行状态**，避免超时或进程退出后误执行旧卡片。
3. `semantic_planner.py` 使用 Qwen 的 OpenAI 兼容接口与JSON模式解析完整请求，返回多任务候选。对尚无澄清问题的候选，再用独立的简短语义复核检查遗漏、歧义、否定条件；复核可以补充执行、解释、报告这三种附加意图，不能改写流程或参数。模型不获得工具或 shell 权限。
4. `plan_proposals.py` 校验格式、版本、步骤 ID、参数类型与范围、文件来源及依赖确认，然后原子合并候选。
5. `intent_router.finalize_plan` 计算技术依赖、检查用户授权与文件可访问性，生成最终方案。
6. 缺依赖或存在歧义时返回问题。用户回答后，将未解决请求与当前状态一起交给模型。
7. 用户通过执行入口确认后，原有 `pipeline_runner.py` 执行规范化方案，并核对实际调度规则与产物。

## 多种意图

支持同时标注 `intent`、`param_suggest`、`provide_inputs`、`dependency_response`、`execute`、`report`、`explain`、`conversation`。其中流程选择和参数修改是主要能力，其他任务用于配合对话与已有执行入口。

例如“只做质控，read至少80bp，关闭去重，解释影响”：

```json
{
  "version": 1,
  "base_plan_revision": 3,
  "tasks": ["intent", "param_suggest", "explain"],
  "scope_change": {"mode": "replace", "goals": ["read_qc"]},
  "parameter_patch": {"fastp.length_required": 80, "fastp.dedup": false},
  "questions": [],
  "message": "提高长度阈值可能减少短读段的保留；关闭去重会保留重复读段。"
}
```

这只是模型建议。后端决定是否接纳、如何合并及是否可执行。参数未提及的字段保持原值；单独改参数不会扩大范围。W1–W6 是分组，可以选择组内步骤或终点。只提及分组而未说明范围时应澄清；跨组缺失依赖必须确认，不能自动补跑。

目前开放7个参数：fastp 的最短read长度、合格碱基质量、平均质量、N碱基数量和去重开关，以及MEGAHIT最短contig长度、contig过滤最短长度。具体范围来自 `parameter_rules.RULES`，不由模型编造。

## 失败处理

| 情况 | 行为 |
| --- | --- |
| 语义含糊、互相矛盾、定性阈值无维护预设 | 追问，整轮修改不应用 |
| JSON结构错误、截断、task与字段不一致 | 最多修复一次，共用总超时 |
| 模型超时、服务不可用、格式修复仍失败或复核格式非法 | 保存阻断状态，反馈重试，不回退正则理解 |
| 非法参数、不支持的参数或步骤、陈旧版本 | 后端拒绝整轮修改，保留原范围和参数 |
| 缺少输入或未授权的依赖 | 保存待确认方案，列出缺失步骤，等待用户或中间产物 |
| 同轮修改并执行 | 先展示新方案，使用执行入口确认后再启动 |
| 修改后查看旧报告 | 使用最近成功运行的结果，明确区分旧结果与未执行的新方案 |

`accepted`表示候选被接纳为草稿，`ready_to_execute`表示依赖、文件和授权校验均通过，两者含义不同。模型错误识别但输出合法 JSON 的问题仍需语义验收发现，后端不保证理解任意自然语言。模型或系统故障时，不允许继续执行旧方案。

## 配置和追踪

| 环境变量 | 默认值 |
| --- | --- |
| `BIOAGENT_PLANNER_BASE_URL` | `ANTHROPIC_BASE_URL` 对应的服务器 |
| `BIOAGENT_PLANNER_MODEL` | `BIOAGENT_MODEL`，当前 `qwen3.8-27b` |
| `BIOAGENT_PLANNER_TIMEOUT_SECONDS` | 75秒，含最多一次格式修复 |

调用关闭thinking，temperature=0，使用JSON输出模式。通常两次模型调用：候选解析及语义复核；主解析格式异常时最多额外修复一次。已明确需要澄清的候选直接反馈；后端算出缺失依赖时先展示权威依赖列表，补齐依赖后再复核语义，避免模型重写技术依赖问题。所有调用共用75秒总预算。没有新增Python依赖。每轮候选、接纳结果、问题、模型名、尝试次数、复核结果、耗时写入会话配置目录下 `bioagent/planning-audit/<chat-id>.jsonl`。令牌和服务错误响应体不会写入对话或该审计日志。

## 验证与后续维护

```sh
python -m unittest discover -s tests -v
```

`tests/test_semantic_planner.py` 用注入的模型替身验证协议与兜底，不代表真实Qwen语义能力。真实Qwen验收脚本与记录在项目 `test-results/semantic-planner-20261104/`，需求均标为人工编写，开发中失败的迭代也保留记录。结果不能作为专家金标准或泛化准确率。

新增参数应先维护 `parameter_rules.RULES`、参数含义和执行映射；新增阶段应先维护流程目录、产物契约和执行适配器，再补语义样例。不要为每种中文说法增加正则。

旧 `resolve_intent`、未提供proposal的旧CLI和旧Skill仍保留用于历史兼容。历史测试显式使用 `planning_backend="legacy"`；正式持久会话默认 `model`。元数据等非持久后台任务仍使用原SDK通道。
