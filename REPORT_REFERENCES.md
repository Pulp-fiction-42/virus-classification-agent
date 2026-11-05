# 分析报告的引用与运行溯源

HTML 报告末尾包含工具和软件库、Skill 调用记录、参考文献。HTML 已有 A4 打印样式，可由浏览器打印为 PDF。报告正文的方法表使用 `[n]` 链接到对应参考文献；DOI 链接由程序生成。

## 记录什么

- 工具：从当前 run 的真实执行日志和已解析产物提取。fastp 版本来自其 JSON，而不是当前机器的安装版本。Snakemake 由完成的执行阶段和任务日志确认。SeqKit 等同一规则内部使用的工具也可记录。
- 软件库：新执行在工作流准备完成后记录实际使用的 Python 和 PyYAML 版本，附官方文档链接。这不是完整的递归依赖清单；未记录的嵌套脚本依赖不会被补造。
- Skill：新版 Qwen 语义规划接口和确定性执行器没有调用 Skill，明确写“未调用”。保留的旧版 SDK 路径只根据实际 `Skill` tool-use/result 消息记录名字、调用 ID、加载状态以及可解析的本地源码 SHA256 和版本。工具加载成功不代表 Skill 的全部步骤完成。未接入审计的历史运行显示“无法追溯”。
- 文献：使用离线维护、经 Crossref 核验的 `citation_registry.json`，只为本次有使用证据的方法生成引用。模型不能自行提供作者、DOI 或版本。

日志中的 shell 声明与 OS 进程追踪不同，报告明确区分“产物确认”“执行器确认”和“日志记录命令”。条件分支、容器内部、嵌套脚本、尚未登记的工具可能不在清单内；不能把它当作完整的软件物料清单。工具名出现于用户文字、方案卡片、安装目录或 dry-run 文件不构成使用证据。

## 数据流与文件

`pipeline_runner` 在实际运行时记录库版本；`report_provenance` 从执行证据和经过核验的引用目录生成清单；`runtime._save_diagnostic_report` 先按原有严格规则校验报告正文，再添加由程序转义的引用附录。模型正文依然不能注入链接、脚本或任意 HTML 属性。

每个成功运行生成：

| 文件 | 内容 |
| --- | --- |
| `diagnostic-opinion.html` | 含引用的完整报告，可打印 PDF |
| `analysis-provenance.json` | 工具、版本、使用证据、Skill、文献和来源摘要 |
| `references.bib` | 本次报告引用的 BibTeX |
| `.bioagent/report-provenance.json` | 保存的运行引用快照 |
| `.bioagent/report-evidence.json` | 结果指标及其文件来源 |
| `.bioagent/skill-usage.json` | 仅 SDK 执行路径产生的工具调用审计 |

前三项可从当前会话对应 run 的结果目录下载，沿用既有会话归属检查。报告恢复不重新运行分析，且复用既有文献快照；来源文件摘要发生变化时阻止沿用旧快照。历史报告恢复不会查询当前环境来伪造过去的版本。

## 扩展工具、文献和 Skill

1. 为新工具登记真实可执行文件名和引用 ID，不能仅把规则名当作可执行文件名。增加相应命令或结果解析器及验证用例。当前目录预置 16 个工具的方法论文，未使用的不会出现在报告里。
2. 新文献通过 DOI 从 Crossref 等权威来源提取元数据，核验标题与工具是否对应，保存作者、年份、期刊、卷、页码或文章编号、DOI、核验 URL 和时间。不能编造缺失字段。
3. 如后续增加检索文献、解释结果的 Agent 功能，后端需记录 `state.provenance_capture.reference_uses`，每项为 `{reference_id, context, evidence}`；`reference_id` 必须匹配经过核验的目录。`context` 是实际使用该文献的位置或用途，`evidence` 是检索/阅读记录的相对位置。未知 ID 只标为未登记，不生成虚构参考文献。当前版本不自动把模型自由文本中的引用当成可靠检索记录。
4. 本地 Skill 可在 frontmatter 的 `metadata` 中提供 `version` 和 `citation_ids` 列表；仅实际出现 Skill 调用后，后端才会读取并关联已核验文献。仅在磁盘上放置 `SKILL.md` 不产生调用记录。命名空间或外部 Skill 的源码无法定位时保留名字与工具消息，摘要显示未记录。

## 验证

运行 `python -m unittest discover -s tests`。覆盖引用去重、未使用方法排除、同规则内部工具、历史信息缺失、实际 Skill 请求/加载/失败状态、会话运行隔离、HTML 转义、文献快照复用和来源变更拦截。文献导出另外使用 citation-management 的校验脚本核验。

本功能开发使用了 [citation-management](https://github.com/K-Dense-AI/scientific-agent-skills) 的 DOI 元数据提取与核验流程。其方法库引用（开发归属，不代表用户的每次生信分析调用了该 Skill）：

Kassis, Timothy; Agarwal, Vinayak; He, Yuhuan; Patel, Darshil; Brueckner, Aubrey M. (2026). *Scientific Agent Skills: A Library of Procedural Knowledge for Research Agents*. arXiv:2609.00065. https://doi.org/10.48550/arXiv.2609.00065 。2026-11-04 根据 https://arxiv.org/abs/2609.00065 核验，页面当前为 v2，未列出版期刊 DOI。
