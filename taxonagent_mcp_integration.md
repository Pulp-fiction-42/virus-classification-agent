# TaxonAgent MCP 接入

当前 BioAgent 由宿主调用 TaxonAgent，再将有界检索结果交给结构化回答模型。
模型不持有 MCP、文件或执行工具权限。历史主仓库 commit
`b3bbb5224083ba2b82a0fc393e68bd028b2f2bac` 用于追溯早期接入，不能照搬其中的配置或工具名。

## 5090 启动

依赖现有 `taxonagent` Conda 环境与 `~/projects/taxonagent/mcp/taxonagent_mcp.py`。
密钥从交互式 Bash 的 `MINIMAX_API_KEY` 继承；不写入 `.env`、命令参数或日志。

在忽略提交的 `agent/.env` 设置：

```dotenv
TAXONAGENT_MCP_URL=http://127.0.0.1:8765/mcp
BIOAGENT_MANAGE_TAXONAGENT=1
```

从主仓库执行 `./dev-services.sh start`。脚本先在 `bioagent-taxonagent` tmux session
中运行 `proxy_off`，再使用 `LLM_BACKEND=minimax MINIMAX_MODEL=MiniMax-M3` 启动服务。
日志为 `/tmp/bioagent-taxonagent.log`。`stop` 只停止自己的服务会话，不影响 Cyclone worker。
只连接外部服务时设置 URL，但保持 `BIOAGENT_MANAGE_TAXONAGENT=0`。

## 对话边界

- 使用 MCP Streamable HTTP 的 `initialize` 和 `taxonagent_query(question)`。
- 只在 `/analysis` 的 ICTV/分类知识意图中检索；不调用 `taxonagent_classify_sequence`。
- 查询最长 300 秒，正文限制为 16,000 字符；WebUI 请求超时设为 600 秒。
- 连接不使用 shell proxy；健康检查仅握手和列工具，最多 2 秒，结果缓存 30 秒。
- 失败、工具错误、空答案及超时均返回明确错误，不调用模型补造答案。
- 回答只能引用本轮检索结果，也可请求澄清；不得修改分析方案。
- `analysis_knowledge_citations` 的 `scope=taxonagent` 独立于工程契约。
  当前服务返回文本，没有提供来源版本和定位；来源保留为空，
  `quality=incomplete`、`review_status=pending`。来源卡片仅保存最多 512 字符摘录。
- 既有工程引用、隔离回放和确定性分析流程保持各自的边界。

## 验证

在 `/analysis` 提问：

```text
What is the classification standard of Coronaviridae?
```

应出现回答和“分类知识来源”卡片；刷新后来源仍能恢复，方案不改变。
自动测试覆盖工具调用、健康缓存、超时、无效来源、检索失败及禁止方案修改。
