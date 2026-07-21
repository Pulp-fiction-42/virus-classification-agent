# TaxonAgent MCP 集成测试报告

**测试时间**: 2026-07-19 23:59:49
**测试环境**:
- BioAgent adapter: http://127.0.0.1:8001 (cyclone, 172.24.195.66)
- TaxonAgent MCP:   http://172.24.202.246:8765/mcp (5090)

## Test 1 — 网络层连通性
- ✅ cyclone → 5090:8765 TCP OPEN

## Test 2 — MCP 协议层（initialize + tools/list）
- ✅ MCP initialize: HTTP 200
- server: TaxonAgent v1.28.1
- protocolVersion: 2025-03-26
- session-id: 3c40ff05139344a7ae37c98ba7dc5a26
- ✅ tools/list: 1 tool(s) found
- tool `ictv_family_info`: Query ICTV classification rules and taxonomy info for a virus family.

Returns

## Test 3 — MCP 应用层（直接调用 ictv_family_info）

**查询**: `ictv_family_info(family_name="Coronaviridae")`
- ✅ 工具调用成功，耗时 154.8s

**返回结果（前 600 字符）**:
```
VERDICT: The ICTV classification rules and demarcation criteria for Coronaviridae, specifically for species demarcation within the family, involve a quantitative analysis using DEmARC (Distance Estimation Analysis Based on Reticulate Trees with Consistency) method.

CONFIDENCE: MEDIUM

EVIDENCE CHAIN:
1. Species demarcation in Coronaviridae is based on pairwise patristic distances.
2. The threshold for species demarcation is defined as "other >= 0.0PUD" across conserved domains, which include RdRp, NiRAN, ZBD, and HEL1 genes.
3. The method used is DEmARC analysis of these pairwise distances.


```

## Test 4 — 端到端（用户问题 → BioAgent → MCP → 返回）

### Case 1 / 2
**用户问题**: What are the ICTV demarcation criteria for Flaviviridae?
- ✅ BioAgent 响应成功，耗时 167.5s
- chat_id: 951be2af-ae9b-4c04-9d03-d8d9f29d0386

**BioAgent 回答（前 800 字符）**:
```
I'll look up the ICTV demarcation criteria for Flaviviridae.# ICTV Demarcation Criteria for *Flaviviridae*

## Genus Demarcation

**Qualitative criteria**

- **Phylogenetic clustering** — Genera are defined by phylogenetic analysis of NS5/RdRp sequences that form distinct well-supported clades. This is evaluated by constructing a Maximum Likelihood (ML) tree of NS5 amino acid sequences with bootstrap support ≥ 70%.
- **Genome organization** — Genera differ in genome organization, polyprotein processing, and replication strategy (requires expert judgment).

## Species Demarcation

**Quantitative threshold**

- **<84.0% amino acid identity in RdRp (NS5)** — Determined by pairwise amino acid comparison of the RNA-dependent RNA polymerase.

**Qualitative criteria**

- **Phylogenetic clustering
```

### Case 2 / 2
**用户问题**: 请介绍一下 Filoviridae 的 ICTV 分类规则
- ✅ BioAgent 响应成功，耗时 251.1s
- chat_id: 54115085-c25d-4820-82e5-a12adabdc2a6

**BioAgent 回答（前 800 字符）**:
```
# Filoviridae（丝状病毒科）ICTV 分类规则

## 1. 属级划分标准（Demarcation Criteria）

Filoviridae 的属级划分采用 **定量 + 定性** 的混合判定方式：

### 定量阈值
- **编码完整基因组差异度 ≥ 55%**（coding-complete divergence）
- 计算方法：**PASC（PAirwise Sequence Comparison）**
- ICTV 原文依据：不同属的丝状病毒编码完整基因组序列差异 ≥ 55%

### 定性标准（需专家判定）
- 基因重叠的数量与位置
- ORF/基因的数量
- 宿主分布
- 地理分布
- 致病性

---

## 2. 公共卫生背景

| 项目 | 内容 |
|---|---|
| WHO 状态 | 优先病原体（Priority Pathogen） |
| 原型病毒 | Zaire ebolavirus（扎伊尔埃博拉病毒） |
| 优先病原体 | Ebola virus、Sudan virus、Marburg virus、Ravn virus |
| 病死率 | 25%–90% |
| 潜伏期 | 2–21 天 |
| 储存宿主 | 狐蝠科果蝠（Pteropodidae；Rousettus aegyptiacus 为马尔堡病毒宿主） |
| 传播方式 | 直接接触体液；接触感染野生动物（灵长类、蝙蝠）；院内传播；康复后性传播 |
| 地理分布 | 中非、西非、东非 |
| 生物安全等级 | BSL-4（活病毒）；灭活标本可在 BSL-2 + BSL-3 操作规范下处理 |
| 管制状态 | HHS Select Agent（42 CFR §73） |

---

## 3. 疫苗与治疗药物

- **已批准疫苗**：Ervebo、Zabdeno/M
```

## 测试汇总

| 测试层 | 内容 | 结果 |
|--------|------|------|
| 网络层 | cyclone → 5090:8765 TCP | ✅ |
| 协议层 | MCP initialize + tools/list | ✅ |
| 应用层 | ictv_family_info(Coronaviridae) | ✅ |
| 端到端 | 用户问题 → BioAgent → MCP | ✅ |

**KG 信息**: 38,228 节点，41,827 边，32 个病毒科，834 个属
**LLM**: TaxonAgent 使用 Ollama glm4（5090 本机），BioAgent 使用 MiniMax M3（API）