# SimTradeData 文档整合方案

**制定日期：** 2025-09-30
**目标：** 减少文档冗余，提高可维护性

---

## 📊 现状分析

### 当前文档清单（15个）

| 类别 | 文档 | 大小 | 状态 | 问题 |
|-----|------|------|------|------|
| **开发指南（重复严重）** | | | | |
| | DEVELOPER_GUIDE.md | 27K | 🔴 主文档 | 内容全面但过长 |
| | DEVELOPMENT_STANDARDS.md | 11K | 🔴 重复 | 70%与DEVELOPER_GUIDE重复 |
| | DEVELOPMENT_BEST_PRACTICES.md | 17K | 🔴 重复 | 刚创建，重复内容多 |
| **架构文档** | | | | |
| | Architecture_Guide.md | 19K | ✅ 保留 | 独特内容，必要 |
| | CLAUDE.local.md | - | ✅ 保留 | 架构白皮书 |
| **API文档** | | | | |
| | API_REFERENCE.md | 11K | ✅ 保留 | API使用说明 |
| | PTrade_API_mini_Reference.md | 226K | ✅ 保留 | PTrade API详细文档 |
| | Ptrade_Financial_API.md | 55K | 🟡 归档 | 可移至reference/ |
| | PTrade_API_Requirements_Final.md | 8.7K | 🟡 归档 | 历史需求文档 |
| **数据源分析（历史文档）** | | | | |
| | BaoStock_Complete_API_Systematic_Analysis.md | 12K | 🟡 归档 | 研究文档 |
| | QStock_Complete_API_Systematic_Analysis.md | 9.0K | 🟡 归档 | 研究文档 |
| | Data_Source_Capability_Research_Summary.md | 6.8K | 🟡 归档 | 研究文档 |
| **其他** | | | | |
| | CLI_USAGE_GUIDE.md | 5.9K | ✅ 保留 | 用户指南 |
| | TEST_COVERAGE_REPORT.md | 11K | 🟡 归档 | 历史报告 |
| | PROJECT_COMPLETION_REPORT.md | 6.0K | 🟡 归档 | 历史报告 |

**统计：**
- 总文档数：15个
- 建议保留：6个
- 建议合并：3个 → 1个
- 建议归档：6个

---

## ✅ 整合方案

### 方案一：激进整合（推荐）⭐

#### 1. 创建统一的开发者手册

**新文件：** `DEVELOPER_HANDBOOK.md` （约30-35K）

**整合内容：**
- ✅ DEVELOPER_GUIDE.md （全部内容作为基础）
- ✅ DEVELOPMENT_STANDARDS.md （合并"规范"章节）
- ✅ DEVELOPMENT_BEST_PRACTICES.md （合并"最佳实践"章节）

**结构：**
```markdown
# SimTradeData 开发者手册

## 第一部分：快速开始
1. 项目简介
2. 环境搭建
3. 快速上手

## 第二部分：架构设计
4. 整体架构
5. 核心模块
6. 数据流程

## 第三部分：开发规范
7. 代码规范（合并自3个文档）
8. 测试规范（合并自3个文档）
9. 命名规范（合并自3个文档）
10. 错误处理

## 第四部分：最佳实践
11. 数据库操作最佳实践
12. 性能优化指南
13. 安全编码实践

## 第五部分：开发工作流
14. 开发流程
15. 代码审查
16. 发布流程

## 附录
A. 常见问题
B. 工具和脚本
C. 参考链接
```

#### 2. 保留的核心文档（6个）

```
docs/
├── DEVELOPER_HANDBOOK.md        ⭐ 新建：合并后的开发者手册
├── Architecture_Guide.md        ✅ 保留：架构详细说明
├── API_REFERENCE.md             ✅ 保留：API使用指南
├── CLI_USAGE_GUIDE.md           ✅ 保留：CLI用户指南
├── PTrade_API_mini_Reference.md ✅ 保留：PTrade API参考
└── README.md                    ✅ 保留：项目入口
```

#### 3. 归档历史文档（6个）

```
docs/archive/
├── research/                    # 研究文档
│   ├── BaoStock_Complete_API_Systematic_Analysis.md
│   ├── QStock_Complete_API_Systematic_Analysis.md
│   └── Data_Source_Capability_Research_Summary.md
├── requirements/                # 历史需求
│   ├── PTrade_API_Requirements_Final.md
│   └── Ptrade_Financial_API.md
└── reports/                     # 历史报告
    ├── TEST_COVERAGE_REPORT.md
    └── PROJECT_COMPLETION_REPORT.md
```

#### 4. 删除的冗余文档（3个）

- ❌ DEVELOPER_GUIDE.md（已合并）
- ❌ DEVELOPMENT_STANDARDS.md（已合并）
- ❌ DEVELOPMENT_BEST_PRACTICES.md（已合并）

---

### 方案二：保守整合

**仅合并明显重复的2个：**
- 合并 DEVELOPMENT_STANDARDS.md → DEVELOPER_GUIDE.md
- 删除 DEVELOPMENT_BEST_PRACTICES.md（刚创建，内容重复）
- 保留 DEVELOPER_GUIDE.md 作为主文档

**优点：** 风险小，改动少
**缺点：** 仍有一定重复

---

## 📋 执行计划

### 阶段1：备份（5分钟）

```bash
# 创建备份
mkdir -p docs/backup_20250930
cp docs/*.md docs/backup_20250930/
```

### 阶段2：归档（10分钟）

```bash
# 创建归档目录
mkdir -p docs/archive/{research,requirements,reports}

# 移动研究文档
mv docs/BaoStock_Complete_API_Systematic_Analysis.md docs/archive/research/
mv docs/QStock_Complete_API_Systematic_Analysis.md docs/archive/research/
mv docs/Data_Source_Capability_Research_Summary.md docs/archive/research/

# 移动需求文档
mv docs/PTrade_API_Requirements_Final.md docs/archive/requirements/
mv docs/Ptrade_Financial_API.md docs/archive/requirements/

# 移动历史报告
mv docs/TEST_COVERAGE_REPORT.md docs/archive/reports/
mv docs/PROJECT_COMPLETION_REPORT.md docs/archive/reports/
```

### 阶段3：合并文档（30分钟）

**自动化脚本：**
```bash
poetry run python scripts/merge_developer_docs.py
```

**手动步骤：**
1. 创建 DEVELOPER_HANDBOOK.md
2. 复制 DEVELOPER_GUIDE.md 作为基础
3. 从 DEVELOPMENT_STANDARDS.md 提取独特内容
4. 从 DEVELOPMENT_BEST_PRACTICES.md 提取独特内容
5. 去重并重新组织章节
6. 更新目录和链接

### 阶段4：清理（5分钟）

```bash
# 删除合并后的旧文档
rm docs/DEVELOPER_GUIDE.md
rm docs/DEVELOPMENT_STANDARDS.md
rm docs/DEVELOPMENT_BEST_PRACTICES.md
```

### 阶段5：验证（10分钟）

```bash
# 检查文档链接
poetry run python scripts/check_doc_links.py

# 检查 README 中的链接
grep -r "DEVELOPER_GUIDE\|DEVELOPMENT_STANDARDS\|DEVELOPMENT_BEST_PRACTICES" docs/
```

---

## 📊 整合效果

### 整合前
```
开发相关文档：
- DEVELOPER_GUIDE.md          27K
- DEVELOPMENT_STANDARDS.md    11K
- DEVELOPMENT_BEST_PRACTICES.md 17K
总计：55K，3个文件

所有文档：15个
```

### 整合后
```
开发相关文档：
- DEVELOPER_HANDBOOK.md       35K
总计：35K，1个文件

活跃文档：6个
归档文档：6个
```

**收益：**
- ✅ 文档数量减少：15 → 6（活跃）
- ✅ 开发文档统一：3 → 1
- ✅ 减少冗余内容：~40%重复内容被合并
- ✅ 提高可维护性：单一事实来源
- ✅ 改善用户体验：一个入口，更清晰

---

## 🎯 推荐执行

**推荐方案：** 方案一（激进整合）

**理由：**
1. ✅ 彻底解决文档冗余问题
2. ✅ 建立清晰的文档结构
3. ✅ 历史文档妥善归档，不丢失
4. ✅ 未来维护成本大幅降低
5. ✅ 新开发者入职更友好

**风险：**
- 🟡 需要仔细合并，避免遗漏内容
- 🟡 需要更新所有引用链接

**缓解措施：**
- ✅ 先备份所有文档
- ✅ 保留归档文档可随时查阅
- ✅ 逐步验证每个章节
- ✅ 使用脚本检查链接完整性

---

## ✅ 执行检查清单

- [ ] 备份现有文档到 `docs/backup_20250930/`
- [ ] 创建归档目录结构
- [ ] 移动研究文档到归档
- [ ] 移动需求文档到归档
- [ ] 移动历史报告到归档
- [ ] 创建 DEVELOPER_HANDBOOK.md
- [ ] 合并 DEVELOPER_GUIDE.md 内容
- [ ] 合并 DEVELOPMENT_STANDARDS.md 内容
- [ ] 合并 DEVELOPMENT_BEST_PRACTICES.md 内容
- [ ] 去重并重新组织
- [ ] 更新目录
- [ ] 删除旧的重复文档
- [ ] 检查所有文档链接
- [ ] 更新 README.md
- [ ] 提交到版本控制

---

**准备好执行了吗？我可以帮你执行这个整合计划。** 🚀