# SimTradeData CLI 使用指南

SimTradeData 提供了强大的命令行接口（CLI）来执行各种数据管理任务，包括数据库初始化、数据同步、查询和系统监控。

## 🚀 快速开始

### 环境准备

```bash
# 克隆项目
git clone <repository-url>
cd SimTradeData

# 安装依赖
poetry install

# 激活虚拟环境
poetry shell
```

### 数据库初始化

```bash
# 创建数据库和表结构
poetry run python scripts/init_database.py --db-path data/simtradedata.db

# 验证数据库创建
ls -la data/simtradedata.db
```

### 验证安装

```bash
# 检查CLI可用性
poetry run python -m simtradedata --help

# 运行基础测试
poetry run python -m pytest tests/test_database.py -v
```

## 📋 命令概览

### 数据同步命令

```bash
# 全量同步
poetry run python -m simtradedata sync --type full --date 2024-01-24

# 增量同步
poetry run python -m simtradedata sync --type incremental --date 2024-01-24

# 同步指定股票
poetry run python -m simtradedata full-sync --symbols 000001.SZ,000002.SZ

# 同步指定频率
poetry run python -m simtradedata full-sync --frequencies 1d,1h
```

### 数据查询命令

```bash
# 查询股票信息
poetry run python -m simtradedata query stocks --symbol 000001.SZ

# 查询历史数据
poetry run python -m simtradedata query history --symbol 000001.SZ --start 2024-01-01 --end 2024-01-31

# 查询技术指标
poetry run python -m simtradedata query indicators --symbol 000001.SZ --date 2024-01-24
```

### 数据库管理命令

```bash
# 检查数据库状态
poetry run python -m simtradedata db status

# 验证数据完整性
poetry run python -m simtradedata db validate

# 清理数据库
poetry run python -m simtradedata db cleanup --days 30

# 备份数据库
poetry run python -m simtradedata db backup --output backup.db
```

### 缺口检测和修复

```bash
# 检测数据缺口
poetry run python -m simtradedata gaps detect --start 2024-01-01 --end 2024-01-31

# 修复数据缺口
poetry run python -m simtradedata gaps fix --symbol 000001.SZ --date 2024-01-24

# 批量修复缺口
poetry run python -m simtradedata gaps fix-all --max-days 7
```

### 监控和诊断

```bash
# 系统状态检查
poetry run python -m simtradedata monitor status

# 性能分析
poetry run python -m simtradedata monitor performance

# 数据质量检查
poetry run python -m simtradedata monitor quality --symbol 000001.SZ
```

## 🔧 配置选项

### 全局配置

```bash
# 设置数据库路径
poetry run python -m simtradedata config set database.path /path/to/database.db

# 设置日志级别
poetry run python -m simtradedata config set logging.level DEBUG

# 查看当前配置
poetry run python -m simtradedata config show
```

### 数据源配置

```bash
# 启用数据源
poetry run python -m simtradedata config set data_sources.baostock.enabled true

# 设置数据源优先级
poetry run python -m simtradedata config set source_priorities.SZ_1d_ohlcv baostock,akshare
```

## 📊 输出格式

### JSON 输出

```bash
# 输出为JSON格式
poetry run python -m simtradedata query stocks --symbol 000001.SZ --format json

# 保存到文件
poetry run python -m simtradedata query history --symbol 000001.SZ --output data.json
```

### CSV 输出

```bash
# 输出为CSV格式
poetry run python -m simtradedata query history --symbol 000001.SZ --format csv

# 保存到文件
poetry run python -m simtradedata query history --symbol 000001.SZ --output data.csv
```

## 🔍 高级用法

### 批处理脚本

```bash
# 创建批处理配置文件
cat > batch_sync.yaml << EOF
symbols:
  - 000001.SZ
  - 000002.SZ
  - 600000.SS
frequencies:
  - 1d
  - 1h
date_range:
  start: 2024-01-01
  end: 2024-01-31
EOF

# 执行批处理
poetry run python -m simtradedata batch --config batch_sync.yaml
```

### 定时任务

```bash
# 设置每日同步任务
poetry run python -m simtradedata schedule add daily-sync \
  --command "sync --type incremental" \
  --time "09:00"

# 查看定时任务
poetry run python -m simtradedata schedule list

# 删除定时任务
poetry run python -m simtradedata schedule remove daily-sync
```

## 🚨 故障排除

### 常见问题

```bash
# 检查数据源连接
poetry run python -m simtradedata diagnose sources

# 检查数据库连接
poetry run python -m simtradedata diagnose database

# 生成诊断报告
poetry run python -m simtradedata diagnose all --output diagnosis.txt
```

### 日志分析

```bash
# 查看错误日志
poetry run python -m simtradedata logs error --lines 50

# 搜索特定错误
poetry run python -m simtradedata logs search "connection failed"

# 导出日志
poetry run python -m simtradedata logs export --start 2024-01-01 --output logs.txt
```

## 📚 更多信息

- [API 参考文档](API_REFERENCE.md)
- [开发者指南](DEVELOPER_GUIDE.md)
- [用户指南](USER_GUIDE.md)
- [架构指南](Architecture_Guide.md)

## 🆘 获取帮助

```bash
# 查看帮助信息
poetry run python -m simtradedata --help

# 查看子命令帮助
poetry run python -m simtradedata sync --help

# 查看版本信息
poetry run python -m simtradedata --version
```
