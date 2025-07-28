# SimTradeData 用户指南

## 📖 概述

SimTradeData 是一个高性能的金融数据缓存和管理系统，专为量化交易和金融数据分析而设计。它提供了统一的数据接口、智能缓存机制、多市场支持和企业级的监控运维功能。

## 🚀 快速开始

### 安装

```bash
# 克隆项目
git clone <repository-url>
cd SimTradeData

# 安装依赖
poetry install

# 激活虚拟环境
poetry shell
```

### 初始化数据库

```bash
# 创建数据库和表结构
poetry run python scripts/init_database.py --db-path data/simtradedata.db
```

### 基本使用

```python
from simtradedata.database.manager import DatabaseManager
from simtradedata.api.router import APIRouter
from simtradedata.config.manager import Config

# 初始化核心组件
config = Config()
db_manager = DatabaseManager("data/simtradedata.db")
api_router = APIRouter(db_manager, config)

# 获取历史数据
data = api_router.get_history(
    symbols=['000001.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    frequency='1d'
)
print(data.head())

# 获取股票基本信息
info = api_router.get_stock_info(['000001.SZ'])
print(info)

# 获取快照数据
snapshot = api_router.get_snapshot(['000001.SZ'])
print(snapshot)
```

## 🏗️ 系统架构

SimTradeData 采用模块化设计，主要包含以下组件：

### 核心模块
- **数据库管理** (`database`): SQLite数据库操作和连接管理，11个专用表设计
- **API路由器** (`api`): 高性能查询路由器，支持缓存和并发处理
- **数据源管理** (`data_sources`): 多数据源适配器，智能故障转移
- **数据同步** (`sync`): 增量同步、缺口检测、断点续传
- **数据预处理** (`preprocessor`): 数据清洗、融合、质量监控
- **配置管理** (`config`): 系统配置和参数管理
- **性能优化** (`performance`): 查询优化、缓存管理
- **接口层** (`interfaces`): PTrade兼容API、REST API

### 数据模块
- **数据源** (`data_sources`): 多数据源支持和数据获取
- **多市场** (`multi_market`): 多市场数据统一管理
- **扩展数据** (`extended_data`): ETF、板块、技术指标等扩展数据
- **数据预处理** (`preprocessor`): 数据清洗、转换和指标计算

### 接口模块
- **PTrade适配器** (`interfaces.ptrade_adapter`): PTrade兼容接口
- **REST API** (`interfaces.rest_api`): HTTP REST API服务
- **API网关** (`interfaces.gateway`): 统一API入口

### 性能模块
- **查询优化** (`performance.query_optimizer`): SQL查询优化和缓存
- **并发处理** (`performance.concurrent_processor`): 多线程并发处理
- **性能监控** (`performance.performance_monitor`): 性能指标监控

### 监控模块
- **系统监控** (`monitoring.system_monitor`): 系统资源监控
- **日志管理** (`monitoring.log_manager`): 日志收集和分析
- **健康检查** (`monitoring.health_checker`): 系统健康状态检查
- **运维工具** (`monitoring.ops_tools`): 备份恢复和维护工具

## 📊 数据管理

### 支持的数据类型

1. **基础行情数据**
   - 日线数据：开高低收、成交量、成交额
   - 分钟数据：1分钟、5分钟、15分钟、30分钟、60分钟
   - 实时数据：最新价格、涨跌幅、成交量

2. **基本信息数据**
   - 股票列表：代码、名称、市场、交易所
   - 上市信息：上市日期、退市日期、状态

3. **扩展数据**
   - ETF信息：基金代码、名称、跟踪指数
   - 板块数据：行业分类、概念板块、成分股
   - 技术指标：MA、RSI、MACD、KDJ等

4. **基本面数据**
   - 财务数据：资产负债表、利润表、现金流量表
   - 估值指标：PE、PB、ROE、ROA等

### 支持的市场

- **A股市场**: 上交所(SS)、深交所(SZ)
- **港股市场**: 港交所(HK)
- **美股市场**: 纳斯达克(NASDAQ)、纽交所(NYSE)
- **其他市场**: 可扩展支持更多市场

### 数据源

- **BaoStock**: 免费的A股数据源
- **AKShare**: 开源金融数据接口
- **QStock**: 量化股票数据库
- **自定义数据源**: 支持扩展自定义数据源

## 🔧 配置管理

### 配置文件

SimTradeData 使用分层配置系统，支持多种配置方式：

```python
from simtradedata.config import Config

# 创建配置对象
config = Config()

# 设置配置项
config.set('database.path', '/path/to/database.db')
config.set('cache.ttl', 300)  # 5分钟缓存
config.set('api.port', 8080)

# 获取配置项
db_path = config.get('database.path')
cache_ttl = config.get('cache.ttl', 600)  # 默认值10分钟
```

### 主要配置项

#### 数据库配置
```python
config.set('database.path', 'simtradedata.db')
config.set('database.pool_size', 10)
config.set('database.timeout', 30)
```

#### 缓存配置
```python
config.set('cache.enable', True)
config.set('cache.ttl', 300)  # 默认TTL 5分钟
config.set('cache.max_size', 1000)  # 最大缓存条目数
```

#### API配置
```python
config.set('api.host', '0.0.0.0')
config.set('api.port', 8080)
config.set('api.enable_cors', True)
```

#### 监控配置
```python
config.set('monitoring.enable', True)
config.set('monitoring.interval', 60)  # 监控间隔60秒
config.set('monitoring.retention_days', 7)  # 数据保留7天
```

## 📡 API接口

### PTrade兼容接口

SimTradeData 提供与PTrade兼容的接口，可以无缝替换PTrade：

```python
from simtradedata.interfaces import PTradeAPIAdapter

# 初始化适配器
adapter = PTradeAPIAdapter()

# 获取股票列表
stocks = adapter.get_stock_list(market='SZ')

# 获取价格数据
prices = adapter.get_price('000001.SZ', '2024-01-01', '2024-01-31')

# 获取基本信息
info = adapter.get_stock_info('000001.SZ')
```

### REST API

提供标准的HTTP REST API接口：

```bash
# 获取股票列表
GET /api/v1/stocks?market=SZ

# 获取股票信息
GET /api/v1/stocks/000001.SZ

# 获取价格数据
GET /api/v1/stocks/000001.SZ/prices?start_date=2024-01-01&end_date=2024-01-31

# 获取技术指标
GET /api/v1/stocks/000001.SZ/indicators?start_date=2024-01-01&end_date=2024-01-31
```



## 🚀 性能优化

### 查询优化

SimTradeData 内置查询优化器，自动优化SQL查询：

```python
from simtradedata.performance import QueryOptimizer

# 创建查询优化器
optimizer = QueryOptimizer(db_manager)

# 执行优化查询（自动缓存）
result = optimizer.execute_with_cache(
    "SELECT * FROM daily_data WHERE symbol = ? AND trade_date BETWEEN ? AND ?",
    ('000001.SZ', '2024-01-01', '2024-01-31')
)

# 获取索引建议
suggestions = optimizer.suggest_indexes('daily_data')
```

### 并发处理

支持高并发数据处理：

```python
from simtradedata.performance import ConcurrentProcessor

# 创建并发处理器
processor = ConcurrentProcessor()

# 提交任务
def process_stock(symbol):
    return get_stock_data(symbol)

# 批量处理
symbols = ['000001.SZ', '000002.SZ', '600000.SS']
tasks = [{'func': process_stock, 'args': (symbol,)} for symbol in symbols]
task_ids = processor.submit_batch_tasks(tasks)

# 获取结果
results = processor.get_batch_results(task_ids)
```

### 缓存策略

多级缓存提升性能：

```python
from simtradedata.performance import CacheManager

# 创建缓存管理器
cache_manager = CacheManager()

# 设置不同类型数据的缓存策略
cache_manager.set('stock_info', stock_data, 'stock_info')  # 长期缓存
cache_manager.set('realtime_price', price_data, 'realtime_data')  # 短期缓存
```

## 📊 简化的系统监控

### 基本状态检查

检查系统基本状态：

```python
from simtradedata.database import DatabaseManager
from simtradedata.config import Config

# 基本的系统检查
config = Config()
db = DatabaseManager(config.get('database.path'))

# 检查数据库状态
try:
    db.fetchone("SELECT 1")
    print("✅ 数据库正常")
except Exception as e:
    print(f"❌ 数据库异常: {e}")

# 设置告警规则
from simtradedata.monitoring.system_monitor import AlertRule
rule = AlertRule(
    name='high_cpu',
    metric_name='system.cpu.usage',
    operator='>',
    threshold=80.0,
    severity='warning'
)
monitor.add_alert_rule(rule)
```

### 健康检查

定期检查系统健康状态：

```python
from simtradedata.monitoring import HealthChecker

# 创建健康检查器
checker = HealthChecker()

# 运行健康检查
health = checker.get_overall_health()
print(f"系统状态: {health['overall_status']}")

# 自定义健康检查
def check_data_freshness():
    # 检查数据是否及时更新
    latest_date = get_latest_trade_date()
    if is_data_fresh(latest_date):
        return HealthCheckResult('data_freshness', HealthStatus.HEALTHY, '数据及时')
    else:
        return HealthCheckResult('data_freshness', HealthStatus.WARNING, '数据延迟')

# 自定义检查
def check_data_freshness():
    # 检查数据是否新鲜
    latest_date = db.fetchone("SELECT MAX(date) as max_date FROM market_data")
    return latest_date is not None

print("数据新鲜度检查:", "✅" if check_data_freshness() else "❌")
```

## 🔍 故障排除

### 常见问题

1. **数据库连接失败**
   ```python
   # 检查数据库路径和权限
   config.set('database.path', '/correct/path/to/database.db')
   
   # 检查数据库连接
   db_manager = DatabaseManager(config)
   db_manager.test_connection()
   ```

2. **缓存未命中**
   ```python
   # 检查缓存配置
   config.set('cache.enable', True)
   config.set('cache.ttl', 300)
   
   # 清空缓存重新加载
   cache_manager.clear_cache()
   ```

3. **API响应慢**
   ```python
   # 启用查询优化
   config.set('query_optimizer.enable_query_cache', True)
   
   # 增加并发处理
   config.set('concurrent_processor.max_workers', 8)
   ```

### 简化的问题诊断

查看系统日志定位问题：

```python
import logging

# 使用标准日志查看错误
logger = logging.getLogger('simtradedata')
logger.setLevel(logging.DEBUG)

# 查看日志文件
with open('logs/simtradedata.log', 'r') as f:
    recent_logs = f.readlines()[-100:]  # 最近100行
    for line in recent_logs:
        if 'ERROR' in line:
            print(line.strip())
```

## 📚 最佳实践

### 数据管理
1. **定期备份**: 设置自动备份策略
2. **数据清理**: 定期清理过期数据
3. **索引优化**: 根据查询模式优化索引
4. **数据验证**: 确保数据质量和一致性

### 性能优化
1. **合理缓存**: 根据数据特点设置缓存策略
2. **并发控制**: 避免过度并发导致资源竞争
3. **查询优化**: 使用查询优化器提升查询性能
4. **资源监控**: 实时监控系统资源使用情况

### 运维管理
1. **监控告警**: 设置合理的监控指标和告警阈值
2. **日志管理**: 定期分析日志，及时发现问题
3. **健康检查**: 定期执行健康检查，确保系统稳定
4. **容量规划**: 根据业务增长规划系统容量

## 🤝 社区支持

- **文档**: 查看完整的API文档和开发指南
- **示例**: 参考examples目录中的示例代码
- **测试**: 运行测试套件验证功能
- **贡献**: 欢迎提交Issue和Pull Request

## 📄 许可证

本项目采用MIT许可证，详见LICENSE文件。

---

*SimTradeData - 高性能金融数据缓存系统*
