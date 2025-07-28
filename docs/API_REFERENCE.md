# SimTradeData API 参考文档

## 📖 概述

SimTradeData 提供多种API接口，包括PTrade兼容接口、REST API和Python API。本文档详细介绍了所有可用的API接口和使用方法。

## 🐍 Python API

### 核心API路由器

#### APIRouter

高性能的数据查询路由器，提供统一的数据访问接口，支持缓存、并发和查询优化。

```python
from simtradedata.api.router import APIRouter
from simtradedata.database.manager import DatabaseManager
from simtradedata.config.manager import Config

# 初始化核心组件
config = Config()
db_manager = DatabaseManager("data/simtradedata.db")
api_router = APIRouter(db_manager, config)
```

#### 核心特性

- **高性能查询**: 优化的SQL生成和执行
- **智能缓存**: 多级缓存策略，提升查询速度
- **并发支持**: 支持高并发查询请求
- **格式化输出**: 自动格式化为DataFrame或JSON
- **错误处理**: 完善的异常处理和日志记录

### 主要API方法

#### 历史数据查询

**get_history(symbols, start_date, end_date, frequency="1d", fields=None)**
- 获取历史行情数据，支持多股票、多频率查询
- 参数:
  - `symbols` (list[str]): 股票代码列表，如 ['000001.SZ', '000002.SZ']
  - `start_date` (str): 开始日期，格式 'YYYY-MM-DD'
  - `end_date` (str): 结束日期，格式 'YYYY-MM-DD'
  - `frequency` (str): 数据频率，支持 '1d', '5m', '15m', '30m', '60m'
  - `fields` (list[str], optional): 指定返回字段
- 返回: pandas.DataFrame

```python
# 获取单只股票日线数据
data = api_router.get_history(
    symbols=['000001.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    frequency='1d'
)

# 获取多只股票分钟数据
data = api_router.get_history(
    symbols=['000001.SZ', '000002.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    frequency='5m'
)
```

#### 实时数据查询

**get_snapshot(symbols, fields=None)**
- 获取股票快照数据
- 参数:
  - `symbols` (list[str]): 股票代码列表
  - `fields` (list[str], optional): 指定返回字段
- 返回: pandas.DataFrame

```python
# 获取股票快照
snapshot = api_router.get_snapshot(['000001.SZ', '000002.SZ'])
```

```python
data = api_manager.get_minute_data('000001.SZ', '2024-01-01 09:30:00', '2024-01-01 15:00:00', '5m')
```

**get_realtime_data(symbols)**
- 获取实时数据
- 参数:
  - `symbols` (list): 股票代码列表
- 返回: pandas.DataFrame

```python
data = api_manager.get_realtime_data(['000001.SZ', '000002.SZ'])
```

**store_daily_data(data)**
- 存储日线数据
- 参数:
  - `data` (pandas.DataFrame): 日线数据
- 返回: bool

```python
import pandas as pd
data = pd.DataFrame({
    'symbol': ['000001.SZ'],
    'trade_date': ['2024-01-20'],
    'open': [10.0],
    'high': [10.5],
    'low': [9.8],
    'close': [10.2],
    'volume': [1000000]
})
api_manager.store_daily_data(data)
```

##### 股票信息方法

**get_stock_list(market, status)**
- 获取股票列表
- 参数:
  - `market` (str, optional): 市场代码，'SZ', 'SS', 'HK', 'US'
  - `status` (str, optional): 状态，'L'(上市), 'D'(退市)
- 返回: pandas.DataFrame

```python
stocks = api_manager.get_stock_list(market='SZ', status='L')
```

**get_stock_info(symbol)**
- 获取股票基本信息
- 参数:
  - `symbol` (str): 股票代码
- 返回: pandas.DataFrame

```python
info = api_manager.get_stock_info('000001.SZ')
```

**store_stock_info(data)**
- 存储股票基本信息
- 参数:
  - `data` (pandas.DataFrame): 股票信息数据
- 返回: bool

```python
info_data = pd.DataFrame({
    'symbol': ['000001.SZ'],
    'name': ['平安银行'],
    'market': ['SZ'],
    'exchange': ['深交所'],
    'list_date': ['1991-04-03'],
    'status': ['L']
})
api_manager.store_stock_info(info_data)
```

### 数据查询API

#### 基本查询

```python
from simtradedata.api import APIRouter

router = APIRouter(db_manager, config)

# 获取股票信息
stocks = router.get_stock_info(symbols=['000001.SZ'])

# 获取历史数据
history = router.get_history(
    symbols=['000001.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31'
)
```

```python
constituents = extended_data.get_sector_constituents('BK001')
```

**get_technical_indicators(symbol, start_date, end_date)**
- 获取技术指标
- 参数:
  - `symbol` (str): 股票代码
  - `start_date` (str): 开始日期
  - `end_date` (str): 结束日期
- 返回: pandas.DataFrame

```python
indicators = extended_data.get_technical_indicators('000001.SZ', '2024-01-01', '2024-01-31')
```

## 🔌 PTrade兼容接口

### PTradeAPIAdapter

提供与PTrade完全兼容的接口。

```python
from simtradedata.interfaces import PTradeAPIAdapter

adapter = PTradeAPIAdapter(api_manager, config)
```

**get_stock_list(market)**
- 获取股票列表（PTrade兼容）
- 参数:
  - `market` (str): 市场代码
- 返回: pandas.DataFrame

```python
stocks = adapter.get_stock_list('SZ')
```

**get_price(symbol, start_date, end_date)**
- 获取价格数据（PTrade兼容）
- 参数:
  - `symbol` (str): 股票代码
  - `start_date` (str): 开始日期
  - `end_date` (str): 结束日期
- 返回: pandas.DataFrame

```python
prices = adapter.get_price('000001.SZ', '2024-01-01', '2024-01-31')
```

**get_stock_info(symbol)**
- 获取股票信息（PTrade兼容）
- 参数:
  - `symbol` (str): 股票代码
- 返回: pandas.DataFrame

```python
info = adapter.get_stock_info('000001.SZ')
```

## 🌐 REST API

### 基础信息

- **Base URL**: `http://localhost:8080/api/v1`
- **Content-Type**: `application/json`
- **认证**: 可选的API Key认证

### 股票接口

#### GET /stocks
获取股票列表

**查询参数:**
- `market` (string, optional): 市场代码 (SZ, SS, HK, US)
- `status` (string, optional): 状态 (L, D)
- `limit` (integer, optional): 返回数量限制，默认100
- `offset` (integer, optional): 偏移量，默认0

**响应示例:**
```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "symbol": "000001.SZ",
      "name": "平安银行",
      "market": "SZ",
      "exchange": "深交所",
      "list_date": "1991-04-03",
      "status": "L"
    }
  ],
  "total": 1,
  "page": 1,
  "per_page": 100
}
```

#### GET /stocks/{symbol}
获取股票基本信息

**路径参数:**
- `symbol` (string): 股票代码

**响应示例:**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "symbol": "000001.SZ",
    "name": "平安银行",
    "market": "SZ",
    "exchange": "深交所",
    "list_date": "1991-04-03",
    "status": "L"
  }
}
```

#### GET /stocks/{symbol}/prices
获取股票价格数据

**路径参数:**
- `symbol` (string): 股票代码

**查询参数:**
- `start_date` (string): 开始日期，格式YYYY-MM-DD
- `end_date` (string): 结束日期，格式YYYY-MM-DD
- `frequency` (string, optional): 数据频率，默认'daily'
  - 'daily': 日线数据
  - '1m', '5m', '15m', '30m', '60m': 分钟数据

**响应示例:**
```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "symbol": "000001.SZ",
      "trade_date": "2024-01-20",
      "open": 10.0,
      "high": 10.5,
      "low": 9.8,
      "close": 10.2,
      "volume": 1000000,
      "amount": 10200000
    }
  ]
}
```

#### GET /stocks/{symbol}/indicators
获取技术指标数据

**路径参数:**
- `symbol` (string): 股票代码

**查询参数:**
- `start_date` (string): 开始日期
- `end_date` (string): 结束日期
- `indicators` (string, optional): 指标列表，逗号分隔，如'ma5,ma20,rsi'

**响应示例:**
```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "symbol": "000001.SZ",
      "trade_date": "2024-01-20",
      "ma5": 10.2,
      "ma20": 10.8,
      "rsi": 65.5,
      "macd": 0.15
    }
  ]
}
```

### 市场接口

#### GET /markets
获取支持的市场列表

**响应示例:**
```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "market": "SZ",
      "name": "深圳证券交易所",
      "timezone": "Asia/Shanghai",
      "trading_hours": "09:30-11:30,13:00-15:00"
    },
    {
      "market": "SS",
      "name": "上海证券交易所",
      "timezone": "Asia/Shanghai",
      "trading_hours": "09:30-11:30,13:00-15:00"
    }
  ]
}
```

#### GET /markets/{market}/stocks
获取指定市场的股票列表

**路径参数:**
- `market` (string): 市场代码

**查询参数:**
- `status` (string, optional): 状态过滤
- `limit` (integer, optional): 返回数量限制
- `offset` (integer, optional): 偏移量

### 扩展数据接口

#### GET /etfs
获取ETF列表

**查询参数:**
- `market` (string, optional): 市场代码
- `limit` (integer, optional): 返回数量限制
- `offset` (integer, optional): 偏移量

#### GET /sectors
获取板块列表

**查询参数:**
- `type` (string, optional): 板块类型 (industry, concept)
- `limit` (integer, optional): 返回数量限制
- `offset` (integer, optional): 偏移量

#### GET /sectors/{sector_code}/constituents
获取板块成分股

**路径参数:**
- `sector_code` (string): 板块代码

### 错误响应

所有API在出错时返回统一的错误格式：

```json
{
  "code": 400,
  "message": "Invalid parameter",
  "error": "start_date is required",
  "timestamp": "2024-01-20T10:30:00Z"
}
```

**错误代码:**
- `400`: 请求参数错误
- `401`: 认证失败
- `403`: 权限不足
- `404`: 资源不存在
- `429`: 请求频率限制
- `500`: 服务器内部错误



## 📊 性能API

### 查询优化器

```python
from simtradedata.performance import QueryOptimizer

optimizer = QueryOptimizer(db_manager, config)

# 执行优化查询
result = optimizer.execute_with_cache(sql, params)

# 获取缓存统计
stats = optimizer.get_cache_stats()

# 获取索引建议
suggestions = optimizer.suggest_indexes('daily_data')
```



## 📈 简化的监控

### 基本状态检查

```python
from simtradedata.database import DatabaseManager
from simtradedata.config import Config

# 基本的数据库连接检查
config = Config()
db = DatabaseManager(config.get('database.path'))

# 检查数据库是否可用
try:
    result = db.fetchone("SELECT 1")
    print("✅ 数据库连接正常")
except Exception as e:
    print(f"❌ 数据库连接失败: {e}")

# 检查表是否存在
tables = ['stocks', 'market_data', 'trading_calendar']
for table in tables:
    if db.table_exists(table):
        count = db.get_table_count(table)
        print(f"✅ {table}: {count} 条记录")
    else:
        print(f"❌ {table}: 表不存在")
```

---

*SimTradeData API Reference - 完整的API接口文档*
