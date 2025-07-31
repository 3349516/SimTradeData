"""
测试性能优化模块

验证查询优化器、缓存管理器、并发处理器和性能监控器功能。
使用真实对象而非mock进行测试。
"""

import logging
import tempfile
from pathlib import Path

import pytest

from simtradedata.config import Config
from simtradedata.database import DatabaseManager
from simtradedata.performance import CacheManager, QueryOptimizer

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestQueryOptimizer:
    """测试查询优化器"""

    @pytest.fixture
    def real_db_manager(self):
        """创建真实数据库管理器"""
        # 创建临时数据库
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        config = Config()
        db_manager = DatabaseManager(db_path, config=config)

        # 创建测试表
        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_data (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, trade_date)
            )
        """
        )

        # 插入测试数据
        test_data = [
            ("000001.SZ", "2024-01-20", 10.5, 1000000),
            ("000002.SZ", "2024-01-20", 12.8, 800000),
            ("600000.SS", "2024-01-20", 8.6, 1200000),
        ]

        db_manager.executemany(
            "INSERT INTO daily_data (symbol, trade_date, close, volume) VALUES (?, ?, ?, ?)",
            test_data,
        )

        yield db_manager

        # 清理
        db_manager.close()
        Path(db_path).unlink(missing_ok=True)

    def test_initialization(self, real_db_manager):
        """测试初始化"""
        config = Config()
        optimizer = QueryOptimizer(real_db_manager, config)

        assert optimizer.db_manager is real_db_manager
        assert optimizer.config is config
        assert optimizer.enable_query_cache == True
        assert len(optimizer.query_patterns) > 0

        logger.info("✅ 查询优化器初始化测试通过")

    def test_optimize_query(self, real_db_manager):
        """测试查询优化"""
        optimizer = QueryOptimizer(real_db_manager)

        # 测试基本查询优化
        sql = "SELECT * FROM daily_data WHERE symbol = '000001.SZ'"
        optimized_sql, params = optimizer.optimize_query(sql, ())

        assert "LIMIT" in optimized_sql
        assert params == ()

        logger.info("✅ 查询优化测试通过")

    def test_execute_with_cache(self, real_db_manager):
        """测试带缓存的查询执行"""
        optimizer = QueryOptimizer(real_db_manager)

        sql = "SELECT * FROM daily_data WHERE symbol = ?"
        params = ("000001.SZ",)

        # 第一次查询（缓存未命中）
        result1 = optimizer.execute_with_cache(sql, params)
        assert result1 is not None
        assert optimizer.cache_stats["misses"] == 1

        # 第二次查询（缓存命中）
        result2 = optimizer.execute_with_cache(sql, params)
        assert result2 == result1
        assert optimizer.cache_stats["hits"] == 1

        logger.info("✅ 缓存查询执行测试通过")

    def test_suggest_indexes(self, real_db_manager):
        """测试索引建议"""
        optimizer = QueryOptimizer(real_db_manager)

        # 获取索引建议
        suggestions = optimizer.suggest_indexes("daily_data")

        assert len(suggestions) > 0
        assert all("table" in s for s in suggestions)
        assert all("columns" in s for s in suggestions)

        logger.info("✅ 索引建议测试通过")

    def test_get_optimizer_stats(self, real_db_manager):
        """测试获取优化器统计"""
        optimizer = QueryOptimizer(real_db_manager)

        stats = optimizer.get_optimizer_stats()

        assert "optimizer_name" in stats
        assert "cache_stats" in stats
        assert "optimization_features" in stats

        logger.info("✅ 优化器统计测试通过")


class TestCacheManager:
    """测试缓存管理器"""

    def test_initialization(self):
        """测试初始化"""
        config = Config()
        cache_manager = CacheManager(config)

        assert cache_manager.config is config
        assert cache_manager.enable_l1_cache == True
        assert cache_manager.l1_cache is not None
        assert len(cache_manager.cache_strategies) > 0

        logger.info("✅ 缓存管理器初始化测试通过")

    def test_cache_operations(self):
        """测试缓存操作"""
        cache_manager = CacheManager()

        # 测试设置和获取
        key = "test_key"
        value = {"data": "test_value"}
        data_type = "test_data"

        # 设置缓存
        success_response = cache_manager.set(key, value, data_type)
        assert success_response["success"] == True
        assert success_response["data"] == True

        # 获取缓存
        get_response = cache_manager.get(key, data_type)
        assert get_response["success"] == True
        assert get_response["data"] == value

        # 检查存在性
        exists_response = cache_manager.exists(key, data_type)
        assert exists_response["success"] == True
        assert exists_response["data"] == True

        # 删除缓存
        delete_response = cache_manager.delete(key, data_type)
        assert delete_response["success"] == True
        assert delete_response["data"] == True

        # 验证删除
        get_after_delete = cache_manager.get(key, data_type)
        assert get_after_delete["success"] == True
        assert get_after_delete["data"] is None

        logger.info("✅ 缓存操作测试通过")

    def test_cache_strategies(self):
        """测试缓存策略"""
        cache_manager = CacheManager()

        # 测试不同数据类型的缓存策略
        cache_manager.set("key1", "value1", "stock_info")
        cache_manager.set("key2", "value2", "daily_data")
        cache_manager.set("key3", "value3", "realtime_data")

        # 验证缓存策略生效
        strategies_response = cache_manager.get_cache_strategies()
        assert strategies_response["success"] == True
        strategies = strategies_response["data"]
        assert "stock_info" in strategies
        assert "daily_data" in strategies
        assert "realtime_data" in strategies

        logger.info("✅ 缓存策略测试通过")

    def test_cache_stats(self):
        """测试缓存统计"""
        cache_manager = CacheManager()

        # 执行一些缓存操作
        cache_manager.set("key1", "value1")
        cache_manager.get("key1")
        cache_manager.get("nonexistent_key")

        # 获取统计信息
        stats_response = cache_manager.get_cache_stats()
        assert stats_response["success"] == True
        stats = stats_response["data"]

        assert "cache_manager" in stats
        assert "l1_cache" in stats
        assert "operations" in stats
        assert stats["operations"]["sets"] > 0

        logger.info("✅ 缓存统计测试通过")


# ConcurrentProcessor和PerformanceMonitor测试已移除 - 这些类尚未实现
# 当这些类实现后，可以重新添加相应的测试


def test_performance_integration():
    """测试性能模块集成"""
    config = Config()

    # 创建真实的内存数据库
    db_manager = DatabaseManager(":memory:", config=config)

    # 创建测试表
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            value REAL
        )
    """
    )

    # 插入测试数据
    db_manager.execute(
        "INSERT INTO test_table (symbol, value) VALUES (?, ?)", ("TEST", 100.0)
    )

    # 测试查询优化器
    optimizer = QueryOptimizer(db_manager, config)
    assert optimizer is not None

    # 测试缓存管理器
    cache_manager = CacheManager(config)
    assert cache_manager is not None

    # 测试基本功能
    cache_manager.set("test_key", "test_value", "test_type")
    get_response = cache_manager.get("test_key", "test_type")
    assert get_response["success"] == True
    assert get_response["data"] == "test_value"

    # 测试查询优化器与真实数据库的配合
    result = optimizer.execute_with_cache(
        "SELECT * FROM test_table WHERE symbol = ?", ("TEST",)
    )
    assert result is not None

    # 清理
    db_manager.close()

    logger.info("✅ 性能模块集成测试通过")


@pytest.mark.integration
def test_performance_real_workload():
    """测试性能模块在真实工作负载下的表现"""
    config = Config()

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        db_manager = DatabaseManager(db_path, config=config)

        # 创建市场数据表
        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, trade_date)
            )
        """
        )

        # 插入大量测试数据
        symbols = [f"00000{i}.SZ" for i in range(1, 11)]
        dates = [f"2024-01-{i:02d}" for i in range(1, 21)]

        test_data = []
        for symbol in symbols:
            for date in dates:
                test_data.append((symbol, date, 10.0 + len(test_data) * 0.1, 1000000))

        db_manager.executemany(
            "INSERT INTO market_data (symbol, trade_date, close, volume) VALUES (?, ?, ?, ?)",
            test_data,
        )

        # 测试查询优化器
        optimizer = QueryOptimizer(db_manager, config)

        # 测试复杂查询的优化
        sql = """
            SELECT symbol, AVG(close) as avg_close, SUM(volume) as total_volume
            FROM market_data 
            WHERE trade_date >= '2024-01-10'
            GROUP BY symbol
        """

        optimized_sql, params = optimizer.optimize_query(sql, ())
        result = optimizer.execute_with_cache(optimized_sql, params)

        assert result is not None
        assert len(result) > 0

        # 测试缓存效果
        result2 = optimizer.execute_with_cache(optimized_sql, params)
        assert optimizer.cache_stats["hits"] > 0

        # 测试缓存管理器的批量操作
        cache_manager = CacheManager(config)

        # 缓存多个查询结果
        for i, symbol in enumerate(symbols[:5]):
            cache_key = f"symbol_data_{symbol}"
            symbol_data = db_manager.fetchall(
                "SELECT * FROM market_data WHERE symbol = ?", (symbol,)
            )
            cache_manager.set(cache_key, symbol_data, "market_data")

        # 验证缓存命中
        cached_result = cache_manager.get("symbol_data_000001.SZ", "market_data")
        assert cached_result["success"] == True
        assert cached_result["data"] is not None

        # 获取统计信息
        optimizer_stats = optimizer.get_optimizer_stats()
        cache_stats = cache_manager.get_cache_stats()

        assert optimizer_stats["cache_stats"]["hits"] > 0
        assert cache_stats["success"] == True

        logger.info("✅ 性能模块真实工作负载测试通过")

    finally:
        # 清理
        if "db_manager" in locals():
            db_manager.close()
        Path(db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    # 运行集成测试
    test_performance_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
