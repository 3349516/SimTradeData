"""
测试性能优化模块

验证查询优化器、缓存管理器、并发处理器和性能监控器功能。
"""

import logging
from unittest.mock import Mock

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
    def mock_db_manager(self):
        """模拟数据库管理器"""
        db_manager = Mock(spec=DatabaseManager)

        # 模拟查询结果
        db_manager.fetchall.return_value = [
            {"symbol": "000001.SZ", "close": 10.5, "trade_date": "2024-01-20"}
        ]

        return db_manager

    def test_initialization(self, mock_db_manager):
        """测试初始化"""
        config = Config()
        optimizer = QueryOptimizer(mock_db_manager, config)

        assert optimizer.db_manager is mock_db_manager
        assert optimizer.config is config
        assert optimizer.enable_query_cache == True
        assert len(optimizer.query_patterns) > 0

        logger.info("✅ 查询优化器初始化测试通过")

    def test_optimize_query(self, mock_db_manager):
        """测试查询优化"""
        optimizer = QueryOptimizer(mock_db_manager)

        # 测试基本查询优化
        sql = "SELECT * FROM daily_data WHERE symbol = '000001.SZ'"
        optimized_sql, params = optimizer.optimize_query(sql, ())

        assert "LIMIT" in optimized_sql
        assert params == ()

        logger.info("✅ 查询优化测试通过")

    def test_execute_with_cache(self, mock_db_manager):
        """测试带缓存的查询执行"""
        optimizer = QueryOptimizer(mock_db_manager)

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

    def test_suggest_indexes(self, mock_db_manager):
        """测试索引建议"""
        optimizer = QueryOptimizer(mock_db_manager)

        # 获取索引建议
        suggestions = optimizer.suggest_indexes("daily_data")

        assert len(suggestions) > 0
        assert all("table" in s for s in suggestions)
        assert all("columns" in s for s in suggestions)

        logger.info("✅ 索引建议测试通过")

    def test_get_optimizer_stats(self, mock_db_manager):
        """测试获取优化器统计"""
        optimizer = QueryOptimizer(mock_db_manager)

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
    db_manager = DatabaseManager(":memory:")

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

    logger.info("✅ 性能模块集成测试通过")


if __name__ == "__main__":
    # 运行集成测试
    test_performance_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
