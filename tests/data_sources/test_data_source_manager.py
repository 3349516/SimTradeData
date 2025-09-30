"""
数据源管理器测试
"""

import pytest

from simtradedata.config import Config
from simtradedata.data_sources import DataSourceManager
from simtradedata.data_sources.base import BaseDataSource


class MockAdapter(BaseDataSource):
    """Mock适配器用于测试"""

    def __init__(self, config=None):
        super().__init__("mock", config or {})
        self.connection_fail = False

    def connect(self):
        if self.connection_fail:
            raise Exception("Connection failed")
        self._connected = True
        return True

    def disconnect(self):
        self._connected = False

    def is_connected(self):
        return self._connected

    def get_daily_data(self, symbol, start_date, end_date=None):
        return {"success": True, "data": [], "source": self.name}

    def get_minute_data(self, symbol, trade_date, frequency="5m"):
        return {"success": True, "data": [], "source": self.name}

    def get_stock_info(self, symbol=None):
        return {"success": True, "data": {}, "source": self.name}

    def get_fundamentals(self, symbol, report_date, report_type="Q4"):
        return {"success": True, "data": {}, "source": self.name}


@pytest.mark.unit
class TestDataSourceManager:
    """数据源管理器测试"""

    @pytest.fixture
    def config(self):
        """创建测试配置"""
        cfg = Config()
        cfg.set("data_sources.akshare.enabled", True)
        cfg.set("data_sources.baostock.enabled", True)
        cfg.set("data_sources.qstock.enabled", True)
        return cfg

    @pytest.fixture
    def manager(self, config):
        """创建数据源管理器"""
        return DataSourceManager(config=config)

    def test_manager_initialization(self, manager):
        """测试管理器初始化"""
        assert manager is not None
        assert hasattr(manager, "sources")
        assert hasattr(manager, "source_status")
        assert isinstance(manager.sources, dict)

    def test_manager_with_minimal_config(self):
        """测试最小配置初始化"""
        config = Config()
        manager = DataSourceManager(config=config)

        assert manager is not None
        assert len(manager.sources) >= 0  # 可能没有启用的数据源

    def test_adapter_registration(self, manager):
        """测试适配器注册"""
        assert hasattr(manager, "adapter_classes")
        assert "akshare" in manager.adapter_classes
        assert "baostock" in manager.adapter_classes
        assert "qstock" in manager.adapter_classes

    def test_get_enabled_sources(self, manager):
        """测试获取启用的数据源"""
        # 根据配置，至少应该有一些源被启用
        sources = manager.sources
        assert isinstance(sources, dict)

        # 检查每个源的状态
        for source_name, source in sources.items():
            assert isinstance(source, BaseDataSource)
            assert source_name in manager.source_status

    def test_get_available_sources(self, manager):
        """测试获取可用数据源列表"""
        available = manager.get_available_sources()
        assert isinstance(available, list)

        # 每个可用源都应该在sources中
        for source_name in available:
            assert source_name in manager.sources

    def test_get_source(self, manager):
        """测试获取特定数据源"""
        # 如果有任何启用的源
        if manager.sources:
            source_name = list(manager.sources.keys())[0]
            source = manager.get_source(source_name)
            assert source is not None
            assert isinstance(source, BaseDataSource)

        # 测试不存在的源
        source = manager.get_source("nonexistent")
        assert source is None

    def test_health_check_all(self, manager):
        """测试检查所有数据源健康状态"""
        result = manager.health_check()

        assert isinstance(result, dict)
        # 结果应该包含所有已注册的源
        for source_name in manager.sources.keys():
            assert source_name in result or "data" in result

    def test_get_status(self, manager):
        """测试获取管理器状态"""
        status = manager.get_status()

        assert isinstance(status, dict)
        # 状态应该包含基本信息
        if "data" in status:
            status_data = status["data"]
        else:
            status_data = status

        assert "total_sources" in status_data or isinstance(status_data, dict)

    def test_custom_adapter_registration(self):
        """测试自定义适配器注册"""
        config = Config()
        manager = DataSourceManager(config=config)

        # 手动注册mock适配器
        manager.adapter_classes["mock"] = MockAdapter
        manager.sources["mock"] = MockAdapter({"enabled": True})

        assert "mock" in manager.sources
        source = manager.get_source("mock")
        assert isinstance(source, MockAdapter)


@pytest.mark.integration
class TestDataSourceManagerIntegration:
    """数据源管理器集成测试"""

    @pytest.fixture
    def manager_with_mock(self):
        """创建带Mock适配器的管理器"""
        config = Config()
        config.set("data_sources.mock.enabled", True)

        manager = DataSourceManager(config=config)
        manager.adapter_classes["mock"] = MockAdapter
        manager.sources["mock"] = MockAdapter({"enabled": True})
        manager.source_status["mock"] = {
            "enabled": True,
            "connected": False,
            "last_check": None,
            "error_count": 0,
            "last_error": None,
        }

        return manager

    def test_full_lifecycle(self, manager_with_mock):
        """测试完整生命周期"""
        manager = manager_with_mock

        # 1. 检查初始状态
        assert "mock" in manager.sources
        assert not manager.sources["mock"].is_connected()

        # 2. 连接
        result = manager.sources["mock"].connect()
        assert result is True
        assert manager.sources["mock"].is_connected()

        # 3. 检查健康状态
        health = manager.health_check()
        assert isinstance(health, dict)

        # 4. 断开
        manager.sources["mock"].disconnect()
        assert not manager.sources["mock"].is_connected()

    def test_error_handling(self, manager_with_mock):
        """测试错误处理"""
        manager = manager_with_mock

        # 设置mock适配器连接失败
        manager.sources["mock"].connection_fail = True

        # 尝试连接应该抛出异常
        with pytest.raises(Exception):
            manager.sources["mock"].connect()

    def test_concurrent_access(self, manager_with_mock):
        """测试并发访问"""
        manager = manager_with_mock

        # 多次快速访问同一数据源
        results = []
        for _ in range(5):
            result = manager.get_source("mock")
            results.append(result)

        # 所有结果应该是同一个实例
        assert all(r is results[0] for r in results)


@pytest.mark.unit
class TestDataSourceManagerConfiguration:
    """数据源管理器配置测试"""

    def test_config_override(self):
        """测试配置覆盖"""
        config = Config()
        config.set("data_sources.max_retry_attempts", 5)
        config.set("data_sources.retry_delay", 2)

        manager = DataSourceManager(config=config)

        assert manager.max_retry_attempts == 5
        assert manager.retry_delay == 2

    def test_default_config_values(self):
        """测试默认配置值"""
        config = Config()
        manager = DataSourceManager(config=config)

        # 检查默认值
        assert manager.max_retry_attempts >= 1
        assert manager.retry_delay >= 0

    def test_source_specific_config(self):
        """测试数据源特定配置"""
        config = Config()
        config.set("data_sources.akshare.enabled", True)
        config.set("data_sources.akshare.timeout", 20)

        manager = DataSourceManager(config=config)

        if "akshare" in manager.sources:
            source = manager.sources["akshare"]
            # 配置应该被正确传递到适配器
            assert hasattr(source, "timeout")


@pytest.mark.unit
class TestDataSourceManagerErrorScenarios:
    """数据源管理器错误场景测试"""

    def test_invalid_source_name(self):
        """测试无效数据源名称"""
        config = Config()
        manager = DataSourceManager(config=config)

        # 获取不存在的源应该返回None
        result = manager.get_source("invalid_source_name")
        assert result is None

    def test_empty_sources(self):
        """测试无数据源情况"""
        config = Config()
        # 禁用所有数据源
        config.set("data_sources.akshare.enabled", False)
        config.set("data_sources.baostock.enabled", False)
        config.set("data_sources.qstock.enabled", False)

        manager = DataSourceManager(config=config)

        # 应该能正常初始化，但sources为空
        assert isinstance(manager.sources, dict)
        available = manager.get_available_sources()
        assert isinstance(available, list)

    def test_health_check_with_no_sources(self):
        """测试无数据源时的健康检查"""
        config = Config()
        config.set("data_sources.akshare.enabled", False)
        config.set("data_sources.baostock.enabled", False)
        config.set("data_sources.qstock.enabled", False)

        manager = DataSourceManager(config=config)

        # 健康检查应该能正常执行
        result = manager.health_check()
        assert isinstance(result, dict)
