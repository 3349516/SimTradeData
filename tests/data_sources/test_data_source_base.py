"""
数据源基类测试
"""

from datetime import date, datetime
from unittest.mock import patch

import pytest

from simtradedata.data_sources.base import (
    BaseDataSource,
    DataSourceConnectionError,
    DataSourceDataError,
    DataSourceError,
)


class MockDataSource(BaseDataSource):
    """Mock数据源用于测试"""

    def __init__(self, name="mock", config=None):
        super().__init__(name, config)
        self.connect_called = False
        self.disconnect_called = False

    def connect(self) -> bool:
        self.connect_called = True
        self._connected = True
        return True

    def disconnect(self):
        self.disconnect_called = True
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def get_daily_data(self, symbol, start_date, end_date=None):
        return {"success": True, "data": []}

    def get_minute_data(self, symbol, trade_date, frequency="5m"):
        return {"success": True, "data": []}

    def get_stock_info(self, symbol=None):
        return {"success": True, "data": {}}

    def get_fundamentals(self, symbol, report_date, report_type="Q4"):
        return {"success": True, "data": {}}


@pytest.mark.unit
class TestBaseDataSource:
    """数据源基类测试"""

    def test_initialization(self):
        """测试初始化"""
        config = {
            "enabled": True,
            "timeout": 15,
            "retry_times": 5,
            "retry_delay": 2,
            "rate_limit": 200,
        }

        source = MockDataSource("test_source", config)

        assert source.name == "test_source"
        assert source.enabled is True
        assert source.timeout == 15
        assert source.retry_times == 5
        assert source.retry_delay == 2
        assert source.rate_limit == 200
        assert source._connected is False

    def test_default_config(self):
        """测试默认配置"""
        source = MockDataSource("test_source")

        assert source.enabled is True
        assert source.timeout == 10
        assert source.retry_times == 3
        assert source.retry_delay == 1
        assert source.rate_limit == 100

    def test_connect_disconnect(self):
        """测试连接和断开"""
        source = MockDataSource("test_source")

        assert source.is_connected() is False

        # 测试连接
        result = source.connect()
        assert result is True
        assert source.connect_called is True
        assert source.is_connected() is True

        # 测试断开
        source.disconnect()
        assert source.disconnect_called is True
        assert source.is_connected() is False

    def test_normalize_symbol(self):
        """测试股票代码标准化"""
        source = MockDataSource()

        # 测试添加市场后缀
        assert source._normalize_symbol("000001") == "000001.SZ"
        assert source._normalize_symbol("600000") == "600000.SS"
        assert source._normalize_symbol("300001") == "300001.SZ"
        assert source._normalize_symbol("688001") == "688001.SS"

        # 测试已有后缀
        assert source._normalize_symbol("000001.SZ") == "000001.SZ"
        assert source._normalize_symbol("600000.SS") == "600000.SS"

        # 测试大小写和空格
        assert source._normalize_symbol("  000001  ") == "000001.SZ"
        assert source._normalize_symbol("000001.sz") == "000001.SZ"

    def test_normalize_symbol_invalid(self):
        """测试无效股票代码"""
        source = MockDataSource()

        with pytest.raises(ValueError, match="股票代码不能为空"):
            source._normalize_symbol("")

        with pytest.raises(ValueError):
            source._normalize_symbol(None)

    def test_normalize_date(self):
        """测试日期标准化"""
        source = MockDataSource()

        # 测试字符串日期
        assert source._normalize_date("2024-01-01") == "2024-01-01"

        # 测试date对象
        test_date = date(2024, 1, 1)
        assert source._normalize_date(test_date) == "2024-01-01"

        # 测试datetime对象
        test_datetime = datetime(2024, 1, 1, 12, 30, 45)
        assert source._normalize_date(test_datetime) == "2024-01-01"

    def test_normalize_date_invalid(self):
        """测试无效日期"""
        source = MockDataSource()

        with pytest.raises(ValueError, match="不支持的日期格式"):
            source._normalize_date(12345)

    def test_validate_frequency(self):
        """测试频率验证"""
        source = MockDataSource()

        # 测试有效频率
        valid_frequencies = ["1m", "5m", "15m", "30m", "60m", "1d", "1w", "1y"]
        for freq in valid_frequencies:
            assert source._validate_frequency(freq) == freq

    def test_validate_frequency_invalid(self):
        """测试无效频率"""
        source = MockDataSource()

        with pytest.raises(ValueError, match="不支持的频率"):
            source._validate_frequency("10s")

        with pytest.raises(ValueError):
            source._validate_frequency("invalid")

    def test_get_capabilities(self):
        """测试获取能力信息"""
        source = MockDataSource("test_source")

        capabilities = source.get_capabilities()

        assert capabilities["name"] == "test_source"
        assert capabilities["enabled"] is True
        assert capabilities["supports_daily"] is True
        assert capabilities["supports_minute"] is True
        assert capabilities["rate_limit"] == 100
        assert "1d" in capabilities["supported_frequencies"]
        assert "SZ" in capabilities["supported_markets"]

    def test_check_rate_limit(self):
        """测试频率限制检查"""
        config = {"rate_limit": 3}  # 设置小限制便于测试
        source = MockDataSource("test_source", config)

        # 快速连续请求3次应该成功
        for _ in range(3):
            source._check_rate_limit()

        # 第4次应该会触发等待（通过检查request_count）
        assert source._request_count == 3

    @patch("time.sleep")
    def test_check_rate_limit_sleep(self, mock_sleep):
        """测试频率限制等待"""
        config = {"rate_limit": 2}
        source = MockDataSource("test_source", config)

        # 触发3次请求，第3次应该触发等待
        for _ in range(3):
            source._check_rate_limit()

        # 验证sleep被调用
        assert mock_sleep.called

    def test_retry_request_success(self):
        """测试请求重试成功"""
        source = MockDataSource()

        def successful_func():
            return "success"

        result = source._retry_request(successful_func)
        assert result == "success"

    def test_retry_request_failure_then_success(self):
        """测试重试后成功"""
        source = MockDataSource()
        call_count = [0]

        def failing_func():
            call_count[0] += 1
            if call_count[0] < 2:
                raise Exception("Temporary error")
            return "success"

        with patch("time.sleep"):  # Mock sleep加速测试
            result = source._retry_request(failing_func)

        assert result == "success"
        assert call_count[0] == 2  # 第一次失败，第二次成功

    def test_retry_request_all_failures(self):
        """测试所有重试都失败"""
        config = {"retry_times": 3}
        source = MockDataSource("test_source", config)

        def always_failing_func():
            raise Exception("Permanent error")

        with patch("time.sleep"):  # Mock sleep加速测试
            with pytest.raises(Exception, match="Permanent error"):
                source._retry_request(always_failing_func)

    def test_context_manager(self):
        """测试上下文管理器"""
        source = MockDataSource()

        assert source.is_connected() is False

        with source as src:
            assert src.connect_called is True
            assert src.is_connected() is True

        assert source.disconnect_called is True
        assert source.is_connected() is False

    def test_string_representation(self):
        """测试字符串表示"""
        source = MockDataSource("test_source")

        str_repr = str(source)
        assert "test_source" in str_repr
        assert "enabled=True" in str_repr

        repr_str = repr(source)
        assert "test_source" in repr_str

    def test_optional_methods_not_implemented(self):
        """测试可选方法未实现时抛出异常"""
        source = MockDataSource()

        # 测试交易日历
        with pytest.raises(NotImplementedError, match="不支持交易日历查询"):
            source.get_trade_calendar("2024-01-01", "2024-01-31")

        # 测试除权除息
        with pytest.raises(NotImplementedError, match="不支持除权除息数据查询"):
            source.get_adjustment_data("000001.SZ", "2024-01-01")

        # 测试估值数据
        with pytest.raises(NotImplementedError, match="不支持估值数据查询"):
            source.get_valuation_data("000001.SZ", "2024-01-01")


@pytest.mark.unit
class TestDataSourceExceptions:
    """数据源异常测试"""

    def test_data_source_error(self):
        """测试基础异常"""
        error = DataSourceError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)

    def test_connection_error(self):
        """测试连接异常"""
        error = DataSourceConnectionError("Connection failed")
        assert isinstance(error, DataSourceError)
        assert str(error) == "Connection failed"

    def test_data_error(self):
        """测试数据异常"""
        error = DataSourceDataError("Data invalid")
        assert isinstance(error, DataSourceError)
        assert str(error) == "Data invalid"
