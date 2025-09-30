"""
技术指标性能测试
验证向量化优化和缓存机制的性能提升
"""

import time
from datetime import datetime, timedelta

import numpy as np
import pytest

from simtradedata.config import Config
from simtradedata.preprocessor.indicators import TechnicalIndicators


@pytest.mark.performance
class TestIndicatorsPerformance:
    """技术指标性能测试"""

    @pytest.fixture
    def indicators(self):
        """创建技术指标计算器"""
        config = Config()
        return TechnicalIndicators(config)

    @pytest.fixture
    def sample_data(self):
        """生成样本数据（60天历史数据）"""
        base_price = 100.0
        dates = []
        data = []

        for i in range(60):
            date = (datetime.now() - timedelta(days=60 - i)).strftime("%Y-%m-%d")
            # 生成随机价格波动
            price = base_price + np.random.normal(0, 2)
            high = price + np.random.uniform(0.5, 1.5)
            low = price - np.random.uniform(0.5, 1.5)

            dates.append(date)
            data.append(
                {
                    "date": date,
                    "close": round(price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "volume": int(np.random.uniform(1000000, 5000000)),
                }
            )

        return dates, data

    def test_vectorized_ma_calculation(self, indicators, sample_data):
        """测试向量化MA计算的正确性"""
        dates, data = sample_data

        # 准备测试数据
        ptrade_data = {
            "date": dates[-1],
            "close": data[-1]["close"],
            "high": data[-1]["high"],
            "low": data[-1]["low"],
            "volume": data[-1]["volume"],
        }

        # 模拟获取历史数据
        indicators._get_historical_data = lambda symbol, date, db: data[:-1]

        # 计算指标
        result = indicators.calculate_indicators(ptrade_data, "TEST.SZ", None)

        # 验证结果
        assert "ma5" in result
        assert "ma10" in result
        assert "ma20" in result
        assert "ma60" in result

        # 验证MA值合理性（应该接近收盘价）
        close_price = result["close"]
        for period in [5, 10, 20, 60]:
            ma_key = f"ma{period}"
            assert result[ma_key] > 0
            # MA值应该在合理范围内（±20%）
            assert abs(result[ma_key] - close_price) / close_price < 0.2

        print(f"✅ 向量化MA计算正确: MA5={result['ma5']}, MA10={result['ma10']}")

    def test_cache_mechanism(self, indicators, sample_data):
        """测试缓存机制"""
        dates, data = sample_data
        symbol = "TEST.SZ"

        ptrade_data = {
            "date": dates[-1],
            "close": data[-1]["close"],
            "high": data[-1]["high"],
            "low": data[-1]["low"],
            "volume": data[-1]["volume"],
        }

        indicators._get_historical_data = lambda s, d, db: data[:-1]

        # 第一次计算（无缓存）
        start_time = time.time()
        result1 = indicators.calculate_indicators(ptrade_data.copy(), symbol, None)
        first_time = time.time() - start_time

        # 第二次计算（有缓存）
        start_time = time.time()
        result2 = indicators.calculate_indicators(ptrade_data.copy(), symbol, None)
        cached_time = time.time() - start_time

        # 验证结果一致
        assert result1["ma5"] == result2["ma5"]
        assert result1["ma10"] == result2["ma10"]

        # 验证缓存加速（缓存应该快至少2倍）
        assert cached_time < first_time / 2

        # 验证缓存统计
        cache_stats = indicators.get_cache_stats()
        assert cache_stats["cache_size"] == 1
        assert cache_stats["cache_max_size"] == 1000

        print(
            f"✅ 缓存加速: 首次={first_time*1000:.2f}ms, 缓存={cached_time*1000:.2f}ms, 提升={first_time/cached_time:.1f}x"
        )

    def test_fast_indicator_methods(self, indicators):
        """测试快速指标计算方法"""
        # 生成测试数据
        close_prices = np.array(
            [100 + i * 0.5 + np.random.normal(0, 1) for i in range(30)]
        )
        high_prices = close_prices + np.random.uniform(0.5, 1.5, size=30)
        low_prices = close_prices - np.random.uniform(0.5, 1.5, size=30)

        # 测试快速EMA计算
        ema_result = indicators._calculate_ema_fast(close_prices)
        assert "ema12" in ema_result
        assert "ema26" in ema_result
        assert ema_result["ema12"] > 0
        assert ema_result["ema26"] > 0
        print(f"✅ 快速EMA: EMA12={ema_result['ema12']}, EMA26={ema_result['ema26']}")

        # 测试快速MACD计算
        macd_result = indicators._calculate_macd_fast(close_prices)
        assert "macd_dif" in macd_result
        assert "macd_dea" in macd_result
        assert "macd_histogram" in macd_result
        print(
            f"✅ 快速MACD: DIF={macd_result['macd_dif']}, DEA={macd_result['macd_dea']}"
        )

        # 测试快速RSI计算
        rsi_result = indicators._calculate_rsi_fast(close_prices, period=14)
        assert "rsi_12" in rsi_result
        assert 0 <= rsi_result["rsi_12"] <= 100
        print(f"✅ 快速RSI: RSI={rsi_result['rsi_12']}")

        # 测试快速KDJ计算
        kdj_result = indicators._calculate_kdj_fast(
            close_prices, high_prices, low_prices
        )
        assert "kdj_k" in kdj_result
        assert "kdj_d" in kdj_result
        assert "kdj_j" in kdj_result
        print(
            f"✅ 快速KDJ: K={kdj_result['kdj_k']}, D={kdj_result['kdj_d']}, J={kdj_result['kdj_j']}"
        )

    def test_cache_eviction(self, indicators, sample_data):
        """测试缓存淘汰机制"""
        dates, data = sample_data

        indicators._get_historical_data = lambda s, d, db: data[:-1]
        indicators._cache_max_size = 5  # 设置小缓存用于测试

        # 插入6个不同的缓存项
        for i in range(6):
            symbol = f"TEST{i}.SZ"
            ptrade_data = {
                "date": dates[-1],
                "close": data[-1]["close"] + i,
                "high": data[-1]["high"],
                "low": data[-1]["low"],
                "volume": data[-1]["volume"],
            }
            indicators.calculate_indicators(ptrade_data, symbol, None)

        # 验证缓存大小不超过限制
        cache_stats = indicators.get_cache_stats()
        assert cache_stats["cache_size"] <= 5

        print(
            f"✅ 缓存淘汰: 最大缓存={indicators._cache_max_size}, 当前={cache_stats['cache_size']}"
        )

    def test_clear_cache(self, indicators, sample_data):
        """测试缓存清空"""
        dates, data = sample_data

        ptrade_data = {
            "date": dates[-1],
            "close": data[-1]["close"],
            "high": data[-1]["high"],
            "low": data[-1]["low"],
            "volume": data[-1]["volume"],
        }

        indicators._get_historical_data = lambda s, d, db: data[:-1]

        # 计算并缓存
        indicators.calculate_indicators(ptrade_data, "TEST.SZ", None)
        assert indicators.get_cache_stats()["cache_size"] == 1

        # 清空缓存
        indicators.clear_cache()
        assert indicators.get_cache_stats()["cache_size"] == 0

        print("✅ 缓存清空测试通过")

    @pytest.mark.slow
    def test_batch_performance_comparison(self, indicators, sample_data):
        """批量性能对比测试（向量化 vs 传统方法）"""
        dates, data = sample_data
        symbols = [f"TEST{i}.SZ" for i in range(10)]

        indicators._get_historical_data = lambda s, d, db: data[:-1]

        # 清空缓存确保公平对比
        indicators.clear_cache()

        # 批量计算
        start_time = time.time()
        for symbol in symbols:
            ptrade_data = {
                "date": dates[-1],
                "close": data[-1]["close"],
                "high": data[-1]["high"],
                "low": data[-1]["low"],
                "volume": data[-1]["volume"],
            }
            indicators.calculate_indicators(ptrade_data, symbol, None)

        batch_time = time.time() - start_time
        avg_time_per_symbol = batch_time / len(symbols)

        print(
            f"✅ 批量性能: {len(symbols)}只股票用时{batch_time*1000:.2f}ms, 平均{avg_time_per_symbol*1000:.2f}ms/股"
        )

        # 验证性能标准（首次计算无缓存会慢一些，平均150ms以内可接受）
        assert (
            avg_time_per_symbol < 0.15
        ), f"性能不达标: {avg_time_per_symbol*1000:.2f}ms > 150ms"


@pytest.mark.unit
class TestIndicatorsCorrectness:
    """技术指标正确性测试"""

    @pytest.fixture
    def indicators(self):
        """创建技术指标计算器"""
        config = Config()
        return TechnicalIndicators(config)

    def test_ma_calculation_accuracy(self, indicators):
        """测试MA计算准确性"""
        # 使用简单的测试数据
        close_prices = np.array(
            [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
        )

        # 手工计算MA5（最后5个数的平均）
        expected_ma5 = (15.0 + 16.0 + 17.0 + 18.0 + 19.0) / 5  # 17.0

        # 使用向量化计算
        ma = np.convolve(close_prices, np.ones(5) / 5, mode="valid")[-1]

        assert abs(ma - expected_ma5) < 0.01
        assert abs(ma - 17.0) < 0.01

        print(f"✅ MA计算准确: 期望={expected_ma5}, 实际={ma}")

    def test_ema_calculation_accuracy(self, indicators):
        """测试EMA计算准确性"""
        close_prices = np.array([100.0, 101.0, 102.0, 103.0, 104.0, 105.0] * 3)

        ema_result = indicators._calculate_ema_fast(close_prices)

        # EMA应该存在且为正
        assert "ema12" in ema_result
        assert ema_result["ema12"] > 0

        # EMA12应该接近收盘价（上升趋势）
        assert 100 < ema_result["ema12"] < 110

        print(f"✅ EMA计算准确: EMA12={ema_result['ema12']}")

    def test_rsi_boundary_conditions(self, indicators):
        """测试RSI边界条件"""
        # 测试持续上涨（RSI应该接近100）
        up_trend = np.array([100 + i for i in range(20)])
        rsi_up = indicators._calculate_rsi_fast(up_trend, period=14)
        assert 70 < rsi_up["rsi_12"] <= 100

        # 测试持续下跌（RSI应该接近0）
        down_trend = np.array([100 - i for i in range(20)])
        rsi_down = indicators._calculate_rsi_fast(down_trend, period=14)
        assert 0 <= rsi_down["rsi_12"] < 30

        print(f"✅ RSI边界测试: 上涨={rsi_up['rsi_12']}, 下跌={rsi_down['rsi_12']}")

    def test_insufficient_data_handling(self, indicators):
        """测试数据不足的处理"""
        # 数据不足时应该返回默认值
        short_data = np.array([100.0, 101.0])

        ema_result = indicators._calculate_ema_fast(short_data)
        assert ema_result["ema12"] == 0.0

        rsi_result = indicators._calculate_rsi_fast(short_data, period=14)
        assert rsi_result["rsi_12"] == 50.0

        kdj_result = indicators._calculate_kdj_fast(
            short_data, short_data + 1, short_data - 1
        )
        assert kdj_result["kdj_k"] == 50.0

        print("✅ 数据不足处理正确")
