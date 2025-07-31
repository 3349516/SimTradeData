"""
同步系统功能完整集成测试

测试数据同步系统的核心功能，包括增量同步、缺口检测、
数据验证、同步管理、错误恢复等全方位功能。
使用真实的项目组件进行测试。
"""

import logging
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

import pytest

from simtradedata.config import Config
from simtradedata.data_sources import DataSourceManager
from simtradedata.database import DatabaseManager
from simtradedata.preprocessor import DataProcessingEngine
from simtradedata.sync import DataValidator, GapDetector, IncrementalSync, SyncManager

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def temp_sync_db():
    """创建临时同步测试数据库"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    config = Config()
    db_manager = DatabaseManager(db_path, config=config)

    # 创建必要的测试表
    _create_sync_test_tables(db_manager)

    # 插入基础测试数据
    _insert_sync_test_data(db_manager)

    yield db_manager

    # 清理
    db_manager.close()
    Path(db_path).unlink(missing_ok=True)


def _create_sync_test_tables(db_manager):
    """创建同步测试所需的表"""
    # 创建股票信息表 - 使用正确的表名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS stocks (
            symbol TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            market TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            list_date DATE,
            industry_l1 TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # 创建市场数据表 - 使用正确的字段名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            frequency TEXT NOT NULL DEFAULT '1d',
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            prev_close REAL,
            change_percent REAL,
            turnover_rate REAL,
            quality_score INTEGER DEFAULT 100,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date, frequency)
        )
    """
    )

    # 创建交易日历表 - 使用正确的表名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_calendar (
            date DATE NOT NULL,
            market TEXT NOT NULL,
            is_trading BOOLEAN NOT NULL,
            PRIMARY KEY (date, market)
        )
    """
    )

    # 创建同步状态表 - 使用正确的字段名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_status (
            symbol TEXT NOT NULL,
            frequency TEXT NOT NULL,
            last_sync_date DATE,
            last_data_date DATE,
            status TEXT DEFAULT 'pending',
            error_message TEXT,
            total_records INTEGER DEFAULT 0,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, frequency)
        )
    """
    )


def _insert_sync_test_data(db_manager):
    """插入同步测试数据"""
    # 插入股票信息 - 使用正确的表名和字段名
    stocks = [
        ("000001.SZ", "平安银行", "SZ", "active", "1991-04-03", "银行"),
        ("000002.SZ", "万科A", "SZ", "active", "1991-01-29", "房地产"),
        ("600000.SS", "浦发银行", "SS", "active", "1999-11-10", "银行"),
    ]

    db_manager.executemany(
        "INSERT OR REPLACE INTO stocks (symbol, name, market, status, list_date, industry_l1) VALUES (?, ?, ?, ?, ?, ?)",
        stocks,
    )

    # 插入交易日历数据（简化版：工作日为交易日）
    start_date = date(2024, 1, 1)
    for i in range(60):  # 2个月的日历
        current_date = start_date + timedelta(days=i)
        is_trading = current_date.weekday() < 5  # 周一到周五

        db_manager.execute(
            "INSERT OR REPLACE INTO trading_calendar (date, market, is_trading) VALUES (?, ?, ?)",
            (str(current_date), "CN", is_trading),
        )

    # 插入一些历史数据（有意留一些缺口用于测试）- 使用正确的字段名
    market_data = [
        (
            "000001.SZ",
            "2024-01-15",
            "1d",
            10.0,
            10.5,
            9.8,
            10.2,
            1000000,
            10200000,
            10.0,
            2.0,
            5.5,
            95,
            "test",
        ),
        (
            "000001.SZ",
            "2024-01-16",
            "1d",
            10.2,
            10.8,
            10.0,
            10.5,
            1200000,
            12600000,
            10.2,
            2.9,
            6.2,
            90,
            "test",
        ),
        # 故意跳过 2024-01-17 创建缺口
        (
            "000001.SZ",
            "2024-01-18",
            "1d",
            10.5,
            11.0,
            10.3,
            10.8,
            1500000,
            16200000,
            10.5,
            2.9,
            7.8,
            85,
            "test",
        ),
        (
            "000002.SZ",
            "2024-01-15",
            "1d",
            8.0,
            8.3,
            7.8,
            8.1,
            800000,
            6480000,
            8.0,
            1.3,
            4.2,
            92,
            "test",
        ),
        (
            "000002.SZ",
            "2024-01-16",
            "1d",
            8.1,
            8.5,
            7.9,
            8.3,
            900000,
            7470000,
            8.1,
            2.5,
            4.8,
            88,
            "test",
        ),
    ]

    db_manager.executemany(
        "INSERT OR REPLACE INTO market_data (symbol, date, frequency, open, high, low, close, volume, amount, prev_close, change_percent, turnover_rate, quality_score, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        market_data,
    )


@pytest.mark.integration
class TestSyncSystemIntegration:
    """同步系统集成测试"""

    def test_incremental_sync_basic_functionality(self, temp_sync_db):
        """测试增量同步基本功能"""
        logger.info("🧪 测试增量同步基本功能...")

        config = Config()
        data_source_manager = DataSourceManager(config)
        processing_engine = DataProcessingEngine(temp_sync_db, config)

        # 创建增量同步器
        incremental_sync = IncrementalSync(
            temp_sync_db, data_source_manager, processing_engine, config
        )

        # 测试获取最后数据日期
        last_date = incremental_sync.get_last_data_date("000001.SZ", "1d")
        assert last_date == date(2024, 1, 18)  # 最新的数据日期

        # 测试计算同步范围
        start_date, end_date = incremental_sync.calculate_sync_range(
            "000001.SZ", date(2024, 1, 20)
        )
        assert start_date == date(2024, 1, 19)  # 最后日期的下一天
        assert end_date == date(2024, 1, 20)

        # 测试无历史数据的股票
        last_date_new = incremental_sync.get_last_data_date("999999.SZ", "1d")
        assert last_date_new is None

        # 测试获取活跃股票列表
        stats = incremental_sync.get_sync_stats()
        assert isinstance(stats, dict)

        logger.info("✅ 增量同步基本功能测试通过")

    def test_gap_detector_functionality(self, temp_sync_db):
        """测试缺口检测器功能"""
        logger.info("🧪 测试缺口检测器功能...")

        config = Config()
        gap_detector = GapDetector(temp_sync_db, config)

        # 检测000001.SZ的缺口（我们故意跳过了2024-01-17）
        gaps = gap_detector.detect_symbol_gaps(
            "000001.SZ", date(2024, 1, 15), date(2024, 1, 18), "1d"
        )

        # 应该检测到2024-01-17的缺口
        assert len(gaps) > 0

        # 检查缺口类型
        date_gaps = [gap for gap in gaps if gap["gap_type"] == "date_missing"]
        assert len(date_gaps) > 0

        # 检查具体的缺口日期 - 修复字段访问
        gap_found = False
        for gap in date_gaps:
            if "2024-01-17" in gap["start_date"] or "2024-01-17" in gap["end_date"]:
                gap_found = True
                break

        assert gap_found, f"未找到2024-01-17的缺口，实际缺口: {date_gaps}"

        logger.info(f"检测到缺口: {date_gaps}")
        logger.info("✅ 缺口检测器功能测试通过")

    def test_data_validator_functionality(self, temp_sync_db):
        """测试数据验证器功能"""
        logger.info("🧪 测试数据验证器功能...")

        # 插入一些异常数据用于测试 - 使用正确的字段名
        problematic_data = [
            # 异常数据：开盘价为0
            (
                "600000.SS",
                "2024-01-15",
                "1d",
                0.0,
                12.5,
                11.8,
                12.2,
                1000000,
                12200000,
                12.0,
                1.7,
                4.5,
                30,
                "test",
            ),
            # 异常数据：负成交量
            (
                "600000.SS",
                "2024-01-16",
                "1d",
                12.2,
                12.8,
                12.0,
                12.5,
                -500000,
                12500000,
                12.2,
                2.5,
                4.8,
                20,
                "test",
            ),
            # 正常数据
            (
                "600000.SS",
                "2024-01-17",
                "1d",
                12.5,
                13.0,
                12.3,
                12.8,
                1100000,
                14080000,
                12.5,
                2.4,
                5.2,
                95,
                "test",
            ),
        ]

        temp_sync_db.executemany(
            "INSERT OR REPLACE INTO market_data (symbol, date, frequency, open, high, low, close, volume, amount, prev_close, change_percent, turnover_rate, quality_score, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            problematic_data,
        )

        config = Config()
        validator = DataValidator(temp_sync_db, config)

        # 验证数据
        validation_result = validator.validate_symbol_data(
            "600000.SS", date(2024, 1, 15), date(2024, 1, 17), "1d"
        )

        # 验证结果
        assert validation_result["symbol"] == "600000.SS"
        assert validation_result["total_records"] == 3
        assert validation_result["invalid_records"] >= 2  # 应该检测到至少2条异常数据
        assert len(validation_result["issues"]) >= 2

        logger.info(
            f"验证结果: 总记录{validation_result['total_records']}, 异常记录{validation_result['invalid_records']}, 问题{len(validation_result['issues'])}个"
        )
        logger.info("✅ 数据验证器功能测试通过")

    def test_sync_manager_comprehensive_workflow(self, temp_sync_db):
        """测试同步管理器综合工作流程"""
        logger.info("🧪 测试同步管理器综合工作流程...")

        config = Config()
        data_source_manager = DataSourceManager(config)
        processing_engine = DataProcessingEngine(temp_sync_db, config)

        # 创建同步管理器
        sync_manager = SyncManager(
            temp_sync_db, data_source_manager, processing_engine, config
        )

        # 测试获取同步状态
        status_response = sync_manager.get_sync_status()
        assert status_response["success"] == True
        assert "recent_syncs" in status_response["data"]
        assert "data_stats" in status_response["data"]

        # 验证数据统计
        data_stats = status_response["data"]["data_stats"]
        assert data_stats["total_records"] > 0  # 我们插入了测试数据
        assert data_stats["total_symbols"] >= 2  # 至少有2个股票有数据

        logger.info(f"同步状态: {status_response['data']['data_stats']}")
        logger.info("✅ 同步管理器综合工作流程测试通过")

    def test_database_operations(self, temp_sync_db):
        """测试数据库操作"""
        logger.info("🧪 测试数据库操作...")

        # 测试查询股票信息 - 使用正确的表名
        stocks = temp_sync_db.fetchall("SELECT * FROM stocks")
        assert len(stocks) >= 3

        # 测试查询市场数据
        market_data = temp_sync_db.fetchall("SELECT * FROM market_data")
        assert len(market_data) >= 5

        # 测试查询交易日历 - 使用正确的表名
        calendar_data = temp_sync_db.fetchall(
            "SELECT * FROM trading_calendar WHERE date BETWEEN ? AND ?",
            ("2024-01-15", "2024-01-20"),
        )
        assert len(calendar_data) > 0

        # 测试数据完整性
        for stock in stocks:
            assert stock["symbol"] is not None
            assert stock["name"] is not None
            assert stock["market"] is not None

        for data in market_data:
            assert data["symbol"] is not None
            assert data["date"] is not None  # 使用正确的字段名
            assert data["close"] is not None

        logger.info("✅ 数据库操作测试通过")

    def test_sync_error_handling(self, temp_sync_db):
        """测试同步错误处理"""
        logger.info("🧪 测试同步错误处理...")

        config = Config()
        data_source_manager = DataSourceManager(config)
        processing_engine = DataProcessingEngine(temp_sync_db, config)

        # 创建增量同步器
        incremental_sync = IncrementalSync(
            temp_sync_db, data_source_manager, processing_engine, config
        )

        # 测试不存在的股票
        last_date = incremental_sync.get_last_data_date("INVALID.SZ", "1d")
        assert last_date is None

        # 测试无效日期范围
        start_date, end_date = incremental_sync.calculate_sync_range(
            "INVALID.SZ", date(2024, 1, 20)
        )
        # 应该返回默认范围或None
        assert end_date == date(2024, 1, 20)

        logger.info("✅ 同步错误处理测试通过")

    def test_performance_basic(self, temp_sync_db):
        """测试基本性能"""
        logger.info("🧪 测试基本性能...")

        # 测试数据库查询性能
        start_time = time.time()

        # 执行复杂查询 - 使用正确的表名和字段名
        result = temp_sync_db.fetchall(
            """
            SELECT 
                s.symbol,
                s.name,
                COUNT(m.date) as data_days,
                AVG(m.close) as avg_close,
                MAX(m.high) as max_high,
                MIN(m.low) as min_low
            FROM stocks s
            LEFT JOIN market_data m ON s.symbol = m.symbol
            WHERE s.status = 'active'
            GROUP BY s.symbol, s.name
            ORDER BY data_days DESC
        """
        )

        end_time = time.time()
        query_time = end_time - start_time

        logger.info(f"复杂查询耗时: {query_time:.3f}秒")
        logger.info(f"查询结果条数: {len(result)}")

        # 基本性能要求
        assert query_time < 1.0  # 查询应在1秒内完成
        assert len(result) > 0

        logger.info("✅ 基本性能测试通过")


@pytest.mark.integration
def test_sync_system_full_integration():
    """同步系统完整集成测试"""
    logger.info("🚀 开始同步系统完整集成测试...")

    # 创建测试环境
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        config = Config()
        db_manager = DatabaseManager(db_path, config=config)

        # 创建完整的表结构
        _create_sync_test_tables(db_manager)
        _insert_sync_test_data(db_manager)

        # 创建组件
        data_source_manager = DataSourceManager(config)
        processing_engine = DataProcessingEngine(db_manager, config)

        # 测试增量同步器
        incremental_sync = IncrementalSync(
            db_manager, data_source_manager, processing_engine, config
        )

        # 测试获取最后数据日期
        last_date = incremental_sync.get_last_data_date("000001.SZ", "1d")
        assert last_date == date(2024, 1, 18)

        # 测试计算同步范围
        start_date, end_date = incremental_sync.calculate_sync_range(
            "000001.SZ", date(2024, 1, 20)
        )
        assert start_date == date(2024, 1, 19)
        assert end_date == date(2024, 1, 20)

        # 测试缺口检测器
        gap_detector = GapDetector(db_manager, config)
        gaps = gap_detector.detect_symbol_gaps(
            "000001.SZ", date(2024, 1, 15), date(2024, 1, 18), "1d"
        )
        assert len(gaps) > 0  # 应该检测到2024-01-17的缺口

        # 测试数据验证器
        validator = DataValidator(db_manager, config)

        # 插入一些异常数据进行验证测试 - 使用正确的字段名
        db_manager.execute(
            "INSERT OR REPLACE INTO market_data (symbol, date, frequency, open, high, low, close, volume, amount, prev_close, change_percent, turnover_rate, quality_score, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "TEST.SZ",
                "2024-01-15",
                "1d",
                0.0,
                10.5,
                9.5,
                10.0,
                -1000,
                10000,
                10.0,
                0.0,
                0.0,
                10,
                "test",
            ),
        )

        validation_result = validator.validate_symbol_data(
            "TEST.SZ", date(2024, 1, 15), date(2024, 1, 15), "1d"
        )
        assert validation_result["symbol"] == "TEST.SZ"
        assert validation_result["invalid_records"] > 0  # 异常数据

        # 测试同步管理器
        sync_manager = SyncManager(
            db_manager, data_source_manager, processing_engine, config
        )

        # 测试获取同步状态
        status = sync_manager.get_sync_status()
        assert status["success"] == True
        assert status["data"]["data_stats"]["total_records"] > 0

        # 验证最终数据库状态 - 使用正确的字段名
        final_stats = db_manager.fetchone(
            """
            SELECT 
                COUNT(DISTINCT symbol) as total_symbols,
                COUNT(*) as total_records,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM market_data
        """
        )

        logger.info(f"最终数据库统计: {dict(final_stats)}")

        assert final_stats["total_symbols"] >= 3
        assert final_stats["total_records"] > 0

        logger.info("🎉 同步系统完整集成测试通过!")

    finally:
        # 清理
        if "db_manager" in locals():
            db_manager.close()
        Path(db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    # 运行完整集成测试
    test_sync_system_full_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
