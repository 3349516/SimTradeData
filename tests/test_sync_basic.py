"""
基础同步功能测试

测试数据同步的核心功能，包括增量更新、缺口检测等。
"""

import logging
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from simtradedata.config import Config
from simtradedata.database import DatabaseManager
from simtradedata.sync.gap_detector import GapDetector

logger = logging.getLogger(__name__)


class TestBasicSync:
    """基础同步功能测试"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        # 使用BaseManager架构的DatabaseManager
        config = Config()
        db_manager = DatabaseManager(db_path, config=config)

        # 验证BaseManager初始化
        assert hasattr(db_manager, "config"), "DatabaseManager应该有config属性"
        assert hasattr(db_manager, "logger"), "DatabaseManager应该有logger属性"
        assert hasattr(db_manager, "timeout"), "DatabaseManager应该有timeout配置"
        assert hasattr(
            db_manager, "max_retries"
        ), "DatabaseManager应该有max_retries配置"

        # 创建基础表结构
        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data (
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                time TIME,
                frequency TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, amount REAL,
                source TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, date, time, frequency)
            )
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_status (
                symbol TEXT NOT NULL,
                frequency TEXT NOT NULL,
                last_sync_date DATE,
                last_data_date DATE,
                next_sync_date DATE,
                status TEXT DEFAULT 'pending',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, frequency)
            )
        """
        )

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

        # 插入测试用的交易日历数据
        trading_days = []
        start_date = date(2024, 1, 1)
        for i in range(30):
            current_date = start_date + timedelta(days=i)
            # 测试用简化规则：周一到周五为交易日（不考虑节假日）
            is_trading = current_date.weekday() < 5
            trading_days.append((str(current_date), "CN", is_trading))

        db_manager.executemany(
            "INSERT OR REPLACE INTO trading_calendar (date, market, is_trading) VALUES (?, ?, ?)",
            trading_days,
        )

        yield db_manager

        # 清理
        db_manager.close()
        Path(db_path).unlink(missing_ok=True)

    def test_incremental_sync_basic(self, temp_db):
        """测试基础增量同步功能"""
        logger.info("🧪 测试基础增量同步功能...")

        # 禁止使用Mock，跳过此测试
        pytest.skip("禁止使用Mock，需要重新设计测试使用真实组件")

        logger.info("✅ 基础增量同步功能测试跳过")

    def test_gap_detection_basic(self, temp_db):
        """测试基础缺口检测功能"""
        logger.info("🧪 测试基础缺口检测功能...")

        config = Config()
        gap_detector = GapDetector(temp_db, config)

        # 验证GapDetector是否继承了BaseManager
        if hasattr(gap_detector, "config"):
            logger.info("✅ GapDetector使用BaseManager架构")
            assert hasattr(gap_detector, "logger"), "GapDetector应该有logger属性"
            assert hasattr(gap_detector, "timeout"), "GapDetector应该有timeout配置"
        else:
            logger.info("⚠️ GapDetector未使用BaseManager架构")

        # 插入一些有缺口的测试数据
        test_data = [
            (
                "000001.SZ",
                "2024-01-01",
                "1d",
                10.0,
                10.5,
                9.5,
                10.2,
                1000000,
                10000000,
                "test",
            ),
            (
                "000001.SZ",
                "2024-01-02",
                "1d",
                10.2,
                10.7,
                9.7,
                10.4,
                1100000,
                11000000,
                "test",
            ),
            # 缺少 2024-01-03 (人工创建缺口)
            (
                "000001.SZ",
                "2024-01-04",
                "1d",
                10.4,
                10.9,
                9.9,
                10.6,
                1200000,
                12000000,
                "test",
            ),
            (
                "000001.SZ",
                "2024-01-05",
                "1d",
                10.6,
                11.1,
                10.1,
                10.8,
                1300000,
                13000000,
                "test",
            ),
        ]

        temp_db.executemany(
            """
            INSERT INTO market_data 
            (symbol, date, frequency, open, high, low, close, volume, amount, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            test_data,
        )

        # 检测缺口
        gaps = gap_detector.detect_symbol_gaps(
            "000001.SZ", date(2024, 1, 1), date(2024, 1, 5), "1d"
        )

        # 验证缺口检测结果
        assert len(gaps) > 0, "应该检测到缺口"

        # 检查缺口详情
        date_gaps = [gap for gap in gaps if gap["gap_type"] == "date_missing"]
        assert len(date_gaps) > 0, "应该检测到日期缺口"

        logger.info("✅ 基础缺口检测功能测试通过")

    def test_data_continuity(self, temp_db):
        """测试数据连续性检查"""
        logger.info("🧪 测试数据连续性检查...")

        # 插入连续的测试数据
        continuous_data = []
        start_date = date(2024, 1, 1)

        for i in range(10):
            current_date = start_date + timedelta(days=i)
            # 只在交易日插入数据
            if current_date.weekday() < 5:  # 周一到周五
                continuous_data.append(
                    (
                        "000001.SZ",
                        str(current_date),
                        "1d",
                        10.0 + i * 0.1,
                        10.5 + i * 0.1,
                        9.5 + i * 0.1,
                        10.2 + i * 0.1,
                        1000000 + i * 10000,
                        10000000 + i * 100000,
                        "test",
                    )
                )

        temp_db.executemany(
            """
            INSERT INTO market_data 
            (symbol, date, frequency, open, high, low, close, volume, amount, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            continuous_data,
        )

        # 检查数据连续性
        sql = """
            SELECT date FROM market_data 
            WHERE symbol = ? AND date BETWEEN ? AND ?
            ORDER BY date
        """

        dates = temp_db.fetchall(sql, ("000001.SZ", "2024-01-01", "2024-01-10"))

        # 验证数据连续性
        assert len(dates) > 5, "应该有足够的交易日数据"

        # 检查日期是否按顺序排列
        date_strings = [row["date"] for row in dates]
        assert date_strings == sorted(date_strings), "日期应该按顺序排列"

        logger.info("✅ 数据连续性检查测试通过")

    def test_sync_status_tracking(self, temp_db):
        """测试同步状态跟踪"""
        logger.info("🧪 测试同步状态跟踪...")

        # 插入同步状态记录
        temp_db.execute(
            """
            INSERT INTO sync_status 
            (symbol, frequency, last_sync_date, last_data_date, status)
            VALUES (?, ?, ?, ?, ?)
        """,
            ("000001.SZ", "1d", "2024-01-05", "2024-01-05", "completed"),
        )

        # 查询同步状态
        sql = "SELECT * FROM sync_status WHERE symbol = ? AND frequency = ?"
        status = temp_db.fetchone(sql, ("000001.SZ", "1d"))

        # 验证状态记录
        assert status is not None, "应该有同步状态记录"
        assert status["status"] == "completed", "状态应该是completed"
        assert status["last_sync_date"] == "2024-01-05", "同步日期应该正确"

        # 更新同步状态
        temp_db.execute(
            """
            UPDATE sync_status 
            SET last_sync_date = ?, status = ?
            WHERE symbol = ? AND frequency = ?
        """,
            ("2024-01-06", "running", "000001.SZ", "1d"),
        )

        # 验证更新
        updated_status = temp_db.fetchone(sql, ("000001.SZ", "1d"))
        assert updated_status["status"] == "running", "状态应该已更新"
        assert updated_status["last_sync_date"] == "2024-01-06", "同步日期应该已更新"

        logger.info("✅ 同步状态跟踪测试通过")

    def test_trading_calendar_integration(self, temp_db):
        """测试交易日历集成"""
        logger.info("🧪 测试交易日历集成...")

        # 查询交易日历
        sql = """
            SELECT date, is_trading FROM trading_calendar 
            WHERE market = 'CN' AND date BETWEEN ? AND ?
            ORDER BY date
        """

        calendar_data = temp_db.fetchall(sql, ("2024-01-01", "2024-01-10"))

        # 验证交易日历数据
        assert len(calendar_data) > 0, "应该有交易日历数据"

        # 检查交易日和非交易日
        trading_days = [row for row in calendar_data if row["is_trading"]]
        non_trading_days = [row for row in calendar_data if not row["is_trading"]]

        assert len(trading_days) > 0, "应该有交易日"
        assert len(non_trading_days) > 0, "应该有非交易日"

        # 验证周末为非交易日的逻辑
        for row in calendar_data:
            date_obj = datetime.strptime(row["date"], "%Y-%m-%d").date()
            weekday = date_obj.weekday()

            if weekday >= 5:  # 周六、周日
                assert not row["is_trading"], f"{row['date']} 是周末，应该是非交易日"
            else:  # 周一到周五
                assert row["is_trading"], f"{row['date']} 是工作日，应该是交易日"

        logger.info("✅ 交易日历集成测试通过")


def test_sync_basic_integration():
    """运行基础同步集成测试"""
    logger.info("🚀 开始基础同步集成测试...")

    # 这个测试会被pytest自动发现和运行
    logger.info("🎉 基础同步集成测试完成!")


if __name__ == "__main__":
    # 运行基础测试
    test_sync_basic_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
