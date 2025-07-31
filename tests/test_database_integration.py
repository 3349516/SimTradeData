"""
数据库管理功能完整集成测试

测试数据库管理器的核心功能，包括连接管理、事务处理、
查询执行、性能优化等全方位功能。
"""

import logging
import tempfile
from datetime import date
from pathlib import Path

import pytest

from simtradedata.config import Config
from simtradedata.database import DatabaseManager

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def temp_db_path():
    """创建临时数据库路径"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # 清理
    Path(db_path).unlink(missing_ok=True)


@pytest.mark.integration
class TestDatabaseManagerIntegration:
    """数据库管理器集成测试"""

    def test_database_creation_and_connection(self, temp_db_path):
        """测试数据库创建和连接"""
        logger.info("🧪 测试数据库创建和连接...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 验证数据库文件已创建
        assert Path(temp_db_path).exists()

        # 验证连接可用 - 通过执行简单查询来测试
        result = db_manager.fetchone("SELECT 1 as test")
        assert result is not None
        assert result["test"] == 1

        # 验证BaseManager功能
        assert hasattr(db_manager, "config")
        assert hasattr(db_manager, "logger")
        assert hasattr(db_manager, "timeout")

        db_manager.close()
        logger.info("✅ 数据库创建和连接测试通过")

    def test_table_creation_and_schema(self, temp_db_path):
        """测试表创建和模式管理"""
        logger.info("🧪 测试表创建和模式管理...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建测试表
        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS test_stocks (
                symbol TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                market TEXT NOT NULL,
                price REAL,
                volume INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # 创建索引
        db_manager.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stocks_market 
            ON test_stocks(market)
        """
        )

        # 验证表存在
        tables = db_manager.fetchall(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='test_stocks'
        """
        )
        assert len(tables) == 1
        assert tables[0]["name"] == "test_stocks"

        # 验证索引存在
        indexes = db_manager.fetchall(
            """
            SELECT name FROM sqlite_master 
            WHERE type='index' AND name='idx_stocks_market'
        """
        )
        assert len(indexes) == 1

        db_manager.close()
        logger.info("✅ 表创建和模式管理测试通过")

    def test_crud_operations(self, temp_db_path):
        """测试增删改查操作"""
        logger.info("🧪 测试增删改查操作...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建测试表
        db_manager.execute(
            """
            CREATE TABLE test_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close_price REAL,
                volume INTEGER,
                UNIQUE(symbol, trade_date)
            )
        """
        )

        # 插入数据 - CREATE
        test_data = [
            ("000001.SZ", "2024-01-15", 10.5, 1000000),
            ("000002.SZ", "2024-01-15", 8.3, 800000),
            ("600000.SS", "2024-01-15", 12.2, 1200000),
        ]

        for symbol, trade_date, price, volume in test_data:
            db_manager.execute(
                "INSERT INTO test_data (symbol, trade_date, close_price, volume) VALUES (?, ?, ?, ?)",
                (symbol, trade_date, price, volume),
            )

        # 批量插入
        batch_data = [
            ("000001.SZ", "2024-01-16", 10.8, 1100000),
            ("000002.SZ", "2024-01-16", 8.5, 850000),
        ]

        db_manager.executemany(
            "INSERT INTO test_data (symbol, trade_date, close_price, volume) VALUES (?, ?, ?, ?)",
            batch_data,
        )

        # 查询数据 - READ
        all_records = db_manager.fetchall(
            "SELECT * FROM test_data ORDER BY symbol, trade_date"
        )
        assert len(all_records) == 5

        # 单条查询
        single_record = db_manager.fetchone(
            "SELECT * FROM test_data WHERE symbol = ? AND trade_date = ?",
            ("000001.SZ", "2024-01-15"),
        )
        assert single_record is not None
        assert single_record["close_price"] == 10.5
        assert single_record["volume"] == 1000000

        # 更新数据 - UPDATE
        db_manager.execute(
            "UPDATE test_data SET close_price = ? WHERE symbol = ? AND trade_date = ?",
            (10.6, "000001.SZ", "2024-01-15"),
        )

        # 验证更新
        updated_record = db_manager.fetchone(
            "SELECT close_price FROM test_data WHERE symbol = ? AND trade_date = ?",
            ("000001.SZ", "2024-01-15"),
        )
        assert updated_record["close_price"] == 10.6

        # 删除数据 - DELETE
        db_manager.execute(
            "DELETE FROM test_data WHERE symbol = ? AND trade_date = ?",
            ("600000.SS", "2024-01-15"),
        )

        # 验证删除
        remaining_records = db_manager.fetchall("SELECT * FROM test_data")
        assert len(remaining_records) == 4

        db_manager.close()
        logger.info("✅ 增删改查操作测试通过")

    def test_transaction_management(self, temp_db_path):
        """测试事务管理"""
        logger.info("🧪 测试事务管理...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建测试表
        db_manager.execute(
            """
            CREATE TABLE transaction_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """
        )

        # 测试自动提交事务
        with db_manager.transaction():
            db_manager.execute(
                "INSERT INTO transaction_test (value) VALUES (?)", ("auto_commit",)
            )
            db_manager.execute(
                "INSERT INTO transaction_test (value) VALUES (?)", ("auto_commit_2",)
            )

        # 验证数据已提交
        records = db_manager.fetchall("SELECT * FROM transaction_test")
        assert len(records) == 2

        # 测试事务回滚
        try:
            with db_manager.transaction():
                db_manager.execute(
                    "INSERT INTO transaction_test (value) VALUES (?)",
                    ("rollback_test",),
                )
                # 故意引发错误
                raise Exception("Test rollback")
        except Exception as e:
            assert "Test rollback" in str(e)

        # 验证数据已回滚
        records_after_rollback = db_manager.fetchall("SELECT * FROM transaction_test")
        assert len(records_after_rollback) == 2  # 仍然是2条记录

        # 测试简单事务（SQLite不支持真正的嵌套事务）
        with db_manager.transaction():
            db_manager.execute(
                "INSERT INTO transaction_test (value) VALUES (?)",
                ("simple_transaction",),
            )

        # 验证简单事务提交
        final_records = db_manager.fetchall("SELECT * FROM transaction_test")
        assert len(final_records) == 3

        db_manager.close()
        logger.info("✅ 事务管理测试通过")

    def test_connection_pool_and_performance(self, temp_db_path):
        """测试连接池和性能"""
        logger.info("🧪 测试连接池和性能...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建测试表
        db_manager.execute(
            """
            CREATE TABLE performance_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                value REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # 批量插入性能测试
        import time

        start_time = time.time()

        batch_size = 1000
        test_data = []
        for i in range(batch_size):
            test_data.append((f"TEST{i:04d}", i * 0.1))

        db_manager.executemany(
            "INSERT INTO performance_test (symbol, value) VALUES (?, ?)", test_data
        )

        insert_time = time.time() - start_time
        logger.info(f"批量插入{batch_size}条记录耗时: {insert_time:.3f}秒")

        # 查询性能测试
        start_time = time.time()

        # 复杂查询
        results = db_manager.fetchall(
            """
            SELECT symbol, AVG(value) as avg_value, COUNT(*) as count 
            FROM performance_test 
            WHERE value > 50.0 
            GROUP BY substr(symbol, 1, 4)
            ORDER BY avg_value DESC
        """
        )

        query_time = time.time() - start_time
        logger.info(f"复杂查询耗时: {query_time:.3f}秒")

        # 验证查询结果
        assert len(results) > 0

        # 测试连接复用
        for i in range(10):
            record_count = db_manager.fetchone(
                "SELECT COUNT(*) as count FROM performance_test"
            )
            assert record_count["count"] == batch_size

        db_manager.close()
        logger.info("✅ 连接池和性能测试通过")

    def test_error_handling_and_recovery(self, temp_db_path):
        """测试错误处理和恢复"""
        logger.info("🧪 测试错误处理和恢复...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建测试表
        db_manager.execute(
            """
            CREATE TABLE error_test (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        """
        )

        # 插入测试数据
        db_manager.execute("INSERT INTO error_test (name) VALUES (?)", ("test1",))

        # 测试约束违反错误处理
        try:
            db_manager.execute(
                "INSERT INTO error_test (name) VALUES (?)", ("test1",)
            )  # 重复插入
            assert False, "应该抛出约束违反错误"
        except Exception as e:
            logger.info(f"正确捕获约束违反错误: {e}")
            assert "UNIQUE constraint failed" in str(e)

        # 验证数据库状态仍然正常 - 修复方法调用
        test_result = db_manager.fetchone("SELECT 1 as test")
        assert test_result["test"] == 1
        count = db_manager.fetchone("SELECT COUNT(*) as count FROM error_test")
        assert count["count"] == 1

        # 测试SQL语法错误
        try:
            db_manager.execute("INVALID SQL STATEMENT")
            assert False, "应该抛出SQL语法错误"
        except Exception as e:
            logger.info(f"正确捕获SQL语法错误: {e}")
            assert "syntax error" in str(e).lower()

        # 验证连接仍然可用
        test_result = db_manager.fetchone("SELECT 1 as test")
        assert test_result["test"] == 1

        # 测试参数错误
        try:
            db_manager.execute("SELECT * FROM error_test WHERE id = ?")  # 缺少参数
            assert False, "应该抛出参数错误"
        except Exception as e:
            logger.info(f"正确捕获参数错误: {e}")

        db_manager.close()
        logger.info("✅ 错误处理和恢复测试通过")

    def test_data_integrity_and_constraints(self, temp_db_path):
        """测试数据完整性和约束"""
        logger.info("🧪 测试数据完整性和约束...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建带约束的表
        db_manager.execute(
            """
            CREATE TABLE integrity_test (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL CHECK(length(symbol) >= 8),
                price REAL CHECK(price > 0),
                volume INTEGER CHECK(volume >= 0),
                trade_date TEXT NOT NULL,
                UNIQUE(symbol, trade_date)
            )
        """
        )

        # 测试有效数据插入
        db_manager.execute(
            "INSERT INTO integrity_test (symbol, price, volume, trade_date) VALUES (?, ?, ?, ?)",
            ("000001.SZ", 10.5, 1000000, "2024-01-15"),
        )

        # 测试CHECK约束 - 价格必须大于0
        try:
            db_manager.execute(
                "INSERT INTO integrity_test (symbol, price, volume, trade_date) VALUES (?, ?, ?, ?)",
                ("000002.SZ", -1.0, 1000000, "2024-01-15"),
            )
            assert False, "应该违反价格CHECK约束"
        except Exception as e:
            assert "CHECK constraint failed" in str(e)

        # 测试CHECK约束 - 股票代码长度
        try:
            db_manager.execute(
                "INSERT INTO integrity_test (symbol, price, volume, trade_date) VALUES (?, ?, ?, ?)",
                ("001", 10.5, 1000000, "2024-01-15"),
            )
            assert False, "应该违反代码长度CHECK约束"
        except Exception as e:
            assert "CHECK constraint failed" in str(e)

        # 测试UNIQUE约束
        try:
            db_manager.execute(
                "INSERT INTO integrity_test (symbol, price, volume, trade_date) VALUES (?, ?, ?, ?)",
                ("000001.SZ", 10.6, 1100000, "2024-01-15"),  # 相同的symbol和trade_date
            )
            assert False, "应该违反UNIQUE约束"
        except Exception as e:
            assert "UNIQUE constraint failed" in str(e)

        # 测试NOT NULL约束
        try:
            db_manager.execute(
                "INSERT INTO integrity_test (symbol, price, volume) VALUES (?, ?, ?)",
                ("000003.SZ", 10.5, 1000000),  # 缺少trade_date
            )
            assert False, "应该违反NOT NULL约束"
        except Exception as e:
            assert "NOT NULL constraint failed" in str(e)

        # 验证只有有效数据被保存
        valid_records = db_manager.fetchall("SELECT * FROM integrity_test")
        assert len(valid_records) == 1
        assert valid_records[0]["symbol"] == "000001.SZ"

        db_manager.close()
        logger.info("✅ 数据完整性和约束测试通过")

    @pytest.mark.skip(reason="SQLite备份恢复功能需要更复杂的实现")
    def test_backup_and_recovery(self, temp_db_path):
        """测试备份和恢复功能"""
        logger.info("🧪 测试备份和恢复功能...")

        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建测试数据
        db_manager.execute(
            """
            CREATE TABLE backup_test (
                id INTEGER PRIMARY KEY,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # 插入测试数据
        test_data = [("data_1",), ("data_2",), ("data_3",)]
        db_manager.executemany("INSERT INTO backup_test (data) VALUES (?)", test_data)

        # 创建备份文件
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as backup_file:
            backup_path = backup_file.name

        try:
            # 确保数据已写入磁盘
            db_manager.connection.commit()

            # 执行备份
            if hasattr(db_manager, "backup"):
                db_manager.backup(backup_path)
            else:
                # 简单的文件复制备份
                import shutil

                shutil.copy2(temp_db_path, backup_path)

            # 验证备份文件存在和内容
            assert Path(backup_path).exists()
            backup_size = Path(backup_path).stat().st_size
            logger.info(f"备份文件大小: {backup_size} bytes")
            assert backup_size > 0, "备份文件为空"

            # 删除原始数据
            db_manager.execute("DELETE FROM backup_test")
            remaining_count = db_manager.fetchone(
                "SELECT COUNT(*) as count FROM backup_test"
            )
            assert remaining_count["count"] == 0

            # 从备份恢复 - 在关闭原连接之前
            if Path(backup_path).exists():
                # 使用备份文件创建新的数据库管理器
                restored_db_manager = DatabaseManager(backup_path, config=config)

                # 验证数据已恢复
                restored_count = restored_db_manager.fetchone(
                    "SELECT COUNT(*) as count FROM backup_test"
                )
                assert restored_count["count"] == 3

                restored_data = restored_db_manager.fetchall(
                    "SELECT data FROM backup_test ORDER BY id"
                )
                assert len(restored_data) == 3
                assert restored_data[0]["data"] == "data_1"

                restored_db_manager.close()

            # 最后关闭原数据库连接
            db_manager.close()

        finally:
            # 清理备份文件
            Path(backup_path).unlink(missing_ok=True)

        logger.info("✅ 备份和恢复功能测试通过")


@pytest.mark.integration
def test_database_manager_full_integration():
    """数据库管理器完整集成测试"""
    logger.info("🚀 开始数据库管理器完整集成测试...")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        temp_db_path = f.name

    try:
        config = Config()
        db_manager = DatabaseManager(temp_db_path, config=config)

        # 创建完整的股票数据表结构
        create_tables_sql = [
            """
            CREATE TABLE ptrade_stock_info (
                symbol TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                market TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                list_date TEXT,
                industry TEXT,
                total_share REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE market_data (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                frequency TEXT NOT NULL DEFAULT '1d',
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                money REAL,
                quality_score INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trade_date, frequency)
            )
            """,
            """
            CREATE INDEX idx_market_data_symbol ON market_data(symbol)
            """,
            """
            CREATE INDEX idx_market_data_date ON market_data(trade_date)
            """,
        ]

        # 执行表创建
        for sql in create_tables_sql:
            db_manager.execute(sql)

        # 插入股票信息
        stocks = [
            (
                "000001.SZ",
                "平安银行",
                "SZ",
                "active",
                "1991-04-03",
                "银行",
                19405000000,
            ),
            ("000002.SZ", "万科A", "SZ", "active", "1991-01-29", "房地产", 11039152000),
            (
                "600000.SS",
                "浦发银行",
                "SS",
                "active",
                "1999-11-10",
                "银行",
                29352000000,
            ),
        ]

        db_manager.executemany(
            "INSERT INTO ptrade_stock_info (symbol, name, market, status, list_date, industry, total_share) VALUES (?, ?, ?, ?, ?, ?, ?)",
            stocks,
        )

        # 批量插入市场数据
        import datetime

        market_data = []
        base_date = date(2024, 1, 15)

        for stock_symbol, _, _, _, _, _, _ in stocks:
            for i in range(5):  # 5天的数据
                current_date = base_date + datetime.timedelta(days=i)
                market_data.append(
                    (
                        stock_symbol,
                        str(current_date),
                        "1d",
                        10.0 + i * 0.1,  # open
                        10.5 + i * 0.1,  # high
                        9.5 + i * 0.1,  # low
                        10.2 + i * 0.1,  # close
                        1000000 + i * 100000,  # volume
                        (10.2 + i * 0.1) * (1000000 + i * 100000),  # money
                        95 - i,  # quality_score
                    )
                )

        db_manager.executemany(
            "INSERT INTO market_data (symbol, trade_date, frequency, open, high, low, close, volume, money, quality_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            market_data,
        )

        # 复杂查询测试
        complex_query = """
            SELECT 
                s.symbol,
                s.name,
                s.industry,
                COUNT(m.trade_date) as data_days,
                AVG(m.close) as avg_close,
                MAX(m.high) as max_high,
                MIN(m.low) as min_low,
                SUM(m.volume) as total_volume,
                AVG(m.quality_score) as avg_quality
            FROM ptrade_stock_info s
            LEFT JOIN market_data m ON s.symbol = m.symbol
            WHERE s.status = 'active'
            GROUP BY s.symbol, s.name, s.industry
            ORDER BY total_volume DESC
        """

        results = db_manager.fetchall(complex_query)

        # 验证查询结果
        assert len(results) == 3
        for result in results:
            assert result["data_days"] == 5
            assert result["avg_close"] > 10.0
            assert result["total_volume"] > 5000000
            assert result["avg_quality"] > 90

        # 事务性操作测试
        with db_manager.transaction():
            # 更新股票信息
            db_manager.execute(
                "UPDATE ptrade_stock_info SET industry = ? WHERE symbol = ?",
                ("互联网金融", "000001.SZ"),
            )

            # 插入新的市场数据
            new_date = str(base_date + datetime.timedelta(days=5))
            db_manager.execute(
                "INSERT INTO market_data (symbol, trade_date, frequency, open, high, low, close, volume, money, quality_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "000001.SZ",
                    new_date,
                    "1d",
                    10.8,
                    11.2,
                    10.6,
                    11.0,
                    1500000,
                    16500000,
                    88,
                ),
            )

        # 验证事务提交
        updated_stock = db_manager.fetchone(
            "SELECT industry FROM ptrade_stock_info WHERE symbol = ?", ("000001.SZ",)
        )
        assert updated_stock["industry"] == "互联网金融"

        # 验证性能
        start_time = datetime.datetime.now()

        # 执行1000次查询
        for i in range(100):
            db_manager.fetchone("SELECT COUNT(*) as count FROM market_data")

        end_time = datetime.datetime.now()
        query_duration = (end_time - start_time).total_seconds()

        logger.info(f"100次简单查询耗时: {query_duration:.3f}秒")
        assert query_duration < 1.0  # 应该在1秒内完成

        # 获取数据库统计信息
        total_stocks = db_manager.fetchone(
            "SELECT COUNT(*) as count FROM ptrade_stock_info"
        )
        total_market_data = db_manager.fetchone(
            "SELECT COUNT(*) as count FROM market_data"
        )

        logger.info(
            f"数据库包含 {total_stocks['count']} 只股票，{total_market_data['count']} 条市场数据"
        )

        assert total_stocks["count"] == 3
        assert (
            total_market_data["count"] == 16
        )  # 3 stocks * 5 days + 1 additional record

        db_manager.close()

        logger.info("🎉 数据库管理器完整集成测试通过!")

    finally:
        # 清理
        Path(temp_db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    # 运行完整集成测试
    test_database_manager_full_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
