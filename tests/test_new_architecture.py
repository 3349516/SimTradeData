"""
测试新的BaseManager架构

验证所有Manager类是否正确继承和使用BaseManager架构。
"""

import logging
import tempfile
from pathlib import Path

import pytest

from simtradedata.config import Config
from simtradedata.data_sources import DataSourceManager
from simtradedata.database import DatabaseManager
from simtradedata.preprocessor.engine import DataProcessingEngine

logger = logging.getLogger(__name__)


class TestNewArchitecture:
    """测试新架构的一致性和正确性"""

    @pytest.fixture
    def config(self):
        """创建测试配置"""
        return Config()

    @pytest.fixture
    def temp_db_manager(self, config):
        """创建临时数据库管理器"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        db_manager = DatabaseManager(db_path, config=config)
        yield db_manager

        db_manager.close()
        Path(db_path).unlink(missing_ok=True)

    def test_database_manager_base_architecture(self, config):
        """测试DatabaseManager的BaseManager架构"""
        logger.info("🧪 测试DatabaseManager的BaseManager架构...")

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            db_manager = DatabaseManager(db_path, config=config)

            # 验证BaseManager属性
            assert hasattr(db_manager, "config"), "应该有config属性"
            assert hasattr(db_manager, "logger"), "应该有logger属性"
            assert hasattr(db_manager, "timeout"), "应该有timeout配置"
            assert hasattr(db_manager, "max_retries"), "应该有max_retries配置"
            assert hasattr(db_manager, "enable_cache"), "应该有enable_cache配置"
            assert hasattr(db_manager, "enable_debug"), "应该有enable_debug配置"

            # 验证配置前缀
            assert hasattr(db_manager, "_config_prefix"), "应该有配置前缀"
            assert (
                db_manager._config_prefix == "database"
            ), f"配置前缀应该是'database'，实际是'{db_manager._config_prefix}'"

            # 验证状态获取
            status = db_manager.get_status()
            assert isinstance(status, dict), "get_status()应该返回字典"
            assert "class_name" in status, "状态应该包含class_name"
            assert status["class_name"] == "DatabaseManager", "class_name应该正确"

            logger.info("✅ DatabaseManager的BaseManager架构测试通过")

        finally:
            try:
                db_manager.close()
            except:
                pass
            Path(db_path).unlink(missing_ok=True)

    def test_data_source_manager_base_architecture(self, config):
        """测试DataSourceManager的BaseManager架构"""
        logger.info("🧪 测试DataSourceManager的BaseManager架构...")

        ds_manager = DataSourceManager(config=config)

        # 验证BaseManager属性
        assert hasattr(ds_manager, "config"), "应该有config属性"
        assert hasattr(ds_manager, "logger"), "应该有logger属性"
        assert hasattr(ds_manager, "timeout"), "应该有timeout配置"
        assert hasattr(ds_manager, "max_retries"), "应该有max_retries配置"

        # 验证配置前缀
        assert hasattr(ds_manager, "_config_prefix"), "应该有配置前缀"
        expected_prefix = "datasource"
        actual_prefix = ds_manager._config_prefix
        assert (
            actual_prefix == expected_prefix
        ), f"配置前缀应该是'{expected_prefix}'，实际是'{actual_prefix}'"

        # 验证状态获取方法存在
        assert hasattr(ds_manager, "get_status"), "应该有get_status方法"

        logger.info("✅ DataSourceManager的BaseManager架构测试通过")

    def test_data_processing_engine_base_architecture(self, temp_db_manager, config):
        """测试DataProcessingEngine的BaseManager架构"""
        logger.info("🧪 测试DataProcessingEngine的BaseManager架构...")

        ds_manager = DataSourceManager(config=config)
        engine = DataProcessingEngine(
            db_manager=temp_db_manager, data_source_manager=ds_manager, config=config
        )

        # 验证BaseManager属性
        assert hasattr(engine, "config"), "应该有config属性"
        assert hasattr(engine, "logger"), "应该有logger属性"
        assert hasattr(engine, "timeout"), "应该有timeout配置"
        assert hasattr(engine, "max_retries"), "应该有max_retries配置"

        # 验证配置前缀
        assert hasattr(engine, "_config_prefix"), "应该有配置前缀"
        expected_prefix = "dataprocessingengine"
        actual_prefix = engine._config_prefix
        assert (
            actual_prefix == expected_prefix
        ), f"配置前缀应该是'{expected_prefix}'，实际是'{actual_prefix}'"

        # 验证依赖注入
        assert hasattr(engine, "db_manager"), "应该有db_manager依赖"
        assert hasattr(engine, "data_source_manager"), "应该有data_source_manager依赖"
        assert engine.db_manager is temp_db_manager, "db_manager依赖应该正确注入"
        assert (
            engine.data_source_manager is ds_manager
        ), "data_source_manager依赖应该正确注入"

        # 验证状态获取
        status = engine.get_status()
        assert isinstance(status, dict), "get_status()应该返回字典"
        assert "class_name" in status, "状态应该包含class_name"

        logger.info("✅ DataProcessingEngine的BaseManager架构测试通过")

    def test_unified_logging(self, temp_db_manager, config):
        """测试统一日志记录"""
        logger.info("🧪 测试统一日志记录...")

        ds_manager = DataSourceManager(config=config)

        # 验证日志记录方法
        assert hasattr(ds_manager, "_log_method_start"), "应该有_log_method_start方法"
        assert hasattr(ds_manager, "_log_method_end"), "应该有_log_method_end方法"
        assert hasattr(ds_manager, "_log_error"), "应该有_log_error方法"
        assert hasattr(ds_manager, "_log_warning"), "应该有_log_warning方法"

        # 测试日志记录
        try:
            ds_manager._log_method_start("test_method", param1="value1")
            ds_manager._log_method_end("test_method", duration=0.1, result="success")
            ds_manager._log_warning("test_method", "这是一个测试警告")
            logger.info("✅ 日志记录方法调用成功")
        except Exception as e:
            logger.error(f"❌ 日志记录方法调用失败: {e}")

        logger.info("✅ 统一日志记录测试完成")

    def test_configuration_consistency(self, config):
        """测试配置一致性"""
        logger.info("🧪 测试配置一致性...")

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            db_manager = DatabaseManager(db_path, config=config)
            ds_manager = DataSourceManager(config=config)

            # 验证所有Manager使用相同的配置对象
            assert db_manager.config is config, "DatabaseManager应该使用传入的配置对象"
            assert (
                ds_manager.config is config
            ), "DataSourceManager应该使用传入的配置对象"

            # 验证基础配置参数存在
            managers = [db_manager, ds_manager]
            for manager in managers:
                assert hasattr(
                    manager, "timeout"
                ), f"{manager.__class__.__name__}应该有timeout配置"
                assert hasattr(
                    manager, "max_retries"
                ), f"{manager.__class__.__name__}应该有max_retries配置"
                assert hasattr(
                    manager, "enable_cache"
                ), f"{manager.__class__.__name__}应该有enable_cache配置"
                assert hasattr(
                    manager, "enable_debug"
                ), f"{manager.__class__.__name__}应该有enable_debug配置"

                # 验证配置值类型
                assert isinstance(
                    manager.timeout, (int, float)
                ), "timeout应该是数字类型"
                assert isinstance(manager.max_retries, int), "max_retries应该是整数"
                assert isinstance(
                    manager.enable_cache, bool
                ), "enable_cache应该是布尔类型"
                assert isinstance(
                    manager.enable_debug, bool
                ), "enable_debug应该是布尔类型"

            logger.info("✅ 配置一致性测试通过")

        finally:
            try:
                db_manager.close()
            except:
                pass
            Path(db_path).unlink(missing_ok=True)


def test_architecture_integration():
    """运行架构集成测试"""
    logger.info("🚀 开始新架构集成测试...")
    logger.info("🎉 新架构集成测试完成!")


if __name__ == "__main__":
    # 运行架构测试
    test_architecture_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
