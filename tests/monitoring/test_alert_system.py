"""
高级告警系统测试
"""

from datetime import datetime, timedelta

import pytest

from simtradedata.database import DatabaseManager
from simtradedata.monitoring import (
    AlertHistory,
    AlertRule,
    AlertRuleFactory,
    AlertSeverity,
    AlertStatus,
    AlertSystem,
    ConsoleNotifier,
    LogNotifier,
)


@pytest.fixture
def db_manager():
    """创建测试数据库管理器"""
    db = DatabaseManager(":memory:")

    # 初始化必要的表
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            frequency TEXT NOT NULL DEFAULT '1d',
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (symbol, date, frequency)
        )
    """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            frequency TEXT NOT NULL,
            sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL,
            error_message TEXT
        )
    """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS stocks (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            status TEXT DEFAULT 'active',
            total_shares REAL,
            list_date TEXT
        )
    """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_calendar (
            date TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT 'CN',
            is_trading INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (date, market)
        )
    """
    )

    yield db
    db.close()


@pytest.mark.unit
class TestAlertRule:
    """告警规则测试"""

    def test_alert_rule_creation(self):
        """测试创建告警规则"""

        def check_func():
            return {"message": "测试告警", "details": {}}

        rule = AlertRule(
            rule_id="test_rule",
            name="测试规则",
            check_func=check_func,
            severity=AlertSeverity.HIGH,
            cooldown_minutes=60,
        )

        assert rule.rule_id == "test_rule"
        assert rule.name == "测试规则"
        assert rule.severity == AlertSeverity.HIGH
        assert rule.cooldown_minutes == 60
        assert rule.enabled is True
        assert rule.last_alert_time is None

    def test_alert_rule_check(self):
        """测试告警规则检查"""
        check_called = False

        def check_func():
            nonlocal check_called
            check_called = True
            return {"message": "测试告警", "details": {"value": 100}}

        rule = AlertRule(
            rule_id="test_rule",
            name="测试规则",
            check_func=check_func,
            severity=AlertSeverity.MEDIUM,
        )

        result = rule.check()

        assert check_called is True
        assert result is not None
        assert result["message"] == "测试告警"
        assert result["details"]["value"] == 100
        assert rule.last_alert_time is not None

    def test_alert_rule_cooldown(self):
        """测试告警冷却时间"""
        call_count = 0

        def check_func():
            nonlocal call_count
            call_count += 1
            return {"message": f"第{call_count}次告警", "details": {}}

        rule = AlertRule(
            rule_id="test_rule",
            name="测试规则",
            check_func=check_func,
            cooldown_minutes=1,  # 1分钟冷却
        )

        # 第一次检查应该触发
        result1 = rule.check()
        assert result1 is not None
        assert call_count == 1

        # 立即第二次检查，应该被冷却机制阻止
        result2 = rule.check()
        assert result2 is None
        assert call_count == 1  # 没有再次调用

    def test_alert_rule_disabled(self):
        """测试禁用告警规则"""

        def check_func():
            return {"message": "测试告警", "details": {}}

        rule = AlertRule(
            rule_id="test_rule",
            name="测试规则",
            check_func=check_func,
            enabled=False,
        )

        result = rule.check()
        assert result is None  # 禁用状态不触发告警


@pytest.mark.unit
class TestAlertNotifiers:
    """告警通知器测试"""

    def test_log_notifier(self, caplog):
        """测试日志通知器"""
        notifier = LogNotifier()
        alert = {
            "severity": "HIGH",
            "message": "测试告警消息",
            "details": {"key": "value"},
        }

        result = notifier.send(alert)
        assert result is True

    def test_console_notifier(self, capsys):
        """测试控制台通知器"""
        notifier = ConsoleNotifier()
        alert = {
            "severity": "MEDIUM",
            "message": "测试告警消息",
            "timestamp": "2025-09-30T20:00:00",
        }

        result = notifier.send(alert)
        assert result is True

        captured = capsys.readouterr()
        assert "测试告警消息" in captured.out
        assert "MEDIUM" in captured.out


@pytest.mark.unit
class TestAlertHistory:
    """告警历史测试"""

    def test_add_alert(self, db_manager):
        """测试添加告警记录"""
        history = AlertHistory(db_manager)

        alert = {
            "rule_id": "test_rule",
            "rule_name": "测试规则",
            "severity": "HIGH",
            "message": "测试告警",
            "details": {"key": "value"},
        }

        alert_id = history.add_alert(alert)
        assert alert_id > 0

        # 验证记录已添加
        active_alerts = history.get_active_alerts()
        assert len(active_alerts) == 1
        assert active_alerts[0]["message"] == "测试告警"

    def test_acknowledge_alert(self, db_manager):
        """测试确认告警"""
        history = AlertHistory(db_manager)

        alert = {
            "rule_id": "test_rule",
            "rule_name": "测试规则",
            "severity": "MEDIUM",
            "message": "测试告警",
            "details": {},
        }

        alert_id = history.add_alert(alert)
        history.acknowledge_alert(alert_id)

        # 验证状态已更新
        result = db_manager.fetchone(
            "SELECT status, acknowledged_at FROM alert_history WHERE id = ?",
            (alert_id,),
        )
        assert result["status"] == AlertStatus.ACKNOWLEDGED.value
        assert result["acknowledged_at"] is not None

    def test_resolve_alert(self, db_manager):
        """测试解决告警"""
        history = AlertHistory(db_manager)

        alert = {
            "rule_id": "test_rule",
            "rule_name": "测试规则",
            "severity": "LOW",
            "message": "测试告警",
            "details": {},
        }

        alert_id = history.add_alert(alert)
        history.resolve_alert(alert_id)

        # 验证状态已更新
        result = db_manager.fetchone(
            "SELECT status, resolved_at FROM alert_history WHERE id = ?", (alert_id,)
        )
        assert result["status"] == AlertStatus.RESOLVED.value
        assert result["resolved_at"] is not None

    def test_get_alert_statistics(self, db_manager):
        """测试获取告警统计"""
        history = AlertHistory(db_manager)

        # 添加多个告警
        for i in range(3):
            history.add_alert(
                {
                    "rule_id": f"rule_{i}",
                    "rule_name": f"规则{i}",
                    "severity": "HIGH" if i % 2 == 0 else "MEDIUM",
                    "message": f"告警{i}",
                    "details": {},
                }
            )

        stats = history.get_alert_statistics()

        assert stats["total_alerts"] == 3
        assert "HIGH" in stats["by_severity"]
        assert "MEDIUM" in stats["by_severity"]
        assert "ACTIVE" in stats["by_status"]


@pytest.mark.integration
class TestAlertSystem:
    """告警系统集成测试"""

    def test_alert_system_initialization(self, db_manager):
        """测试告警系统初始化"""
        alert_system = AlertSystem(db_manager)

        assert alert_system.db_manager is not None
        assert alert_system.history is not None
        assert len(alert_system.notifiers) >= 1  # 至少有默认的日志通知器
        assert len(alert_system.rules) == 0  # 初始无规则

    def test_add_and_remove_rule(self, db_manager):
        """测试添加和删除规则"""
        alert_system = AlertSystem(db_manager)

        def check_func():
            return {"message": "测试", "details": {}}

        rule = AlertRule(
            rule_id="test_rule",
            name="测试规则",
            check_func=check_func,
            severity=AlertSeverity.MEDIUM,
        )

        alert_system.add_rule(rule)
        assert "test_rule" in alert_system.rules

        alert_system.remove_rule("test_rule")
        assert "test_rule" not in alert_system.rules

    def test_enable_disable_rule(self, db_manager):
        """测试启用和禁用规则"""
        alert_system = AlertSystem(db_manager)

        def check_func():
            return {"message": "测试", "details": {}}

        rule = AlertRule(
            rule_id="test_rule",
            name="测试规则",
            check_func=check_func,
        )

        alert_system.add_rule(rule)
        assert alert_system.rules["test_rule"].enabled is True

        alert_system.disable_rule("test_rule")
        assert alert_system.rules["test_rule"].enabled is False

        alert_system.enable_rule("test_rule")
        assert alert_system.rules["test_rule"].enabled is True

    def test_check_all_rules(self, db_manager):
        """测试检查所有规则"""
        alert_system = AlertSystem(db_manager)
        alert_system.add_notifier(ConsoleNotifier())

        # 添加会触发的规则
        def check_trigger():
            return {"message": "触发告警", "details": {"value": 100}}

        rule1 = AlertRule(
            rule_id="trigger_rule",
            name="触发规则",
            check_func=check_trigger,
            severity=AlertSeverity.HIGH,
        )

        # 添加不会触发的规则
        def check_no_trigger():
            return None

        rule2 = AlertRule(
            rule_id="no_trigger_rule",
            name="不触发规则",
            check_func=check_no_trigger,
        )

        alert_system.add_rule(rule1)
        alert_system.add_rule(rule2)

        # 检查所有规则
        alerts = alert_system.check_all_rules()

        assert len(alerts) == 1
        assert alerts[0]["rule_id"] == "trigger_rule"
        assert alerts[0]["severity"] == "HIGH"
        assert alerts[0]["message"] == "触发告警"

    def test_alert_summary(self, db_manager):
        """测试告警摘要"""
        alert_system = AlertSystem(db_manager)

        def check_func():
            return {"message": "测试", "details": {}}

        rule = AlertRule(
            rule_id="test_rule",
            name="测试规则",
            check_func=check_func,
        )

        alert_system.add_rule(rule)

        summary = alert_system.get_alert_summary()

        assert "active_alerts_count" in summary
        assert "total_rules" in summary
        assert summary["total_rules"] == 1
        assert "enabled_rules" in summary
        assert summary["enabled_rules"] == 1


@pytest.mark.integration
class TestAlertRuleFactory:
    """告警规则工厂测试"""

    def test_create_data_quality_rule(self, db_manager):
        """测试创建数据质量规则"""
        rule = AlertRuleFactory.create_data_quality_rule(db_manager, threshold=80.0)

        assert rule.rule_id == "data_quality_check"
        assert rule.name == "数据质量检查"
        assert rule.severity == AlertSeverity.MEDIUM
        assert rule.enabled is True

    def test_create_stale_data_rule(self, db_manager):
        """测试创建陈旧数据规则"""
        rule = AlertRuleFactory.create_stale_data_rule(db_manager, days=7)

        assert rule.rule_id == "stale_data_check"
        assert rule.name == "陈旧数据检查"
        assert rule.severity == AlertSeverity.HIGH

    def test_create_all_default_rules(self, db_manager):
        """测试创建所有默认规则"""
        rules = AlertRuleFactory.create_all_default_rules(db_manager)

        assert len(rules) == 6  # 应该有6个默认规则
        rule_ids = [rule.rule_id for rule in rules]

        assert "data_quality_check" in rule_ids
        assert "sync_failure_check" in rule_ids
        assert "database_size_check" in rule_ids
        assert "missing_data_check" in rule_ids
        assert "stale_data_check" in rule_ids
        assert "duplicate_data_check" in rule_ids


@pytest.mark.integration
class TestAlertSystemWithRealData:
    """告警系统真实数据测试"""

    def test_stale_data_alert(self, db_manager):
        """测试陈旧数据告警"""
        alert_system = AlertSystem(db_manager)

        # 添加陈旧数据规则（1天阈值，用于测试）
        rule = AlertRuleFactory.create_stale_data_rule(db_manager, days=1)
        alert_system.add_rule(rule)

        # 插入旧数据
        old_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        db_manager.execute(
            """
            INSERT INTO market_data (symbol, date, frequency, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("TEST.SZ", old_date, "1d", 10.0, 11.0, 9.0, 10.5, 1000000),
        )

        # 检查规则
        alerts = alert_system.check_all_rules()

        # 应该触发陈旧数据告警
        assert len(alerts) > 0
        stale_alerts = [a for a in alerts if a["rule_id"] == "stale_data_check"]
        assert len(stale_alerts) == 1
        assert "未更新" in stale_alerts[0]["message"]

    def test_duplicate_data_alert(self, db_manager):
        """测试重复数据告警"""
        alert_system = AlertSystem(db_manager)

        # 添加重复数据规则
        rule = AlertRuleFactory.create_duplicate_data_rule(db_manager)
        alert_system.add_rule(rule)

        # 先删除PRIMARY KEY约束，创建允许重复的临时表
        db_manager.execute("DROP TABLE IF EXISTS market_data")
        db_manager.execute(
            """
            CREATE TABLE market_data (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                frequency TEXT NOT NULL DEFAULT '1d',
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER
            )
        """
        )

        # 插入重复数据
        today = datetime.now().strftime("%Y-%m-%d")
        for _ in range(2):
            db_manager.execute(
                """
                INSERT INTO market_data (symbol, date, frequency, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("DUP.SZ", today, "1d", 10.0, 11.0, 9.0, 10.5, 1000000),
            )

        # 检查规则
        alerts = alert_system.check_all_rules()

        # 应该触发重复数据告警
        dup_alerts = [a for a in alerts if a["rule_id"] == "duplicate_data_check"]
        assert len(dup_alerts) == 1
        assert "重复数据" in dup_alerts[0]["message"]
