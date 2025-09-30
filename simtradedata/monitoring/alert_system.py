"""
高级告警系统

提供灵活的告警规则引擎、通知系统和告警历史管理。
"""

import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """告警严重程度"""

    INFO = "INFO"  # 信息
    LOW = "LOW"  # 低
    MEDIUM = "MEDIUM"  # 中
    HIGH = "HIGH"  # 高
    CRITICAL = "CRITICAL"  # 严重


class AlertStatus(Enum):
    """告警状态"""

    ACTIVE = "ACTIVE"  # 激活
    ACKNOWLEDGED = "ACKNOWLEDGED"  # 已确认
    RESOLVED = "RESOLVED"  # 已解决
    CLOSED = "CLOSED"  # 已关闭


class AlertRule:
    """告警规则"""

    def __init__(
        self,
        rule_id: str,
        name: str,
        check_func: Callable[[], Optional[Dict[str, Any]]],
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        enabled: bool = True,
        cooldown_minutes: int = 60,
        description: str = "",
    ):
        """
        初始化告警规则

        Args:
            rule_id: 规则ID
            name: 规则名称
            check_func: 检查函数，返回None表示无告警，返回Dict表示触发告警
            severity: 严重程度
            enabled: 是否启用
            cooldown_minutes: 冷却时间（分钟），避免重复告警
            description: 规则描述
        """
        self.rule_id = rule_id
        self.name = name
        self.check_func = check_func
        self.severity = severity
        self.enabled = enabled
        self.cooldown_minutes = cooldown_minutes
        self.description = description
        self.last_alert_time = None

    def can_alert(self) -> bool:
        """检查是否可以告警（冷却时间检查）"""
        if not self.enabled:
            return False

        if self.last_alert_time is None:
            return True

        time_since_last = datetime.now() - self.last_alert_time
        return time_since_last.total_seconds() >= self.cooldown_minutes * 60

    def check(self) -> Optional[Dict[str, Any]]:
        """执行检查"""
        if not self.can_alert():
            return None

        try:
            result = self.check_func()
            if result:
                self.last_alert_time = datetime.now()
                return result
        except Exception as e:
            logger.error(f"告警规则{self.rule_id}检查失败: {e}")

        return None


class AlertNotifier:
    """告警通知器基类"""

    def send(self, alert: Dict[str, Any]) -> bool:
        """
        发送告警通知

        Args:
            alert: 告警信息

        Returns:
            bool: 是否发送成功
        """
        raise NotImplementedError


class LogNotifier(AlertNotifier):
    """日志通知器"""

    def send(self, alert: Dict[str, Any]) -> bool:
        """通过日志记录告警"""
        severity = alert.get("severity", "MEDIUM")
        message = alert.get("message", "")
        details = alert.get("details", {})

        log_msg = f"[{severity}] {message}"
        if details:
            log_msg += f" | 详情: {details}"

        if severity in ["HIGH", "CRITICAL"]:
            logger.error(log_msg)
        elif severity == "MEDIUM":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        return True


class ConsoleNotifier(AlertNotifier):
    """控制台通知器"""

    def send(self, alert: Dict[str, Any]) -> bool:
        """通过控制台输出告警"""
        severity = alert.get("severity", "MEDIUM")
        message = alert.get("message", "")
        timestamp = alert.get("timestamp", "")

        # 根据严重程度选择颜色前缀
        prefix_map = {
            "INFO": "ℹ️",
            "LOW": "⚠️",
            "MEDIUM": "⚠️",
            "HIGH": "🔴",
            "CRITICAL": "🚨",
        }

        prefix = prefix_map.get(severity, "⚠️")
        print(f"\n{prefix} [{timestamp}] [{severity}] {message}")

        return True


class AlertHistory:
    """告警历史管理"""

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化告警历史管理

        Args:
            db_manager: 数据库管理器
        """
        self.db_manager = db_manager
        self._init_table()

    def _init_table(self):
        """初始化告警历史表"""
        self.db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT DEFAULT 'ACTIVE',
                message TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                acknowledged_at TIMESTAMP,
                resolved_at TIMESTAMP,
                closed_at TIMESTAMP
            )
            """
        )

        # 创建索引
        self.db_manager.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_rule_id ON alert_history(rule_id)"
        )
        self.db_manager.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_status ON alert_history(status)"
        )
        self.db_manager.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_created_at ON alert_history(created_at)"
        )

    def add_alert(self, alert: Dict[str, Any]) -> int:
        """
        添加告警记录

        Args:
            alert: 告警信息

        Returns:
            int: 告警记录ID
        """
        import json

        sql = """
        INSERT INTO alert_history
        (rule_id, rule_name, severity, message, details, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """

        cursor = self.db_manager.execute(
            sql,
            (
                alert.get("rule_id", "unknown"),
                alert.get("rule_name", ""),
                alert.get("severity", "MEDIUM"),
                alert.get("message", ""),
                json.dumps(alert.get("details", {}), ensure_ascii=False),
                AlertStatus.ACTIVE.value,
            ),
        )

        return cursor.lastrowid

    def acknowledge_alert(self, alert_id: int):
        """确认告警"""
        self.db_manager.execute(
            """
            UPDATE alert_history
            SET status = ?, acknowledged_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (AlertStatus.ACKNOWLEDGED.value, alert_id),
        )

    def resolve_alert(self, alert_id: int):
        """解决告警"""
        self.db_manager.execute(
            """
            UPDATE alert_history
            SET status = ?, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (AlertStatus.RESOLVED.value, alert_id),
        )

    def close_alert(self, alert_id: int):
        """关闭告警"""
        self.db_manager.execute(
            """
            UPDATE alert_history
            SET status = ?, closed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (AlertStatus.CLOSED.value, alert_id),
        )

    def get_active_alerts(self, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取激活的告警"""
        if severity:
            sql = """
            SELECT * FROM alert_history
            WHERE status = ? AND severity = ?
            ORDER BY created_at DESC
            """
            results = self.db_manager.fetchall(
                sql, (AlertStatus.ACTIVE.value, severity)
            )
        else:
            sql = """
            SELECT * FROM alert_history
            WHERE status = ?
            ORDER BY created_at DESC
            """
            results = self.db_manager.fetchall(sql, (AlertStatus.ACTIVE.value,))

        return [dict(row) for row in results]

    def get_alert_statistics(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取告警统计"""
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        # 总告警数
        total = self.db_manager.fetchone(
            """
            SELECT COUNT(*) as count FROM alert_history
            WHERE date(created_at) BETWEEN ? AND ?
            """,
            (start_date, end_date),
        )["count"]

        # 按严重程度统计
        by_severity = self.db_manager.fetchall(
            """
            SELECT severity, COUNT(*) as count FROM alert_history
            WHERE date(created_at) BETWEEN ? AND ?
            GROUP BY severity
            """,
            (start_date, end_date),
        )

        # 按状态统计
        by_status = self.db_manager.fetchall(
            """
            SELECT status, COUNT(*) as count FROM alert_history
            WHERE date(created_at) BETWEEN ? AND ?
            GROUP BY status
            """,
            (start_date, end_date),
        )

        # 平均响应时间（确认时间）
        avg_ack_time = self.db_manager.fetchone(
            """
            SELECT AVG(
                CAST((julianday(acknowledged_at) - julianday(created_at)) * 24 * 60 AS INTEGER)
            ) as minutes
            FROM alert_history
            WHERE acknowledged_at IS NOT NULL
            AND date(created_at) BETWEEN ? AND ?
            """,
            (start_date, end_date),
        )["minutes"]

        # 平均解决时间
        avg_resolve_time = self.db_manager.fetchone(
            """
            SELECT AVG(
                CAST((julianday(resolved_at) - julianday(created_at)) * 24 * 60 AS INTEGER)
            ) as minutes
            FROM alert_history
            WHERE resolved_at IS NOT NULL
            AND date(created_at) BETWEEN ? AND ?
            """,
            (start_date, end_date),
        )["minutes"]

        return {
            "period": {"start": start_date, "end": end_date},
            "total_alerts": total,
            "by_severity": {row["severity"]: row["count"] for row in by_severity},
            "by_status": {row["status"]: row["count"] for row in by_status},
            "avg_acknowledgement_time_minutes": (
                round(avg_ack_time, 2) if avg_ack_time else 0
            ),
            "avg_resolution_time_minutes": (
                round(avg_resolve_time, 2) if avg_resolve_time else 0
            ),
        }


class AlertSystem:
    """高级告警系统"""

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化告警系统

        Args:
            db_manager: 数据库管理器
        """
        self.db_manager = db_manager
        self.rules: Dict[str, AlertRule] = {}
        self.notifiers: List[AlertNotifier] = []
        self.history = AlertHistory(db_manager)

        # 默认添加日志通知器
        self.add_notifier(LogNotifier())

        logger.info("告警系统初始化完成")

    def add_rule(self, rule: AlertRule):
        """
        添加告警规则

        Args:
            rule: 告警规则
        """
        self.rules[rule.rule_id] = rule
        logger.debug(f"添加告警规则: {rule.rule_id} - {rule.name}")

    def remove_rule(self, rule_id: str):
        """删除告警规则"""
        if rule_id in self.rules:
            del self.rules[rule_id]
            logger.debug(f"删除告警规则: {rule_id}")

    def enable_rule(self, rule_id: str):
        """启用告警规则"""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = True
            logger.debug(f"启用告警规则: {rule_id}")

    def disable_rule(self, rule_id: str):
        """禁用告警规则"""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = False
            logger.debug(f"禁用告警规则: {rule_id}")

    def add_notifier(self, notifier: AlertNotifier):
        """添加通知器"""
        self.notifiers.append(notifier)

    def check_all_rules(self) -> List[Dict[str, Any]]:
        """
        检查所有告警规则

        Returns:
            List[Dict[str, Any]]: 触发的告警列表
        """
        triggered_alerts = []

        for rule in self.rules.values():
            if not rule.enabled:
                continue

            try:
                alert_data = rule.check()
                if alert_data:
                    alert = {
                        "rule_id": rule.rule_id,
                        "rule_name": rule.name,
                        "severity": rule.severity.value,
                        "message": alert_data.get("message", ""),
                        "details": alert_data.get("details", {}),
                        "timestamp": datetime.now().isoformat(),
                    }

                    # 记录到历史
                    alert_id = self.history.add_alert(alert)
                    alert["alert_id"] = alert_id

                    # 发送通知
                    self._notify(alert)

                    triggered_alerts.append(alert)

            except Exception as e:
                logger.error(f"检查告警规则{rule.rule_id}失败: {e}")

        return triggered_alerts

    def _notify(self, alert: Dict[str, Any]):
        """发送告警通知"""
        for notifier in self.notifiers:
            try:
                notifier.send(alert)
            except Exception as e:
                logger.error(f"发送告警通知失败: {e}")

    def get_alert_summary(self) -> Dict[str, Any]:
        """获取告警摘要"""
        active_alerts = self.history.get_active_alerts()
        stats = self.history.get_alert_statistics()

        return {
            "active_alerts_count": len(active_alerts),
            "active_alerts_by_severity": self._count_by_severity(active_alerts),
            "total_rules": len(self.rules),
            "enabled_rules": sum(1 for r in self.rules.values() if r.enabled),
            "statistics": stats,
        }

    def _count_by_severity(self, alerts: List[Dict[str, Any]]) -> Dict[str, int]:
        """按严重程度统计告警"""
        counts = {s.value: 0 for s in AlertSeverity}
        for alert in alerts:
            severity = alert.get("severity", "MEDIUM")
            if severity in counts:
                counts[severity] += 1
        return counts
