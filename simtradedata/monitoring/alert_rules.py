"""
预定义告警规则

包含常用的告警规则实现。
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from ..database import DatabaseManager
from .alert_system import AlertRule, AlertSeverity
from .data_quality import DataQualityMonitor

logger = logging.getLogger(__name__)


class AlertRuleFactory:
    """告警规则工厂"""

    @staticmethod
    def create_data_quality_rule(
        db_manager: DatabaseManager, threshold: float = 80.0
    ) -> AlertRule:
        """
        创建数据质量告警规则

        Args:
            db_manager: 数据库管理器
            threshold: 质量阈值

        Returns:
            AlertRule: 告警规则
        """
        monitor = DataQualityMonitor(db_manager)

        def check_data_quality() -> Optional[Dict[str, Any]]:
            alert_data = monitor.alert_quality_issues(threshold)
            if alert_data:
                return {
                    "message": alert_data["message"],
                    "details": {
                        "score": alert_data["details"]["summary"][
                            "overall_quality_score"
                        ],
                        "grade": alert_data["details"]["summary"]["quality_grade"],
                        "recommendations": alert_data["details"]["summary"][
                            "recommendations"
                        ],
                    },
                }
            return None

        return AlertRule(
            rule_id="data_quality_check",
            name="数据质量检查",
            check_func=check_data_quality,
            severity=AlertSeverity.MEDIUM,
            cooldown_minutes=60,
            description=f"检查数据质量是否低于{threshold}%阈值",
        )

    @staticmethod
    def create_sync_failure_rule(db_manager: DatabaseManager) -> AlertRule:
        """
        创建同步失败告警规则

        Args:
            db_manager: 数据库管理器

        Returns:
            AlertRule: 告警规则
        """

        def check_sync_failure() -> Optional[Dict[str, Any]]:
            # 检查最近1小时的同步记录
            one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            result = db_manager.fetchone(
                """
                SELECT COUNT(*) as count FROM sync_records
                WHERE sync_time >= ? AND status = 'failed'
                """,
                (one_hour_ago,),
            )

            failed_count = result["count"] if result else 0

            if failed_count > 0:
                # 获取失败详情
                failures = db_manager.fetchall(
                    """
                    SELECT symbol, frequency, error_message
                    FROM sync_records
                    WHERE sync_time >= ? AND status = 'failed'
                    LIMIT 10
                    """,
                    (one_hour_ago,),
                )

                return {
                    "message": f"最近1小时内有{failed_count}次同步失败",
                    "details": {
                        "failed_count": failed_count,
                        "samples": [dict(row) for row in failures],
                    },
                }

            return None

        return AlertRule(
            rule_id="sync_failure_check",
            name="同步失败检查",
            check_func=check_sync_failure,
            severity=AlertSeverity.HIGH,
            cooldown_minutes=30,
            description="检查最近1小时的同步失败记录",
        )

    @staticmethod
    def create_database_size_rule(
        db_manager: DatabaseManager, max_size_gb: float = 10.0
    ) -> AlertRule:
        """
        创建数据库大小告警规则

        Args:
            db_manager: 数据库管理器
            max_size_gb: 最大数据库大小（GB）

        Returns:
            AlertRule: 告警规则
        """

        def check_database_size() -> Optional[Dict[str, Any]]:
            import os

            db_path = db_manager.db_path
            if os.path.exists(db_path):
                size_bytes = os.path.getsize(db_path)
                size_gb = size_bytes / (1024**3)

                if size_gb > max_size_gb:
                    return {
                        "message": f"数据库大小{size_gb:.2f}GB，超过限制{max_size_gb}GB",
                        "details": {
                            "size_gb": round(size_gb, 2),
                            "limit_gb": max_size_gb,
                            "path": db_path,
                        },
                    }

            return None

        return AlertRule(
            rule_id="database_size_check",
            name="数据库大小检查",
            check_func=check_database_size,
            severity=AlertSeverity.MEDIUM,
            cooldown_minutes=360,  # 6小时检查一次
            description=f"检查数据库大小是否超过{max_size_gb}GB",
        )

    @staticmethod
    def create_missing_data_rule(db_manager: DatabaseManager) -> AlertRule:
        """
        创建数据缺失告警规则

        Args:
            db_manager: 数据库管理器

        Returns:
            AlertRule: 告警规则
        """

        def check_missing_data() -> Optional[Dict[str, Any]]:
            # 获取最近的交易日
            latest_trading_day = db_manager.fetchone(
                """
                SELECT date FROM trading_calendar
                WHERE is_trading = 1 AND date < date('now')
                ORDER BY date DESC
                LIMIT 1
                """
            )

            if not latest_trading_day:
                return None

            latest_date = latest_trading_day["date"]

            # 检查有多少股票缺少该日数据
            missing_stocks = db_manager.fetchone(
                """
                SELECT COUNT(DISTINCT s.symbol) as count
                FROM stocks s
                LEFT JOIN market_data m ON s.symbol = m.symbol AND m.date = ?
                WHERE m.symbol IS NULL
                AND s.status = 'active'
                """,
                (latest_date,),
            )

            missing_count = missing_stocks["count"] if missing_stocks else 0

            # 获取活跃股票总数
            total_stocks = db_manager.fetchone(
                "SELECT COUNT(*) as count FROM stocks WHERE status = 'active'"
            )
            total_count = total_stocks["count"] if total_stocks else 0

            if missing_count > 0 and total_count > 0:
                missing_rate = (missing_count / total_count) * 100

                if missing_rate > 10:  # 超过10%缺失率告警
                    return {
                        "message": f"最近交易日({latest_date})有{missing_count}只股票({missing_rate:.1f}%)缺少数据",
                        "details": {
                            "date": latest_date,
                            "missing_count": missing_count,
                            "total_count": total_count,
                            "missing_rate": round(missing_rate, 2),
                        },
                    }

            return None

        return AlertRule(
            rule_id="missing_data_check",
            name="数据缺失检查",
            check_func=check_missing_data,
            severity=AlertSeverity.HIGH,
            cooldown_minutes=120,
            description="检查最近交易日的数据缺失情况",
        )

    @staticmethod
    def create_stale_data_rule(db_manager: DatabaseManager, days: int = 7) -> AlertRule:
        """
        创建陈旧数据告警规则

        Args:
            db_manager: 数据库管理器
            days: 数据陈旧天数阈值

        Returns:
            AlertRule: 告警规则
        """

        def check_stale_data() -> Optional[Dict[str, Any]]:
            threshold_date = (datetime.now() - timedelta(days=days)).strftime(
                "%Y-%m-%d"
            )

            # 检查最新数据日期
            latest_data = db_manager.fetchone(
                "SELECT MAX(date) as latest_date FROM market_data"
            )

            if not latest_data or not latest_data["latest_date"]:
                return {
                    "message": "数据库中没有任何市场数据",
                    "details": {"latest_date": None, "days_old": None},
                }

            latest_date = latest_data["latest_date"]

            if latest_date < threshold_date:
                days_old = (
                    datetime.now() - datetime.strptime(latest_date, "%Y-%m-%d")
                ).days
                return {
                    "message": f"数据已经{days_old}天未更新（最新: {latest_date}）",
                    "details": {
                        "latest_date": latest_date,
                        "threshold_date": threshold_date,
                        "days_old": days_old,
                    },
                }

            return None

        return AlertRule(
            rule_id="stale_data_check",
            name="陈旧数据检查",
            check_func=check_stale_data,
            severity=AlertSeverity.HIGH,
            cooldown_minutes=240,  # 4小时检查一次
            description=f"检查数据是否超过{days}天未更新",
        )

    @staticmethod
    def create_duplicate_data_rule(db_manager: DatabaseManager) -> AlertRule:
        """
        创建重复数据告警规则

        Args:
            db_manager: 数据库管理器

        Returns:
            AlertRule: 告警规则
        """

        def check_duplicate_data() -> Optional[Dict[str, Any]]:
            # 检查market_data表中的重复记录
            duplicates = db_manager.fetchone(
                """
                SELECT COUNT(*) as count FROM (
                    SELECT symbol, date, frequency, COUNT(*) as dup_count
                    FROM market_data
                    GROUP BY symbol, date, frequency
                    HAVING dup_count > 1
                )
                """
            )

            dup_count = duplicates["count"] if duplicates else 0

            if dup_count > 0:
                # 获取重复示例
                samples = db_manager.fetchall(
                    """
                    SELECT symbol, date, frequency, COUNT(*) as dup_count
                    FROM market_data
                    GROUP BY symbol, date, frequency
                    HAVING dup_count > 1
                    LIMIT 10
                    """
                )

                return {
                    "message": f"发现{dup_count}组重复数据",
                    "details": {
                        "duplicate_groups": dup_count,
                        "samples": [dict(row) for row in samples],
                    },
                }

            return None

        return AlertRule(
            rule_id="duplicate_data_check",
            name="重复数据检查",
            check_func=check_duplicate_data,
            severity=AlertSeverity.MEDIUM,
            cooldown_minutes=120,
            description="检查数据库中的重复记录",
        )

    @staticmethod
    def create_all_default_rules(db_manager: DatabaseManager) -> list[AlertRule]:
        """
        创建所有默认告警规则

        Args:
            db_manager: 数据库管理器

        Returns:
            list[AlertRule]: 告警规则列表
        """
        return [
            AlertRuleFactory.create_data_quality_rule(db_manager, threshold=80.0),
            AlertRuleFactory.create_sync_failure_rule(db_manager),
            AlertRuleFactory.create_database_size_rule(db_manager, max_size_gb=10.0),
            AlertRuleFactory.create_missing_data_rule(db_manager),
            AlertRuleFactory.create_stale_data_rule(db_manager, days=7),
            AlertRuleFactory.create_duplicate_data_rule(db_manager),
        ]
