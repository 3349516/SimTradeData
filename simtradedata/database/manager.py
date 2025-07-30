"""
数据库连接管理器

提供SQLite数据库的连接管理、事务处理、查询执行等核心功能。
"""

import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..core.base_manager import BaseManager

logger = logging.getLogger(__name__)


class DatabaseManager(BaseManager):
    """SQLite数据库管理器"""

    def __init__(self, db_path: str = None, config=None, **kwargs):
        """
        初始化数据库管理器

        Args:
            db_path: 数据库文件路径
            config: 配置对象
            **kwargs: SQLite连接参数
        """
        # 数据库路径处理 - 在super().__init__前设置
        if db_path:
            self.db_path = Path(db_path)
        else:
            # 从配置中获取数据库路径需要先创建临时配置
            from ..config import Config

            temp_config = config or Config()
            db_path = temp_config.get("database.path", "data/simtradedata.db")
            self.db_path = Path(db_path)

        # 只保留sqlite3.connect()支持的参数
        valid_sqlite_params = {
            "timeout",
            "detect_types",
            "isolation_level",
            "check_same_thread",
            "factory",
            "cached_statements",
            "uri",
        }

        self.connection_kwargs = {
            "timeout": kwargs.get("timeout", 30.0),
            "check_same_thread": False,
            "isolation_level": None,  # 自动提交模式
        }

        # 只添加有效的SQLite参数
        for key, value in kwargs.items():
            if key in valid_sqlite_params:
                self.connection_kwargs[key] = value

        # 调用BaseManager初始化
        super().__init__(config=config, **kwargs)

        # 线程本地存储
        self._local = threading.local()

        # 确保数据库目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 初始化数据库
        self._initialize_database()

        self.logger.info(f"数据库管理器初始化完成: {self.db_path}")

    def _init_components(self):
        """初始化数据库组件"""
        pass  # 组件初始化在__init__中完成

    def _get_required_attributes(self) -> list:
        """获取必需属性列表"""
        return ["db_path", "connection_kwargs"]

    def _initialize_database(self):
        """初始化数据库设置"""
        with self.get_connection() as conn:
            # 启用外键约束
            conn.execute("PRAGMA foreign_keys = ON")

            # 设置WAL模式提高并发性能
            conn.execute("PRAGMA journal_mode = WAL")

            # 设置同步模式
            conn.execute("PRAGMA synchronous = NORMAL")

            # 设置缓存大小 (10MB)
            conn.execute("PRAGMA cache_size = -10000")

            # 设置临时存储为内存
            conn.execute("PRAGMA temp_store = MEMORY")

            logger.info("数据库初始化设置完成")

    @property
    def connection(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接"""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                str(self.db_path), **self.connection_kwargs
            )
            # 设置行工厂为字典
            self._local.connection.row_factory = sqlite3.Row

        return self._local.connection

    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = self.connection
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            # 不关闭连接，保持线程本地连接
            pass

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        conn = self.connection
        conn.execute("BEGIN")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"事务回滚: {e}")
            raise

    def execute(self, sql: str, params: Optional[Tuple] = None) -> sqlite3.Cursor:
        """执行SQL语句"""
        with self.get_connection() as conn:
            if params:
                return conn.execute(sql, params)
            else:
                return conn.execute(sql)

    def executemany(self, sql: str, params_list: List[Tuple]) -> sqlite3.Cursor:
        """批量执行SQL语句"""
        with self.get_connection() as conn:
            return conn.executemany(sql, params_list)

    def fetchone(
        self, sql: str, params: Optional[Tuple] = None
    ) -> Optional[sqlite3.Row]:
        """查询单行数据"""
        cursor = self.execute(sql, params)
        return cursor.fetchone()

    def fetchall(self, sql: str, params: Optional[Tuple] = None) -> List[sqlite3.Row]:
        """查询所有数据"""
        cursor = self.execute(sql, params)
        return cursor.fetchall()

    def fetchmany(
        self, sql: str, size: int, params: Optional[Tuple] = None
    ) -> List[sqlite3.Row]:
        """查询指定数量的数据"""
        cursor = self.execute(sql, params)
        return cursor.fetchmany(size)

    def to_dataframe(self, sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """查询结果转换为DataFrame"""
        with self.get_connection() as conn:
            if params:
                return pd.read_sql_query(sql, conn, params=params)
            else:
                return pd.read_sql_query(sql, conn)

    def insert_dataframe(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "append", **kwargs
    ) -> int:
        """将DataFrame插入数据库"""
        with self.get_connection() as conn:
            return df.to_sql(
                table_name, conn, if_exists=if_exists, index=False, **kwargs
            )

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        sql = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """
        result = self.fetchone(sql, (table_name,))
        return result is not None

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表结构信息"""
        sql = f"PRAGMA table_info({table_name})"
        rows = self.fetchall(sql)
        return [dict(row) for row in rows]

    def get_table_count(self, table_name: str) -> int:
        """获取表记录数"""
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.fetchone(sql)
        return result["count"] if result else 0

    def get_database_size(self) -> int:
        """获取数据库文件大小（字节）"""
        return self.db_path.stat().st_size if self.db_path.exists() else 0

    def close(self):
        """关闭数据库连接"""
        if hasattr(self._local, "connection") and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
        logger.info("数据库连接已关闭")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass
