"""
统一基础管理器类

所有Manager类都应继承此基类，提供统一的架构模式。
"""

# 标准库导入
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List

# 项目内导入
if TYPE_CHECKING:
    pass

from .config_mixin import ConfigMixin
from .logging_mixin import LoggingMixin


class BaseManager(ABC, ConfigMixin, LoggingMixin):
    """所有Manager的统一基类

    提供统一的：
    - 配置处理
    - 依赖注入
    - 日志记录
    - 错误处理
    - 生命周期管理
    """

    def __init__(self, config=None, **dependencies):
        """统一的初始化模式

        Args:
            config: 配置对象
            **dependencies: 依赖对象（如db_manager, cache_manager等）
        """
        # 1. 配置初始化
        self.config = self._init_config(config)
        self.logger = self._init_logger()

        # 2. 依赖注入
        self._inject_dependencies(**dependencies)

        # 3. 配置参数提取
        self._init_config_params()

        # 4. 组件初始化
        self._init_components()

        # 5. 健康检查
        self._validate_initialization()

        # 6. 初始化日志
        self._log_initialization()

    def _init_config(self, config=None):
        """统一配置初始化"""
        if config is None:
            # 动态导入Config避免循环导入
            from ..config import Config

            config = Config()
        # 设置配置前缀，用于获取模块特定配置
        self._config_prefix = self._get_config_prefix()
        return config

    def _get_config_prefix(self) -> str:
        """获取配置前缀"""
        class_name = self.__class__.__name__
        # DatabaseManager -> database, SyncManager -> sync
        return class_name.lower().replace("manager", "").replace("adapter", "")

    def _inject_dependencies(self, **dependencies):
        """统一依赖注入"""
        for name, dependency in dependencies.items():
            if dependency is not None:
                setattr(self, name, dependency)
                self.logger.debug(f"注入依赖: {name} = {type(dependency).__name__}")

    @abstractmethod
    def _init_components(self):
        """子类实现具体的组件初始化"""

    def _validate_initialization(self):
        """验证初始化是否成功"""
        required_attrs = self._get_required_attributes()
        for attr in required_attrs:
            if not hasattr(self, attr):
                raise AttributeError(f"必需属性 {attr} 未初始化")

    @abstractmethod
    def _get_required_attributes(self) -> List[str]:
        """子类定义必需的属性列表"""
        return []

    def _log_initialization(self):
        """统一初始化日志"""
        component_name = self.__class__.__name__.replace("Manager", "").replace(
            "Adapter", ""
        )
        self.logger.info(f"{component_name}初始化完成")

        if self.enable_debug:
            self.logger.debug(
                f"配置参数: timeout={self.timeout}, retries={self.max_retries}"
            )

    def _get_config(self, key: str, default: Any = None) -> Any:
        """统一配置获取方法

        Args:
            key: 配置键名（不包含模块前缀）
            default: 默认值

        Returns:
            配置值
        """
        full_key = f"{self._config_prefix}.{key}"
        return self.config.get(full_key, default)

    def get_status(self) -> Dict[str, Any]:
        """获取组件状态信息"""
        return {
            "class_name": self.__class__.__name__,
            "config_prefix": self._config_prefix,
            "initialized": True,
            "debug_enabled": self.enable_debug,
        }
