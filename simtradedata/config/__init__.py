"""
配置管理模块

处理系统配置、市场配置、数据源配置等。
支持开发环境和生产环境配置。
"""

from .defaults import get_default_config
from .manager import Config, ConfigManager
from .production import get_production_config, merge_configs

__all__ = [
    "Config",
    "ConfigManager",
    "get_default_config",
    "get_production_config",
    "merge_configs",
]
