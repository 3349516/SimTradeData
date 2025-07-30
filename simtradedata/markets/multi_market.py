"""
多市场管理器

统一管理多个市场的数据适配和查询功能。
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from ..core.base_manager import BaseManager
from ..database import DatabaseManager
from .hk_market import HKMarketAdapter
from .us_market import USMarketAdapter

logger = logging.getLogger(__name__)


class MultiMarketManager(BaseManager):
    """多市场管理器"""

    def __init__(self, db_manager: DatabaseManager = None, config=None, **dependencies):
        """
        初始化多市场管理器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
            **dependencies: 其他依赖对象
        """
        # 获取数据库管理器 - 在super().__init__前设置
        self.db_manager = db_manager
        if not self.db_manager:
            raise ValueError("数据库管理器不能为空")

        # 初始化市场适配器 - 在super().__init__前设置
        self.adapters = {}

        # 调用BaseManager初始化
        super().__init__(config=config, db_manager=db_manager, **dependencies)

        # 初始化适配器
        self._init_adapters()

        # 市场配置
        self.default_market = self.config.get("multi_market.default_market", "SZ")
        self.enabled_markets = self.config.get(
            "multi_market.enabled_markets", ["SZ", "SS", "HK", "US"]
        )

        self.logger.info(
            f"多市场管理器初始化完成，支持市场: {list(self.adapters.keys())}"
        )

    def _init_components(self):
        """初始化多市场组件"""
        pass  # 组件初始化在__init__中完成

    def _get_required_attributes(self) -> list:
        """获取必需属性列表"""
        return ["db_manager", "adapters"]

    def _init_adapters(self):
        """初始化市场适配器"""
        try:
            # 港股适配器
            if self.config.get("markets.hk.enabled", True):
                self.adapters["HK"] = HKMarketAdapter(self.db_manager, self.config)

            # 美股适配器
            if self.config.get("markets.us.enabled", True):
                self.adapters["US"] = USMarketAdapter(self.db_manager, self.config)

            # A股适配器 (使用默认处理)
            self.adapters["SZ"] = None  # A股深圳
            self.adapters["SS"] = None  # A股上海

        except Exception as e:
            logger.error(f"初始化市场适配器失败: {e}")

    def get_market_adapter(self, market: str) -> Optional[Any]:
        """
        获取市场适配器

        Args:
            market: 市场代码

        Returns:
            Optional[Any]: 市场适配器，如果不存在则返回None
        """
        return self.adapters.get(market.upper())

    def adapt_stock_info(self, raw_data: Dict[str, Any], market: str) -> Dict[str, Any]:
        """
        适配股票信息

        Args:
            raw_data: 原始股票信息数据
            market: 市场代码

        Returns:
            Dict[str, Any]: 适配后的股票信息
        """
        try:
            adapter = self.get_market_adapter(market)

            if adapter and hasattr(adapter, "adapt_stock_info"):
                return adapter.adapt_stock_info(raw_data)
            else:
                # 使用默认适配 (A股)
                return self._default_adapt_stock_info(raw_data, market)

        except Exception as e:
            logger.error(f"股票信息适配失败 {market}: {e}")
            return raw_data

    def adapt_price_data(self, raw_data: Dict[str, Any], market: str) -> Dict[str, Any]:
        """
        适配价格数据

        Args:
            raw_data: 原始价格数据
            market: 市场代码

        Returns:
            Dict[str, Any]: 适配后的价格数据
        """
        try:
            adapter = self.get_market_adapter(market)

            if adapter and hasattr(adapter, "adapt_price_data"):
                return adapter.adapt_price_data(raw_data)
            else:
                # 使用默认适配 (A股)
                return self._default_adapt_price_data(raw_data, market)

        except Exception as e:
            logger.error(f"价格数据适配失败 {market}: {e}")
            return raw_data

    def get_trading_calendar(
        self, market: str, start_date: date, end_date: date
    ) -> List[Dict[str, Any]]:
        """
        获取交易日历

        Args:
            market: 市场代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            List[Dict[str, Any]]: 交易日历数据
        """
        try:
            adapter = self.get_market_adapter(market)

            if adapter and hasattr(adapter, "get_trading_calendar"):
                return adapter.get_trading_calendar(start_date, end_date)
            else:
                # 不再提供简化的交易日历，必须使用正确的数据源
                logger.error(f"市场 {market} 没有可用的交易日历数据源")
                return []

        except Exception as e:
            logger.error(f"获取交易日历失败 {market}: {e}")
            return []

    def get_market_info(self, market: str) -> Dict[str, Any]:
        """
        获取市场信息

        Args:
            market: 市场代码

        Returns:
            Dict[str, Any]: 市场信息
        """
        try:
            adapter = self.get_market_adapter(market)

            if adapter and hasattr(adapter, "get_market_info"):
                return adapter.get_market_info()
            else:
                # 返回默认市场信息 (A股)
                return self._default_get_market_info(market)

        except Exception as e:
            logger.error(f"获取市场信息失败 {market}: {e}")
            return {}

    def get_all_markets_info(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有市场信息

        Returns:
            Dict[str, Dict[str, Any]]: 所有市场信息
        """
        markets_info = {}

        for market in self.enabled_markets:
            try:
                markets_info[market] = self.get_market_info(market)
            except Exception as e:
                logger.error(f"获取市场信息失败 {market}: {e}")
                markets_info[market] = {"error": str(e)}

        return markets_info

    def parse_symbol_market(self, symbol: str) -> str:
        """
        从股票代码解析市场

        Args:
            symbol: 股票代码

        Returns:
            str: 市场代码
        """
        if not symbol:
            return self.default_market

        symbol = symbol.upper()

        # 根据后缀判断市场
        if symbol.endswith(".HK"):
            return "HK"
        elif symbol.endswith(".US"):
            return "US"
        elif symbol.endswith(".SZ"):
            return "SZ"
        elif symbol.endswith(".SS"):
            return "SS"

        # 根据代码格式判断
        symbol_code = symbol.split(".")[0]

        # 港股：5位数字
        if symbol_code.isdigit() and len(symbol_code) == 5:
            return "HK"

        # 美股：字母
        if symbol_code.isalpha():
            return "US"

        # A股：6位数字
        if symbol_code.isdigit() and len(symbol_code) == 6:
            if symbol_code.startswith(("00", "30")):
                return "SZ"  # 深圳
            elif symbol_code.startswith(("60", "68")):
                return "SS"  # 上海

        return self.default_market

    def normalize_symbol(self, symbol: str, market: str = None) -> str:
        """
        标准化股票代码

        Args:
            symbol: 原始股票代码
            market: 市场代码，如果不提供则自动解析

        Returns:
            str: 标准化后的股票代码
        """
        if not symbol:
            return ""

        if market is None:
            market = self.parse_symbol_market(symbol)

        adapter = self.get_market_adapter(market)

        if adapter and hasattr(adapter, "_normalize_symbol"):
            return adapter._normalize_symbol(symbol)
        else:
            # 默认标准化 (A股)
            return self._default_normalize_symbol(symbol, market)

    def _default_adapt_stock_info(
        self, raw_data: Dict[str, Any], market: str
    ) -> Dict[str, Any]:
        """默认股票信息适配 (A股)"""
        adapted_data = {
            "symbol": raw_data.get("symbol", ""),
            "name": raw_data.get("name", ""),
            "market": market,
            "exchange": "SZSE" if market == "SZ" else "SSE",
            "currency": "CNY",
            "timezone": "Asia/Shanghai",
            "trading_hours": "09:30-11:30,13:00-15:00",
            "status": raw_data.get("status", "active"),
            "stock_type": raw_data.get("type", "ordinary"),
            "industry": raw_data.get("industry", ""),
            "sector": raw_data.get("sector", ""),
            "list_date": raw_data.get("list_date"),
            "delist_date": raw_data.get("delist_date"),
            "total_share": raw_data.get("total_share"),
            "float_share": raw_data.get("float_share"),
        }

        return adapted_data

    def _default_adapt_price_data(
        self, raw_data: Dict[str, Any], market: str
    ) -> Dict[str, Any]:
        """默认价格数据适配 (A股)"""
        adapted_data = {
            "symbol": raw_data.get("symbol", ""),
            "market": market,
            "trade_date": raw_data.get("trade_date", ""),
            "trade_time": raw_data.get("trade_time", ""),
            "frequency": raw_data.get("frequency", "1d"),
            "currency": "CNY",
            "timezone": "Asia/Shanghai",
            "open": raw_data.get("open"),
            "high": raw_data.get("high"),
            "low": raw_data.get("low"),
            "close": raw_data.get("close"),
            "volume": raw_data.get("volume"),
            "money": raw_data.get("money"),
            "preclose": raw_data.get("preclose"),
            "high_limit": raw_data.get("high_limit"),
            "low_limit": raw_data.get("low_limit"),
            "unlimited": raw_data.get("unlimited", False),
        }

        return adapted_data

    def _default_get_market_info(self, market: str) -> Dict[str, Any]:
        """默认市场信息 (A股)"""
        market_info = {
            "SZ": {
                "market_code": "SZ",
                "market_name": "深圳证券交易所",
                "market_name_en": "Shenzhen Stock Exchange",
                "exchange": "SZSE",
                "currency": "CNY",
                "timezone": "Asia/Shanghai",
                "trading_hours": "09:30-11:30,13:00-15:00",
                "price_precision": 2,
                "has_price_limit": True,
                "price_limit_pct": 10.0,
            },
            "SS": {
                "market_code": "SS",
                "market_name": "上海证券交易所",
                "market_name_en": "Shanghai Stock Exchange",
                "exchange": "SSE",
                "currency": "CNY",
                "timezone": "Asia/Shanghai",
                "trading_hours": "09:30-11:30,13:00-15:00",
                "price_precision": 2,
                "has_price_limit": True,
                "price_limit_pct": 10.0,
            },
        }

        return market_info.get(market, {})

    def _default_normalize_symbol(self, symbol: str, market: str) -> str:
        """默认股票代码标准化 (A股)"""
        if not symbol:
            return ""

        symbol = symbol.strip().upper()

        # 移除市场后缀
        if "." in symbol:
            symbol = symbol.split(".")[0]

        # 补齐到6位
        if symbol.isdigit() and len(symbol) <= 6:
            symbol = symbol.zfill(6)

        # 添加市场后缀
        return f"{symbol}.{market}"

    def get_supported_markets(self) -> List[str]:
        """获取支持的市场列表"""
        return self.enabled_markets.copy()

    def is_market_enabled(self, market: str) -> bool:
        """检查市场是否启用"""
        return market.upper() in self.enabled_markets

    def get_manager_stats(self) -> Dict[str, Any]:
        """获取管理器统计信息"""
        return {
            "supported_markets": self.get_supported_markets(),
            "enabled_adapters": list(self.adapters.keys()),
            "default_market": self.default_market,
            "adapter_status": {
                market: adapter is not None for market, adapter in self.adapters.items()
            },
        }
