"""
PTrade API兼容层

提供与PTrade原生API兼容的接口，确保现有代码无缝迁移。
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Union

import numpy as np
import pandas as pd

from ..api import APIRouter
from ..config import Config
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class PTradeAPIAdapter:
    """PTrade API适配器"""

    def __init__(
        self, db_manager: DatabaseManager, api_router: APIRouter, config: Config = None
    ):
        """
        初始化PTrade API适配器

        Args:
            db_manager: 数据库管理器
            api_router: API路由器
            config: 配置对象
        """
        self.db_manager = db_manager
        self.api_router = api_router
        self.config = config or Config()

        # API兼容性配置
        self.enable_legacy_format = self.config.get(
            "ptrade_api.enable_legacy_format", True
        )
        self.default_market = self.config.get("ptrade_api.default_market", "SZ")
        self.max_records = self.config.get("ptrade_api.max_records", 10000)
        self.default_calendar_market = self.config.get(
            "ptrade_api.default_calendar_market", "CN"
        )

        # 延迟初始化的依赖
        self._sector_manager = None
        self._mootdx_client = None  # mootdx Quotes客户端懒加载缓存

        # 股票信息字段映射，确保与数据库结构保持一致
        self.stock_info_fields = [
            "symbol",
            "name",
            "market",
            "industry_l1",
            "industry_l2",
            "concepts",
            "list_date",
            "delist_date",
            "total_shares",
            "float_shares",
            "status",
            "is_st",
        ]

        logger.info("PTrade API adapter initialized")

    def get_stock_list(self, market: str = None) -> pd.DataFrame:
        """
        获取股票列表 (兼容PTrade原生API)

        Args:
            market: 市场代码 ('SZ', 'SS', 'HK', 'US')

        Returns:
            pd.DataFrame: 股票列表
        """
        try:
            # 调用API路由器获取股票信息
            result = self.api_router.get_stock_info(
                market=market, fields=self.stock_info_fields, format_type="dataframe"
            )

            if isinstance(result, pd.DataFrame):
                # 确保兼容PTrade格式
                return self._format_stock_list(result)
            else:
                logger.warning("API router returned non-DataFrame format")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"retrieving stock list failed : {e}")
            return pd.DataFrame()

    def get_price(
        self,
        symbol: Union[str, List[str]],
        start_date: str = None,
        end_date: str = None,
        frequency: str = "1d",
    ) -> pd.DataFrame:
        """
        获取股票价格数据 (兼容PTrade原生API)

        Args:
            symbol: 股票代码或代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率 ('1d', '1m', '5m', '15m', '30m', '60m')

        Returns:
            pd.DataFrame: 价格数据
        """
        try:
            # 标准化参数
            if isinstance(symbol, str):
                symbols = [symbol]
            else:
                symbols = symbol

            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")

            if start_date is None:
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

            # 调用API路由器获取历史数据
            result = self.api_router.get_history(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                format_type="dataframe",
            )

            if isinstance(result, pd.DataFrame):
                # 确保兼容PTrade格式
                return self._format_price_data(result)
            else:
                logger.warning("API router returned non-DataFrame format")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"retrieving price data failed : {e}")
            return pd.DataFrame()

    def get_fundamentals(
        self, symbol: Union[str, List[str]], fields: List[str] = None
    ) -> pd.DataFrame:
        """
        获取基本面数据 (兼容PTrade原生API)

        Args:
            symbol: 股票代码或代码列表
            fields: 字段列表

        Returns:
            pd.DataFrame: 基本面数据
        """
        try:
            # 标准化参数
            if isinstance(symbol, str):
                symbols = [symbol]
            else:
                symbols = symbol

            if fields is None:
                fields = ["pe", "pb", "ps", "market_cap", "total_share", "float_share"]

            # 调用API路由器获取财务数据
            result = self.api_router.get_fundamentals(
                symbols=symbols, fields=fields, format_type="dataframe"
            )

            if isinstance(result, pd.DataFrame):
                return self._format_fundamentals(result)
            else:
                logger.warning("API router returned non-DataFrame format")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"failed to retrieve fundamental data : {e}")
            return pd.DataFrame()

    def get_industry(
        self, symbol: Union[str, List[str]], standard: str = "sw"
    ) -> pd.DataFrame:
        """
        获取行业分类数据 (兼容PTrade原生API)

        Args:
            symbol: 股票代码或代码列表
            standard: 分类标准 ('sw', 'citic', 'zjh', 'gics')

        Returns:
            pd.DataFrame: 行业分类数据
        """
        try:
            # 标准化参数
            if isinstance(symbol, str):
                symbols = [symbol]
            else:
                symbols = symbol

            # 构建查询参数
            query_params = {
                "data_type": "industry_classification",
                "symbols": symbols,
                "standard": standard,
                "format": "dataframe",
            }

            # 调用API路由器
            result = self.api_router.query(query_params)

            if isinstance(result, pd.DataFrame):
                return self._format_industry_data(result)
            else:
                logger.warning("API router returned non-DataFrame format")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"failed to retrieve industry classification : {e}")
            return pd.DataFrame()

    def get_etf_holdings(self, etf_symbol: str, date: str = None) -> pd.DataFrame:
        """
        获取ETF成分股 (扩展PTrade API)

        Args:
            etf_symbol: ETF代码
            date: 持仓日期

        Returns:
            pd.DataFrame: ETF成分股数据
        """
        try:
            # 构建查询参数
            query_params = {
                "data_type": "etf_holdings",
                "etf_symbol": etf_symbol,
                "format": "dataframe",
            }

            if date:
                query_params["date"] = date

            # 调用API路由器
            result = self.api_router.query(query_params)

            if isinstance(result, pd.DataFrame):
                return self._format_etf_holdings(result)
            else:
                logger.warning("API router returned non-DataFrame format")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"failed to retrieve ETF constituent stocks : {e}")
            return pd.DataFrame()

    def get_technical_indicators(
        self,
        symbol: str,
        indicators: List[str],
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        获取技术指标 (扩展PTrade API)

        Args:
            symbol: 股票代码
            indicators: 指标列表 ['ma', 'rsi', 'macd', 'bollinger', 'kdj']
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            pd.DataFrame: 技术指标数据
        """
        try:
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")

            if start_date is None:
                start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

            # 构建查询参数
            query_params = {
                "data_type": "technical_indicators",
                "symbol": symbol,
                "indicators": indicators,
                "start_date": start_date,
                "end_date": end_date,
                "format": "dataframe",
            }

            # 调用API路由器
            result = self.api_router.query(query_params)

            if isinstance(result, pd.DataFrame):
                return self._format_technical_indicators(result)
            else:
                logger.warning("API router returned non-DataFrame format")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"failed to retrieve technical indicators : {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # 交易日历相关接口
    # ------------------------------------------------------------------

    def get_trading_day(
        self,
        day: int = 0,
        reference_date: Union[str, datetime, date, None] = None,
        market: str = None,
    ) -> Optional[date]:
        """获取相对交易日"""

        market = (market or self.default_calendar_market).upper()
        target_date = self._normalize_calendar_date(reference_date)

        try:
            base_row = self.db_manager.fetchone(
                """
                SELECT date FROM trading_calendar
                WHERE market = ? AND is_trading = 1 AND date <= ?
                ORDER BY date DESC LIMIT 1
                """,
                (market, target_date.strftime("%Y-%m-%d")),
            )

            if not base_row:
                logger.warning(
                    "no trading day found before %s for market %s",
                    target_date,
                    market,
                )
                return None

            base_date = datetime.strptime(base_row["date"], "%Y-%m-%d").date()

            if day == 0:
                return base_date

            if day > 0:
                rows = self.db_manager.fetchall(
                    """
                    SELECT date FROM trading_calendar
                    WHERE market = ? AND is_trading = 1 AND date > ?
                    ORDER BY date ASC LIMIT ?
                    """,
                    (market, base_date.strftime("%Y-%m-%d"), day),
                )

                if len(rows) < day:
                    logger.warning(
                        "insufficient forward trading days (requested=%s, available=%s)",
                        day,
                        len(rows),
                    )
                    return None

                return datetime.strptime(rows[day - 1]["date"], "%Y-%m-%d").date()

            backwards = abs(day)
            rows = self.db_manager.fetchall(
                """
                SELECT date FROM trading_calendar
                WHERE market = ? AND is_trading = 1 AND date < ?
                ORDER BY date DESC LIMIT ?
                """,
                (market, base_date.strftime("%Y-%m-%d"), backwards),
            )

            if len(rows) < backwards:
                logger.warning(
                    "insufficient backward trading days (requested=%s, available=%s)",
                    backwards,
                    len(rows),
                )
                return None

            return datetime.strptime(rows[backwards - 1]["date"], "%Y-%m-%d").date()

        except Exception as exc:
            logger.error("retrieving trading day failed : %s", exc)
            return None

    def get_previous_trade_day(
        self,
        reference_date: Union[str, datetime, date, None] = None,
        market: str = None,
    ) -> Optional[date]:
        """获取上一交易日"""

        return self.get_trading_day(-1, reference_date=reference_date, market=market)

    def get_all_trades_days(
        self,
        reference_date: Union[str, datetime, date, None] = None,
        market: str = None,
    ) -> np.ndarray:
        """获取截止某日期的全部交易日"""

        market = (market or self.default_calendar_market).upper()
        target_date = self._normalize_calendar_date(reference_date)

        try:
            rows = self.db_manager.fetchall(
                """
                SELECT date FROM trading_calendar
                WHERE market = ? AND is_trading = 1 AND date <= ?
                ORDER BY date ASC
                """,
                (market, target_date.strftime("%Y-%m-%d")),
            )

            return self._rows_to_date_array(rows)

        except Exception as exc:
            logger.error("retrieving all trade days failed : %s", exc)
            return np.array([], dtype="datetime64[ns]")

    def get_trade_days(
        self,
        start_date: Union[str, datetime, date, None] = None,
        end_date: Union[str, datetime, date, None] = None,
        count: Optional[int] = None,
        market: str = None,
    ) -> np.ndarray:
        """获取指定区间交易日"""

        market = (market or self.default_calendar_market).upper()

        if start_date and count is not None:
            raise ValueError("start_date and count cannot be used together")
        if count is not None and count <= 0:
            raise ValueError("count must be positive")

        try:
            if count is not None:
                end_dt = self._normalize_calendar_date(end_date)

                anchor_row = self.db_manager.fetchone(
                    """
                    SELECT date FROM trading_calendar
                    WHERE market = ? AND is_trading = 1 AND date <= ?
                    ORDER BY date DESC LIMIT 1
                    """,
                    (market, end_dt.strftime("%Y-%m-%d")),
                )

                if not anchor_row:
                    return np.array([], dtype="datetime64[ns]")

                anchor_date = anchor_row["date"]
                rows = self.db_manager.fetchall(
                    """
                    SELECT date FROM trading_calendar
                    WHERE market = ? AND is_trading = 1 AND date <= ?
                    ORDER BY date DESC LIMIT ?
                    """,
                    (market, anchor_date, count),
                )

                rows.reverse()
                return self._rows_to_date_array(rows)

            if end_date is None:
                end_date = datetime.now().date()

            start_dt = self._normalize_calendar_date(start_date)
            end_dt = self._normalize_calendar_date(end_date)

            rows = self.db_manager.fetchall(
                """
                SELECT date FROM trading_calendar
                WHERE market = ? AND is_trading = 1 AND date BETWEEN ? AND ?
                ORDER BY date ASC
                """,
                (
                    market,
                    start_dt.strftime("%Y-%m-%d"),
                    end_dt.strftime("%Y-%m-%d"),
                ),
            )

            return self._rows_to_date_array(rows)

        except Exception as exc:
            logger.error("retrieving trade days failed : %s", exc)
            return np.array([], dtype="datetime64[ns]")

    # ------------------------------------------------------------------
    # 股票基础信息接口
    # ------------------------------------------------------------------

    def get_Ashares(self) -> pd.DataFrame:
        """获取A股列表"""

        try:
            df = self.api_router.get_stock_info(
                fields=self.stock_info_fields, format_type="dataframe"
            )
            if df.empty:
                return df

            df = df.reset_index(drop=False)
            if "market" in df.columns:
                df = df[df["market"].isin(["SZ", "SS"])]

            return self._format_stock_list(df)

        except Exception as exc:
            logger.error("retrieving A-share list failed : %s", exc)
            return pd.DataFrame()

    def get_all_securities(self) -> pd.DataFrame:
        """获取所有证券列表"""

        try:
            df = self.api_router.get_stock_info(
                fields=self.stock_info_fields, format_type="dataframe"
            )
            if df.empty:
                return df

            return self._format_stock_list(df.reset_index(drop=False))

        except Exception as exc:
            logger.error("retrieving all securities failed : %s", exc)
            return pd.DataFrame()

    def get_security_info(self, symbol: str) -> Dict[str, Any]:
        """获取单只证券信息"""

        try:
            result = self.api_router.get_stock_info(
                symbols=[symbol], fields=self.stock_info_fields, format_type="records"
            )

            if isinstance(result, list) and result:
                return result[0]

            return {}

        except Exception as exc:
            logger.error("retrieving security info failed : %s", exc)
            return {}

    # ------------------------------------------------------------------
    # 市场行情接口
    # ------------------------------------------------------------------

    def get_snapshot(
        self,
        symbols: Union[str, List[str]],
        trade_date: Union[str, datetime, date, None] = None,
        market: str = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """获取行情快照"""

        symbol_list = [symbols] if isinstance(symbols, str) else list(symbols)

        trade_date_str = None
        if trade_date is not None:
            trade_date_str = self._normalize_calendar_date(trade_date).strftime(
                "%Y-%m-%d"
            )

        try:
            result = self.api_router.get_snapshot(
                symbols=symbol_list,
                trade_date=trade_date_str,
                market=market,
                fields=fields,
                format_type="dataframe",
            )

            if isinstance(result, pd.DataFrame):
                return result

            return pd.DataFrame(result or [])

        except Exception as exc:
            logger.error("retrieving snapshot failed : %s", exc)
            return pd.DataFrame()

    def get_bars(
        self,
        symbol: str,
        start_date: Union[str, datetime, date, None] = None,
        end_date: Union[str, datetime, date, None] = None,
        frequency: str = "1d",
    ) -> pd.DataFrame:
        """获取K线数据"""

        start = (
            self._normalize_calendar_date(start_date)
            if start_date is not None
            else datetime.now().date() - timedelta(days=30)
        )
        end = (
            self._normalize_calendar_date(end_date)
            if end_date is not None
            else datetime.now().date()
        )

        try:
            return self.get_price(
                symbol,
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d"),
                frequency=frequency,
            )
        except Exception as exc:
            logger.error("retrieving bars failed : %s", exc)
            return pd.DataFrame()

    def get_individual_entrust(
        self, symbol: str, start: int = 0, limit: int = 200
    ) -> pd.DataFrame:
        """获取逐笔委托数据"""

        client = self._get_mootdx_client()
        if client is None:
            return pd.DataFrame()

        try:
            df = client.transaction(symbol=symbol, start=start, offset=limit)
        except Exception as exc:
            logger.error("retrieving individual entrust failed : %s", exc)
            return pd.DataFrame()

        return self._format_transaction_data(df, symbol)

    def get_individual_transaction(
        self, symbol: str, start: int = 0, limit: int = 200
    ) -> pd.DataFrame:
        """获取逐笔成交数据"""

        client = self._get_mootdx_client()
        if client is None:
            return pd.DataFrame()

        try:
            df = client.transaction(symbol=symbol, start=start, offset=limit)
        except Exception as exc:
            logger.error("retrieving individual transaction failed : %s", exc)
            return pd.DataFrame()

        return self._format_transaction_data(df, symbol)

    def get_tick_direction(
        self,
        symbol: str,
        query_date: Union[str, datetime, date, int, None] = None,
        start_pos: int = 0,
        search_direction: int = 1,
        data_count: int = 50,
    ) -> pd.DataFrame:
        """获取分时成交方向数据"""

        client = self._get_mootdx_client()
        if client is None:
            return pd.DataFrame()

        normalized_date = self._normalize_calendar_date(query_date)
        date_str = normalized_date.strftime("%Y%m%d")

        try:
            df = client.minutes(symbol=symbol, date=date_str)
        except Exception as exc:
            logger.error("retrieving tick direction failed : %s", exc)
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        if search_direction >= 0:
            sliced = df.iloc[start_pos : start_pos + data_count]
        else:
            end_pos = max(len(df) - start_pos, 0)
            begin_pos = max(end_pos - data_count, 0)
            sliced = df.iloc[begin_pos:end_pos]

        return self._format_tick_direction_data(sliced, symbol)

    def get_market_list(self) -> pd.DataFrame:
        """获取市场列表及证券数量"""

        try:
            rows = self.db_manager.fetchall(
                "SELECT market, COUNT(*) as count FROM stocks GROUP BY market"
            )
            if rows:
                return pd.DataFrame([dict(row) for row in rows])
        except Exception as exc:
            logger.warning("failed to aggregate market list from database : %s", exc)

        client = self._get_mootdx_client()
        if client is None or not hasattr(client, "stock_count"):
            return pd.DataFrame()

        try:
            counts = client.stock_count()
            if isinstance(counts, dict):
                return pd.DataFrame(
                    [
                        {"market": key.upper(), "count": value}
                        for key, value in counts.items()
                    ]
                )
            return pd.DataFrame(counts)
        except Exception as exc:
            logger.error("retrieving market list failed : %s", exc)
            return pd.DataFrame()

    def get_market_detail(
        self, symbols: Union[str, List[str], None] = None
    ) -> pd.DataFrame:
        """获取市场详情快照"""

        if symbols is None:
            return self.get_snapshot(symbols=[], market=self.default_market)

        client_symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        snapshot = self.get_snapshot(client_symbols)
        if isinstance(snapshot, pd.DataFrame) and not snapshot.empty:
            return snapshot

        client = self._get_mootdx_client()
        if client is None or not hasattr(client, "quotes"):
            return pd.DataFrame()

        try:
            data = client.quotes(symbols=client_symbols)
            return pd.DataFrame(data)
        except Exception as exc:
            logger.error("retrieving market detail failed : %s", exc)
            return pd.DataFrame()

    def get_stock_exrights(
        self,
        symbol: str,
        start_date: Union[str, datetime, date, None] = None,
        end_date: Union[str, datetime, date, None] = None,
    ) -> pd.DataFrame:
        """获取除权除息信息"""

        if not self._table_exists("corporate_actions"):
            logger.warning("corporate_actions table not found, exrights unavailable")
            return pd.DataFrame()

        conditions = ["symbol = ?"]
        params = [symbol]

        if start_date is not None:
            start_dt = self._normalize_calendar_date(start_date)
            conditions.append("ex_date >= ?")
            params.append(start_dt.strftime("%Y-%m-%d"))

        if end_date is not None:
            end_dt = self._normalize_calendar_date(end_date)
            conditions.append("ex_date <= ?")
            params.append(end_dt.strftime("%Y-%m-%d"))

        where_clause = " AND ".join(conditions)

        try:
            rows = self.db_manager.fetchall(
                f"SELECT * FROM corporate_actions WHERE {where_clause} ORDER BY ex_date ASC",
                params,
            )
            return pd.DataFrame([dict(row) for row in rows])
        except Exception as exc:
            logger.error("retrieving stock exrights failed : %s", exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # 板块/行业接口
    # ------------------------------------------------------------------
    def get_stock_blocks(self, symbol: str) -> List[Dict[str, Any]]:
        """获取股票所属板块信息"""

        manager = self._get_sector_manager()
        if manager is None:
            return []

        try:
            return manager.get_stock_sectors(symbol) or []
        except Exception as exc:
            logger.error("retrieving stock blocks failed : %s", exc)
            return []

    def get_index_stocks(
        self, index_code: str, effective_date: Union[str, datetime, date, None] = None
    ) -> List[Dict[str, Any]]:
        """获取指数成分股"""

        manager = self._get_sector_manager()
        if manager is None:
            return []

        eff_date = None
        if effective_date is not None:
            eff_date = self._normalize_calendar_date(effective_date)

        try:
            return manager.get_sector_constituents(index_code, eff_date)
        except Exception as exc:
            logger.error("retrieving index constituents failed : %s", exc)
            return []

    def get_concept(self) -> List[Dict[str, Any]]:
        """获取概念板块列表"""

        manager = self._get_sector_manager()
        if manager is None:
            return []

        try:
            return manager.get_sector_list(sector_type="concept")
        except Exception as exc:
            logger.error("retrieving concept sectors failed : %s", exc)
            return []

    def _format_stock_list(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化股票列表为PTrade兼容格式"""
        if df.empty:
            return df

        # PTrade标准字段映射
        column_mapping = {
            "symbol": "code",
            "stock_name": "name",
            "market": "market",
            "exchange": "exchange",
            "industry_l1": "industry",
            "industry_l2": "industry_sub",
            "list_date": "list_date",
            "status": "status",
        }

        # 重命名列
        formatted_df = df.copy()
        for old_col, new_col in column_mapping.items():
            if old_col in formatted_df.columns:
                formatted_df = formatted_df.rename(columns={old_col: new_col})

        # 确保必需字段存在
        required_fields = ["code", "name", "market"]
        for field in required_fields:
            if field not in formatted_df.columns:
                formatted_df[field] = ""

        return formatted_df

    def _format_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化价格数据为PTrade兼容格式"""
        if df.empty:
            return df

        # 重置索引，将symbol和trade_date变为列
        formatted_df = df.reset_index()

        # PTrade标准字段映射
        column_mapping = {
            "symbol": "code",
            "trade_date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "money": "amount",
        }

        # 重命名列
        for old_col, new_col in column_mapping.items():
            if old_col in formatted_df.columns:
                formatted_df = formatted_df.rename(columns={old_col: new_col})

        # 设置索引
        if "date" in formatted_df.columns:
            formatted_df["date"] = pd.to_datetime(formatted_df["date"])
            if "code" in formatted_df.columns:
                formatted_df = formatted_df.set_index(["code", "date"])
            else:
                formatted_df = formatted_df.set_index("date")

        return formatted_df

    def _format_fundamentals(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化基本面数据为PTrade兼容格式"""
        if df.empty:
            return df

        # PTrade标准字段映射
        column_mapping = {
            "symbol": "code",
            "pe": "pe_ratio",
            "pb": "pb_ratio",
            "ps": "ps_ratio",
            "market_cap": "market_cap",
            "total_share": "total_shares",
            "float_share": "float_shares",
        }

        # 重命名列
        formatted_df = df.copy()
        for old_col, new_col in column_mapping.items():
            if old_col in formatted_df.columns:
                formatted_df = formatted_df.rename(columns={old_col: new_col})

        return formatted_df

    def _format_industry_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化行业数据为PTrade兼容格式"""
        if df.empty:
            return df

        # PTrade标准字段映射
        column_mapping = {
            "symbol": "code",
            "level1_name": "industry_l1",
            "level2_name": "industry_l2",
            "level3_name": "industry_l3",
            "standard": "classification",
        }

        # 重命名列
        formatted_df = df.copy()
        for old_col, new_col in column_mapping.items():
            if old_col in formatted_df.columns:
                formatted_df = formatted_df.rename(columns={old_col: new_col})

        return formatted_df

    def _format_etf_holdings(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化ETF成分股为PTrade兼容格式"""
        if df.empty:
            return df

        # PTrade标准字段映射
        column_mapping = {
            "stock_symbol": "code",
            "stock_name": "name",
            "weight": "weight",
            "shares": "shares",
            "market_value": "market_value",
        }

        # 重命名列
        formatted_df = df.copy()
        for old_col, new_col in column_mapping.items():
            if old_col in formatted_df.columns:
                formatted_df = formatted_df.rename(columns={old_col: new_col})

        return formatted_df

    def _format_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化技术指标为PTrade兼容格式"""
        if df.empty:
            return df

        # 技术指标通常保持原格式，只需要确保日期索引
        formatted_df = df.copy()

        if "trade_date" in formatted_df.columns:
            formatted_df["trade_date"] = pd.to_datetime(formatted_df["trade_date"])
            formatted_df = formatted_df.set_index("trade_date")

        return formatted_df

    def get_adapter_info(self) -> Dict[str, Any]:
        """获取适配器信息"""
        return {
            "adapter_name": "PTrade API Adapter",
            "version": "1.0.0",
            "compatible_apis": [
                "get_stock_list",
                "get_Ashares",
                "get_all_securities",
                "get_security_info",
                "get_price",
                "get_fundamentals",
                "get_industry",
                "get_trading_day",
                "get_trade_days",
                "get_all_trades_days",
                "get_previous_trade_day",
                "get_snapshot",
                "get_bars",
                "get_individual_entrust",
                "get_individual_transaction",
                "get_tick_direction",
                "get_market_list",
                "get_market_detail",
                "get_stock_exrights",
                "get_etf_holdings",
                "get_technical_indicators",
                "get_stock_blocks",
                "get_index_stocks",
                "get_concept",
            ],
            "enable_legacy_format": self.enable_legacy_format,
            "default_market": self.default_market,
            "max_records": self.max_records,
            "supported_markets": ["SZ", "SS", "HK", "US"],
            "supported_frequencies": ["1m", "5m", "15m", "30m", "60m", "1d"],
        }

    # ------------------------------------------------------------------
    # 内部工具方法
    # ------------------------------------------------------------------

    def _get_sector_manager(self):
        """延迟加载板块数据管理器"""

        if self._sector_manager is None:
            try:
                from ..extended_data.sector_data import SectorDataManager

                self._sector_manager = SectorDataManager(
                    db_manager=self.db_manager, config=self.config
                )
            except Exception as exc:
                logger.error("initializing sector manager failed : %s", exc)
                self._sector_manager = None

        return self._sector_manager

    def _normalize_calendar_date(
        self, value: Union[str, datetime, date, int, None]
    ) -> date:
        """标准化日期输入"""

        if value is None:
            return datetime.now().date()

        if isinstance(value, int):
            return datetime.now().date() + timedelta(days=value)

        if isinstance(value, date) and not isinstance(value, datetime):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            value = value.strip()
            fmt = "%Y-%m-%d" if "-" in value else "%Y%m%d"
            return datetime.strptime(value, fmt).date()

        raise ValueError(f"unsupported date type: {type(value)!r}")

    @staticmethod
    def _rows_to_date_array(rows: Iterable[Dict[str, Any]]) -> np.ndarray:
        """将数据库行转换为numpy日期数组"""

        dates: List[datetime] = []
        for row in rows or []:
            try:
                dates.append(datetime.strptime(row["date"], "%Y-%m-%d").date())
            except Exception:
                continue

        if not dates:
            return np.array([], dtype="datetime64[ns]")

        return np.array(dates, dtype="datetime64[ns]")

    def _get_mootdx_client(self):
        """获取mootdx Quotes客户端实例"""

        if self._mootdx_client is False:
            return None

        if self._mootdx_client is not None:
            return self._mootdx_client

        try:
            from mootdx.quotes import Quotes  # type: ignore
        except Exception as exc:
            logger.warning("mootdx quotes client unavailable : %s", exc)
            self._mootdx_client = False
            return None

        market_code = self.config.get("data_sources.mootdx.market", "std")
        timeout = self.config.get("data_sources.mootdx.timeout", 15)

        try:
            client = Quotes.factory(
                market=market_code, multithread=True, heartbeat=True, timeout=timeout
            )
            self._mootdx_client = client
            return client
        except Exception as exc:
            logger.error("failed to initialize mootdx client : %s", exc)
            self._mootdx_client = False
            return None

    def _format_transaction_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """标准化逐笔委托/成交数据"""

        if df is None or df.empty:
            return pd.DataFrame()

        formatted = df.copy()
        rename_map = {
            "vol": "volume",
            "volumn": "volume",
            "num": "number",
            "buyorsell": "direction",
            "buy_or_sell": "direction",
        }

        for old, new in rename_map.items():
            if old in formatted.columns and new != old:
                formatted = formatted.rename(columns={old: new})

        formatted["symbol"] = symbol

        if "time" in formatted.columns:
            formatted["time"] = formatted["time"].astype(str)

        return formatted

    def _format_tick_direction_data(
        self, df: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        """标准化分时成交方向数据"""

        if df is None or df.empty:
            return pd.DataFrame()

        formatted = df.copy()
        if "time" in formatted.columns:
            formatted["time"] = formatted["time"].astype(str)
        if "vol" in formatted.columns and "volume" not in formatted.columns:
            formatted = formatted.rename(columns={"vol": "volume"})

        formatted["symbol"] = symbol
        return formatted

    def _table_exists(self, table_name: str) -> bool:
        """检查指定数据表是否存在"""

        try:
            row = self.db_manager.fetchone(
                "SELECT name FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
                (table_name,),
            )
            return row is not None
        except Exception:
            return False
