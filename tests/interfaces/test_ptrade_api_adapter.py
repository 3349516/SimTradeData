from datetime import date

import numpy as np
import pandas as pd
import pytest

from simtradedata.api import APIRouter
from simtradedata.config import Config
from simtradedata.database import DatabaseManager, create_database_schema
from simtradedata.interfaces.ptrade_api import PTradeAPIAdapter


@pytest.fixture
def adapter_with_db(tmp_path):
    config = Config()
    db_path = tmp_path / "ptrade_api_test.db"
    db_manager = DatabaseManager(str(db_path), config=config)
    create_database_schema(db_manager)
    api_router = APIRouter(db_manager, config)
    adapter = PTradeAPIAdapter(db_manager, api_router, config)
    yield adapter, db_manager


def _to_date_list(array_values: np.ndarray) -> list:
    if array_values.size == 0:
        return []
    return [pd.Timestamp(val).date() for val in array_values]


def test_get_trade_days_by_count(adapter_with_db):
    adapter, db_manager = adapter_with_db
    db_manager.execute("DELETE FROM trading_calendar")

    trading_days = [
        ("2024-01-01", "CN", 1),
        ("2024-01-02", "CN", 1),
        ("2024-01-03", "CN", 1),
        ("2024-01-04", "CN", 1),
        ("2024-01-05", "CN", 1),
        ("2024-01-06", "CN", 0),
        ("2024-01-07", "CN", 0),
        ("2024-01-08", "CN", 1),
    ]
    db_manager.executemany(
        "INSERT INTO trading_calendar (date, market, is_trading) VALUES (?, ?, ?)",
        trading_days,
    )

    result = adapter.get_trade_days(count=3, end_date="2024-01-05", market="CN")

    assert _to_date_list(result) == [
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]


def test_get_trading_day_handles_non_trading(adapter_with_db):
    adapter, db_manager = adapter_with_db
    db_manager.execute("DELETE FROM trading_calendar")

    calendar_entries = [
        ("2024-01-05", "CN", 1),
        ("2024-01-06", "CN", 0),
        ("2024-01-07", "CN", 0),
        ("2024-01-08", "CN", 1),
    ]
    db_manager.executemany(
        "INSERT INTO trading_calendar (date, market, is_trading) VALUES (?, ?, ?)",
        calendar_entries,
    )

    current = adapter.get_trading_day(0, reference_date="2024-01-06", market="CN")
    next_day = adapter.get_trading_day(1, reference_date="2024-01-05", market="CN")
    prev_day = adapter.get_previous_trade_day("2024-01-08", market="CN")

    assert current == date(2024, 1, 5)
    assert next_day == date(2024, 1, 8)
    assert prev_day == date(2024, 1, 5)


def test_get_Ashares_filters_markets(adapter_with_db):
    adapter, db_manager = adapter_with_db
    db_manager.execute("DELETE FROM stocks")

    stock_rows = [
        {
            "symbol": "000001.SZ",
            "name": "Ping An Bank",
            "market": "SZ",
            "industry_l1": "Banking",
            "industry_l2": "Retail Banking",
            "concepts": '["Finance"]',
            "list_date": "2007-04-03",
            "delist_date": None,
            "lot_size": 100,
            "total_shares": 1_000_000_000,
            "float_shares": 800_000_000,
            "status": "active",
            "is_st": 0,
        },
        {
            "symbol": "600000.SS",
            "name": "Shanghai Pudong",
            "market": "SS",
            "industry_l1": "Banking",
            "industry_l2": "Corporate Banking",
            "concepts": '["Finance"]',
            "list_date": "2000-11-10",
            "delist_date": None,
            "lot_size": 100,
            "total_shares": 2_000_000_000,
            "float_shares": 1_500_000_000,
            "status": "active",
            "is_st": 0,
        },
        {
            "symbol": "00700.HK",
            "name": "Tencent",
            "market": "HK",
            "industry_l1": "Technology",
            "industry_l2": "Internet",
            "concepts": '["Tech"]',
            "list_date": "2004-06-16",
            "delist_date": None,
            "lot_size": 100,
            "total_shares": 950_000_000,
            "float_shares": 950_000_000,
            "status": "active",
            "is_st": 0,
        },
    ]

    db_manager.executemany(
        """
        INSERT INTO stocks (
            symbol, name, market, industry_l1, industry_l2, concepts,
            list_date, delist_date, lot_size, total_shares, float_shares, status, is_st
        ) VALUES (:symbol, :name, :market, :industry_l1, :industry_l2, :concepts,
                  :list_date, :delist_date, :lot_size, :total_shares, :float_shares, :status, :is_st)
        """,
        stock_rows,
    )

    result = adapter.get_Ashares()

    assert not result.empty
    assert set(result["code"].tolist()) == {"000001.SZ", "600000.SS"}


def test_sector_related_apis(adapter_with_db):
    adapter, db_manager = adapter_with_db

    db_manager.execute("DROP TABLE IF EXISTS ptrade_concept_sectors")
    db_manager.execute("DROP TABLE IF EXISTS ptrade_sector_constituents")

    db_manager.execute(
        """
        CREATE TABLE ptrade_concept_sectors (
            sector_code TEXT PRIMARY KEY,
            sector_name TEXT,
            sector_type TEXT,
            description TEXT,
            creation_date TEXT,
            status TEXT,
            last_update TEXT
        )
        """
    )

    db_manager.execute(
        """
        CREATE TABLE ptrade_sector_constituents (
            sector_code TEXT,
            symbol TEXT,
            stock_name TEXT,
            weight REAL,
            shares REAL,
            market_value REAL,
            effective_date TEXT,
            last_update TEXT
        )
        """
    )

    db_manager.executemany(
        "INSERT INTO ptrade_concept_sectors VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "CONCEPT_NEW",
                "New Energy",
                "concept",
                "",
                "2024-01-01",
                "active",
                "2024-01-01",
            ),
            (
                "INDEX_TOP",
                "Top Index",
                "index",
                "",
                "2024-01-01",
                "active",
                "2024-01-01",
            ),
        ],
    )

    db_manager.executemany(
        "INSERT INTO ptrade_sector_constituents VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "CONCEPT_NEW",
                "000001.SZ",
                "Ping An Bank",
                0.15,
                1000000,
                10000000,
                "2024-01-05",
                "2024-01-05",
            ),
            (
                "INDEX_TOP",
                "600000.SS",
                "Shanghai Pudong",
                0.20,
                1500000,
                20000000,
                "2024-01-05",
                "2024-01-05",
            ),
        ],
    )

    blocks = adapter.get_stock_blocks("000001.SZ")
    index_constituents = adapter.get_index_stocks("INDEX_TOP")
    concepts = adapter.get_concept()

    assert blocks and blocks[0]["sector_code"] == "CONCEPT_NEW"
    assert index_constituents and index_constituents[0]["symbol"] == "600000.SS"
    assert {item["sector_code"] for item in concepts} == {"CONCEPT_NEW"}


class StubMootdxClient:
    def __init__(self):
        self._transaction = pd.DataFrame(
            {
                "time": ["09:30:00", "09:30:01"],
                "price": [10.5, 10.55],
                "vol": [100, 200],
                "num": [1, 2],
                "buyorsell": ["B", "S"],
                "type": [0, 0],
            }
        )
        self._minutes = pd.DataFrame(
            {
                "time": ["09:30", "09:31"],
                "price": [10.5, 10.6],
                "vol": [1000, 1200],
                "amount": [10500, 12720],
            }
        )
        self._stock_count = {"SZ": 2, "SS": 1}
        self._quotes = [
            {"symbol": "000001.SZ", "last": 10.5, "open": 10.4},
            {"symbol": "600000.SS", "last": 9.2, "open": 9.1},
        ]

    def transaction(self, symbol: str, start: int = 0, offset: int = 200):
        return self._transaction.copy()

    def minutes(self, symbol: str, date: str):
        return self._minutes.copy()

    def stock_count(self):
        return self._stock_count

    def quotes(self, symbols=None):
        return self._quotes


def test_mootdx_deep_market_methods(adapter_with_db):
    adapter, db_manager = adapter_with_db

    # 确保股票表中存在基础数据
    db_manager.execute("DELETE FROM stocks")
    db_manager.executemany(
        "INSERT INTO stocks (symbol, name, market, industry_l1, industry_l2, concepts, list_date, total_shares, float_shares, status, is_st) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "000001.SZ",
                "Ping An Bank",
                "SZ",
                "Banking",
                "Retail",
                '["Finance"]',
                "2007-04-03",
                1000,
                800,
                "active",
                0,
            ),
            (
                "600000.SS",
                "Shanghai Pudong",
                "SS",
                "Banking",
                "Corporate",
                '["Finance"]',
                "2000-11-10",
                2000,
                1500,
                "active",
                0,
            ),
            (
                "00700.HK",
                "Tencent",
                "HK",
                "Technology",
                "Internet",
                '["Tech"]',
                "2004-06-16",
                950,
                950,
                "active",
                0,
            ),
        ],
    )

    # 准备除权表数据
    db_manager.execute("DELETE FROM corporate_actions")
    db_manager.execute(
        "INSERT INTO corporate_actions (symbol, ex_date, record_date, cash_dividend, stock_dividend, rights_ratio, rights_price, split_ratio, adj_factor, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "000001.SZ",
            "2024-01-05",
            "2024-01-04",
            0.5,
            0.0,
            0.0,
            0.0,
            1.0,
            0.98,
            "test",
        ),
    )

    stub = StubMootdxClient()
    adapter._mootdx_client = stub

    entrust_df = adapter.get_individual_entrust("000001.SZ")
    transaction_df = adapter.get_individual_transaction("000001.SZ")
    tick_df = adapter.get_tick_direction("000001.SZ", query_date="2024-01-05")
    market_list_df = adapter.get_market_list()
    market_detail_df = adapter.get_market_detail(["000001.SZ", "600000.SS"])
    exrights_df = adapter.get_stock_exrights("000001.SZ")

    assert not entrust_df.empty and set(["volume", "number", "direction"]).issubset(
        entrust_df.columns
    )
    assert not transaction_df.empty and set(["symbol", "time"]).issubset(
        transaction_df.columns
    )
    assert not tick_df.empty and "symbol" in tick_df.columns
    assert not market_list_df.empty and set(market_list_df.columns) == {
        "market",
        "count",
    }
    assert not market_detail_df.empty
    assert not exrights_df.empty and exrights_df.iloc[0]["cash_dividend"] == 0.5
