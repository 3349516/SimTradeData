#!/usr/bin/env python3
"""
Schema 一致性验证脚本

用途：验证数据库实际结构与 schema.py 定义是否一致
使用：poetry run python scripts/validate_schema.py
"""

import sqlite3
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from simtradedata.database.schema import DATABASE_INDEXES, DATABASE_SCHEMA


def validate_schema(db_path: str = "data/simtradedata.db"):
    """验证数据库与 schema 定义是否一致"""
    print("=" * 60)
    print("SimTradeData Schema 一致性验证")
    print("=" * 60)
    print()

    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    issues_found = 0

    # ==========================================
    # 1. 验证表
    # ==========================================
    print("📋 检查表结构...")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    actual_tables = {row[0] for row in cursor.fetchall()}

    # 排除 SQLite 内部表
    actual_tables = {t for t in actual_tables if not t.startswith("sqlite_")}

    expected_tables = set(DATABASE_SCHEMA.keys())

    missing_tables = expected_tables - actual_tables
    extra_tables = actual_tables - expected_tables

    if missing_tables:
        print(f"  ❌ 缺失的表: {', '.join(sorted(missing_tables))}")
        issues_found += len(missing_tables)
    else:
        print("  ✅ 所有表都存在")

    if extra_tables:
        print(f"  ⚠️  多余的表: {', '.join(sorted(extra_tables))}")
        issues_found += len(extra_tables)

    print()

    # ==========================================
    # 2. 验证索引
    # ==========================================
    print("🔍 检查索引...")
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    actual_indexes = {row[0] for row in cursor.fetchall()}
    expected_indexes = set(DATABASE_INDEXES.keys())

    missing_indexes = expected_indexes - actual_indexes
    extra_indexes = actual_indexes - expected_indexes

    if missing_indexes:
        print(f"  ❌ 缺失的索引:")
        for idx in sorted(missing_indexes):
            print(f"     - {idx}")
        issues_found += len(missing_indexes)
    else:
        print("  ✅ 所有索引都存在")

    if extra_indexes:
        print(f"  ℹ️  额外的索引: {', '.join(sorted(extra_indexes))}")

    print()

    # ==========================================
    # 3. 检查关键表的字段
    # ==========================================
    print("📊 检查关键表字段...")

    critical_tables = {
        "stocks": [
            "symbol",
            "name",
            "market",
            "status",
            "total_shares",
            "float_shares",
        ],
        "market_data": [
            "symbol",
            "date",
            "frequency",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
        ],
        "valuations": ["symbol", "date", "pe_ratio", "pb_ratio", "source"],
        "data_source_quality": [
            "source_name",
            "symbol",
            "data_type",
            "date",
            "success_rate",
        ],
    }

    for table_name, required_fields in critical_tables.items():
        if table_name not in actual_tables:
            continue

        cursor.execute(f"PRAGMA table_info({table_name})")
        actual_fields = {row[1] for row in cursor.fetchall()}

        missing_fields = set(required_fields) - actual_fields
        if missing_fields:
            print(f"  ❌ {table_name} 表缺失字段: {', '.join(sorted(missing_fields))}")
            issues_found += len(missing_fields)
        else:
            print(f"  ✅ {table_name} 表字段完整")

    print()

    # ==========================================
    # 4. 统计信息
    # ==========================================
    print("📈 数据库统计信息:")

    # 表数量
    print(f"  表数量: {len(actual_tables)}")

    # 索引数量
    print(f"  索引数量: {len(actual_indexes)}")

    # 数据量统计
    data_tables = ["stocks", "market_data", "valuations", "financials"]
    for table in data_tables:
        if table in actual_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table} 记录数: {count:,}")

    print()

    # ==========================================
    # 5. 总结
    # ==========================================
    print("=" * 60)
    if issues_found == 0:
        print("✅ Schema 验证通过！数据库结构与定义完全一致。")
        return_code = 0
    else:
        print(f"❌ 发现 {issues_found} 个问题，请检查上述输出。")
        print()
        print("修复建议：")
        if missing_indexes:
            print("  1. 运行索引创建脚本：")
            print(
                "     sqlite3 data/simtradedata.db < scripts/create_missing_indexes.sql"
            )
        if missing_tables:
            print("  2. 运行数据库初始化脚本：")
            print("     poetry run python scripts/init_database.py")
        return_code = 1

    print("=" * 60)

    conn.close()
    return return_code


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="验证数据库 Schema 一致性")
    parser.add_argument(
        "--db",
        default="data/simtradedata.db",
        help="数据库文件路径（默认: data/simtradedata.db）",
    )
    args = parser.parse_args()

    exit_code = validate_schema(args.db)
    sys.exit(exit_code)
