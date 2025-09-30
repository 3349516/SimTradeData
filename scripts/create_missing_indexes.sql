-- SimTradeData 缺失索引创建脚本
-- 文件：scripts/create_missing_indexes.sql
-- 用途：创建分析报告中发现缺失的数据库索引
-- 使用：sqlite3 data/simtradedata.db < scripts/create_missing_indexes.sql

-- ==========================================
-- valuations 表索引
-- ==========================================
-- 功能：优化估值数据查询性能
-- 预期收益：10-50倍性能提升

CREATE INDEX IF NOT EXISTS idx_valuations_symbol_date
ON valuations(symbol, date DESC);

CREATE INDEX IF NOT EXISTS idx_valuations_date
ON valuations(date DESC);

CREATE INDEX IF NOT EXISTS idx_valuations_created_at
ON valuations(created_at DESC);

-- ==========================================
-- data_source_quality 表索引
-- ==========================================
-- 功能：优化数据质量监控查询性能
-- 预期收益：5-20倍性能提升

CREATE INDEX IF NOT EXISTS idx_data_quality_source
ON data_source_quality(source_name, data_type, date DESC);

CREATE INDEX IF NOT EXISTS idx_data_quality_symbol
ON data_source_quality(symbol, source_name);

-- ==========================================
-- 验证索引创建结果
-- ==========================================

.headers on
.mode column

SELECT '-- valuations 表索引 --' as info;
SELECT name, tbl_name
FROM sqlite_master
WHERE type='index' AND name LIKE 'idx_valuation%'
ORDER BY name;

SELECT '' as info;
SELECT '-- data_source_quality 表索引 --' as info;
SELECT name, tbl_name
FROM sqlite_master
WHERE type='index' AND name LIKE 'idx_data%'
ORDER BY name;

SELECT '' as info;
SELECT '✅ 索引创建完成！' as info;