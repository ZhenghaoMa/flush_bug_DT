-- ============================================================
-- 测试 4: iceberg_flush_progress (进度查询)
-- 验证多表查询、过滤、字段完整性
-- ============================================================
\echo '========== 测试 4: iceberg_flush_progress (进度查询) =========='

-- 4a. 查询所有表
\echo '--- 4a. 所有表的 flush 进度 ---'
SELECT * FROM iceberg_flush_progress();

-- 4b. 查询指定表
\echo '--- 4b. 指定表 flush_t1 的进度 ---'
SELECT * FROM iceberg_flush_progress('flush_t1');

-- 4c. 查询不存在的表 —— 应返回 0 行
\echo '--- 4c. 不存在的表 (应返回 0 行) ---'
SELECT * FROM iceberg_flush_progress('nonexistent_table');

-- 4d. 验证返回字段完整性
\echo '--- 4d. 字段存在性检查 ---'
SELECT
    table_name IS NOT NULL AS has_table_name,
    job_id IS NULL OR job_id > 0 AS job_id_valid,
    job_status IS NOT NULL AS has_job_status,
    flush_status IS NOT NULL AS has_flush_status,
    snapshot_count IS NOT NULL AS has_snapshot_count
FROM iceberg_flush_progress('flush_t1');
