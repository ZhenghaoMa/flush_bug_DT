-- ============================================================
-- 测试 1: iceberg_flush(text) → bigint (异步 flush)
-- 核心场景: INSERT 后调用 flush，验证返回 job_id > 0
-- ============================================================
\echo '========== 测试 1: iceberg_flush (异步) =========='

-- 1a. 无数据的 flush —— 应返回 0 或 NULL
\echo '--- 1a. 空 delta flush (应返回 0 或 NULL) ---'
SELECT iceberg_flush('flush_t1') AS empty_flush_result;

-- 1b. 插入数据
\echo '--- 1b. 插入 3 行数据 ---'
INSERT INTO flush_t1 (id, amount, name) VALUES
    (101, 10.5, 'apple_test1'),
    (102, 20.5, 'banana_test1'),
    (103, 30.5, 'cherry_test1');

-- 1c. flush —— 应返回 job_id (>0)
\echo '--- 1c. flush 应返回 job_id (>0) ---'
SELECT iceberg_flush('flush_t1') AS job_id_round1;

-- 1d. 查看 flush 进度 —— job 应该成功完成
\echo '--- 1d. 查看 flush 进度 ---'
SELECT * FROM iceberg_flush_progress('flush_t1');

-- 1e. 验证数据可见 (10 + 3 = 13 行)
\echo '--- 1e. 验证 flush 后数据可见 (期望 13 行) ---'
SELECT count(*) AS total_rows FROM flush_t1;

-- 1f. 连续第二次 flush (无新数据) —— 应返回 0 或 NULL
\echo '--- 1f. 连续第二次空 flush (应返回 0 或 NULL) ---'
SELECT iceberg_flush('flush_t1') AS empty_flush_round2;
