-- ============================================================
-- 测试 2: iceberg_flush_sync(text) → boolean (同步 flush)
-- 核心场景: INSERT 后同步 flush，验证立即返回成功
-- ============================================================
\echo '========== 测试 2: iceberg_flush_sync (同步) =========='

-- 挂载第二张表用于测试
SELECT iceberg_mount('public', 'flush_t2', '/tmp/test_iceberg/basic');

-- 2a. 空表同步 flush —— 应返回 false
\echo '--- 2a. 空 delta 同步 flush (应返回 false) ---'
SELECT iceberg_flush_sync('flush_t2') AS sync_empty_result;

-- 2b. 插入数据后同步 flush
\echo '--- 2b. 插入数据 + 同步 flush ---'
INSERT INTO flush_t2 (id, amount, name) VALUES
    (201, 11.5, 'sync_apple'),
    (202, 22.5, 'sync_banana'),
    (203, 33.5, 'sync_cherry');

-- 2c. 同步 flush —— 应返回 true
\echo '--- 2c. 同步 flush 应返回 true ---'
SELECT iceberg_flush_sync('flush_t2') AS sync_flush_result;

-- 2d. 验证数据 (10 + 3 = 13)
\echo '--- 2d. 验证数据可见 (期望 13 行) ---'
SELECT count(*) AS total_rows FROM flush_t2;

-- 2e. 查看进度
\echo '--- 2e. 查看 flush 进度 ---'
SELECT * FROM iceberg_flush_progress('flush_t2');

-- 2f. 连续第二次同步 flush —— 应返回 false
\echo '--- 2f. 连续第二次空 flush (应返回 false) ---'
SELECT iceberg_flush_sync('flush_t2') AS sync_empty_round2;
