-- ============================================================
-- Flush 功能完整测试 — 使用 V3 fixture
-- 每个表独立 fixture 副本, 避免 flush 后互相干扰
-- ============================================================

-- 准备独立 fixture 副本
\echo '准备 fixture 副本...'
\! rm -rf /tmp/test_iceberg/f1 /tmp/test_iceberg/f2 /tmp/test_iceberg/f3
\! cp -r /tmp/test_iceberg/v3_min /tmp/test_iceberg/f1
\! cp -r /tmp/test_iceberg/v3_min /tmp/test_iceberg/f2
\! cp -r /tmp/test_iceberg/v3_min /tmp/test_iceberg/f3
\echo 'fixture 副本就绪'

-- 清理
SELECT iceberg_unmount('f1'); SELECT iceberg_unmount('f2'); SELECT iceberg_unmount('f3');

\echo '========================================'
\echo 'TEST 1: iceberg_flush (异步) + worker 消费'
\echo '========================================'

SELECT iceberg_mount('public', 'f1', '/tmp/test_iceberg/f1');

-- 1a. 空 delta → 返回 NULL
\echo '--- 1a: 空 delta flush (期望 NULL) ---'
SELECT iceberg_flush('f1') AS empty_result;

-- 1b. INSERT 后 flush → 返回 job_id > 0
\echo '--- 1b: INSERT + flush (期望 job_id > 0) ---'
INSERT INTO f1 (id, amount, name) VALUES (101, 10.5, 'Apple'), (102, 20.5, 'Banana');
SELECT iceberg_flush('f1') AS job_id;

-- 1c. 验证数据 (10 + 2 = 12)
\echo '--- 1c: 验证数据 (期望 12) ---'
SELECT count(*) AS rows_after_flush FROM f1;

-- 1d. flush_jobs — async, job 在 pending 等待 worker
\echo '--- 1d: flush_jobs (期望 pending) ---'
SELECT job_id, status FROM _gsiceberg.flush_jobs WHERE table_name = 'f1' ORDER BY job_id DESC LIMIT 1;

-- 1e. worker 消费 pending job → completed
\echo '--- 1e: worker 消费 (期望 1) ---'
SELECT iceberg_flush_worker('f1') AS worker_result;

-- 1f. 验证 job 完成
\echo '--- 1f: job 状态 (期望 completed) ---'
SELECT job_id, status FROM _gsiceberg.flush_jobs WHERE table_name = 'f1' ORDER BY job_id DESC LIMIT 1;

\echo '========================================'
\echo 'TEST 2: iceberg_flush_sync (同步)'
\echo '========================================'

SELECT iceberg_mount('public', 'f2', '/tmp/test_iceberg/f2');

-- 2a. 空 delta → 返回 false
\echo '--- 2a: 空 delta sync (期望 false) ---'
SELECT iceberg_flush_sync('f2') AS empty_sync;

-- 2b. INSERT + sync → 返回 true
\echo '--- 2b: INSERT + sync (期望 true) ---'
INSERT INTO f2 (id, amount, name) VALUES (201, 11.5, 'SyncApple');
SELECT iceberg_flush_sync('f2') AS sync_result;

-- 2c. 验证 VIEW 是否还在
\echo '--- 2c: VIEW 是否存在 ---'
SELECT count(*) > 0 AS view_exists FROM pg_views WHERE viewname = 'f2';

-- 2d. 验证数据
\echo '--- 2d: 数据验证 ---'
SELECT count(*) AS rows_after_sync FROM f2;

\echo '========================================'
\echo 'TEST 3: iceberg_flush_phase1 + phase2 (分阶段)'
\echo '========================================'

-- 3a. INSERT
\echo '--- 3a: INSERT ---'
INSERT INTO f1 (id, amount, name) VALUES (301, 31.5, 'PhaseApple');

-- 3b. 仅 Phase 1
\echo '--- 3b: phase1 ---'
SELECT iceberg_flush_phase1('f1') AS phase1_result;

-- 3c. 查看队列
\echo '--- 3c: 队列 ---'
SELECT job_id, status FROM _gsiceberg.flush_jobs
WHERE table_name = 'f1' ORDER BY job_id DESC LIMIT 1;

-- 3d. 手动 Phase 2
\echo '--- 3d: phase2 ---'
SELECT iceberg_flush_phase2('f1',
    (SELECT max(job_id) FROM _gsiceberg.flush_jobs WHERE table_name = 'f1')
) AS phase2_result;

\echo '========================================'
\echo 'TEST 4: iceberg_flush_progress (进度)'
\echo '========================================'

-- 4a. 全量
\echo '--- 4a: 全量查询 ---'
SELECT table_name, job_status, flush_status, snapshot_count
FROM iceberg_flush_progress()
WHERE table_name IN ('f1', 'f2');

-- 4b. 单表过滤
\echo '--- 4b: 单表过滤 (f2) ---'
SELECT table_name, job_status, flush_status FROM iceberg_flush_progress('f2');

-- 4c. 不存在的表
\echo '--- 4c: 不存在的表 (0 rows) ---'
SELECT count(*) = 0 AS no_results FROM iceberg_flush_progress('no_such_table');

\echo '========================================'
\echo 'TEST 5: iceberg_flush_worker (后台消费)'
\echo '========================================'

-- 新挂载 f3
SELECT iceberg_mount('public', 'f3', '/tmp/test_iceberg/f3');

-- 5a. INSERT + 仅 phase1 (创建 pending job)
\echo '--- 5a: INSERT + phase1 ---'
INSERT INTO f3 (id, amount, name) VALUES (401, 41.5, 'WorkerApple');
SELECT iceberg_flush_phase1('f3') AS phase1_job;

-- Clean stale jobs so worker only processes f3
DELETE FROM _gsiceberg.flush_jobs WHERE table_name != 'f3';

-- 5b. 调用 worker 消费 (需 admin 权限)
\echo '--- 5b: worker 消费 (需 admin, 期望 1) ---'
SELECT iceberg_flush_worker('f3') AS worker_result;

-- 5c. 验证数据 (10 + 1 = 11)
\echo '--- 5c: 数据验证 (期望 11) ---'
SELECT count(*) AS rows_after_worker FROM f3;

-- 5d. 无任务时 worker (期望 1, 队列已空排空成功)
\echo '--- 5d: 空闲 worker (期望 1) ---'
SELECT iceberg_flush_worker('f3') AS worker_idle;

\echo '========================================'
\echo 'TEST 6: 边界场景'
\echo '========================================'

-- 6a. NULL 参数 (STRICT 函数应返回 NULL)
\echo '--- 6a: NULL flush (期望 NULL) ---'
SELECT iceberg_flush(NULL::text) AS null_result;

-- 6b. 不存在表名的 sync
\echo '--- 6b: 不存在表 sync (期望报错) ---'
SELECT iceberg_flush_sync('ghost_table_xyz');

\echo '========================================'
\echo '=== 测试完成 ==='
\echo '========================================'
