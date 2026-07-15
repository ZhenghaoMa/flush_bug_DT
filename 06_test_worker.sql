-- ============================================================
-- 测试 5: iceberg_flush_worker (后台消费)
-- 验证 worker 能正确消费队列中的 job
-- ============================================================
\echo '========== 测试 5: iceberg_flush_worker (后台消费) =========='

-- 5a. 挂载第三张表
SELECT iceberg_mount('flush_t3', '/tmp/test_iceberg/basic');

-- 5b. 插入数据 + 仅做 Phase 1 (创建 job 但不执行 Phase 2)
\echo '--- 5b. 插入数据 + Phase 1 ---'
INSERT INTO flush_t3 (id, amount, name) VALUES
    (501, 51.5, 'worker_apple'),
    (502, 52.5, 'worker_banana');

SELECT iceberg_flush_phase1('flush_t3') AS worker_phase1_job;

-- 5c. 查看待处理 job
\echo '--- 5c. 待处理 job ---'
SELECT job_id, table_name, status FROM _gsiceberg.flush_jobs
WHERE table_name = 'flush_t3' AND status = 'pending'
ORDER BY job_id DESC;

-- 5d. 调用 worker —— 应返回 1 (处理了 1 个 job)
\echo '--- 5d. iceberg_flush_worker (应返回 1) ---'
SELECT iceberg_flush_worker('flush_t3') AS worker_result;

-- 5e. 验证数据 (10 + 2 = 12)
\echo '--- 5e. 验证数据 (期望 12 行) ---'
SELECT count(*) AS total_rows FROM flush_t3;

-- 5f. 查看 job 完成状态
\echo '--- 5f. job 完成状态 ---'
SELECT job_id, table_name, status
FROM _gsiceberg.flush_jobs
WHERE table_name = 'flush_t3'
ORDER BY job_id DESC LIMIT 3;

-- 5g. 无任务时调用 worker —— 应返回 0
\echo '--- 5g. 无任务时 worker (应返回 0) ---'
SELECT iceberg_flush_worker('flush_t3') AS worker_idle_result;
