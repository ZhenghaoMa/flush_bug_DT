-- ============================================================
-- 测试 3: iceberg_flush_phase1 + iceberg_flush_phase2 (分阶段 flush)
-- 验证内部阶段函数可按步骤执行
-- ============================================================
\echo '========== 测试 3: Phase1 + Phase2 分阶段 =========='

-- 3a. 插入数据
\echo '--- 3a. 插入 3 行新数据到 flush_t1 ---'
INSERT INTO flush_t1 (id, amount, name) VALUES
    (301, 41.5, 'phase_apple'),
    (302, 42.5, 'phase_banana'),
    (303, 43.5, 'phase_cherry');

-- 3b. 仅执行 Phase 1 —— 应返回 job_id
\echo '--- 3b. iceberg_flush_phase1 应返回 job_id (>0) ---'
SELECT iceberg_flush_phase1('flush_t1') AS phase1_job_id;

-- 3c. 查看 flush_state 和 flush_jobs 状态
\echo '--- 3c. 查看 flush_jobs 队列（应有 pending 或 in_progress） ---'
SELECT job_id, table_name, status, retry_count, error_msg
FROM _gsiceberg.flush_jobs
WHERE table_name = 'flush_t1'
ORDER BY job_id DESC
LIMIT 5;

-- 3d. 手动执行 Phase 2 (用上面返回的 job_id)
\echo '--- 3d. iceberg_flush_phase2 (用最新 job_id) ---'
SELECT iceberg_flush_phase2('flush_t1',
    (SELECT max(job_id) FROM _gsiceberg.flush_jobs WHERE table_name = 'flush_t1')
) AS phase2_result;

-- 3e. 验证数据
\echo '--- 3e. 验证数据可见 (期望 16 行) ---'
SELECT count(*) AS total_rows FROM flush_t1;

-- 3f. 查看最终 job 状态
\echo '--- 3f. 查看最终 job 状态 ---'
SELECT job_id, table_name, status, started_at, finished_at
FROM _gsiceberg.flush_jobs
WHERE table_name = 'flush_t1'
ORDER BY job_id DESC
LIMIT 5;
