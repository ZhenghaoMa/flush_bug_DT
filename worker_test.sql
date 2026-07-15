-- 单独测试 worker
SELECT iceberg_unmount('fw');

-- 重新生成 V3 fixture 需要新目录，用 basic 测试
SELECT iceberg_mount('fw', '/tmp/test_iceberg/basic');

-- INSERT
INSERT INTO fw (id, amount, name) VALUES (501, 51.5, 'Worker1');

-- 仅 phase1，让 job 进入 pending 状态
SELECT 'phase1' AS step, iceberg_flush_phase1('fw') AS result;

-- 查看 pending job
SELECT 'pending_job' AS step, job_id, table_name, status
FROM _gsiceberg.flush_jobs WHERE table_name = 'fw' AND status = 'pending'
ORDER BY job_id DESC LIMIT 3;

-- worker 消费 (需要 admin 权限)
SELECT 'worker' AS step, iceberg_flush_worker('fw') AS result;

-- 查看 job 状态
SELECT 'job_status' AS step, job_id, status
FROM _gsiceberg.flush_jobs WHERE table_name = 'fw'
ORDER BY job_id DESC LIMIT 3;

-- 空闲 worker
SELECT 'worker_idle' AS step, iceberg_flush_worker('fw') AS result;
