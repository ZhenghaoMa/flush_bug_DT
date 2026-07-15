-- ============================================================
-- 测试 6: 边界场景和异常情况
-- ============================================================
\echo '========== 测试 6: 边界场景和异常 =========='

-- 6a. 非 owner 调用 flush_sync (需要另一个用户，在 gv 用户下应该通过)
\echo '--- 6a. flush_sync 权限检查 (当前用户应有权限) ---'
SELECT iceberg_flush_sync('flush_t1') AS sync_auth_test;

-- 6b. NULL 参数测试 (STRICT 函数应拒绝)
\echo '--- 6b. flush(NULL) 应报错 (STRICT 函数) ---'
SELECT iceberg_flush(NULL::text);

-- 6c. 不存在的表名 flush
\echo '--- 6c. flush 不存在的表 (应报错) ---'
SELECT iceberg_flush('table_does_not_exist_xyz');

-- 6d. phase2 使用不存在的 job_id
\echo '--- 6d. phase2 使用不存在的 job_id (应报错或返回 false) ---'
SELECT iceberg_flush_phase2('flush_t1', 999999) AS phase2_bad_job;

-- 6e. 验证 flush_state 状态一致性
\echo '--- 6e. flush_state 状态 ---'
SELECT table_name, flush_status, started_at, delta_rows, snapshot_id
FROM _gsiceberg.flush_state
WHERE table_name LIKE 'flush_t%';
