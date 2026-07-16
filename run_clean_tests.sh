#!/bin/bash
# 从干净状态逐个测试每个 flush 函数
export PATH=/home/gv/workspace/gsvector-deps/install/bin:$PATH
export LD_LIBRARY_PATH=/home/gv/workspace/gsvector-deps/install/lib:/home/gv/workspace/gsvector-deps/install/lib/postgresql:$LD_LIBRARY_PATH
OUT=./output
mkdir -p "$OUT"

echo "=== 清理 ==="
psql -p 5432 -d gv -c "SELECT iceberg_unmount('ft1');" 2>&1 | grep -v "does not exist" || true
psql -p 5432 -d gv -c "SELECT iceberg_unmount('ft2');" 2>&1 | grep -v "does not exist" || true
psql -p 5432 -d gv -c "SELECT iceberg_unmount('ft3');" 2>&1 | grep -v "does not exist" || true

echo ""
echo "============================================================"
echo "TEST A: iceberg_flush (异步) — 基础路径"
echo "============================================================"
psql -p 5432 -d gv <<'SQL' 2>&1 | tee $OUT/A_async.txt
SELECT iceberg_mount('public', 'ft1', '/tmp/test_iceberg/basic');
-- A1: 空表 flush
SELECT 'A1_empty_flush' AS test, iceberg_flush('ft1') AS result;
-- A2: INSERT + flush
INSERT INTO ft1 (id, amount, name) VALUES (1001, 1.5, 'A2');
SELECT 'A2_flush' AS test, iceberg_flush('ft1') AS result;
-- A3: 验证数据
SELECT 'A3_count' AS test, count(*) FROM ft1;
SQL

echo ""
echo "============================================================"
echo "TEST B: iceberg_flush_sync (同步)"
echo "============================================================"
# 重新挂载干净表
psql -p 5432 -d gv -c "SELECT iceberg_unmount('ft1');" 2>&1 | grep -v "does not exist" || true
psql -p 5432 -d gv <<'SQL' 2>&1 | tee $OUT/B_sync.txt
SELECT iceberg_mount('public', 'ft2', '/tmp/test_iceberg/basic');
-- B1: 空表 sync
SELECT 'B1_empty_sync' AS test, iceberg_flush_sync('ft2') AS result;
-- B2: INSERT + sync
INSERT INTO ft2 (id, amount, name) VALUES (2001, 2.5, 'B2');
SELECT 'B2_sync' AS test, iceberg_flush_sync('ft2') AS result;
-- B3: flush_sync 后 VIEW 是否存在
SELECT 'B3_view_exists' AS test, count(*) > 0 FROM pg_views WHERE viewname = 'ft2';
SQL

echo ""
echo "============================================================"
echo "TEST C: iceberg_flush_progress (进度查询)"
echo "============================================================"
psql -p 5432 -d gv <<'SQL' 2>&1 | tee $OUT/C_progress.txt
-- C1: 全量查询
SELECT 'C1_all_tables' AS test, count(*) > 0 FROM iceberg_flush_progress();
-- C2: 单表过滤
SELECT 'C2_filter' AS test, table_name, job_status, flush_status, snapshot_count
FROM iceberg_flush_progress('ft2');
-- C3: 不存在的表
SELECT 'C3_nonexist' AS test, count(*) = 0 FROM iceberg_flush_progress('nonexist_xyz');
SQL

echo ""
echo "============================================================"
echo "TEST D: iceberg_flush_phase1 + phase2 (手工分阶段)"
echo "============================================================"
psql -p 5432 -d gv <<'SQL' 2>&1 | tee $OUT/D_phase12.txt
SELECT iceberg_mount('public', 'ft3', '/tmp/test_iceberg/basic');
-- D1: INSERT
INSERT INTO ft3 (id, amount, name) VALUES (3001, 3.5, 'D1');
-- D2: phase1
SELECT 'D2_phase1' AS test, iceberg_flush_phase1('ft3') AS job_id;
-- D3: 查看队列
SELECT 'D3_queue' AS test, job_id, status FROM _gsiceberg.flush_jobs
WHERE table_name = 'ft3' ORDER BY job_id DESC LIMIT 1;
-- D4: phase2
SELECT 'D4_phase2' AS test, iceberg_flush_phase2('ft3',
    (SELECT max(job_id) FROM _gsiceberg.flush_jobs WHERE table_name = 'ft3')
) AS result;
SQL

echo ""
echo "============================================================"
echo "TEST E: iceberg_flush_worker (后台消费)"
echo "============================================================"
# 重新挂载干净表
psql -p 5432 -d gv -c "SELECT iceberg_unmount('ft3');" 2>&1 | grep -v "does not exist" || true
psql -p 5432 -d gv <<'SQL' 2>&1 | tee $OUT/E_worker.txt
SELECT iceberg_mount('public', 'ft3', '/tmp/test_iceberg/basic');
-- E1: INSERT + phase1 only (创建 job 但不执行)
INSERT INTO ft3 (id, amount, name) VALUES (4001, 4.5, 'E1');
SELECT 'E1_phase1' AS test, iceberg_flush_phase1('ft3') AS job_id;
-- E2: 队列中有 job
SELECT 'E2_pending' AS test, count(*) AS pending_count
FROM _gsiceberg.flush_jobs WHERE table_name = 'ft3' AND status = 'pending';
-- E3: worker 消费
SELECT 'E3_worker' AS test, iceberg_flush_worker('ft3') AS processed;
-- E4: 队列为空
SELECT 'E4_no_more' AS test, iceberg_flush_worker('ft3') AS processed;
SQL

echo ""
echo "============================================================"
echo "TEST F: 边界场景"
echo "============================================================"
psql -p 5432 -d gv <<'SQL' 2>&1 | tee $OUT/F_edge.txt
-- F1: NULL 参数 (STRICT 函数应拒绝)
SELECT 'F1_null' AS test, iceberg_flush(NULL::text);
SQL

echo ""
echo "=== ALL DONE ==="
