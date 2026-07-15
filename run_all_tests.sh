#!/bin/bash
# flush_test/run_all_tests.sh — 完整 flush 功能测试套件
# 用法: bash flush_test/run_all_tests.sh

set -euo pipefail

WORKSPACE_DIR="/mnt/c/Lze"
TEST_DIR="$WORKSPACE_DIR/flush_test"
OUTPUT_DIR="$TEST_DIR/output"
mkdir -p "$OUTPUT_DIR"

PGPORT=5432
PGUSER=gv
PGDB=gv

echo "=== gsiceberg flush 功能测试套件 ==="
echo "时间: $(date)"
echo ""

# Check PG
if ! pg_isready -p $PGPORT > /dev/null 2>&1; then
    echo "ERROR: PostgreSQL is not running on port $PGPORT"
    echo "  Try: pg_ctl -D /home/gv/pgdata -l /home/gv/pgdata/logfile start"
    exit 1
fi
echo "PG OK: $(pg_isready -p $PGPORT)"

# Check extensions
echo ""
echo "=== 已安装扩展 ==="
psql -p $PGPORT -d $PGDB -c "SELECT extname, extversion FROM pg_extension ORDER BY extname;" 2>&1 | tee "$OUTPUT_DIR/00_extensions.txt"

# Generate fixtures
echo ""
echo "=== 生成测试数据 ==="
cd ~/workspace/gsiceberg
bash test/fixtures/gen_all.sh 2>&1 | tail -5
echo "Fixture OK"

# Prepare: recompile and install gsiceberg
echo ""
echo "=== 重新编译和安装 gsiceberg ==="
source scripts/setup-env.sh
make -j$(nproc) 2>&1 | tail -3
make install 2>&1 | tail -3
echo "Build OK"

# Run each test file
run_sql_file() {
    local sql_file="$1"
    local out_name="$2"
    local full_path="$TEST_DIR/$sql_file"
    echo ""
    echo ">>> 执行: $sql_file"
    psql -p $PGPORT -d $PGDB -f "$full_path" 2>&1 | tee "$OUTPUT_DIR/$out_name"
}

# 1. Setup
run_sql_file "01_setup_and_mount.sql" "01_setup.txt"

# 2. Async flush
run_sql_file "02_test_async_flush.sql" "02_async_flush.txt"

# 3. Sync flush
run_sql_file "03_test_sync_flush.sql" "03_sync_flush.txt"

# 4. Phase1 + Phase2
run_sql_file "04_test_phase12.sql" "04_phase12.txt"

# 5. Progress
run_sql_file "05_test_progress.sql" "05_progress.txt"

# 6. Worker
run_sql_file "06_test_worker.sql" "06_worker.txt"

# 7. Edge cases (run last, may error)
set +e
echo ""
echo ">>> 执行: 07_test_edge_cases.sql (允许错误)"
psql -p $PGPORT -d $PGDB -f "$TEST_DIR/07_test_edge_cases.sql" 2>&1 | tee "$OUTPUT_DIR/07_edge_cases.txt"
set -e

# Cleanup
echo ""
echo "=== 清理测试表 ==="
psql -p $PGPORT -d $PGDB -c "SELECT iceberg_unmount('flush_t1');" 2>&1
psql -p $PGPORT -d $PGDB -c "SELECT iceberg_unmount('flush_t2');" 2>&1
psql -p $PGPORT -d $PGDB -c "SELECT iceberg_unmount('flush_t3');" 2>&1

echo ""
echo "=== 测试完成 ==="
echo "输出文件: $OUTPUT_DIR/"
ls -la "$OUTPUT_DIR/"
