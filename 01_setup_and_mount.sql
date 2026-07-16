-- ============================================================
-- Flush 功能测试套件
-- 测试所有 6 个 flush 相关函数
-- ============================================================

-- 环境准备：清理 + 重挂载
\echo '========== 1. 环境准备 =========='

-- 生成测试数据
\echo '生成测试数据 fixture...'

-- 卸载旧表 (忽略错误)
SELECT iceberg_unmount('flush_t1') AS unmount_t1;
SELECT iceberg_unmount('flush_t2') AS unmount_t2;
SELECT iceberg_unmount('flush_t3') AS unmount_t3;

-- 挂载测试表
\echo '挂载 flush_t1...'
SELECT iceberg_mount('public', 'flush_t1', '/tmp/test_iceberg/basic') AS mount_t1;

-- 查看初始状态
\echo '初始数据 (10行):'
SELECT id, amount, name FROM flush_t1 ORDER BY id;

-- 查看 flush 进度初始状态
\echo '初始 flush 进度:'
SELECT * FROM iceberg_flush_progress('flush_t1');
