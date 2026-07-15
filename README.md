# gsiceberg Flush 功能测试报告

> 测试日期: 2026-07-15
> 测试版本: gsiceberg v0.1.0
> 测试环境: PostgreSQL 16.14 (源码编译), Ubuntu 22.04 WSL2

---

## 一、Flush 函数总览

gsiceberg v0.1.0 共提供 6 个 flush 相关函数，全部定义在 `sql/02a-lifecycle.sql`：

| # | 函数名 | 签名 | 层级 | 用途 |
|---|--------|------|------|------|
| 1 | `iceberg_flush` | `(text) → bigint` | C | 异步 flush，Phase 1 同步执行后返回 job_id |
| 2 | `iceberg_flush_sync` | `(text) → boolean` | PL/pgSQL | 同步 flush，Phase 1+2 串行，等 Parquet 写完 |
| 3 | `iceberg_flush_phase1` | `(text) → bigint` | C wrapper | 仅冻结 delta: RENAME + 创建新 delta + 入队 |
| 4 | `iceberg_flush_phase2` | `(text, bigint) → boolean` | C wrapper | 仅写 Parquet + 更新 catalog + 清理 |
| 5 | `iceberg_flush_progress` | `([text]) → TABLE(...)` | SQL | 查询每张表的 flush 进度 |
| 6 | `iceberg_flush_worker` | `(text) → int` | PL/pgSQL | 后台消费 `flush_jobs` 队列中的任务 |

### Flush 两阶段架构

```
INSERT/DELETE/UPDATE → _delta 表 (PG 堆表)

Phase 1 (iceberg_flush, 同步):
  advisory lock → RENAME _delta → _delta_flushing
  → CREATE 新的空 _delta → INSERT flush_jobs → 返回 job_id

Phase 2 (worker 或 sync 调用的 phase2):
  读 _delta_flushing → I/D 配对抵消 → 写 Parquet (Arrow C++)
  → 更新 snapshots/data_files → 生成 manifest
  → 训练索引 → DROP _delta_flushing → UPDATE flush_jobs
```

---

## 二、测试用例说明

| 文件 | 被测函数 | 测试场景 |
|------|----------|----------|
| `01_setup_and_mount.sql` | `iceberg_mount`, `iceberg_flush_progress` | 挂载表 + 初始状态检查 |
| `02_test_async_flush.sql` | `iceberg_flush` | 空 delta → INSERT → flush 返回 job_id → 数据验证 → 二次空 flush |
| `03_test_sync_flush.sql` | `iceberg_flush_sync` | 空 delta → INSERT → flush 返回 true → VIEW 存在性 → 数据验证 |
| `04_test_phase12.sql` | `iceberg_flush_phase1`, `iceberg_flush_phase2` | 分步执行 → 队列状态 → phase2 手动执行 → job 状态 |
| `05_test_progress.sql` | `iceberg_flush_progress` | 全表查询 → 单表过滤 → 不存在表 → 字段完整性 |
| `06_test_worker.sql` | `iceberg_flush_worker` | phase1 入队 → worker 消费 → 队列空 → 空闲状态 |
| `07_test_edge_cases.sql` | 所有函数 | NULL 参数 → 不存在表名 → 坏 job_id → flush_state 一致性 |
| `final_test.sql` | 全部 6 个函数 | **最终完整测试，结果汇总在此** |
| `worker_test.sql` | `iceberg_flush_worker` | worker 单独深入测试 |

### 运行方式

```bash
# 1. 启动 PG
pg_ctl -D /home/gv/pgdata -l /home/gv/pgdata/logfile start

# 2. 生成测试数据
cd ~/workspace/gsiceberg && bash test/fixtures/gen_all.sh

# 3. 执行测试
export PATH=/home/gv/workspace/gsvector-deps/install/bin:$PATH
export LD_LIBRARY_PATH=/home/gv/workspace/gsvector-deps/install/lib:/home/gv/workspace/gsvector-deps/install/lib/postgresql:$LD_LIBRARY_PATH
psql -p 5432 -d gv -f /path/to/flush_bug_DT/final_test.sql
```

---

## 三、测试结果总览

| # | 被测函数 | 基础功能 | 严重缺陷 | 评价 |
|---|----------|----------|----------|------|
| 1 | `iceberg_flush` | ✅ | ⚠️ BUG-2, BUG-3 | 单次可用，连续调用崩溃 |
| 2 | `iceberg_flush_sync` | ✅ | ❌ BUG-1 | flush 后 VIEW 被删除，表不可用 |
| 3 | `iceberg_flush_phase1` | ⚠️ | ❌ BUG-3 | 崩溃恢复路径 DROP 失败 |
| 4 | `iceberg_flush_phase2` | ✅ | ❌ BUG-1 | 执行后 VIEW 被 CASCADE 删除 |
| 5 | `iceberg_flush_progress` | ✅ | 无 | **唯一完全正常的函数** |
| 6 | `iceberg_flush_worker` | ✅ | ❌ BUG-1 | 队列消费正常，但触发 VIEW 删除 |

---

## 四、缺陷详情

### ❌ BUG-1: Phase 2 DROP _delta_flushing CASCADE 删除公共 VIEW（严重）

**现象**：
```
NOTICE:  drop cascades to view public.f2
ERROR:  relation "f2" does not exist
```

**根因**: Phase 2 结尾执行 `DROP TABLE _delta_flushing CASCADE`。公共 VIEW 引用了 `_delta_flushing`，CASCADE 导致 VIEW 被级联删除。删除后 VIEW 没有重建。

**复现步骤**:
```sql
SELECT iceberg_mount('t1', '/tmp/test_iceberg/v3_min');
INSERT INTO t1 (id, amount, name) VALUES (1, 10.5, 'test');
SELECT iceberg_flush_sync('t1');  -- 成功返回 true
SELECT count(*) FROM t1;           -- ERROR: relation "t1" does not exist
```

**影响范围**: `flush_sync`、`phase2`、`worker` 全部受影响。执行一次 flush 后表完全不可用，必须 unmount + remount。

**建议修复**: Phase 2 结束时重建 VIEW（调用 `iceberg_refresh_views` 或等效逻辑），或在 DROP 前解除 VIEW 依赖。

---

### ❌ BUG-2: 第二次空 flush 导致 PG 进程崩溃（严重）

**现象**:
```
server closed the connection unexpectedly
This probably means the server terminated abnormally
```

**复现步骤**:
```sql
SELECT iceberg_mount('t1', '/tmp/test_iceberg/v3_min');
INSERT INTO t1 (id, amount, name) VALUES (1, 10.5, 'test');
SELECT iceberg_flush('t1');       -- 第一次：正常返回 job_id
SELECT iceberg_flush('t1');       -- 第二次：PG 崩溃
```

**根因**: 与 BUG-1 关联——第一次 flush 后 VIEW 状态不一致，第二次访问到损坏的内部对象导致 crash。已知问题记录在 `gsiceberg_SETUP_SUMMARY.md`。

**建议修复**: 修复 BUG-1 后此问题应自动解决。同时增加 flush 入口的对象存在性检查。

---

### ❌ BUG-3: flush_phase1 崩溃恢复路径 DROP 失败（中等）

**现象**:
```
ERROR: cannot drop table _gsiceberg._t1_delta_flushing because other objects depend on it
DETAIL: view t1 depends on table _gsiceberg._t1_delta_flushing
HINT: Use DROP ... CASCADE to drop the dependent objects too.
```

**根因**: Phase 1 的 `iceberg_flush_phase1()` 中，崩溃恢复检测到 `_delta_flushing` 存在后尝试 `DROP TABLE IF EXISTS`，但 VIEW 引用了该表，导致 DROP 被 PG 拒绝。

**复现步骤**: 在 Phase 2 未完成时 crash → 重启 PG → 再次调用 `iceberg_flush_phase1()`。

**影响范围**: 崩溃恢复不可用，遗留的 `_delta_flushing` 无法自动清理。

**建议修复**: 恢复路径使用 `DROP TABLE ... CASCADE` 并在清理后重建 VIEW。

---

### ⚠️ BUG-4: flush_state 未正确记录状态（中等）

**现象**: flush 执行后 `_gsiceberg.flush_state` 表为空。
```sql
SELECT * FROM _gsiceberg.flush_state WHERE table_name = 't1';
-- (0 rows)
```

**根因**: `flush_state_done()` 执行 DELETE 删除行。但 `iceberg_flush()` C 函数内部未调用 `flush_state_begin()` 来插入初始行。只有某些路径（如 Phase 2 内部的 `flush_write_parquet`）调用了 `flush_state_set_file`。

**建议修复**: 统一 flush_state 生命周期：Phase 1 调用 `flush_state_begin`，Phase 2 调用 `flush_state_set_file`/`flush_state_set_snapshot`，完成后更新状态为 `completed` 而非 DELETE。

---

### ⚠️ BUG-5: iceberg_flush 异步语义不完整（低）

**现象**: `iceberg_flush()` 只执行 Phase 1 返回 job_id，Phase 2 标记为异步。但 flush 后数据立即可见。

**预期行为**: 
- 异步模式下，Phase 2 未完成时新数据应仅存在于 `_delta_flushing`
- VIEW 应看不到 `_delta_flushing` 中的数据
- 数据仅在 Phase 2 完成后（Parquet 写入 + 目录更新）可见

**实际行为**: 
```sql
SELECT iceberg_flush('t1');  -- job 状态: pending
SELECT count(*) FROM t1;     -- 12 rows（包含了刚 INSERT 的 2 行）
```

**建议修复**: 确认 VIEW 定义是否正确排除了 `_delta_flushing`，或确认 `iceberg_flush()` 是否意外执行了 Phase 2。

---

### ⚠️ BUG-6: flush 后 metadata 损坏，无法重新挂载（中等）

**现象**:
```
ERROR: gsiceberg: cannot read metadata (non-V3 format or corrupt): /tmp/test_iceberg/v3_min/metadata/v1.metadata.json
```

**根因**: flush 写入的 manifest 或 Parquet 文件与 V3 规范不完全兼容。可能在 snapshot、manifest 或 data_files 记录中存在格式偏差。

**复现步骤**:
```sql
SELECT iceberg_mount('t1', '/tmp/test_iceberg/v3_min');
INSERT INTO t1 (id, amount, name) VALUES (1, 10.5, 'test');
SELECT iceberg_flush_sync('t1');
SELECT iceberg_unmount('t1');
SELECT iceberg_mount('t1', '/tmp/test_iceberg/v3_min');  -- ERROR
```

**建议修复**: 对比 flush 生成的 manifest 与 pyiceberg 标准 V3 格式，修正差异。

---

## 五、功能完备度评估

```
iceberg_flush          ████████░░  80%  单次可用，连续调用崩溃
iceberg_flush_sync     ██████░░░░  60%  flush 成功但 VIEW 丢失
iceberg_flush_phase1   ████████░░  80%  核心逻辑正确，恢复路径有 bug
iceberg_flush_phase2   ██████░░░░  60%  Parquet 写入正确，VIEW 被删除
iceberg_flush_progress ██████████ 100%  完全正常
iceberg_flush_worker   ██████░░░░  60%  队列消费正确，VIEW 被删除
```

## 六、测试执行日志（final_test.sql 完整输出）

```
TEST 1: iceberg_flush (异步)
  1a: 空 delta flush → NULL ✅
  1b: INSERT + flush → job_id=106 ✅
  1c: 数据验证 → 12 rows ✅
  1d: flush_state → (0 rows) ❌ BUG-4
  1e: flush_jobs → status=pending ⚠️ BUG-5

TEST 2: iceberg_flush_sync (同步)
  2a: 空 delta sync → false ✅
  2b: INSERT + sync → true (NOTICE: drop cascades to view public.f2) ✅
  2c: VIEW 是否存在 → false ❌ BUG-1
  2d: 数据验证 → ERROR: relation "f2" does not exist ❌ BUG-1

TEST 3: iceberg_flush_phase1 + phase2
  3b: phase1 → ERROR: cannot drop _delta_flushing ❌ BUG-3
  3d: phase2 → true (NOTICE: drop cascades to view f1) ❌ BUG-1

TEST 4: iceberg_flush_progress
  4a: 全量查询 → 2 rows ✅
  4b: 单表过滤 → 1 row ✅
  4c: 不存在的表 → 0 rows ✅

TEST 5: iceberg_flush_worker
  mount: ERROR: cannot read metadata ❌ BUG-6
  worker: 0 (无可用表)

TEST 6: 边界场景
  6a: NULL flush → NULL ✅
  6b: 不存在表 → false (NOTICE) ⚠️ 应抛 ERROR
```

## 七、修复优先级建议

| 优先级 | Bug | 理由 |
|--------|-----|------|
| **P0** | BUG-1: DROP CASCADE 删除 VIEW | 所有 flush 路径都受影响，表不可用 |
| **P0** | BUG-2: 二次 flush 崩溃 | 数据丢失风险，PG 进程崩溃 |
| **P1** | BUG-3: 崩溃恢复 DROP 失败 | 崩溃后无法自动恢复 |
| **P1** | BUG-6: metadata 损坏 | 无法重新挂载已 flush 的表 |
| **P2** | BUG-4: flush_state 未更新 | 可观测性缺失 |
| **P2** | BUG-5: 异步语义不完整 | 不影响功能但误导用户 |
