# FLUSH 功能点（AR）分析报告

> **生成日期:** 2026-07-15
> **数据源:** 源码 `/home/gv/openGauss_lake2/gsiceberg/fdw/flush/` + 设计文档 `docs/release/v0.1.0/`
> **方法论:** 源码优先，设计文档辅助；三路对抗性审查修正
> **审查状态:** ✅ 已通过对抗性审查（AR定义、代码度量、完备度评估各一路）

---

## 前置说明：三条执行路径

当前 FLUSH 存在三条可调用的执行路径，各 AR 在不同路径中的归属不同。

### 路径 1：`iceberg_flush()` — 异步入口

```
iceberg_flush(table_name)              -- C函数 (flush_phase2.c:162-207)
  └─ iceberg_flush_phase1(table_name)  -- Phase 1: 冻结 delta + 注册 job
  └─ RETURN job_id                     -- 直接返回，不执行 Phase 2
```

纯异步：只冻结 delta、注册 job，返回 job_id。后续由 `iceberg_flush_worker()` 处理 Phase 2。

### 路径 2：`iceberg_flush_sync()` — 同步入口

```
iceberg_flush_sync(table_name)                  -- PL/pgSQL (02a-lifecycle.sql:273-308)
  └─ iceberg_flush_phase1(table_name)           -- Phase 1
  └─ iceberg_flush_stage_a2(table_name, job_id) -- Stage A2: foreign delta 提交
  └─ iceberg_flush_stage_a1(table_name, job_id) -- Stage A1: delta 数据提交 + CoW
  └─ iceberg_flush_stage_c(table_name, job_id)  -- Stage C: 索引训练 (loop)
  └─ iceberg_flush_stage_d(table_name, job_id)  -- Stage D: 清理
```

执行顺序：**Phase 1 → A2 → A1 → C(loop) → D**。四个 Stage 各有独立的 SPI 事务。

### 路径 3：`iceberg_flush_worker()` — 后台结算入口

```
iceberg_flush_worker(table_name)                     -- PL/pgSQL (02a-lifecycle.sql:154-253)
  └─ 超时重置 + FOR UPDATE SKIP LOCKED 领取 job
  └─ iceberg_flush_stage_a2(table_val, job_id_val)   -- Stage A2
  └─ iceberg_flush_stage_a1(table_val, job_id_val)   -- Stage A1
  └─ iceberg_flush_stage_c(table_val, job_id_val)    -- Stage C (loop)
  └─ iceberg_flush_stage_d(table_val, job_id_val)    -- Stage D
```

执行顺序与 `iceberg_flush_sync` 相同：**A2 → A1 → C(loop) → D**，但每 Stage 有 `BEGIN...EXCEPTION` 错误隔离，失败时标记 job 为 failed。

### 路径 4（legacy，不再被 worker/sync 调用）：`iceberg_flush_phase2()`

```
iceberg_flush_phase2(table_name, job_id)  -- C函数 (flush_phase2.c:18-161)
  └─ flush_delta_read()                   -- 旧版: 单 SPI 事务内串行执行
  └─ flush_resolve_pairs()
  └─ flush_write_parquet()
  └─ flush_commit_catalog()
  └─ flush_foreign_delta()
  └─ flush_train_indexes()
  └─ cleanup (goto label)
```

此函数仍被编译和导出（`PG_FUNCTION_INFO_V1`），但不再被新 worker 或 `iceberg_flush_sync` 调用。外部脚本若直接调用 `iceberg_flush_phase2(table, job_id)` 仍会执行此旧路径。

---

## 一、各 AR 释义

### AR1 — 后台定时拉起 FLUSH

**一句话:** 定时触发 FLUSH 的外部调度入口，当前不在内核实现。

**负责:**
- 调度周期配置（依赖外部调度器：pg_cron / crontab / K8s CronJob）
- 周期性调用 `iceberg_flush_worker()` 驱动任务执行
- 当前源码中**无内置定时器、无 bgworker launcher、无 daemon 模式**——设计文档 `06-flush.md:126` 明确声明「需要外部调度器」

**源码依据:** Worker 函数位于 `sql/02a-lifecycle.sql:154-253`（PL/pgSQL），所有 `.c` 文件中无调度相关逻辑。

---

### AR2 — 后台结算 FLUSH

**一句话:** Worker 从 `flush_jobs` 队列领取 job，按 **Phase 1 → A2 → A1 → C(loop) → D** 顺序执行完整 FLUSH 流水线。

**负责:**
- **路由 1 — `iceberg_flush()` (async):** C 函数入口，只执行 Phase 1 冻结 + 注册 job，返回 job_id。Worker 后续异步处理。源码：[flush_phase2.c:162-207]。
- **路由 2 — `iceberg_flush_worker()`:** PL/pgSQL 函数，`FOR UPDATE SKIP LOCKED` 并发领取 job → 超时重置（>5 分钟 in_progress → pending）→ 重试上限（`gsiceberg.flush_retry_count` GUC，默认 3）→ 调用各 Stage C 函数。源码：`02a-lifecycle.sql:154-253`。
- **路由 3 — `iceberg_flush_sync()`:** PL/pgSQL 函数，Phase 1 + 4 Stage 同步串行执行。源码：`02a-lifecycle.sql:273-308`。

**当前缺失:** worker 并发数控制、partition prepare→commit 两阶段编排、执行时间上限。

---

### AR3 — 冻结 Delta 与提交 FLUSH（Phase 1）

**一句话:** 原子冻结 `_delta` 表（rename → `_delta_flushing`），同步冻结 `_foreign_delta`，创建新空表供后续写入，注册 flush job。

**负责:**
1. **崩溃恢复（三步检测）:**
   - `_delta_flushing` 存在 + snapshot 已提交 → DROP（清理孤儿）
   - `_delta_flushing` 存在 + snapshot 未提交 → 重新注册 job（恢复执行）
   - `_delta_flushing` 存在 + `flush_state` 无记录 → DROP + 重建 VIEW（清理残骸，#1089）
2. **原子 RENAME:** `ALTER TABLE _delta RENAME TO _delta_flushing`（PG DDL 事务内原子）
3. **同步冻结:** `ALTER TABLE _foreign_delta RENAME TO _foreign_delta_flushing`
4. **创建新空表:** `_delta` (LIKE `_data` INCLUDING ALL) + `_foreign_delta`（供下一轮导入）
5. **注册 job:** `INSERT INTO _gsiceberg.flush_jobs RETURNING job_id`
6. **短路逻辑:** `_delta` 不存在 → 返回 0；`_delta` 为空 → flush_state_done + 返回 0

**⚠️ 已知缺陷:** 纯 `_foreign_delta` 场景（只有 bulk import 数据，无 DML `_delta`）会被短路跳过，import 文件永不提交到 Iceberg catalog。见[flush_phase1.c:107-128]。

**源码依据:** [flush_phase1.c:11-246] `iceberg_flush_phase1()`。

---

### AR4 — 元数据提交与 ADD COLUMN（Phase 2-1）

**一句话:** 将 FLUSH 产出的 Parquet 文件注册到 Iceberg 元数据目录（snapshot + data_files + manifest），并承载 ADD COLUMN 带来的 schema 变更——将新 schema 写入 snapshot/manifest。

**负责:**
1. **Snapshot 插入:** `INSERT INTO _gsiceberg.snapshots`（idempotent, `ON CONFLICT DO NOTHING`），含 V3 字段 `sequence_number`、`schema_id`、`first_row_id`。
2. **Data files 插入:** `INSERT INTO _gsiceberg.data_files`（idempotent），含 `first_row_id` / `row_upper` 范围，自动写入 `_row_range_facts`。
3. **列统计填充:** 从 Parquet metadata 提取 column_sizes、value_counts、null_counts、lower_bounds、upper_bounds、partition_data。
4. **V3 Manifest 生成:** `iceberg_generate_manifests()` → manifest list + manifest .avro。
5. **CoW 文件退役:** 标记被重写的旧文件 `end_snapshot_id` / `end_snapshot`。
6. **Schema 演化感知:** 从 `_gsiceberg.tables.current_schema` 读取当前 schema jsonb 传递给 snapshot 插入和 manifest 生成——这是 ADD COLUMN 与 FLUSH 的交互点。

**ADD COLUMN 与 FLUSH 的交互机制:**

`iceberg_add_column()`（`02b-ddl.sql:295-365`）本身不调用 flush，但它修改了三样东西：
- ALTER TABLE `_delta` — 新写入的 delta 行包含新列
- ALTER FOREIGN TABLE `_data` — 数据表结构更新
- UPDATE `_gsiceberg.tables.current_schema` — jsonb schema 版本更新

下一次 FLUSH 运行时，`stage_a1_commit_catalog()`（[flush_stage_a1.c:822-824]）和 `flush_commit_catalog()`（[flush_catalog_commit.c:69-85]）从 `_gsiceberg.tables.current_schema` 读取最新 schema 并传递给 snapshot 和 manifest。**交互模式是：ADD COLUMN 写入 schema → FLUSH 读取 schema 并推进快照。**

**⚠️ 当前缺口:** `schema_id` 在 snapshot 插入时**硬编码为 0**（[flush_stage_a1.c:840] / [flush_catalog_commit.c:98]），即使 `current_schema` 已被 ADD COLUMN 更新。需改为从 `current_schema` 中提取真实的 schema_id。

**源码依据:**
- 新版 catalog commit: [flush_stage_a1.c:778-903] `stage_a1_commit_catalog()`
- 旧版 catalog commit: [flush_catalog_commit.c:46-204] `flush_commit_catalog()`
- 原子 INSERT: [flush_catalog.c:9-120] `next_snapshot_id()` / `insert_snapshot()` / `insert_data_file()`
- Manifest 生成: [manifest_writer.cpp:1-183]
- ADD COLUMN 实现: `02b-ddl.sql:295-365`

---

### AR5 — 数据提交（含增量数据准备与文件提交，Phase 2-2）

**一句话:** 从 `_delta_flushing` 读取增量操作，解析 I/D/U 语义，执行 CoW 选择性重写，过滤产出 Parquet 文件。

**负责:**
1. **Delta 读取:** 计数 `_delta_flushing` 行数 → [flush_stage_a1.c:103-130]。
2. **CoW 选择性重写:** `stage_a1_cow_rewrite()` → [flush_stage_a1.c:139-469]。
   - 构建 D_set（被删除的 `_row_id` 集合，从 `WHERE _op='D' AND _row_id >= 0` 查询）
   - 通过 `_row_range_facts` 定位受影响的 Parquet 文件
   - 逐文件 Arrow 读取 → 过滤 D_set → 幸存行 INSERT 到 `_gs_flush_out` 临时表
   - 含 `_row_id` 合成逻辑（#1082：mounted 文件无物理 `_row_id` 列时按 `first_row_id + row_index` 合成）
3. **I/D/U 配对解析:** `stage_a1_resolve_pairs()` → [flush_stage_a1.c:471-649]。
   - 读取所有 delta 行，按 `_row_id, _ts` 排序
   - D 操作 → tombstone；I/U 操作 → INSERT 到 `_gs_flush_out`
   - 负 `_row_id`（新插入行）从 `_gsiceberg.tables.next_row_id` 原子分配正 `_row_id`
4. **Parquet 写入:** `stage_a1_write_parquet()` → [flush_stage_a1.c:651-752]。
   - 崩溃恢复：检查 `flush_state` 是否有已写但未提交的 Parquet
   - 通过 C++ bridge `gsiceberg_write_parquet()` 写 Arrow Parquet
   - `.tmp` → 原子 rename → 最终路径 + `gsfile_register_internal`
5. **Post-flush hook 分发:** [flush_stage_a1.c:71-80] — `ampostflush` for scalar + vector AM。

**⚠️ 已知缺陷（性能阻塞器）:** CoW 和 resolve_pairs 均使用**逐行 INSERT**（每个幸存行一条 `INSERT INTO _gs_flush_out`），大数据量下不可用（N 行 = N 次 SQL 解析+规划+执行）。

**源码依据:**
- 新版完整管道: [flush_stage_a1.c:37-99] `iceberg_flush_stage_a1()`
- 旧版遗留（仅 `iceberg_flush_phase2` 旧路径使用）: [flush_delta.c], [flush_pairs.c], [flush_parquet.c]

---

### AR6 — 索引元数据提交（例如增加索引，Phase 2-3）

**一句话:** 每个索引按 micro-stage 粒度逐个完成 L0 物理训练，并写入 `index_catalog`。

**负责:**
1. **枚举索引定义:** 从 `index_catalog` 查询所有 `level=1, status='active'` 的索引。
2. **Micro-stage 循环:** 每次调用处理一个 `stage_seq` 对应一个索引，通过 `flush_state.stage_seq` 跟踪进度。
3. **L0 向量索引训练:** 调用 `gsvector_train_index(table_name, col_name, l0_dir)`。
4. **幂等检查:** 若该 snapshot 的 L0 已存在于 `index_catalog` → 跳过。
5. **输出目录:** `{table_path}/index/{idx_name}/L0/v{snap:03d}/`。
6. **写入 index_catalog:** `level=0, index_type='vector'`，含 metric/dim/schema_id/field_id。
7. **完成后 advance:** 所有 micro-stage 完成 → advance 到 Stage D。

**⚠️ 已知缺陷:**
- 仅支持 'T' (train) 微阶段类型
- 标量索引训练（`compact_scalar_indexes`）在 Stage A1 中作为 side-effect 调用（[flush_stage_a1.c:67-68]），不走 Stage C 的 per-index 事务隔离
- 无降级/重试策略（circuit breaker）

**源码依据:**
- Stage C: [flush_stage_c.c:33-288] `iceberg_flush_stage_c()`
- 旧版遗留: [flush_train.c:1-182] `flush_train_indexes()`

---

### AR7 — 索引提交（按代理索引粒度提交，Phase 2-4）

**一句话:** 通过 `ampostflush` hook 向各索引 AM 插件分发通知，由插件负责创建代理索引路由行（`is_proxy=true, level=0`），标记"该索引在当前 snapshot 可用"。

**负责（核心 FDW 侧）:**
- 在 Stage A1 结束后调用 `ampostflush` hook：[flush_stage_a1.c:71-80]
- 在 legacy Phase 2 结束后调用 `ampostflush` hook：[flush_phase2.c:84-92]
- 分别 dispatch 给 `FDW_AM_SCALAR` 和 `FDW_AM_VECTOR` 两个 AM 类型
- Hook 接口定义：[index_hooks.h:48-51] — `ampostflush` 回调签名

**负责（插件侧，不在本仓库范围）:**
- 在 `ampostflush` 回调中创建 `index_catalog` proxy 行（`is_proxy=true, level=0`）
- 幂等保证: `AND NOT EXISTS (SELECT 1 ... WHERE is_proxy=true AND snapshot_id=...)`

> **说明:** 按设计架构，AR7 的核心 FDW 侧已完成（hook 分发机制），端到端完备度取决于各插件（`gsiceberg_scalar`、`gsiceberg_vector`）的 `ampostflush` 实现。

**源码依据:**
- Hook dispatch: [flush_stage_a1.c:71-80], [flush_phase2.c:84-92]
- Hook 接口: [index_hooks.h:29-83] `FdwIndexAmRoutine`
- Hook 注册: [index_hooks.c:1-41]

---

### AR8 — 删除 Delta 与结算 FLUSH（Phase 3）

**一句话:** FLUSH 完成后的清理——删除临时表、标记 job 完成、清除 flush_state。

**负责:**
1. **VIEW 重建（关键安全操作）:** 在 DROP 之前调用 `iceberg_build_object()` 将 VIEW 指向新 `_delta`，切断对 `_delta_flushing` 的依赖，防止 CASCADE 向上传播误删父级 VIEW（#1089）。
2. **DROP `_delta_flushing` CASCADE**
3. **DROP `_foreign_delta_flushing`**
4. **标记 job 完成:** `UPDATE flush_jobs SET status='completed', finished_at=now()`
5. **清除 flush_state:** `flush_state_done()` — DELETE 整行

**源码依据:**
- Stage D: [flush_stage_d.c:10-91] `iceberg_flush_stage_d()`
- VIEW 重建: line 39-48
- DROP: line 54-68
- Job 完成: line 72-79
- 旧版遗留（仅 `iceberg_flush_phase2` 旧路径）: [flush_phase2.c:94-161] cleanup goto label

---

## 二、各 AR 需看护的文件及代码量

> **说明:** 行数为 `wc -l` 实测值。"(新版)" 表示被 `iceberg_flush_sync` / `iceberg_flush_worker` 调用；"(legacy)" 表示仅被旧版 `iceberg_flush_phase2()` 调用，不再被新路径使用；"(shared)" 表示被多个 AR 共享。

### 汇总表

| AR | 核心文件 | 行数 | 说明 |
|----|---------|------|------|
| **AR1** | `sql/02a-lifecycle.sql`（worker 入口） | ~30 SQL | 外部调度器调用点，内核无定时器 |
| **AR2** | `flush_phase2.c` + `flush_job.c` + SQL | ~350 | Worker/Sync 主编排 |
| **AR3** | `flush_phase1.c` | 259 | Phase 1 完整实现 |
| **AR4** | `flush_catalog.c` + `flush_catalog_commit.c`(legacy) + `manifest_writer.cpp/.h` + `flush_stage_a1.c`(commit_catalog 部分) | ~625 | 元数据 INSERT + manifest 生成 |
| **AR5** | `flush_stage_a1.c`(数据部分) + `flush_delta.c`(legacy) + `flush_pairs.c`(legacy) + `flush_parquet.c`(legacy) + `flush_internal.h`(shared) + `flush_utils.c`(shared) | ~1,200 | 数据读→过滤→Parquet 写 |
| **AR6** | `flush_stage_c.c` + `flush_train.c`(legacy) | ~480 | 索引训练 micro-stage |
| **AR7** | `flush_stage_a1.c`(hook) + `flush_phase2.c`(hook, legacy) + `index_hooks.h/.c`(shared) | ~50 (核心) | Hook 分发 |
| **AR8** | `flush_stage_d.c` | 99 | 清理 |
| **共享** | `flush_state.c` + `flush_stage_a2.c` + `flush_foreign_delta.c`(legacy) + `flush_writer.h` | ~620 | 状态管理 + foreign_delta 提交 |

### 各 AR 详细文件清单

#### AR1 — 后台定时拉起 FLUSH

| 文件 | 行数 | 说明 |
|------|------|------|
| `sql/02a-lifecycle.sql`（worker 调用点） | ~30 | `iceberg_flush_worker()` PL/pgSQL 入口 |

> AR1 的定时调度在仓库外实现（pg_cron / crontab），内核只提供 worker 函数的调用约定和 GUC 参数（`gsiceberg.flush_retry_count`）。

#### AR2 — 后台结算 FLUSH

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_phase2.c](gsiceberg/fdw/flush/flush_phase2.c) | 221 | `iceberg_flush()` (async, 仅Phase1) + `iceberg_flush_phase2()` (legacy) |
| [flush_job.c](gsiceberg/fdw/flush/flush_job.c) | 72 | `flush_job_init()` / `flush_job_destroy()` |
| `sql/02a-lifecycle.sql`（worker 调度） | ~50 | PL/pgSQL job 领取 + Stage 编排 |

#### AR3 — 冻结 Delta 与提交 FLUSH（Phase 1）

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_phase1.c](gsiceberg/fdw/flush/flush_phase1.c) | 259 | `iceberg_flush_phase1()` 完整实现 |

#### AR4 — 元数据提交与 ADD COLUMN

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_catalog.c](gsiceberg/fdw/flush/flush_catalog.c) | 120 | `next_snapshot_id()` / `insert_snapshot()` / `insert_data_file()` — 原子 INSERT |
| [flush_catalog_commit.c](gsiceberg/fdw/flush/flush_catalog_commit.c) | 204 | `flush_commit_catalog()` — 旧版 catalog commit (legacy) |
| [manifest_writer.h](gsiceberg/fdw/catalog/manifest_writer.h) | 118 | Manifest 生成接口 |
| [manifest_writer.cpp](gsiceberg/fdw/catalog/manifest_writer.cpp) | 183 | Manifest .avro 生成实现 |
| [flush_stage_a1.c](gsiceberg/fdw/flush/flush_stage_a1.c)（commit 部分） | ~154 | `stage_a1_commit_catalog()` — 新版 catalog commit |
| `sql/02b-ddl.sql`（ADD COLUMN） | ~70 | `iceberg_add_column()` — 修改 schema（不在 fdw/flush/ 目录） |

#### AR5 — 数据提交（Phase 2-2）

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_stage_a1.c](gsiceberg/fdw/flush/flush_stage_a1.c)（数据部分） | ~752 | 新版完整管道：read_delta + cow_rewrite + resolve_pairs + write_parquet |
| [flush_delta.c](gsiceberg/fdw/flush/flush_delta.c) | 37 | `flush_delta_read()` (legacy) |
| [flush_pairs.c](gsiceberg/fdw/flush/flush_pairs.c) | 167 | `flush_resolve_pairs()` (legacy) |
| [flush_parquet.c](gsiceberg/fdw/flush/flush_parquet.c) | 114 | `flush_write_parquet()` (legacy) |
| [flush_internal.h](gsiceberg/fdw/flush/flush_internal.h) | 97 | 共享头文件 — `FlushJob` 结构体 + 函数声明 (shared) |
| [flush_utils.c](gsiceberg/fdw/flush/flush_utils.c) | 76 | 工具函数: `flush_mkdir_p` / `escape_sql_literal` / `relation_exists` (shared) |

#### AR6 — 索引元数据提交（Phase 2-3）

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_stage_c.c](gsiceberg/fdw/flush/flush_stage_c.c) | 296 | `iceberg_flush_stage_c()` — micro-stage 循环 + L0 训练 |
| [flush_train.c](gsiceberg/fdw/flush/flush_train.c) | 182 | `flush_train_indexes()` — 旧版索引训练 + proxy 行 (legacy) |

#### AR7 — 索引提交（Phase 2-4）

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_stage_a1.c](gsiceberg/fdw/flush/flush_stage_a1.c)（hook dispatch） | ~11 | line 71-80: `ampostflush` for scalar + vector |
| [flush_phase2.c](gsiceberg/fdw/flush/flush_phase2.c)（hook dispatch） | ~9 | line 84-92: `ampostflush` (legacy 路径) |
| [index_hooks.h](gsiceberg/fdw/index/index_hooks.h) | 96 | `FdwIndexAmRoutine` 接口定义 (shared) |
| [index_hooks.c](gsiceberg/fdw/index/index_hooks.c) | 41 | AM 注册/查找函数 (shared) |

> 代理索引行（`is_proxy=true, level=0`）的实际创建代码在插件仓库中。

#### AR8 — 删除 Delta 与结算 FLUSH（Phase 3）

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_stage_d.c](gsiceberg/fdw/flush/flush_stage_d.c) | 99 | `iceberg_flush_stage_d()` — VIEW 重建 → DROP → job 完成 |

#### 跨 AR 共享基础设施

| 文件 | 行数 | 说明 |
|------|------|------|
| [flush_state.c](gsiceberg/fdw/flush/flush_state.c) | 323 | 多阶段崩溃恢复状态管理（被 AR2-AR8 所有阶段使用） |
| [flush_stage_a2.c](gsiceberg/fdw/flush/flush_stage_a2.c) | 213 | `iceberg_flush_stage_a2()` — foreign delta 独立提交 (新版) |
| [flush_foreign_delta.c](gsiceberg/fdw/flush/flush_foreign_delta.c) | 80 | `flush_foreign_delta()` — 旧版 foreign delta 处理 (legacy) |
| [flush_writer.h](gsiceberg/fdw/flush/flush_writer.h) | 54 | `FlushJob` 结构体 + `QI_GUARDED_PFREE` 宏 + 公开接口 |

---

## 三、完备状态评估

| AR | 完备度 | 评级依据 | 关键缺口 |
|----|--------|---------|----------|
| **AR1** | **10%** | Worker 函数和 GUC 已定义，零内置调度逻辑 | ① 内置 bgworker 定时循环；② `flush_interval` GUC；③ shared_preload_libraries 自动启动 |
| **AR2** | **50%** | `FOR UPDATE SKIP LOCKED` + 超时重置 + 重试上限完整，Stage 循环编排正确 | ① worker 并发数控制；② partition prepare→commit 两阶段；③ 执行时间上限 |
| **AR3** | **70%** | 崩溃恢复三种情况全覆盖，原子 rename 正确，双表同步冻结 | ① **functional bug**: 纯 `_foreign_delta`（无 `_delta`）静默跳过；② 新旧路径双维护 |
| **AR4** | **50%** | Snapshot/data_files INSERT 完整且幂等，列统计填充正常，V3 manifest 可用，ADD COLUMN 通过 `current_schema` 与 FLUSH 间接交互 | ① `schema_id` 硬编码为 0，未从 `current_schema` 提取真实 ID；② ADD COLUMN 与 FLUSH 无事务互锁（并发 ADD COLUMN + FLUSH 可能写不一致的 schema） |
| **AR5** | **55%** | CoW 逻辑正确（含 `_row_id` 合成 #1082），I/D/U 配对正确，崩溃恢复完整 | ① **性能阻塞器**: per-row INSERT；② 新旧路径功能重复 |
| **AR6** | **60%** | Micro-stage 循环正确，L0 向量训练可工作，幂等检查到位 | ① 标量索引不走 micro-stage 事务隔离；② 无降级/重试策略 |
| **AR7** | **90% (核心)** | Hook dispatch 完整（两个 AM 类型 × 两个调用点），接口定义干净 | ① 核心侧已完成；② 端到端取决于插件 `ampostflush` 实现 |
| **AR8** | **85%** | VIEW 重建→CASCADE DROP 流程正确，双表清理完整 | ① 旧路径 cleanup goto 与新 Stage D 逻辑重复 |

### 完备度一览

```
AR1  ██░░░░░░░░  10%
AR2  █████░░░░░  50%
AR3  ███████░░░  70%
AR4  █████░░░░░  50%
AR5  █████░░░░░  55%
AR6  ██████░░░░  60%
AR7  █████████░  90% (核心)
AR8  ████████░░  85%
        ——
加权  █████░░░░░  ~55%
```

### 剩余工作量评估

| 优先级 | AR | 工作项 | 人天 |
|--------|-----|--------|------|
| **P0** | AR5 | CoW per-row INSERT → 批量 INSERT / COPY 协议 | 5-7 |
| **P0** | AR4 | `schema_id` 硬编码 0 → 动态提取 + ADD COLUMN/FLUSH 互锁 | 4-6 |
| **P0** | AR3 | 修复纯 `_foreign_delta` 场景数据静默丢失 | 2-3 |
| **P0** | AR2 | Worker 并发数控制 + 执行超时 | 2-3 |
| **P1** | AR6 | 标量索引纳入 Stage C micro-stage 体系 | 3-4 |
| **P1** | AR1 | 内置 bgworker 定时调度 | 5-7 |
| **P1** | AR2 | Partition prepare→commit 两阶段编排 | 3-5 |
| **P2** | AR5 | 统一新旧路径（移除 legacy 代码） | 2-3 |
| **P2** | AR6 | 降级/重试策略（circuit breaker） | 3-5 |
| **P2** | AR7 | 验证各插件 `ampostflush` 实现 | 1-2 |
| **P2** | AR8 | 统一清理路径 | 0.5-1 |
| | | **合计** | **31-46 人天** |

按 2 人并行开发，预计 **4-6 周**可达到全部 AR 85%+ 完备。

---

## 附录 A：旧版遗留函数对照表

| 旧版函数 | 文件 | 新版替代 | 状态 |
|----------|------|---------|------|
| `iceberg_flush_phase2()` | flush_phase2.c:18-161 | `iceberg_flush_worker()` / `iceberg_flush_sync()` + 各 Stage | **死代码**（不再被 worker/sync 调用） |
| `flush_delta_read()` | flush_delta.c | `stage_a1_read_delta()` (flush_stage_a1.c) | **死代码** |
| `flush_resolve_pairs()` | flush_pairs.c | `stage_a1_resolve_pairs()` (flush_stage_a1.c) | **死代码** |
| `flush_write_parquet()` | flush_parquet.c | `stage_a1_write_parquet()` (flush_stage_a1.c) | **死代码** |
| `flush_commit_catalog()` | flush_catalog_commit.c | `stage_a1_commit_catalog()` (flush_stage_a1.c) | **死代码** |
| `flush_foreign_delta()` | flush_foreign_delta.c | `iceberg_flush_stage_a2()` (flush_stage_a2.c) | **死代码** |
| `flush_train_indexes()` | flush_train.c | `iceberg_flush_stage_c()` (flush_stage_c.c) + ampostflush hook | **死代码** |

旧版函数全部在 [flush_internal.h:82-89] 中声明，均仅被 `iceberg_flush_phase2()` 调用。移除建议：确认所有外部调用者已迁移后统一删除。

---

## 附录 B：对抗性审查修正清单

本次分析经过三路独立对抗性审查，以下是所有被指出并修正的问题：

| 审查维度 | 问题 | 修正 |
|----------|------|------|
| AR 定义 | 初版声称 "ADD COLUMN 不在 FLUSH 代码路径中" | 用户纠正：ADD COLUMN 修改 `current_schema`+`_delta`+`_data`，FLUSH 读取 `current_schema` 推进快照，交互是"写入方→读取方"模式。已修正 AR4 描述 |
| AR 定义 | 初版将 A2→A1→C→D 归于 `iceberg_flush()` | `iceberg_flush()`（异步）回退后只做 Phase 1。A2→A1→C→D 是 `iceberg_flush_sync` / `iceberg_flush_worker` 的执行顺序。已修正路径说明 |
| AR 定义 | 旧版 6 函数链标注不清晰 | 增加附录 A 对照表，标注所有 legacy 函数及其新版替代 |
| 代码度量 | `flush_phase2.c` 行数 270→221 | 修正 |
| 代码度量 | `flush_writer.h` 在 AR2 和 AR5 双计 | 移至共享基础设施 |
| 代码度量 | AR4/AR5/AR6 算数和与实际总计不一致 | 全部修正为实测值 |
| 完备度 | AR3 foreign_delta-only 场景是 functional bug | 70%（非 95%） |
| 完备度 | AR4 schema_id=0 硬编码 + 无 ADD COLUMN/FLUSH 互锁 | 50%（非 75%） |
| 完备度 | AR5 per-row INSERT 是性能阻塞器 | 55%（非 85%） |
| 完备度 | AR7 核心 FDW hook 分发已完整，50% 误将插件状态计入 | 90% 核心（非 50%） |
| 完备度 | AR6 标量索引不在 micro-stage 体系 | 60%（非 80%） |
| 工作量 | 20-29→31-46 人天 | 多因子上调 |
