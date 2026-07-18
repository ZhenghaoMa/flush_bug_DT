/* gsiceberg — Apache Iceberg FDW
 *
 * Six source files are concatenated by the Makefile into
 * gsiceberg--<version>.sql:
 *   01-schema.sql       Bootstrap: FDW, schemas, 7 base tables, RLS
 *   02a-lifecycle.sql   Core lifecycle: mount/refresh/unmount + flush + snapshot
 *   02b-ddl.sql         DDL mutations + schema grants
 *   02c-filesystem.sql  File lifecycle: whitelist/blacklist + register/scan/GC
 *   02d-index.sql       Index management: vector + scalar + compaction
 *   02e-util.sql        Cross-cutting: guards + selectivity + type mapping
 */

-- Foreign data wrapper
CREATE FUNCTION gsiceberg_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION gsiceberg_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER gsiceberg_fdw
  HANDLER   gsiceberg_fdw_handler
  VALIDATOR gsiceberg_fdw_validator;

-- Foreign server (used by iceberg_create_views for FOREIGN TABLE creation)
DO $$ BEGIN
    CREATE SERVER gsiceberg_server FOREIGN DATA WRAPPER gsiceberg_fdw;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ============================================================
-- gsiceberg schema — Iceberg metadata system tables
-- ============================================================
DO $$ BEGIN
    CREATE SCHEMA _gsiceberg;
EXCEPTION WHEN duplicate_schema THEN NULL;
END $$;
-- _gsfs schema is created by the gsfilesystem dependency extension

-- Namespace registry (#689). Single-level Iceberg namespace. Every table
-- belongs to exactly one namespace; 'default' is seeded and cannot be dropped.
CREATE TABLE _gsiceberg.namespaces (
    name        text PRIMARY KEY,
    created_at  timestamptz DEFAULT now()
);
INSERT INTO _gsiceberg.namespaces(name) VALUES ('default');

-- The default namespace maps to a PG schema (#906 Phase B).
-- 'default' is a reserved word — must be quoted.
DO $$ BEGIN
    CREATE SCHEMA "default";
EXCEPTION WHEN duplicate_schema THEN NULL;
END $$;

CREATE TABLE _gsiceberg.tables (
    table_name      text NOT NULL CHECK (length(table_name) <= 32),
    table_path      text NOT NULL,
    owner           text NOT NULL DEFAULT current_user,
    namespace       text NOT NULL DEFAULT 'default' REFERENCES _gsiceberg.namespaces(name),
    "current_schema"  text NOT NULL,
    partition_spec  text,
    created_at      timestamptz DEFAULT now(),
    next_row_id     bigint NOT NULL DEFAULT 1,  -- per-table atomic _row_id allocator (#849)
    PRIMARY KEY (namespace, table_name)
);
-- PG 16 defaults to LZ4 TOAST compression for text.  Some environments
-- trigger "compressed lz4 data is corrupt" on decompression (#33).
-- SET STORAGE EXTERNAL disables compression, avoiding the issue.
ALTER TABLE _gsiceberg.tables ALTER COLUMN "current_schema" SET STORAGE EXTERNAL;
ALTER TABLE _gsiceberg.tables ALTER COLUMN partition_spec SET STORAGE EXTERNAL;

-- (#978: PG identifier limit = NAMEDATALEN-1 = 63 bytes)
ALTER TABLE _gsiceberg.tables
    ADD CONSTRAINT tables_name_length CHECK (char_length(table_name) BETWEEN 1 AND 63);
-- Migration: add next_row_id if upgrading from pre-#849 schema.
-- PG 16 supports ADD COLUMN IF NOT EXISTS; the column is defined in
-- CREATE TABLE above for fresh installs.  This handles upgrades.
DO $$ BEGIN
    ALTER TABLE _gsiceberg.tables ADD COLUMN next_row_id bigint NOT NULL DEFAULT 1;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
-- Backfill next_row_id for existing tables from their _data and _delta rows.
-- Pre-release: typically no data, but idempotent for safety.
DO $$
DECLARE
    rec RECORD;
    max_id bigint;
    max_d bigint;
    max_del bigint;
BEGIN
    FOR rec IN SELECT table_name FROM _gsiceberg.tables LOOP
        max_d := 0; max_del := 0;
        BEGIN
            EXECUTE format(
                'SELECT COALESCE(MAX(_row_id), 0) FROM _gsiceberg.%I',
                '_' || rec.table_name || '_data'
            ) INTO max_d;
        EXCEPTION WHEN undefined_table THEN END;
        BEGIN
            EXECUTE format(
                'SELECT COALESCE(MAX(_row_id), 0) FROM _gsiceberg.%I',
                '_' || rec.table_name || '_delta'
            ) INTO max_del;
        EXCEPTION WHEN undefined_table THEN END;
        max_id := GREATEST(max_d, max_del) + 1;
        IF max_id > 1 THEN
            UPDATE _gsiceberg.tables SET next_row_id = max_id
            WHERE table_name = rec.table_name AND next_row_id < max_id;
        END IF;
    END LOOP;
END $$;

CREATE TABLE _gsiceberg.snapshots (
    table_name      text NOT NULL,
    namespace       text NOT NULL DEFAULT 'default',
    snapshot_id     bigint NOT NULL,
    parent_id       bigint,
    timestamp       timestamptz NOT NULL,
    manifest_list   text NOT NULL,
    summary         text,
    schema_id       int,
    sequence_number bigint DEFAULT 0,   -- V3 sequence-number (#777)
    first_row_id    bigint DEFAULT 0,   -- V3 first-row-id (#777)
    FOREIGN KEY (namespace, table_name) REFERENCES _gsiceberg.tables (namespace, table_name),
    PRIMARY KEY (table_name, snapshot_id)
);
ALTER TABLE _gsiceberg.snapshots ALTER COLUMN summary SET STORAGE EXTERNAL;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.snapshots ADD COLUMN author  text DEFAULT session_user;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.snapshots ADD COLUMN message text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE TABLE _gsiceberg.data_files (
    table_name         text NOT NULL,
    namespace          text NOT NULL DEFAULT 'default',
    begin_snapshot_id  bigint NOT NULL,
    end_snapshot_id    bigint,
    file_path          text NOT NULL,
    file_format        text DEFAULT 'PARQUET',
    record_count       bigint,
    file_size_bytes    bigint,
    column_sizes       text,
    value_counts       text,
    null_counts        text,
    lower_bounds       text,
    upper_bounds       text,
    partition_data     text,
    first_row_id       bigint,   -- _row_id synthesis base (per-file, from manifest entry)
    row_upper          bigint,   -- last _row_id in this file (first_row_id + record_count - 1)
    PRIMARY KEY (table_name, begin_snapshot_id, file_path),
    CONSTRAINT data_files_file_format_check CHECK (file_format IN ('PARQUET', 'AVRO', 'ORC'))
);
ALTER TABLE _gsiceberg.data_files ALTER COLUMN column_sizes SET STORAGE EXTERNAL;
ALTER TABLE _gsiceberg.data_files ALTER COLUMN value_counts SET STORAGE EXTERNAL;
ALTER TABLE _gsiceberg.data_files ALTER COLUMN null_counts SET STORAGE EXTERNAL;
ALTER TABLE _gsiceberg.data_files ALTER COLUMN lower_bounds SET STORAGE EXTERNAL;
ALTER TABLE _gsiceberg.data_files ALTER COLUMN upper_bounds SET STORAGE EXTERNAL;
ALTER TABLE _gsiceberg.data_files ALTER COLUMN partition_data SET STORAGE EXTERNAL;

CREATE INDEX ON _gsiceberg.data_files (table_name, begin_snapshot_id);

-- ============================================================
-- _row_range_facts — _row_id → (file, offset) interval mapping.
-- Replaces data_files.first_row_id/row_upper with multi-row
-- interval model. One row per contiguous _row_id range (#717).
-- ============================================================
CREATE TABLE IF NOT EXISTS _gsiceberg._row_range_facts (
    table_name       text NOT NULL,
    namespace        text NOT NULL DEFAULT 'default',
    first_row_id     bigint NOT NULL,
    begin_snapshot   bigint NOT NULL,
    row_upper        bigint NOT NULL,
    file_path        text NOT NULL,
    row_offset       bigint NOT NULL DEFAULT 0,
    end_snapshot     bigint,
    PRIMARY KEY (table_name, first_row_id, begin_snapshot)
);

-- Index for _row_id point lookup: B-tree reverse scan finds
-- the most recent interval covering a given _row_id.
DO $$ BEGIN
    CREATE INDEX _row_range_facts_lookup
        ON _gsiceberg._row_range_facts
        (table_name, first_row_id DESC, begin_snapshot DESC);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- Partial unique index: at most one open interval per file (#1124).
-- The scan setup LEFT JOINs on (table_name, file_path, end_snapshot IS NULL);
-- duplicate open intervals would multiply the file in the scan result.
-- One open interval is the normal state (the file is active); zero open
-- intervals means the file lacks _row_id synthesis (017 case7b guard).
DO $$ BEGIN
    CREATE UNIQUE INDEX _row_range_facts_one_open
        ON _gsiceberg._row_range_facts (table_name, namespace, file_path)
        WHERE end_snapshot IS NULL;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- Migrate: populate _row_range_facts from existing data_files.
-- Each file gets one interval row (row_offset=0, end_snapshot=NULL).
INSERT INTO _gsiceberg._row_range_facts
    (table_name, first_row_id, begin_snapshot, row_upper,
     file_path, row_offset, end_snapshot)
SELECT df.table_name, df.first_row_id, df.begin_snapshot_id, df.row_upper,
       df.file_path, 0, df.end_snapshot_id
FROM _gsiceberg.data_files df
WHERE df.first_row_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM _gsiceberg._row_range_facts r
    WHERE r.table_name = df.table_name
      AND r.first_row_id = df.first_row_id
      AND r.begin_snapshot = df.begin_snapshot_id
  );

-- Column registry — stable field_id-based type reference layer.
-- Populated at mount time from Iceberg schema JSON.  Provides pg_attribute
-- equivalent so downstream consumers (statistics, DDL, index metadata)
-- can use field_id references instead of repeated text traversal.
CREATE TABLE _gsiceberg.columns (
    table_name   text NOT NULL,
    namespace    text NOT NULL DEFAULT 'default',
    field_id     int NOT NULL,
    col_name     text NOT NULL,
    iceberg_type text NOT NULL,
    pg_type      text NOT NULL,
    pg_oid       int NOT NULL,
    is_required  bool DEFAULT false,
    position     int DEFAULT 0,
    FOREIGN KEY (namespace, table_name) REFERENCES _gsiceberg.tables (namespace, table_name) ON DELETE CASCADE,
    PRIMARY KEY (table_name, field_id)
);

-- Column storage files for independently-stored columns (v3.1).
-- When a column is added via ALTER FOREIGN TABLE ADD COLUMN,
-- subsequent flushes write it to a separate Parquet file under
-- columns/{col_name}/.  This keeps base data files unchanged
-- and preserves the _row_range_facts one-to-one mapping.
CREATE TABLE _gsiceberg.column_files (
    table_name   text NOT NULL,
    snapshot_id  bigint NOT NULL,
    col_name     text NOT NULL,
    file_path    text NOT NULL,
    record_count bigint DEFAULT 0,
    file_size_bytes bigint DEFAULT 0,
    PRIMARY KEY (table_name, snapshot_id, col_name)
);

-- Multi-stage flush_state: tracks flush lifecycle per table
-- with stage/seq granularity for crash recovery at any point.
-- Full job tracking is in _gsiceberg.flush_jobs.
-- Legacy columns (parquet_file, delta_rows, snapshot_id) retained
-- for backward compatibility; new code uses stage_detail text.
CREATE TABLE _gsiceberg.flush_state (
    table_name    text NOT NULL,
    namespace     text NOT NULL DEFAULT 'default',
    flush_status  text NOT NULL DEFAULT 'idle',
    stage         text,              -- 'freeze'|'foreign'|'flush'|'train'|'cleanup' — current stage
    stage_seq     int DEFAULT 0,     -- micro-stage index (Stage C only)
    stage_detail  text,             -- stage context: {parquet_file, delta_rows, snapshot_id, ...}
    retry_count   int DEFAULT 0,
    error_msg     text,
    started_at    timestamptz DEFAULT now(),
    -- Legacy columns (deprecated — data now in stage_detail text)
    parquet_file  text,
    delta_rows    bigint,
    snapshot_id   bigint,
    PRIMARY KEY (namespace, table_name)
);

-- #985: domain constraints on status columns (NOT VALID — existing
-- data may pre-date the constraint; new writes are enforced).
ALTER TABLE _gsiceberg.flush_state
    ADD CONSTRAINT flush_state_status_check CHECK (flush_status IN ('idle', 'in_progress', 'error')) NOT VALID,
    ADD CONSTRAINT flush_state_stage_check CHECK (stage IN ('freeze', 'foreign', 'flush', 'train', 'cleanup')) NOT VALID,
    ADD CONSTRAINT flush_state_retry_check CHECK (retry_count >= 0) NOT VALID;
ALTER TABLE _gsiceberg.data_files
    ADD CONSTRAINT data_files_record_count_check CHECK (record_count IS NULL OR record_count >= 0) NOT VALID,
    ADD CONSTRAINT data_files_file_size_check CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0) NOT VALID,
    ADD CONSTRAINT data_files_begin_snap_check CHECK (begin_snapshot_id > 0) NOT VALID;

-- Migration: add new columns if upgrading from pre-stage schema.
-- PG 16 does not support ADD COLUMN IF NOT EXISTS, so use
-- DO block with exception handling.
DO $$ BEGIN
    ALTER TABLE _gsiceberg.flush_state ADD COLUMN stage text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.flush_state ADD COLUMN stage_seq int DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.flush_state ADD COLUMN stage_detail text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.flush_state ADD COLUMN retry_count int DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.flush_state ADD COLUMN error_msg text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- Background flush job queue
CREATE TABLE _gsiceberg.flush_jobs (
    job_id       bigserial PRIMARY KEY,
    table_name   text NOT NULL,
    namespace    text NOT NULL DEFAULT 'default',
    flush_table  text NOT NULL,
    started_at   timestamptz DEFAULT now(),
    finished_at  timestamptz,
    status       text NOT NULL DEFAULT 'pending',
    retry_count  int DEFAULT 0,
    error_msg    text
);

-- Delta temporary row-id allocator: INSERT triggers take negative values
-- from this sequence for _row_id in _delta tables (#1016 PR1).
DO $$ BEGIN
    CREATE SEQUENCE _gsiceberg._rowid_temp_seq START WITH 1 INCREMENT BY 1;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- DBA role: bypasses RLS and accesses _gsiceberg/_gsfs schemas.
DO $$ BEGIN
    CREATE ROLE gsiceberg_admin WITH NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Row-level security: users see only their own tables or public tables.
-- DBA (gsiceberg_admin role) bypasses RLS for troubleshooting.
-- (Wrapped in DO blocks for PG 9.2 compatibility — RLS is PG 9.5+)
DO $$ BEGIN
    ALTER TABLE _gsiceberg.tables ENABLE ROW LEVEL SECURITY;
    CREATE POLICY tables_owner_policy ON _gsiceberg.tables
      FOR ALL USING (owner = current_user OR owner = 'public'
                     OR pg_has_role('gsiceberg_admin', 'MEMBER'));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- snapshots, data_files, flush_state inherit access control
-- through table_name.  Join to tables for ownership check:
DO $$ BEGIN
    ALTER TABLE _gsiceberg.snapshots ENABLE ROW LEVEL SECURITY;
    CREATE POLICY snapshots_owner_policy ON _gsiceberg.snapshots FOR ALL
      USING (EXISTS (SELECT 1 FROM _gsiceberg.tables t
                      WHERE t.table_name = _gsiceberg.snapshots.table_name
                        AND (t.owner = current_user OR t.owner = 'public'
                             OR pg_has_role('gsiceberg_admin', 'MEMBER'))));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE _gsiceberg.data_files ENABLE ROW LEVEL SECURITY;
    CREATE POLICY data_files_owner_policy ON _gsiceberg.data_files FOR ALL
      USING (EXISTS (SELECT 1 FROM _gsiceberg.tables t
                      WHERE t.table_name = _gsiceberg.data_files.table_name
                        AND (t.owner = current_user OR t.owner = 'public'
                             OR pg_has_role('gsiceberg_admin', 'MEMBER'))));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE _gsiceberg.flush_state ENABLE ROW LEVEL SECURITY;
    CREATE POLICY flush_state_owner_policy ON _gsiceberg.flush_state FOR ALL
      USING (EXISTS (SELECT 1 FROM _gsiceberg.tables t
                      WHERE t.table_name = _gsiceberg.flush_state.table_name
                        AND (t.owner = current_user OR t.owner = 'public'
                             OR pg_has_role('gsiceberg_admin', 'MEMBER'))));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;



-- #985 R3: flush_jobs domain, next_row_id guard.
ALTER TABLE _gsiceberg.flush_jobs
    ADD CONSTRAINT flush_jobs_status_check CHECK (status IN ('pending', 'active', 'in_progress', 'completed', 'failed')) NOT VALID,
    ADD CONSTRAINT flush_jobs_retry_check CHECK (retry_count >= 0) NOT VALID;
ALTER TABLE _gsiceberg.tables
    ADD CONSTRAINT tables_next_row_id_check CHECK (next_row_id >= 1) NOT VALID;
-- ============================================================
-- #985 R2: positive-only IDs and snapshot sequence invariants.
-- ============================================================
ALTER TABLE _gsiceberg.snapshots
    ADD CONSTRAINT snapshots_snap_id_check CHECK (snapshot_id > 0) NOT VALID,
    ADD CONSTRAINT snapshots_parent_check CHECK (parent_id IS NULL OR parent_id > 0) NOT VALID,
    ADD CONSTRAINT snapshots_sequence_check CHECK (sequence_number >= 0) NOT VALID;
ALTER TABLE _gsiceberg._row_range_facts
    ADD CONSTRAINT row_range_row_offset_check CHECK (row_offset >= 0) NOT VALID;

-- Mount an Iceberg table. Parses metadata.json and populates
-- _gsiceberg.tables/snapshots/data_files.
-- Build version: git SHA embedded at compile time. Tests should emit
-- this to establish traceability between results and the source revision.

-- #985 R4: nullable snapshot endpoint + owned_files + schema_id guards.
ALTER TABLE _gsiceberg.data_files
    ADD CONSTRAINT data_files_end_snap_check CHECK (end_snapshot_id IS NULL OR end_snapshot_id > 0) NOT VALID;
ALTER TABLE _gsiceberg.snapshots
    ADD CONSTRAINT snapshots_schema_id_check CHECK (schema_id IS NULL OR schema_id >= 0) NOT VALID;

-- #985 R5: columns domain, nullable first_row_id guard.
ALTER TABLE _gsiceberg.columns
    ADD CONSTRAINT columns_position_check CHECK (position >= 0) NOT VALID;
ALTER TABLE _gsiceberg.data_files
    ADD CONSTRAINT data_files_first_row_id_check CHECK (first_row_id IS NULL OR first_row_id >= 0) NOT VALID;
-- SQL wrappers for must-C functions (#1292 C→SQL migration)
CREATE FUNCTION gsfile_register_internal(path text, table_name text)
    RETURNS integer AS 'MODULE_PATHNAME' LANGUAGE C STRICT;
CREATE FUNCTION flush_mkdir_p(path text)
    RETURNS integer AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- IcebergWriter SQL-callable wrappers (#1292)
CREATE FUNCTION _gsiceberg.writer_start_transaction(
    table_name text, namespace text, parent_snap_id bigint)
    RETURNS bigint AS 'MODULE_PATHNAME', 'gsiceberg_writer_start_transaction'
    LANGUAGE C STRICT;
CREATE FUNCTION _gsiceberg.writer_add_file(
    snap_id bigint, file_path text, file_format text,
    record_count bigint, file_size_bytes bigint,
    first_row_id bigint, row_upper bigint)
    RETURNS integer AS 'MODULE_PATHNAME', 'gsiceberg_writer_add_file'
    LANGUAGE C STRICT;
CREATE FUNCTION _gsiceberg.writer_remove_file(
    snap_id bigint, file_path text)
    RETURNS integer AS 'MODULE_PATHNAME', 'gsiceberg_writer_remove_file'
    LANGUAGE C STRICT;
CREATE FUNCTION _gsiceberg.writer_commit(
    snap_id bigint, table_path text, operation text,
    summary_json text, schema_json text, partition_spec_json text)
    RETURNS integer AS 'MODULE_PATHNAME', 'gsiceberg_writer_commit'
    LANGUAGE C;
CREATE FUNCTION _gsiceberg.writer_abort(snap_id bigint)
    RETURNS void AS 'MODULE_PATHNAME', 'gsiceberg_writer_abort'
    LANGUAGE C;

-- Parquet write bridges (#1292 follow-up): encrypt SPITupleTable*→Arrow gap.
-- PL/pgSQL writes Parquet via SELECT o_rows,o_bytes FROM write_parquet_table_flat(...)
CREATE FUNCTION _gsiceberg.write_parquet_table_flat(
    source_table text, output_path text,
    OUT o_rows bigint, OUT o_bytes bigint)
    RETURNS record AS 'MODULE_PATHNAME', 'gsiceberg_write_parquet_table_flat'
    LANGUAGE C STRICT;
CREATE FUNCTION _gsiceberg.write_parquet_column_flat(
    source_table text, col_name text, output_path text,
    OUT o_rows bigint, OUT o_bytes bigint)
    RETURNS record AS 'MODULE_PATHNAME', 'gsiceberg_write_parquet_column_flat'
    LANGUAGE C STRICT;

-- Misc C functions exposed for PL/pgSQL flush stages (#1292 follow-up)
CREATE FUNCTION _gsiceberg.index_freeze_deltas(table_name text)
    RETURNS void AS 'MODULE_PATHNAME', 'gsiceberg_index_freeze_deltas'
    LANGUAGE C STRICT;
CREATE FUNCTION _gsiceberg.cache_invalidate_table(table_name text)
    RETURNS void AS 'MODULE_PATHNAME', 'gsiceberg_cache_invalidate_table'
    LANGUAGE C STRICT;
-- flush_catalog PL/pgSQL replacements (#1292 C→SQL migration)
CREATE FUNCTION _gsiceberg.next_snapshot_id(p_table_name text)
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT COALESCE(MAX(snapshot_id), 0) + 1
    FROM _gsiceberg.snapshots
    WHERE table_name = p_table_name;
$$;

CREATE FUNCTION _gsiceberg.insert_snapshot(
    p_table_name     text,
    p_snap_id        bigint,
    p_parent_id      bigint,
    p_manifest_list  text,
    p_operation      text,
    p_sequence_number bigint,
    p_schema_id      int,
    p_first_row_id   bigint,
    p_message        text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_namespace text;
    v_summary   text;
BEGIN
    SELECT namespace INTO v_namespace FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    v_summary := format('{"operation":"%s"}', COALESCE(p_operation, 'unknown'));
    IF NOT EXISTS (SELECT 1 FROM _gsiceberg.snapshots
                   WHERE table_name = p_table_name AND snapshot_id = p_snap_id) THEN
        INSERT INTO _gsiceberg.snapshots
            (table_name, snapshot_id, parent_id, timestamp,
             manifest_list, summary, sequence_number, schema_id,
             first_row_id, namespace, message, author)
        VALUES (p_table_name, p_snap_id,
                CASE WHEN p_parent_id > 0 THEN p_parent_id ELSE NULL END,
                now(), p_manifest_list, v_summary::text,
                p_sequence_number, p_schema_id, p_first_row_id,
                v_namespace, p_message, session_user);
    END IF;
    RETURN 0;
END;
$$;

CREATE FUNCTION _gsiceberg.insert_data_file(
    p_table_name     text,
    p_begin_snap_id  bigint,
    p_file_path      text,
    p_record_count   bigint,
    p_file_size      bigint,
    p_first_row_id   bigint,
    p_row_upper      bigint)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_namespace text;
BEGIN
    SELECT namespace INTO v_namespace FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    IF NOT EXISTS (SELECT 1 FROM _gsiceberg.data_files
                   WHERE table_name = p_table_name
                     AND begin_snapshot_id = p_begin_snap_id
                     AND file_path = p_file_path) THEN
        INSERT INTO _gsiceberg.data_files
            (table_name, begin_snapshot_id, file_path, file_format,
             record_count, file_size_bytes, first_row_id, row_upper, namespace)
        VALUES (p_table_name, p_begin_snap_id, p_file_path, 'PARQUET',
                p_record_count, p_file_size, p_first_row_id, p_row_upper, v_namespace);
    END IF;
    IF p_first_row_id >= 0 THEN
        IF NOT EXISTS (SELECT 1 FROM _gsiceberg._row_range_facts
                       WHERE table_name = p_table_name
                         AND first_row_id = p_first_row_id
                         AND begin_snapshot = p_begin_snap_id) THEN
            INSERT INTO _gsiceberg._row_range_facts
                (table_name, first_row_id, begin_snapshot, row_upper,
                 file_path, row_offset, end_snapshot, namespace)
            VALUES (p_table_name, p_first_row_id, p_begin_snap_id,
                    p_row_upper, p_file_path, 0, NULL, v_namespace);
        END IF;
    END IF;
    RETURN 0;
END;
$$;
-- flush_state PL/pgSQL replacements (#1292 C→SQL migration)
-- Replaces fdw/flush/flush_state.c (323 lines) with native PL/pgSQL.
-- Schema-qualified namespace: _gsiceberg.flush_state_*

CREATE FUNCTION _gsiceberg.flush_state_enter_stage(
    p_table_name text, p_stage text,
    p_stage_seq int, p_detail_json text DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ns text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    IF p_detail_json IS NOT NULL THEN
        UPDATE _gsiceberg.flush_state SET
            flush_status = 'in_progress',
            stage = p_stage,
            stage_seq = p_stage_seq,
            stage_detail = p_detail_json::text,
            started_at = now(),
            retry_count = 0, error_msg = NULL,
            parquet_file = NULL, delta_rows = NULL, snapshot_id = NULL
        WHERE namespace = v_ns AND table_name = p_table_name;
        IF NOT FOUND THEN
            INSERT INTO _gsiceberg.flush_state
                (table_name, flush_status, stage, stage_seq, stage_detail, namespace)
            VALUES (p_table_name, 'in_progress', p_stage, p_stage_seq,
                    p_detail_json::text, v_ns);
        END IF;
    ELSE
        UPDATE _gsiceberg.flush_state SET
            flush_status = 'in_progress',
            stage = p_stage,
            stage_seq = p_stage_seq,
            started_at = now(),
            retry_count = 0, error_msg = NULL,
            parquet_file = NULL, delta_rows = NULL, snapshot_id = NULL
        WHERE namespace = v_ns AND table_name = p_table_name;
        IF NOT FOUND THEN
            INSERT INTO _gsiceberg.flush_state
                (table_name, flush_status, stage, stage_seq, namespace)
            VALUES (p_table_name, 'in_progress', p_stage, p_stage_seq, v_ns);
        END IF;
    END IF;
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_update_detail(
    p_table_name text, p_detail_json text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ns text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    UPDATE _gsiceberg.flush_state
        SET stage_detail = COALESCE(stage_detail, '{}'::text)
                           || COALESCE(p_detail_json, '{}')::text
        WHERE table_name = p_table_name AND namespace = v_ns;
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_complete_stage(
    p_table_name text, p_next_stage text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ns text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    UPDATE _gsiceberg.flush_state
        SET stage = p_next_stage, stage_seq = 0
        WHERE table_name = p_table_name AND namespace = v_ns;
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_get_stage(
    p_table_name text,
    OUT o_stage text, OUT o_detail text)
RETURNS record LANGUAGE plpgsql AS $$
DECLARE
    v_ns text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    SELECT stage, stage_detail::text INTO o_stage, o_detail
        FROM _gsiceberg.flush_state
        WHERE table_name = p_table_name AND namespace = v_ns
          AND flush_status = 'in_progress';
    IF NOT FOUND THEN
        o_stage := NULL; o_detail := NULL;
    END IF;
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_get_seq(p_table_name text)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    v_ns text;
    v_seq int;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    SELECT COALESCE(stage_seq, 0) INTO v_seq
        FROM _gsiceberg.flush_state
        WHERE table_name = p_table_name AND namespace = v_ns
          AND flush_status = 'in_progress';
    RETURN COALESCE(v_seq, 0);
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_done(p_table_name text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ns text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    DELETE FROM _gsiceberg.flush_state
        WHERE table_name = p_table_name AND namespace = v_ns;
END;
$$;

-- Legacy API delegates
CREATE FUNCTION _gsiceberg.flush_state_begin(p_table_name text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM _gsiceberg.flush_state_enter_stage(
        p_table_name, 'flush', 0, '{"phase":"freeze_done"}');
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_set_file(
    p_table_name text, p_path text, p_n_rows bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ns     text;
    v_detail text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    v_detail := format('{"parquet_file":"%s","delta_rows":%s}',
                       COALESCE(p_path, ''), p_n_rows);
    PERFORM _gsiceberg.flush_state_update_detail(p_table_name, v_detail);
    UPDATE _gsiceberg.flush_state
        SET parquet_file = COALESCE(p_path, ''),
            delta_rows = p_n_rows
        WHERE table_name = p_table_name AND namespace = v_ns;
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_set_snapshot(
    p_table_name text, p_snapshot_id bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_ns     text;
    v_detail text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    v_detail := format('{"snapshot_id":%s}', p_snapshot_id);
    PERFORM _gsiceberg.flush_state_update_detail(p_table_name, v_detail);
    UPDATE _gsiceberg.flush_state
        SET snapshot_id = p_snapshot_id
        WHERE table_name = p_table_name AND namespace = v_ns;
END;
$$;

CREATE FUNCTION _gsiceberg.flush_state_recover(
    p_table_name text, p_current_n_rows int,
    OUT o_parquet_path text, OUT o_found bool)
RETURNS record LANGUAGE plpgsql AS $$
DECLARE
    v_ns       text;
    v_path     text;
    v_dr       bigint;
    v_legacy_path text;
    v_legacy_dr   text;
    v_stat     record;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;

    -- Try text stage_detail first
    SELECT stage_detail->>'parquet_file',
           (stage_detail->>'delta_rows')::bigint
        INTO v_path, v_dr
        FROM _gsiceberg.flush_state
        WHERE table_name = p_table_name AND namespace = v_ns
          AND flush_status = 'in_progress';

    IF v_path IS NOT NULL AND length(v_path) > 0 THEN
        -- Verify file exists via pg_stat_file (throws on missing)
        BEGIN
            SELECT * INTO v_stat FROM pg_stat_file(v_path);
        EXCEPTION WHEN undefined_file THEN
            o_found := false; RETURN;
        END;
        IF p_current_n_rows > 0 AND v_dr IS NOT NULL
           AND v_dr != p_current_n_rows THEN
            o_found := false; RETURN;
        END IF;
        o_parquet_path := v_path;
        o_found := true; RETURN;
    END IF;

    -- Fallback: legacy columns
    SELECT parquet_file, delta_rows::text
        INTO v_legacy_path, v_legacy_dr
        FROM _gsiceberg.flush_state
        WHERE table_name = p_table_name AND namespace = v_ns
          AND flush_status = 'in_progress';

    IF v_legacy_path IS NULL OR length(v_legacy_path) = 0 THEN
        o_found := false; RETURN;
    END IF;
    IF p_current_n_rows > 0 AND v_legacy_dr IS NOT NULL
       AND v_legacy_dr::int != p_current_n_rows THEN
        o_found := false; RETURN;
    END IF;

    BEGIN
        SELECT * INTO v_stat FROM pg_stat_file(v_legacy_path);
    EXCEPTION WHEN undefined_file THEN
        o_found := false; RETURN;
    END;
    o_parquet_path := v_legacy_path;
    o_found := true;
END;
$$;
CREATE OR REPLACE FUNCTION iceberg_flush_stage_cleanup(
    table_name text, job_id bigint)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
DECLARE
    v_stem text;
    v_ns   text;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = $1;
    v_stem := v_ns || '_' || $1;

    PERFORM _gsiceberg.flush_state_enter_stage($1, 'cleanup', 0);

    -- Rebuild view before DROP (severs dependency from _delta_flushing)
    PERFORM _gsiceberg.iceberg_build_object($1, v_ns);

    -- DROP flushing tables with CASCADE for safety
    EXECUTE format('DROP TABLE IF EXISTS _gsiceberg._%s_delta_flushing CASCADE', v_stem);
    EXECUTE format('DROP TABLE IF EXISTS _gsiceberg._%s_foreign_delta_flushing', v_stem);

    -- Mark job completed
    UPDATE _gsiceberg.flush_jobs
        SET status = 'completed', finished_at = now()
        WHERE job_id = $2;

    PERFORM _gsiceberg.flush_state_done($1);
    RETURN true;
END;
$$;
CREATE OR REPLACE FUNCTION iceberg_flush_stage_foreign(
    table_name text, job_id bigint)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
DECLARE
    v_ns        text;
    v_table_path text;
    v_stem      text;
    v_fq        text;
    v_snap_id   bigint;
    v_max_rup   bigint := 0;
    v_schema    text;
    v_part      text;
    v_rec       record;
    v_cum_rows  bigint := 0;
    v_frid      bigint;
    v_rup       bigint;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = $1;
    v_stem := v_ns || '_' || $1;

    PERFORM _gsiceberg.flush_state_enter_stage($1, 'foreign', 0);

    SELECT table_path INTO v_table_path FROM _gsiceberg.tables
        WHERE table_name = $1;

    -- Start Writer transaction
    SELECT _gsiceberg.writer_start_transaction($1, v_ns, 0) INTO v_snap_id;

    -- Get baseline next_row_id for legacy entries
    SELECT next_row_id INTO v_cum_rows FROM _gsiceberg.tables
        WHERE table_name = $1;

    -- Process foreign_delta_flushing entries if table exists
    v_fq := format('_gsiceberg._%s_foreign_delta_flushing', v_stem);
    IF EXISTS (SELECT 1 FROM pg_class c
               JOIN pg_namespace n ON c.relnamespace = n.oid
               WHERE n.nspname = '_gsiceberg'
                 AND c.relname = '_' || v_stem || '_foreign_delta_flushing') THEN
        FOR v_rec IN EXECUTE
            format('SELECT file_path, record_count, file_size_bytes, '
                   'first_row_id, row_upper FROM %I', v_fq)
        LOOP
            -- Determine row_id bounds (#849: new entries carry their own)
            IF v_rec.first_row_id >= 0 AND v_rec.row_upper >= v_rec.first_row_id THEN
                v_frid := v_rec.first_row_id;
                v_rup  := v_rec.row_upper;
            ELSE
                v_frid := v_cum_rows;
                v_rup  := v_cum_rows + v_rec.record_count - 1;
                v_cum_rows := v_cum_rows + v_rec.record_count;
            END IF;
            IF v_rup > v_max_rup THEN
                v_max_rup := v_rup;
            END IF;

            PERFORM _gsiceberg.writer_add_file(
                v_snap_id, v_rec.file_path, 'PARQUET',
                v_rec.record_count, v_rec.file_size_bytes,
                v_frid, v_rup);

            -- Register file (idempotent: ON CONFLICT DO NOTHING)
            PERFORM gsfile_register_internal(v_rec.file_path, $1);
        END LOOP;
    END IF;

    -- Advance next_row_id past max _row_id seen
    IF v_max_rup > 0 THEN
        UPDATE _gsiceberg.tables
            SET next_row_id = GREATEST(next_row_id, v_max_rup + 1)
            WHERE table_name = $1;
    END IF;

    -- Commit Writer transaction
    SELECT "current_schema"::text INTO v_schema FROM _gsiceberg.tables
        WHERE table_name = $1;
    SELECT "partition_spec"::text INTO v_part FROM _gsiceberg.tables
        WHERE table_name = $1;

    PERFORM _gsiceberg.writer_commit(
        v_snap_id, v_table_path, 'import', NULL,
        COALESCE(v_schema, '{}'),
        COALESCE(v_part, '{"fields":[]}'));

    PERFORM _gsiceberg.flush_state_complete_stage($1, 'train');
    RETURN true;
END;
$$;
-- stage_train PL/pgSQL replacement (#1292 C→SQL migration)
-- Replaces fdw/flush/flush_stage_train.c logic (319→33 line C wrapper).
CREATE FUNCTION _gsiceberg.stage_train_sql(
    p_table_name text, p_job_id bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    v_ns          text;
    v_table_path  text;
    v_snap_id     bigint;
    v_parquet_path text;
    v_final_rows  bigint;
    v_idx         record;
    v_current_seq int;
    v_total       int := 0;
    v_seq         int := 0;
    v_l0_dir      text;
    v_train_ok    boolean;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;

    PERFORM _gsiceberg.flush_state_enter_stage(
        p_table_name, 'train', _gsiceberg.flush_state_get_seq(p_table_name));

    -- Rebuild context from catalog
    SELECT table_path INTO v_table_path FROM _gsiceberg.tables
        WHERE table_name = p_table_name AND namespace = v_ns;

    SELECT (stage_detail->>'snapshot_id')::bigint INTO v_snap_id
        FROM _gsiceberg.flush_state
        WHERE table_name = p_table_name AND namespace = v_ns;
    IF v_snap_id IS NULL OR v_snap_id = 0 THEN
        SELECT COALESCE(MAX(snapshot_id), 0) INTO v_snap_id
            FROM _gsiceberg.snapshots
            WHERE table_name = p_table_name AND namespace = v_ns;
    END IF;

    SELECT stage_detail->>'parquet_file',
           (stage_detail->>'delta_rows')::bigint
        INTO v_parquet_path, v_final_rows
        FROM _gsiceberg.flush_state
        WHERE table_name = p_table_name AND namespace = v_ns;

    -- Count total micro-stages
    SELECT count(*) INTO v_total
        FROM _gsiceberg.index_physical
        WHERE table_name = p_table_name AND end_snapshot_id IS NULL;

    IF v_total = 0 THEN
        PERFORM _gsiceberg.flush_state_complete_stage(p_table_name, 'cleanup');
        RETURN true;
    END IF;

    v_current_seq := _gsiceberg.flush_state_get_seq(p_table_name);
    IF v_current_seq >= v_total THEN
        PERFORM _gsiceberg.flush_state_complete_stage(p_table_name, 'cleanup');
        RETURN true;
    END IF;

    -- Process current micro-stage
    SELECT index_name, column_name, COALESCE(metric,'L2'),
           COALESCE(dim,128)::int2, COALESCE(schema_id,0)::int4,
           COALESCE(field_id,0)::int4, version_path
        INTO v_idx
        FROM _gsiceberg.index_physical
        WHERE table_name = p_table_name AND end_snapshot_id IS NULL
        ORDER BY index_name
        OFFSET v_current_seq LIMIT 1;

    IF v_idx.index_name IS NOT NULL AND v_idx.column_name IS NOT NULL
       AND v_idx.version_path IS NOT NULL THEN
        -- Idempotency check
        IF NOT EXISTS (SELECT 1 FROM _gsiceberg.index_physical
                       WHERE index_name = v_idx.index_name
                         AND begin_snapshot_id = v_snap_id) THEN

            IF v_parquet_path IS NOT NULL AND length(v_parquet_path) > 0
               AND v_final_rows > 0 AND v_table_path IS NOT NULL THEN
                v_l0_dir := v_table_path || '/index/' || v_idx.index_name
                            || '/L0/v' || lpad(v_snap_id::text, 3, '0');

                PERFORM flush_mkdir_p(v_l0_dir);

                -- Call gsvector_train_index (from gsvector extension)
                v_train_ok := (SELECT public.gsvector_train_index(
                    p_table_name, v_idx.column_name, v_l0_dir));

                IF v_train_ok THEN
                    IF NOT EXISTS (SELECT 1 FROM _gsiceberg.index_proxy
                                   WHERE index_name = v_idx.index_name) THEN
                        INSERT INTO _gsiceberg.index_proxy
                            (index_name, table_name, column_name, index_type,
                             physical_ref, snapshot_id, schema_id, field_id)
                        VALUES (v_idx.index_name, p_table_name, v_idx.column_name,
                                'vector', v_l0_dir, v_snap_id,
                                v_idx.schema_id, v_idx.field_id);
                    END IF;

                    PERFORM public.gsfile_register_internal(v_l0_dir, p_table_name);
                    PERFORM public.gsfile_blacklist_increment(
                        v_table_path || '/index/' || v_idx.index_name || '/L0');
                ELSE
                    RETURN false;
                END IF;
            END IF;
        END IF;
    END IF;

    -- Advance or complete
    IF v_current_seq + 1 >= v_total THEN
        PERFORM _gsiceberg.flush_state_complete_stage(p_table_name, 'cleanup');
    ELSE
        PERFORM _gsiceberg.flush_state_enter_stage(
            p_table_name, 'train', v_current_seq + 1);
    END IF;

    RETURN true;
END;
$$;
-- flush_stage_freeze PL/pgSQL replacement (#1292 follow-up)
-- Replaces fdw/flush/flush_stage_freeze.c (294 → 25 line C wrapper).
-- Handles: crash recovery, ALTER TABLE RENAME delta→delta_flushing,
-- CREATE new _delta/_foreign_delta, index delta freeze, flush job register.

CREATE FUNCTION _gsiceberg.flush_stage_freeze(p_table_name text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    v_ns         text;
    v_stem       text;
    v_n_delta    int;
    v_job_id     bigint;
    v_sid        bigint;
    v_has_flushing bool;
    v_has_job    bool;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    v_stem := v_ns || '_' || p_table_name;

    -- ── Crash recovery: check for orphaned _delta_flushing ──────────
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = '_gsiceberg'
          AND c.relname = '_' || v_stem || '_delta_flushing'
    ) INTO v_has_flushing;

    IF v_has_flushing THEN
        SELECT snapshot_id::bigint INTO v_sid
            FROM _gsiceberg.flush_state
            WHERE table_name = p_table_name AND namespace = v_ns;

        IF v_sid IS NOT NULL AND v_sid > 0 THEN
            IF EXISTS (
                SELECT 1 FROM _gsiceberg.snapshots
                WHERE table_name = p_table_name AND snapshot_id = v_sid
                  AND namespace = v_ns
            ) THEN
                -- Already committed — just DROP
                EXECUTE format('DROP TABLE _gsiceberg._%s_delta_flushing', v_stem);
                v_has_flushing := false;
                RAISE NOTICE 'gsiceberg: recovered orphan _delta_flushing for %, snapshot % already committed', p_table_name, v_sid;
            ELSE
                RAISE NOTICE 'gsiceberg: recovered _delta_flushing for %, snapshot % not found, re-registering job', p_table_name, v_sid;
            END IF;
        ELSE
            RAISE NOTICE 'gsiceberg: recovered _delta_flushing for % without snapshot_id, re-registering job', p_table_name;
        END IF;

        -- Check for pending jobs if no flush_state row
        IF v_has_flushing AND v_sid IS NULL THEN
            SELECT EXISTS (
                SELECT 1 FROM _gsiceberg.flush_jobs
                WHERE table_name = p_table_name AND namespace = v_ns
                  AND status = 'pending'
            ) INTO v_has_job;

            IF v_has_job THEN
                RAISE NOTICE 'gsiceberg: flush already in progress for % (pending job, _delta_flushing preserved)', p_table_name;
                RETURN 0;
            ELSE
                PERFORM _gsiceberg.iceberg_build_object(p_table_name, v_ns);
                EXECUTE format('DROP TABLE IF EXISTS _gsiceberg._%s_delta_flushing', v_stem);
                v_has_flushing := false;
                RAISE NOTICE 'gsiceberg: dropped stale _delta_flushing for %', p_table_name;
            END IF;
        END IF;
    END IF;

    -- ── Phase 1: Freeze delta ───────────────────────────────────────
    IF NOT v_has_flushing THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = '_gsiceberg'
              AND c.relname = '_' || v_stem || '_delta'
        ) THEN
            RAISE NOTICE 'gsiceberg: table % has no _delta', p_table_name;
            RETURN 0;
        END IF;

        EXECUTE format('SELECT count(*) FROM _gsiceberg._%s_delta', v_stem)
            INTO v_n_delta;

        IF v_n_delta = 0 THEN
            PERFORM _gsiceberg.flush_state_done(p_table_name);
            RAISE NOTICE 'gsiceberg: delta table for % is empty', p_table_name;
            RETURN 0;
        END IF;

        -- Rename _delta → _delta_flushing (atomic within PG transaction)
        EXECUTE format(
            'ALTER TABLE _gsiceberg._%s_delta RENAME TO _%s_delta_flushing',
            v_stem, v_stem);

        -- Freeze _foreign_delta if it exists (#462)
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = '_gsiceberg'
              AND c.relname = '_' || v_stem || '_foreign_delta'
        ) THEN
            EXECUTE format(
                'ALTER TABLE _gsiceberg._%s_foreign_delta RENAME TO _%s_foreign_delta_flushing',
                v_stem, v_stem);
        END IF;

        -- Create new empty _delta (#1161: internal cols, LIKE _data)
        EXECUTE format(
            'CREATE TABLE _gsiceberg._%s_delta (_op char(1) NOT NULL, _ts timestamptz NOT NULL DEFAULT now(), _row_id bigint NOT NULL, LIKE _gsiceberg._%s_data INCLUDING ALL)',
            v_stem, v_stem);

        -- Re-create empty _foreign_delta for next import cycle (#593)
        EXECUTE format(
            'CREATE TABLE _gsiceberg._%s_foreign_delta ('
            '  file_path text PRIMARY KEY,'
            '  record_count bigint NOT NULL DEFAULT 0,'
            '  file_size_bytes bigint NOT NULL DEFAULT 0,'
            '  first_row_id bigint,'
            '  row_upper    bigint,'
            '  mode         text DEFAULT ''append'')',
            v_stem);

        RAISE NOTICE 'gsiceberg: froze % rows for %, new _delta ready', v_n_delta, p_table_name;
    END IF;

    -- ── Freeze index deltas (all AM types) ─────────────────────────
    PERFORM _gsiceberg.index_freeze_deltas(p_table_name);

    -- ── Register flush job ────────────────────────────────────────
    INSERT INTO _gsiceberg.flush_jobs (table_name, flush_table, namespace)
    VALUES (p_table_name, p_table_name, v_ns)
    RETURNING job_id INTO v_job_id;

    IF v_job_id > 0 THEN
        RAISE NOTICE 'gsiceberg: registered flush job % for %', v_job_id, p_table_name;
    END IF;
    RETURN v_job_id;
END;
$$;
-- 02a-lifecycle.sql — Core Iceberg lifecycle: mount/refresh/unmount + flush + snapshot + import

CREATE FUNCTION gsiceberg_version() RETURNS text
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION iceberg_mount(text, text, text) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT SECURITY DEFINER;

-- Refresh metadata for a previously mounted table. Incremental:
-- only processes new snapshots.
CREATE FUNCTION iceberg_refresh(text, text) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT SECURITY DEFINER;

-- Export PG metadata to standard Iceberg format files
CREATE FUNCTION iceberg_export_metadata(text) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT SECURITY DEFINER;

CREATE FUNCTION iceberg_unmount(text) RETURNS boolean
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT SECURITY DEFINER;

CREATE OR REPLACE FUNCTION iceberg_status(table_name text)
RETURNS TABLE(total_rows bigint, snapshot_count bigint, latest_snap bigint)
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    ns  text;
    tbl text := iceberg_status.table_name;
BEGIN
    SELECT tables.namespace INTO ns FROM _gsiceberg.tables
    WHERE tables.table_name = tbl;
    PERFORM public.gsiceberg_require_table_owner(COALESCE(ns,''), tbl);
    RETURN QUERY
    SELECT COALESCE(SUM(df.record_count), 0)::bigint,
           (SELECT COUNT(*)::bigint FROM _gsiceberg.snapshots s WHERE s.table_name = tbl),
           COALESCE((SELECT MAX(s.snapshot_id) FROM _gsiceberg.snapshots s WHERE s.table_name = tbl), 0)::bigint
    FROM _gsiceberg.data_files df
    WHERE df.table_name = $1;
END;
$$;

CREATE OR REPLACE FUNCTION iceberg_flush(table_name text)
RETURNS bigint LANGUAGE plpgsql STRICT SECURITY DEFINER
SET search_path = pg_catalog AS $$
DECLARE
    v_lock_id int;
    v_job_id  bigint;
BEGIN
    v_lock_id := hashtext(table_name);
    IF NOT pg_try_advisory_xact_lock(v_lock_id) THEN
        RAISE NOTICE 'gsiceberg: flush already in progress for %', table_name;
        RETURN 0;
    END IF;

    PERFORM public.gsiceberg_require_primary();
    PERFORM public.gsiceberg_require_table_owner(table_name);

    SELECT _gsiceberg.flush_stage_freeze(table_name) INTO v_job_id;
    RETURN v_job_id;
END;
$$;

-- Flush progress: one row per table with latest job + state + snapshot count.

CREATE OR REPLACE FUNCTION iceberg_flush_progress(filter_table text DEFAULT NULL)
RETURNS TABLE(
    table_name text,
    job_id bigint,
    job_status text,
    flush_status text,
    started_at timestamptz,
    finished_at timestamptz,
    snapshot_count bigint,
    error_msg text
) LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT t.table_name,
           j.job_id,
           j.status AS job_status,
           COALESCE(s.flush_status, 'idle') AS flush_status,
           j.started_at,
           j.finished_at,
           COALESCE(snp.cnt, 0)::bigint AS snapshot_count,
           j.error_msg
    FROM _gsiceberg.tables t
    LEFT JOIN _gsiceberg.flush_state s ON t.table_name = s.table_name
    LEFT JOIN (
        SELECT job_id, status, started_at, finished_at, error_msg
        FROM _gsiceberg.flush_jobs
        WHERE table_name = t.table_name
        ORDER BY job_id DESC LIMIT 1
    ) j ON true
    LEFT JOIN (
        SELECT count(*) AS cnt
        FROM _gsiceberg.snapshots
        WHERE table_name = t.table_name
    ) snp ON true
    WHERE (t.owner = session_user
            OR pg_has_role(session_user, 'gsiceberg_admin', 'MEMBER'))
      AND ($1 IS NULL OR t.table_name = $1)
    ORDER BY t.table_name;
$$;


-- Import pre-generated Parquet files directly into the catalog.
-- If _delta has pending changes, auto-flushes first.

-- Returns the number of files imported.
CREATE FUNCTION iceberg_import_parquet(text, text) RETURNS integer
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT SECURITY DEFINER;

CREATE VIEW _gsiceberg.mounted_tables AS
  SELECT table_name, table_path, created_at
  FROM _gsiceberg.tables;

DO $$ BEGIN
    GRANT SELECT ON _gsiceberg.mounted_tables TO PUBLIC;
EXCEPTION WHEN undefined_object THEN NULL;
END $$;

-- Build a vector index on an Iceberg foreign table column.

CREATE OR REPLACE FUNCTION iceberg_drop_snapshot(table_name text, snap_id bigint)
RETURNS integer LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    paths text[];
    path  text;
    n     integer := 0;
    ns    text;
    tbl   text := iceberg_drop_snapshot.table_name;
    sid   bigint := snap_id;
BEGIN
    SELECT tables.namespace INTO ns FROM _gsiceberg.tables
    WHERE tables.table_name = tbl;
    PERFORM public.gsiceberg_require_table_owner(COALESCE(ns,''), tbl);
    -- Collect file paths before DELETE (subsequent DELETE clears the rows)
    SELECT array_agg(file_path) INTO paths
    FROM _gsiceberg.data_files df
    WHERE df.table_name = $1 AND df.begin_snapshot_id = $2;

    -- Proxy index cleanup (#1025 Spec 4): cascade drop to physical AM.
    -- 1. For each proxy in this snapshot, decrement blacklist refcount
    --    on the physical index directory (Spec 1).
    -- 2. Mark physical index rows dead (end_snapshot_id = snap_id).
    -- 3. Delete proxy catalog rows.
    DECLARE
        prec RECORD;
        phys_dir text;
    BEGIN
        FOR prec IN
            SELECT ip.index_name, ip.physical_ref,
                   regexp_replace(iph.version_path, '/[^/]+$', '') AS dir
            FROM _gsiceberg.index_proxy ip
            JOIN _gsiceberg.index_physical iph
              ON iph.index_name = ip.physical_ref
             AND iph.begin_snapshot_id <= ip.snapshot_id
             AND (iph.end_snapshot_id IS NULL OR iph.end_snapshot_id > ip.snapshot_id)
            WHERE ip.table_name = $1 AND ip.snapshot_id = $2
        LOOP
            -- Release proxy's hold on the physical index directory.
            IF prec.dir IS NOT NULL AND prec.dir <> '' THEN
                PERFORM public.gsfile_blacklist_decrement(prec.dir);
            END IF;
        END LOOP;

        -- Mark physical rows as dead (Spec 2: end_snapshot_id = alive→dead).
        UPDATE _gsiceberg.index_physical iph
        SET end_snapshot_id = $2
        WHERE iph.table_name = $1
          AND iph.begin_snapshot_id <= $2
          AND iph.end_snapshot_id IS NULL;

        DELETE FROM _gsiceberg.index_proxy ip
        WHERE ip.table_name = $1 AND ip.snapshot_id = $2;
    END;

    -- DELETE catalog first (PG-atomic)
    DELETE FROM _gsiceberg.data_files df
    WHERE df.table_name = $1 AND df.begin_snapshot_id = $2;
    DELETE FROM _gsiceberg.snapshots s
    WHERE s.table_name = $1 AND s.snapshot_id = $2;

    -- Then unlink files (best-effort)
    IF paths IS NOT NULL THEN
        FOREACH path IN ARRAY paths LOOP
            PERFORM public.gsfile_unregister_internal_c(path);
            n := n + 1;
        END LOOP;
    END IF;
    RAISE NOTICE 'gsiceberg: dropped snapshot % for % (% files)', snap_id, table_name, n;
    RETURN n;
END;
$$;

CREATE OR REPLACE FUNCTION iceberg_flush_worker(table_name text)
RETURNS int LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    job_id_val   bigint;
    table_val    text;
    ok           bool;
BEGIN
    PERFORM public.gsiceberg_require_admin();
    -- Reset stale in_progress jobs
    UPDATE _gsiceberg.flush_jobs
        SET status = 'pending',
            retry_count = COALESCE(retry_count, 0) + 1
        WHERE status = 'in_progress'
          AND started_at < now() - interval '5 minutes';

    -- Loop: drain all pending jobs in one invocation so concurrent
    -- workers with SKIP LOCKED + LIMIT 1 don't leave unclaimed jobs
    -- when each exits after processing exactly one.  #924
    LOOP
    -- Pick next pending or failed job (FOR UPDATE)
    SELECT job_id, table_name INTO job_id_val, table_val
        FROM _gsiceberg.flush_jobs
        WHERE status IN ('pending', 'failed')
          AND COALESCE(retry_count, 0) < current_setting('gsiceberg.flush_retry_count')::int
        ORDER BY job_id LIMIT 1
        FOR UPDATE;

    IF NOT FOUND THEN
        EXIT;  -- queue drained
    END IF;

    RAISE NOTICE 'gsiceberg: flush_worker picked job % for table %', job_id_val, table_val;

    -- Mark in_progress
    UPDATE _gsiceberg.flush_jobs
        SET status = 'in_progress'
        WHERE job_id = job_id_val;

    -- Stage foreign: Foreign delta commit (settles first -- §B)
    BEGIN
        SELECT public.iceberg_flush_stage_foreign(table_val, job_id_val) INTO ok;
        IF NOT ok THEN RAISE EXCEPTION 'stage_foreign_failed'; END IF;
    EXCEPTION WHEN OTHERS THEN
        UPDATE _gsiceberg.flush_jobs
            SET status = 'failed', retry_count = COALESCE(retry_count, 0) + 1,
                error_msg = SQLERRM, finished_at = now()
            WHERE job_id = job_id_val;
        RETURN 0;
    END;

    -- Stage flush: Delta data commit + CoW
    BEGIN
        SELECT public.iceberg_flush_stage_flush(table_val, job_id_val) INTO ok;
        IF NOT ok THEN RAISE EXCEPTION 'stage_flush_failed'; END IF;
    EXCEPTION WHEN OTHERS THEN
        UPDATE _gsiceberg.flush_jobs
            SET status = 'failed', retry_count = COALESCE(retry_count, 0) + 1,
                error_msg = SQLERRM, finished_at = now()
            WHERE job_id = job_id_val;
        RETURN 0;
    END;

    -- Stage train: Index training + proxy (micro-stage loop, #806)
    <<stage_c_loop>>
    BEGIN
        LOOP
            BEGIN
                SELECT public.iceberg_flush_stage_train(table_val, job_id_val) INTO ok;
                IF NOT ok THEN RAISE EXCEPTION 'stage_c_micro_failed'; END IF;
                -- Check if all micro-stages complete (stage advanced to 'cleanup')
                IF EXISTS (SELECT 1 FROM _gsiceberg.flush_state
                           WHERE table_name = table_val AND stage = 'cleanup') THEN
                    EXIT stage_c_loop;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                UPDATE _gsiceberg.flush_jobs
                    SET status = 'failed', retry_count = COALESCE(retry_count, 0) + 1,
                        error_msg = SQLERRM, finished_at = now()
                    WHERE job_id = job_id_val;
                RETURN 0;
            END;
        END LOOP;
    END;

    -- Stage cleanup: Cleanup
    BEGIN
        SELECT public.iceberg_flush_stage_cleanup(table_val, job_id_val) INTO ok;
        IF NOT ok THEN RAISE EXCEPTION 'stage_cleanup_failed'; END IF;
    EXCEPTION WHEN OTHERS THEN
        UPDATE _gsiceberg.flush_jobs
            SET status = 'failed', retry_count = COALESCE(retry_count, 0) + 1,
                error_msg = SQLERRM, finished_at = now()
            WHERE job_id = job_id_val;
        RETURN 0;
    END;

    RAISE NOTICE 'gsiceberg: flush_worker completed job % for table %', job_id_val, table_val;
    END LOOP;

    RETURN 1;
END;
$$;


CREATE FUNCTION iceberg_flush_stage_freeze(text) RETURNS bigint
    AS 'MODULE_PATHNAME', 'iceberg_flush_stage_freeze_wrapper' LANGUAGE C STRICT;

-- Multi-stage flush functions (#804)
CREATE FUNCTION iceberg_flush_stage_flush(text, bigint) RETURNS boolean
    AS 'MODULE_PATHNAME', 'iceberg_flush_stage_flush_wrapper' LANGUAGE C STRICT;

CREATE FUNCTION iceberg_flush_stage_train(text, bigint) RETURNS boolean
    AS 'MODULE_PATHNAME', 'iceberg_flush_stage_train_wrapper' LANGUAGE C STRICT;





-- flush_stage_flush PL/pgSQL body (#1292 follow-up)
-- Replaces fdw/flush/flush_stage_flush.c body (639→50 line C wrapper).
-- C wrapper calls this via SPI SELECT, then dispatches post-flush index hooks.
CREATE FUNCTION _gsiceberg.flush_stage_flush_sql(
    p_table_name text, p_job_id bigint)
RETURNS boolean LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_ns          text;
    v_stem        text;
    v_snap_id     bigint;
    v_parent_snap bigint;
    v_table_path  text;
    v_seq         bigint;
    v_n_delta     int;
    v_rows        bigint;
    v_bytes       bigint;
    v_frid        bigint;
    v_rup         bigint;
    v_base_rid    bigint;
    v_n_neg       int;
    v_dir         text;
    v_tmp_path    text;
    v_final_path  text;
    v_token       text;
    v_schema_json text;
    v_part_json   text;
    v_w_snap_id   bigint;
    v_col         record;
BEGIN
    SELECT namespace INTO v_ns FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    v_stem := v_ns || '_' || p_table_name;

    PERFORM _gsiceberg.flush_state_enter_stage(p_table_name, 'flush', 0);

    -- Read snapshot info
    SELECT COALESCE(MAX(snapshot_id), 0) INTO v_parent_snap
        FROM _gsiceberg.snapshots
        WHERE table_name = p_table_name AND namespace = v_ns;

    SELECT _gsiceberg.next_snapshot_id(p_table_name) INTO v_snap_id;
    IF v_snap_id = 0 THEN
        RAISE EXCEPTION 'gsiceberg: cannot compute next snapshot_id for %', p_table_name;
    END IF;

    SELECT table_path INTO v_table_path FROM _gsiceberg.tables
        WHERE table_name = p_table_name;

    SELECT COALESCE(MAX(sequence_number), 0) + 1 INTO v_seq
        FROM _gsiceberg.snapshots WHERE table_name = p_table_name;

    -- Check _delta_flushing row count
    EXECUTE format('SELECT count(*) FROM _gsiceberg._%s_delta_flushing', v_stem)
        INTO v_n_delta;

    IF v_n_delta = 0 THEN
        PERFORM _gsiceberg.flush_state_complete_stage(p_table_name, 'train');
        RETURN true;
    END IF;

    -- Step 1: SQL-based CoW rewrite
    PERFORM _gsiceberg.stage_flush_sql(p_table_name, p_job_id, v_snap_id, v_seq);

    -- Step 2: Materialize _gs_flush_output → _gs_flush_out
    EXECUTE 'CREATE TEMP TABLE _gs_flush_out ON COMMIT DROP AS SELECT * FROM _gs_flush_output';

    -- Step 3: Batch-allocate positive _row_ids
    SELECT count(*) INTO v_n_neg FROM _gs_flush_out WHERE _row_id < 0;
    IF v_n_neg > 0 THEN
        UPDATE _gsiceberg.tables
            SET next_row_id = next_row_id + v_n_neg
            WHERE table_name = p_table_name
            RETURNING next_row_id - v_n_neg INTO v_base_rid;

        EXECUTE format(
            'UPDATE _gs_flush_out SET _row_id = %s + sub.rn '
            'FROM (SELECT ctid, row_number() OVER (ORDER BY _row_id) - 1 AS rn '
            'FROM _gs_flush_out WHERE _row_id < 0) sub '
            'WHERE _gs_flush_out.ctid = sub.ctid',
            v_base_rid);
    END IF;

    SELECT count(*) INTO v_rows FROM _gs_flush_out;

    -- Step 4: Parquet write using SQL bridge
    v_dir := v_table_path || '/data';
    PERFORM flush_mkdir_p(v_dir);

    IF v_rows > 0 THEN
        v_token := floor(extract(epoch from clock_timestamp()))::text
                   || '-' || lpad((random() * 999999)::int::text, 6, '0');
        v_tmp_path  := v_dir || '/.flush-' || v_token || '.parquet.tmp';
        v_final_path := v_dir || '/flush-' || v_token || '.parquet';

        SELECT o_rows, o_bytes INTO v_rows, v_bytes
            FROM _gsiceberg.write_parquet_table_flat('_gs_flush_out', v_tmp_path);

        PERFORM gsfile_register_internal(v_final_path, p_table_name);
        PERFORM pg_file_rename(v_tmp_path, v_final_path, NULL);
    ELSE
        v_final_path := '';
    END IF;

    -- Step 5: Independent column files (#777)
    FOR v_col IN
        SELECT field->>'name' AS col
        FROM _gsiceberg.tables t,
             json_array_elements(t."current_schema"->'fields') field
        WHERE t.table_name = p_table_name
          AND (field->>'storage') IS NOT NULL
          AND field->'storage'->>'mode' = 'separate'
    LOOP
        DECLARE
            v_col_tmp   text;
            v_col_final text;
            v_cr bigint; v_cb bigint;
        BEGIN
            PERFORM flush_mkdir_p(v_table_path || '/columns');
            PERFORM flush_mkdir_p(v_table_path || '/columns/' || v_col.col);

            v_col_final := v_table_path || '/columns/' || v_col.col || '/'
                || v_col.col || '-snap-' || lpad(v_snap_id::text, 4, '0') || '.parquet';
            v_col_tmp := v_col_final || '.tmp';

            SELECT o_rows, o_bytes INTO v_cr, v_cb
                FROM _gsiceberg.write_parquet_column_flat(
                    '_gs_flush_out', v_col.col, v_col_tmp);

            PERFORM pg_file_rename(v_col_tmp, v_col_final, NULL);
            PERFORM gsfile_register_internal(v_col_final, p_table_name);

            IF NOT EXISTS (SELECT 1 FROM _gsiceberg.column_files
                           WHERE table_name = p_table_name
                             AND snapshot_id = v_snap_id
                             AND col_name = v_col.col) THEN
                INSERT INTO _gsiceberg.column_files
                    (table_name, snapshot_id, col_name, file_path,
                     record_count, file_size_bytes)
                VALUES (p_table_name, v_snap_id, v_col.col, v_col_final,
                        v_cr, v_cb);
            END IF;
        END;
    END LOOP;

    -- Step 6: Writer commit (stats auto-read by C layer)
    SELECT MIN(_row_id), MAX(_row_id) INTO v_frid, v_rup
        FROM _gs_flush_out WHERE _row_id IS NOT NULL;
    IF v_frid IS NULL THEN
        SELECT next_row_id INTO v_frid FROM _gsiceberg.tables
            WHERE table_name = p_table_name;
        v_rup := v_frid + v_rows - 1;
    END IF;

    SELECT "current_schema"::text INTO v_schema_json FROM _gsiceberg.tables
        WHERE table_name = p_table_name;
    SELECT "partition_spec"::text INTO v_part_json FROM _gsiceberg.tables
        WHERE table_name = p_table_name;

    SELECT _gsiceberg.writer_start_transaction(p_table_name, v_ns, v_parent_snap)
        INTO v_w_snap_id;

    PERFORM _gsiceberg.writer_add_file(
        v_w_snap_id, v_final_path, 'PARQUET',
        v_rows, COALESCE(v_bytes, 0), v_frid, v_rup);

    PERFORM _gsiceberg.writer_commit(
        v_w_snap_id, v_table_path, 'flush', NULL,
        COALESCE(v_schema_json, '{}'),
        COALESCE(v_part_json, '{"fields":[]}'));

    PERFORM _gsiceberg.flush_state_complete_stage(p_table_name, 'train');
    RETURN true;
END;
$$;

-- SQL-based CoW flush: replaces the C CoW rewrite + resolve_pairs
-- pipeline with SQL VIEWs + FOREIGN TABLE + anti-join (#1217 spec).
CREATE OR REPLACE FUNCTION _gsiceberg.stage_flush_sql(
    p_table_name text,
    p_job_id bigint,
    p_new_snap_id bigint,
    p_sequence_number bigint
) RETURNS boolean LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_stem        text;
    v_delta_flush text;
    v_data_rel    text;
    v_n_cow       int;
    v_survivor_sql text;
    v_data_cols   text;
    v_cow_file    record;
BEGIN
    v_stem := (SELECT namespace || '_' || table_name
               FROM _gsiceberg.tables
               WHERE table_name = p_table_name);
    v_delta_flush := quote_ident('_' || v_stem || '_delta_flushing');
    v_data_rel := format('_gsiceberg._%s_data', v_stem);

    -- Build column list matching _data (excludes _op, _ts from delta_flushing)
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
    INTO v_data_cols
    FROM pg_attribute
    WHERE attrelid = v_data_rel::regclass
      AND attnum > 0 AND NOT attisdropped;

    -- Step 1: Identify affected files (those containing D-row _row_ids)
    EXECUTE format(
        'CREATE TEMP VIEW _gs_cow_files AS '
        'SELECT DISTINCT df.file_path, df.first_row_id, df.record_count '
        'FROM _gsiceberg.data_files df '
        'WHERE df.table_name = %L '
        '  AND df.end_snapshot_id IS NULL '
        '  AND EXISTS ( '
        '    SELECT 1 FROM _gsiceberg.%s d '
        '    WHERE d._op = ''D'' AND d._row_id >= 0 '
        '      AND d._row_id >= df.first_row_id '
        '      AND d._row_id < df.first_row_id + df.record_count '
        '  )',
        p_table_name, v_delta_flush);

    SELECT count(*) INTO v_n_cow FROM _gs_cow_files;

    -- Step 2+3: Survivors via _data table with _row_id range filter + anti-join.
    -- Uses the existing _data foreign table (no need for per-file foreign tables).
    -- The _row_id range confines the scan to the affected file's row range.
    v_survivor_sql := '';
    FOR v_cow_file IN SELECT * FROM _gs_cow_files LOOP
        IF v_survivor_sql != '' THEN
            v_survivor_sql := v_survivor_sql || ' UNION ALL ';
        END IF;
        v_survivor_sql := v_survivor_sql || format(
            'SELECT * FROM %s WHERE _row_id >= %s AND _row_id < %s '
            'AND _row_id NOT IN ('
            'SELECT _row_id FROM _gsiceberg.%s '
            'WHERE _op = ''D'' AND _row_id >= 0'
            ')',
            v_data_rel,
            v_cow_file.first_row_id,
            v_cow_file.first_row_id + v_cow_file.record_count,
            v_delta_flush);
    END LOOP;

    -- Step 4: Output view (survivors + delta, with I/D negative-pair cancel)
    EXECUTE format(
        'CREATE TEMP VIEW _gs_flush_output AS '
        '%s'
        'SELECT %s FROM _gsiceberg.%s '
        'WHERE _op != ''D'' '
        '  AND _row_id NOT IN ( '
        '    SELECT _row_id FROM _gsiceberg.%s '
        '    WHERE _op = ''D'' AND _row_id < 0 '
        '  )',
        CASE WHEN v_survivor_sql != '' THEN v_survivor_sql || ' UNION ALL ' ELSE '' END,
        v_data_cols, v_delta_flush, v_delta_flush);

    -- Step 5: Retire old files (dual-write end_snapshot)
    EXECUTE format(
        'UPDATE _gsiceberg.data_files '
        'SET end_snapshot_id = %L '
        'WHERE table_name = %L '
        '  AND file_path IN (SELECT file_path FROM _gs_cow_files) '
        '  AND end_snapshot_id IS NULL',
        p_new_snap_id, p_table_name);

    EXECUTE format(
        'UPDATE _gsiceberg._row_range_facts '
        'SET end_snapshot = %L '
        'WHERE table_name = %L '
        '  AND file_path IN (SELECT file_path FROM _gs_cow_files) '
        '  AND end_snapshot IS NULL',
        p_new_snap_id, p_table_name);

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION iceberg_flush_sync(table_name text)
RETURNS bool LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    job_id_val bigint;
    ok         bool;
    ns         text;
    tbl        text := iceberg_flush_sync.table_name;
BEGIN
    SELECT tables.namespace INTO ns FROM _gsiceberg.tables
    WHERE tables.table_name = tbl;
    PERFORM public.gsiceberg_require_table_owner(COALESCE(ns,''), tbl);
    SELECT public.iceberg_flush_stage_freeze(table_name) INTO job_id_val;
    IF job_id_val = 0 THEN
        RETURN false;
    END IF;
    -- Stage foreign: foreign delta commit (settles first -- §B)
    SELECT public.iceberg_flush_stage_foreign(table_name, job_id_val) INTO ok;
    IF NOT ok THEN RETURN false; END IF;
    -- Stage flush: delta data commit + CoW
    SELECT public.iceberg_flush_stage_flush(table_name, job_id_val) INTO ok;
    IF NOT ok THEN RETURN false; END IF;
    -- Stage train: index training + proxy (micro-stage loop, #806)
    LOOP
        SELECT public.iceberg_flush_stage_train(table_name, job_id_val) INTO ok;
        IF NOT ok THEN RETURN false; END IF;
        -- Check if all micro-stages complete
        IF EXISTS (SELECT 1 FROM _gsiceberg.flush_state fs
                   WHERE fs.table_name = iceberg_flush_sync.table_name AND fs.stage = 'cleanup') THEN
            EXIT;
        END IF;
    END LOOP;
    -- Stage cleanup: cleanup
    SELECT public.iceberg_flush_stage_cleanup(table_name, job_id_val) INTO ok;
    RETURN ok;
END;
$$;
-- 02b-ddl.sql — DDL mutations + schema grants

DO $$ BEGIN
    ALTER TABLE _gsiceberg.snapshots ADD COLUMN schema_id int;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- ============================================================
-- gsfilesystem tables — file ownership registry
-- (_gsfs schema created at top of file alongside _gsiceberg)
-- ============================================================

-- Schema permissions: _gsiceberg and _gsfs are internal.
-- The v1 security model (#938) is single-tenant: reads via the public VIEW
-- are gated by GRANT on the view; writes are protected by SECURITY DEFINER
-- trigger functions.  Non-admin users must be able to SELECT from catalog
-- tables because the FDW scan path (DELETE/UPDATE WHERE, SELECT) resolves
-- table metadata as the INVOKER (#992).
REVOKE ALL ON SCHEMA _gsiceberg FROM PUBLIC;
REVOKE ALL ON SCHEMA _gsfs FROM PUBLIC;
-- gsiceberg_admin role may not exist on fresh install (#214).
-- Extension owner (superuser) has implicit access to all schemas.
DO $$ BEGIN
  GRANT USAGE ON SCHEMA _gsiceberg TO gsiceberg_admin;
  GRANT USAGE ON SCHEMA _gsfs TO gsiceberg_admin;
  -- #992: non-admin DML needs read access to catalog tables for the FDW
  -- scan path (resolves table namespace, data files, etc. as the invoker).
  -- Writes remain protected by SECURITY DEFINER triggers.
  -- GRANTs on existing tables + DEFAULT privileges for future tables.
  GRANT USAGE ON SCHEMA _gsiceberg TO PUBLIC;
  GRANT SELECT ON ALL TABLES IN SCHEMA _gsiceberg TO PUBLIC;
  ALTER DEFAULT PRIVILEGES IN SCHEMA _gsiceberg GRANT SELECT ON TABLES TO PUBLIC;
EXCEPTION WHEN undefined_object THEN NULL;
END $$;


-- Write-time _row_id allocator: atomically advances tables.next_row_id and
-- returns the allocated value. Shared by row-level INSERT and file import.
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_alloc_row_id(p_table text)
RETURNS bigint LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    UPDATE _gsiceberg.tables SET next_row_id = next_row_id + 1
    WHERE table_name = p_table
    RETURNING next_row_id - 1;
$$;

-- Single builder: atomically recreates the public view (passing real _row_id)
-- and all three INSTEAD OF triggers (INSERT/DELETE/UPDATE) for a mounted table.
-- Reads column list from current_schema text (not _gsiceberg.columns) to stay
-- correct after drop/rename column DDL (spec SS4.6).
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_build_object(p_table text, p_namespace text DEFAULT 'default')
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    cols       text := '';
    new_refs   text := '';
    old_refs   text := '';
    sep        text := '';
    obj_stem   text := p_namespace || '_' || p_table;
    data_rel   text := '_' || obj_stem || '_data';
    delta_rel  text := '_' || obj_stem || '_delta';
    pg_ns      text;
    disp_name  text;
    saved_acl  aclitem[];
    acl_rec    record;
BEGIN
    -- Map 'default' namespace to 'public' PG schema for backward compat (#906).
    -- Non-default namespaces are used directly as the PG schema name.
    IF p_namespace = 'default' THEN
        pg_ns := 'public';
    ELSE
        pg_ns := p_namespace;
    END IF;
    -- #1007: internal objects are namespace-prefixed (_ns_table_data).
    -- The VIEW display name is just the table_name (strip prefix if present).
    IF p_table LIKE pg_ns || '\_%' THEN
        disp_name := substr(p_table, length(pg_ns) + 2);
    ELSE
        disp_name := p_table;
    END IF;

    -- Build column lists from text schema, ordered by field id.
    SELECT string_agg(quote_ident(field->>'name'), ', ' ORDER BY (field->>'id')::int),
           string_agg('NEW.' || quote_ident(field->>'name'), ', ' ORDER BY (field->>'id')::int),
           string_agg('OLD.' || quote_ident(field->>'name'), ', ' ORDER BY (field->>'id')::int)
      INTO cols, new_refs, old_refs
      FROM _gsiceberg.tables t,
           json_array_elements(t."current_schema"->'fields') field
     WHERE t.table_name = p_table;

    IF cols IS NOT NULL AND cols != '' THEN
        sep := ', ';
    ELSE
        cols     := '';
        new_refs := '';
        old_refs := '';
    END IF;

    -- #992: DROP VIEW wipes user grants, so every flush/DDL rebuild
    -- silently revoked non-admin DML access.  Capture the ACL before
    -- the drop and re-apply it after CREATE VIEW.
    SELECT c.relacl INTO saved_acl
      FROM pg_class c
      JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = pg_ns AND c.relname = disp_name;

    -- #1007: schema-qualified VIEW via namespace column
    EXECUTE format(
        'DROP VIEW IF EXISTS %I.%I;'
        'CREATE VIEW %I.%I AS'
        '  SELECT %s%s_row_id FROM _gsiceberg.%I'
        '  UNION ALL'
        '  SELECT %s%s_row_id FROM _gsiceberg.%I WHERE _op <> ''D''',
        pg_ns, disp_name, pg_ns, disp_name,
        cols, sep, data_rel, cols, sep, delta_rel);

    -- #992: restore pre-rebuild grants (grantee 0 = PUBLIC)
    IF saved_acl IS NOT NULL THEN
        FOR acl_rec IN SELECT * FROM aclexplode(saved_acl) LOOP
            IF acl_rec.privilege_type IN ('SELECT','INSERT','UPDATE','DELETE') THEN
                EXECUTE format('GRANT %s ON %I.%I TO %s',
                    acl_rec.privilege_type, pg_ns, disp_name,
                    CASE WHEN acl_rec.grantee = 0 THEN 'PUBLIC'
                         ELSE quote_ident(pg_get_userbyid(acl_rec.grantee)) END);
            END IF;
        END LOOP;
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION _gsiceberg.%I() RETURNS trigger '
        'LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $b$'
        'DECLARE idx_rec RECORD; col_val bigint;'
        'BEGIN'
        '  INSERT INTO _gsiceberg.%I (_op, _row_id%s)'
        '  VALUES (''I'', -nextval(''_gsiceberg._rowid_temp_seq'')%s);'
        '  FOR idx_rec IN '
        '    SELECT ip.index_name, ip.column_name, t.table_path '
        '    FROM _gsiceberg.index_physical ip '
        '    JOIN _gsiceberg.tables t ON t.table_name = ip.table_name '
        '    WHERE ip.table_name = ''%s'' '
        '      AND ip.index_type = ''scalar'' '
        '      AND ip.end_snapshot_id IS NULL AND ip.level = 0 '
        '  LOOP '
        '    EXECUTE format(''SELECT ($1).%%I::bigint'', idx_rec.column_name) '
        '      USING NEW INTO col_val; '
        '    IF col_val IS NOT NULL THEN '
        '      PERFORM scalar_index_append_delta( '
        '        idx_rec.table_path, idx_rec.index_name, '
        '        col_val, -currval(''_gsiceberg._rowid_temp_seq''), ''I''); '
        '    END IF; '
        '  END LOOP; '
        '  RETURN NEW;'
        'END $b$',
        p_table || '_ins_fn',
        delta_rel, CASE WHEN sep = '' THEN '' ELSE ', ' || cols END,
        CASE WHEN sep = '' THEN '' ELSE ', ' || new_refs END,
        p_table);

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION _gsiceberg.%I() RETURNS trigger '
        'LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $b$'
        'DECLARE idx_rec RECORD; col_val bigint;'
        'BEGIN'
        '  IF OLD._row_id IS NULL THEN'
        '    RAISE EXCEPTION ''gsiceberg: cannot DELETE row with NULL _row_id from %%'', TG_TABLE_NAME'
        '      USING HINT = ''_row_id synthesis unavailable — check _gsiceberg._row_range_facts for this table'';'
        '  END IF;'
        '  IF OLD._row_id < 0 THEN'
        '    DELETE FROM _gsiceberg.%I WHERE _row_id = OLD._row_id AND _op = ''I'';'
        '    IF NOT FOUND THEN'
        '      RAISE EXCEPTION ''gsiceberg: pending row (_row_id %%) taken by a concurrent flush on %%'', OLD._row_id, TG_TABLE_NAME'
        '        USING HINT = ''the row is being flushed and will get a permanent _row_id; retry the DELETE after the flush completes'';'
        '    END IF;'
        '  ELSE'
        '    INSERT INTO _gsiceberg.%I (_op, _row_id) VALUES (''D'', OLD._row_id);'
        '    FOR idx_rec IN '
        '      SELECT ip.index_name, ip.column_name, t.table_path '
        '      FROM _gsiceberg.index_physical ip '
        '      JOIN _gsiceberg.tables t ON t.table_name = ip.table_name '
        '      WHERE ip.table_name = ''%s'' '
        '        AND ip.index_type = ''scalar'' '
        '        AND ip.end_snapshot_id IS NULL AND ip.level = 0 '
        '    LOOP '
        '      EXECUTE format(''SELECT ($1).%%I::bigint'', idx_rec.column_name) '
        '        USING OLD INTO col_val; '
        '      IF col_val IS NOT NULL THEN '
        '        PERFORM scalar_index_append_delta( '
        '          idx_rec.table_path, idx_rec.index_name, '
        '          col_val, OLD._row_id, ''D''); '
        '      END IF; '
        '    END LOOP; '
        '  END IF;'
        '  RETURN OLD;'
        'END $b$',
        p_table || '_del_fn', delta_rel, delta_rel, p_table);

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION _gsiceberg.%I() RETURNS trigger '
        'LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $b$'
        'DECLARE idx_rec RECORD; col_val bigint;'
        'BEGIN'
        '  IF OLD._row_id IS NULL THEN'
        '    RAISE EXCEPTION ''gsiceberg: cannot UPDATE row with NULL _row_id from %%'', TG_TABLE_NAME'
        '      USING HINT = ''_row_id synthesis unavailable — check _gsiceberg._row_range_facts for this table'';'
        '  END IF;'
        '  IF OLD._row_id < 0 THEN'
        '    DELETE FROM _gsiceberg.%I WHERE _row_id = OLD._row_id AND _op = ''I'';'
        '    IF NOT FOUND THEN'
        '      RAISE EXCEPTION ''gsiceberg: pending row (_row_id %%) taken by a concurrent flush on %%'', OLD._row_id, TG_TABLE_NAME'
        '        USING HINT = ''the row is being flushed and will get a permanent _row_id; retry the UPDATE after the flush completes'';'
        '    END IF;'
        '  ELSE'
        '    INSERT INTO _gsiceberg.%I (_op, _row_id) VALUES (''D'', OLD._row_id);'
        '    FOR idx_rec IN '
        '      SELECT ip.index_name, ip.column_name, t.table_path '
        '      FROM _gsiceberg.index_physical ip '
        '      JOIN _gsiceberg.tables t ON t.table_name = ip.table_name '
        '      WHERE ip.table_name = ''%s'' '
        '        AND ip.index_type = ''scalar'' '
        '        AND ip.end_snapshot_id IS NULL AND ip.level = 0 '
        '    LOOP '
        '      EXECUTE format(''SELECT ($1).%%I::bigint'', idx_rec.column_name) '
        '        USING OLD INTO col_val; '
        '      IF col_val IS NOT NULL THEN '
        '        PERFORM scalar_index_append_delta( '
        '          idx_rec.table_path, idx_rec.index_name, '
        '          col_val, OLD._row_id, ''D''); '
        '      END IF; '
        '    END LOOP; '
        '  END IF;'
        '  INSERT INTO _gsiceberg.%I (_op, _row_id%s)'
        '  VALUES (''I'', -nextval(''_gsiceberg._rowid_temp_seq'')%s);'
        '  FOR idx_rec IN '
        '    SELECT ip.index_name, ip.column_name, t.table_path '
        '    FROM _gsiceberg.index_physical ip '
        '    JOIN _gsiceberg.tables t ON t.table_name = ip.table_name '
        '    WHERE ip.table_name = ''%s'' '
        '      AND ip.index_type = ''scalar'' '
        '      AND ip.end_snapshot_id IS NULL AND ip.level = 0 '
        '  LOOP '
        '    EXECUTE format(''SELECT ($1).%%I::bigint'', idx_rec.column_name) '
        '      USING NEW INTO col_val; '
        '    IF col_val IS NOT NULL THEN '
        '      PERFORM scalar_index_append_delta( '
        '        idx_rec.table_path, idx_rec.index_name, '
        '        col_val, -currval(''_gsiceberg._rowid_temp_seq''), ''I''); '
        '    END IF; '
        '  END LOOP; '
        '  RETURN NEW;'
        'END $b$',
        p_table || '_upd_fn',
        delta_rel, delta_rel, p_table,
        delta_rel, CASE WHEN sep = '' THEN '' ELSE ', ' || cols END,
        CASE WHEN sep = '' THEN '' ELSE ', ' || new_refs END,
        p_table);

    -- #1007: schema-qualified triggers
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I;'
                   'CREATE TRIGGER %I INSTEAD OF INSERT ON %I.%I'
                   '  FOR EACH ROW EXECUTE PROCEDURE _gsiceberg.%I();',
                   p_table || '_ins_trg', pg_ns, disp_name,
                   p_table || '_ins_trg', pg_ns, disp_name,
                   p_table || '_ins_fn');
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I;'
                   'CREATE TRIGGER %I INSTEAD OF DELETE ON %I.%I'
                   '  FOR EACH ROW EXECUTE PROCEDURE _gsiceberg.%I();',
                   p_table || '_del_trg', pg_ns, disp_name,
                   p_table || '_del_trg', pg_ns, disp_name,
                   p_table || '_del_fn');
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I;'
                   'CREATE TRIGGER %I INSTEAD OF UPDATE ON %I.%I'
                   '  FOR EACH ROW EXECUTE PROCEDURE _gsiceberg.%I();',
                   p_table || '_upd_trg', pg_ns, disp_name,
                   p_table || '_upd_trg', pg_ns, disp_name,
                   p_table || '_upd_fn');
END;
$$;

-- djb2 hash matching flush C code: identical to gsiceberg_name_hash()
-- in fdw/hooks/fdw_hooks.c.  Used by ADD/DROP COLUMN for flush mutex.
CREATE OR REPLACE FUNCTION _gsiceberg._djb2_hash(text)
RETURNS int LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    h bigint := 0; i int;
BEGIN
    FOR i IN 1..length($1) LOOP
        h := (h * 31 + ascii(substr($1, i, 1))) % 4294967296;
    END LOOP;
    IF h >= 2147483648 THEN RETURN (h - 4294967296)::int;
    ELSE RETURN h::int; END IF;
END;
$$;

-- Add a column to a mounted Iceberg table.
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_add_column(
    table_name text, col_name text, col_type text)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
DECLARE
    next_id int;
    ns      text;
BEGIN
    -- Validate column name (prevent SQL injection via identifiers)
    IF col_name ~ '[^a-zA-Z0-9_]' OR col_name ~ '^[0-9]' THEN
        RAISE NOTICE 'gsiceberg: invalid column name "%"', col_name;
        RETURN false;
    END IF;

    -- 1. Get next field id
    SELECT COALESCE(MAX((field->>'id')::int), 0) + 1 INTO next_id
    FROM _gsiceberg.tables t,
         json_array_elements(t."current_schema"->'fields') field
    WHERE t.table_name = iceberg_add_column.table_name;

    IF next_id IS NULL THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    -- 2. Look up namespace for prefixed internal object names (#906).
    SELECT tables.namespace INTO ns
    FROM _gsiceberg.tables
    WHERE tables.table_name = iceberg_add_column.table_name;
    IF NOT FOUND THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    -- 3. Flush mutex (G1 #1118): prevent concurrent flush + schema change.
    --     Advisory lock uses same djb2 hash as flush C code.
    --     Use $1 to avoid PL/pgSQL variable vs column ambiguity.
    IF NOT pg_try_advisory_xact_lock(_gsiceberg._djb2_hash($1)) THEN
        RAISE EXCEPTION 'gsiceberg: table "%" is locked by flush, cannot add column',
                        $1;
    END IF;
    IF EXISTS (SELECT 1 FROM _gsiceberg.flush_state fs
               WHERE fs.table_name = $1
                 AND fs.flush_status = 'in_progress') THEN
        RAISE EXCEPTION 'gsiceberg: flush in progress for "%", cannot add column',
                        $1;
    END IF;
    IF EXISTS (SELECT 1 FROM _gsiceberg.flush_jobs fj
               WHERE fj.table_name = $1
                 AND fj.status IN ('pending', 'in_progress') LIMIT 1) THEN
        RAISE EXCEPTION 'gsiceberg: flush job pending for "%", cannot add column',
                        $1;
    END IF;

    -- 4. ALTER underlying tables
    BEGIN
        EXECUTE format('ALTER TABLE _gsiceberg.%I ADD COLUMN %I %s',
                       '_' || ns || '_' || table_name || '_delta', col_name, col_type);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'gsiceberg: ALTER TABLE _delta failed: %', SQLERRM;
        RETURN false;
    END;

    BEGIN
        EXECUTE format('ALTER FOREIGN TABLE _gsiceberg.%I ADD COLUMN %I %s',
                       '_' || ns || '_' || table_name || '_data', col_name, col_type);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'gsiceberg: ALTER FOREIGN TABLE _data failed: %', SQLERRM;
        RETURN false;
    END;

    -- 5. Update text schema
    UPDATE _gsiceberg.tables
    SET "current_schema" = json_set(
        "current_schema",
        '{fields}',
        COALESCE("current_schema"->'fields', '[]'::text) ||
        json_build_array(json_build_object(
            'id',       next_id,
            'name',     col_name,
            'type',     col_type,
            'required', false
        ))
    )
    WHERE tables.table_name = iceberg_add_column.table_name;

    -- 6. Recreate views with updated column list
    PERFORM _gsiceberg.iceberg_build_object(table_name);

    RAISE NOTICE 'gsiceberg: added column "%" to "%"', col_name, table_name;
    RETURN true;
END;
$$;

-- Drop a column from a mounted Iceberg table.
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_drop_column(
    table_name text, col_name text)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
DECLARE
    field_found bool := false;
    ns          text;
BEGIN
    -- 0. Look up namespace for prefixed internal object names (#906).
    SELECT tables.namespace INTO ns
    FROM _gsiceberg.tables
    WHERE tables.table_name = iceberg_drop_column.table_name;
    IF NOT FOUND THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    -- 1. Flush mutex (G1 #1118): prevent concurrent flush + schema change.
    --     Use $1 to avoid PL/pgSQL variable vs column ambiguity.
    IF NOT pg_try_advisory_xact_lock(_gsiceberg._djb2_hash($1)) THEN
        RAISE EXCEPTION 'gsiceberg: table "%" is locked by flush, cannot drop column',
                        $1;
    END IF;
    IF EXISTS (SELECT 1 FROM _gsiceberg.flush_state fs
               WHERE fs.table_name = $1
                 AND fs.flush_status = 'in_progress') THEN
        RAISE EXCEPTION 'gsiceberg: flush in progress for "%", cannot drop column',
                        $1;
    END IF;
    IF EXISTS (SELECT 1 FROM _gsiceberg.flush_jobs fj
               WHERE fj.table_name = $1
                 AND fj.status IN ('pending', 'in_progress') LIMIT 1) THEN
        RAISE EXCEPTION 'gsiceberg: flush job pending for "%", cannot drop column',
                        $1;
    END IF;

    -- 2. ALTER underlying tables
    BEGIN
        EXECUTE format('ALTER TABLE _gsiceberg.%I DROP COLUMN IF EXISTS %I',
                       '_' || ns || '_' || table_name || '_delta', col_name);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'gsiceberg: ALTER TABLE _delta failed: %', SQLERRM;
        RETURN false;
    END;

    BEGIN
        EXECUTE format('ALTER FOREIGN TABLE _gsiceberg.%I DROP COLUMN IF EXISTS %I',
                       '_' || ns || '_' || table_name || '_data', col_name);
    EXCEPTION WHEN OTHERS THEN
        -- Foreign tables may refuse DROP COLUMN; non-fatal
        RAISE NOTICE 'gsiceberg: ALTER FOREIGN TABLE _data warning: %', SQLERRM;
    END;

    -- 2. Remove from text schema
    WITH filtered AS (
        SELECT json_agg(field ORDER BY (field->>'id')::int) AS new_fields
        FROM _gsiceberg.tables t,
             json_array_elements(t."current_schema"->'fields') field
        WHERE t.table_name = iceberg_drop_column.table_name
          AND field->>'name' != iceberg_drop_column.col_name
    )
    UPDATE _gsiceberg.tables
    SET "current_schema" = json_set(
        "current_schema", '{fields}', COALESCE((SELECT new_fields FROM filtered), '[]'::text))
    WHERE tables.table_name = iceberg_drop_column.table_name
    RETURNING true INTO field_found;

    IF NOT field_found THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    -- 3. Recreate views
    PERFORM _gsiceberg.iceberg_build_object(table_name);

    RAISE NOTICE 'gsiceberg: dropped column "%" from "%"', col_name, table_name;
    RETURN true;
END;
$$;

-- Rename a column in a mounted Iceberg table.
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_rename_column(
    table_name text, old_name text, new_name text)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
DECLARE
    field_found bool := false;
    ns          text;
BEGIN
    -- Validate new column name
    IF new_name ~ '[^a-zA-Z0-9_]' OR new_name ~ '^[0-9]' THEN
        RAISE NOTICE 'gsiceberg: invalid column name "%"', new_name;
        RETURN false;
    END IF;

    -- 0. Look up namespace for prefixed internal object names (#906).
    SELECT tables.namespace INTO ns
    FROM _gsiceberg.tables
    WHERE tables.table_name = iceberg_rename_column.table_name;
    IF NOT FOUND THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    -- 1. ALTER underlying tables
    BEGIN
        EXECUTE format('ALTER TABLE _gsiceberg.%I RENAME COLUMN %I TO %I',
                       '_' || ns || '_' || table_name || '_delta', old_name, new_name);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'gsiceberg: ALTER TABLE _delta failed: %', SQLERRM;
        RETURN false;
    END;

    BEGIN
        EXECUTE format('ALTER FOREIGN TABLE _gsiceberg.%I RENAME COLUMN %I TO %I',
                       '_' || ns || '_' || table_name || '_data', old_name, new_name);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'gsiceberg: ALTER FOREIGN TABLE _data warning: %', SQLERRM;
    END;

    -- 2. Update text schema
    UPDATE _gsiceberg.tables
    SET "current_schema" = json_set(
        "current_schema",
        '{fields}',
        (SELECT json_agg(
            CASE WHEN field->>'name' = iceberg_rename_column.old_name
                 THEN field || json_build_object('name', iceberg_rename_column.new_name)
                 ELSE field
            END
            ORDER BY (field->>'id')::int)
         FROM json_array_elements("current_schema"->'fields') field)
    )
    WHERE tables.table_name = iceberg_rename_column.table_name
    RETURNING true INTO field_found;

    IF NOT field_found THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    -- 3. Recreate views. iceberg_build_object uses DROP+CREATE VIEW which
    --    reads column names from current_schema text (not ALTER VIEW RENAME),
    --    so the renamed column appears correctly in the new view. #917 fixed.
    PERFORM _gsiceberg.iceberg_build_object(table_name);

    RAISE NOTICE 'gsiceberg: renamed column "%" → "%" in "%"', old_name, new_name, table_name;
    RETURN true;
END;
$$;

-- DDL helper: ALTER the _delta and _data tables to SET NOT NULL.
-- Extracted from iceberg_set_not_null to isolate DDL (which triggers
-- catalog invalidation) from the SELECT that looks up the namespace.
-- Mixing DDL and cached SQL plans in a single plpgsql function can
-- leave stale plan nodes -> "unrecognized node type: 0" (#1106).
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_set_not_null_alter(
    ns text, table_name text, col_name text)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
BEGIN
    BEGIN
        EXECUTE format('ALTER TABLE _gsiceberg.%I ALTER COLUMN %I SET NOT NULL',
                       '_' || ns || '_' || table_name || '_delta', col_name);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'gsiceberg: ALTER TABLE _delta failed: %', SQLERRM;
        RETURN false;
    END;

    BEGIN
        EXECUTE format('ALTER FOREIGN TABLE _gsiceberg.%I ALTER COLUMN %I SET NOT NULL',
                       '_' || ns || '_' || table_name || '_data', col_name);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'gsiceberg: ALTER FOREIGN TABLE _data warning: %', SQLERRM;
    END;

    RETURN true;
END;
$$;

-- Set NOT NULL constraint on a column.
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_set_not_null(
    table_name text, col_name text)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
DECLARE
    field_found bool := false;
    ns          text;
    ok          boolean;
BEGIN
    -- 0. Look up namespace for prefixed internal object names (#906).
    SELECT tables.namespace INTO ns
    FROM _gsiceberg.tables
    WHERE tables.table_name = iceberg_set_not_null.table_name;
    IF NOT FOUND THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    -- 1. ALTER underlying tables (DDL delegated to separate function;
    --    see #1106 for the plan-cache corruption this prevents).
    SELECT _gsiceberg.iceberg_set_not_null_alter(ns, table_name, col_name) INTO ok;
    IF NOT ok THEN
        RETURN false;
    END IF;

    -- 2. Update text schema (set required=true)
    UPDATE _gsiceberg.tables
    SET "current_schema" = json_set(
        "current_schema",
        '{fields}',
        (SELECT json_agg(
            CASE WHEN field->>'name' = iceberg_set_not_null.col_name
                 THEN field || '{"required": true}'::text
                 ELSE field
            END
            ORDER BY (field->>'id')::int)
         FROM json_array_elements("current_schema"->'fields') field)
    )
    WHERE tables.table_name = iceberg_set_not_null.table_name
    RETURNING true INTO field_found;

    IF NOT field_found THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', table_name;
        RETURN false;
    END IF;

    RAISE NOTICE 'gsiceberg: set NOT NULL on "%"."%"', table_name, col_name;
    RETURN true;
END;
$$;

-- iceberg_truncate removed (issue #124).  It bypassed the refcount-aware
-- gsfile_unregister_internal path and did not satisfy ACID requirements.
-- A proper truncate will be implemented as part of the ACID redesign (#121).

-- Rename a mounted Iceberg table.
CREATE OR REPLACE FUNCTION _gsiceberg.iceberg_rename_table(
    old_name text, new_name text)
RETURNS boolean LANGUAGE plpgsql STRICT AS $$
DECLARE
    old_schema text;
    new_schema text;
    col_list   text;
    snap       record;
BEGIN
    -- Validate new name: single regex covers empty, starts-with-digit,
    -- special chars, AND byte-length 0-and-1 vs 64+ in one shot (#978, #977).
    IF new_name !~ '^[a-zA-Z_][a-zA-Z0-9_]{0,62}$' THEN
        RAISE EXCEPTION 'gsiceberg: invalid table name "%"', new_name;
    END IF;

    -- Check old table exists
    IF NOT EXISTS (SELECT 1 FROM _gsiceberg.tables WHERE table_name = old_name) THEN
        RAISE NOTICE 'gsiceberg: table "%" is not mounted', old_name;
        RETURN false;
    END IF;

    -- Check new name not already in use
    IF EXISTS (SELECT 1 FROM _gsiceberg.tables WHERE table_name = new_name) THEN
        RAISE NOTICE 'gsiceberg: table "%" already exists', new_name;
        RETURN false;
    END IF;

    old_schema := quote_ident(old_name);
    new_schema := quote_ident(new_name);

    -- 1. Rename internal objects (_oldname → _newname)
    EXECUTE format('ALTER TABLE IF EXISTS _gsiceberg.%I RENAME TO %I',
                   '_' || old_name || '_delta',
                   '_' || new_name || '_delta');
    EXECUTE format('ALTER FOREIGN TABLE IF EXISTS _gsiceberg.%I RENAME TO %I',
                   '_' || old_name || '_data',
                   '_' || new_name || '_data');
    EXECUTE format('ALTER VIEW IF EXISTS _gsiceberg.%I RENAME TO %I',
                   '_' || old_name || '_snapshot',
                   '_' || new_name || '_snapshot');

    -- Rename per-snapshot FOREIGN TABLEs and VIEWs
    FOR snap IN
        SELECT snapshot_id FROM _gsiceberg.snapshots
        WHERE table_name = old_name ORDER BY snapshot_id
    LOOP
        EXECUTE format('ALTER FOREIGN TABLE IF EXISTS _gsiceberg.%I RENAME TO %I',
                       '_' || old_name || '_snapshot_data_' || snap.snapshot_id,
                       '_' || new_name || '_snapshot_data_' || snap.snapshot_id);
        EXECUTE format('ALTER VIEW IF EXISTS _gsiceberg.%I RENAME TO %I',
                       '_' || old_name || '_snapshot_' || snap.snapshot_id,
                       '_' || new_name || '_snapshot_' || snap.snapshot_id);
    END LOOP;

    -- Drop old trigger functions (created by iceberg_build_object naming)
    EXECUTE format('DROP FUNCTION IF EXISTS _gsiceberg.%I()',
                   '_' || old_name || '_ins_fn');
    EXECUTE format('DROP FUNCTION IF EXISTS _gsiceberg.%I()',
                   '_' || old_name || '_del_fn');
    EXECUTE format('DROP FUNCTION IF EXISTS _gsiceberg.%I()',
                   '_' || old_name || '_upd_fn');

    -- #1122: iceberg_refresh_views removed; iceberg_build_object handles this
    EXECUTE format('DROP VIEW IF EXISTS %s', old_schema);

    -- 2. Update gsiceberg metadata tables
    UPDATE _gsiceberg.tables      SET table_name = new_name WHERE table_name = old_name;
    UPDATE _gsiceberg.snapshots   SET table_name = new_name WHERE table_name = old_name;
    UPDATE _gsiceberg.data_files  SET table_name = new_name WHERE table_name = old_name;
    UPDATE _gsiceberg.flush_state SET table_name = new_name WHERE table_name = old_name;
    UPDATE _gsiceberg._row_range_facts SET table_name = new_name WHERE table_name = old_name;
    UPDATE _gsiceberg.columns          SET table_name = new_name WHERE table_name = old_name;
    UPDATE _gsfs.owned_files           SET table_name = new_name WHERE table_name = old_name;

    -- 3. Recreate public view with new table name
    PERFORM _gsiceberg.iceberg_build_object(new_name);

    RAISE NOTICE 'gsiceberg: renamed table "%" → "%"', old_name, new_name;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'gsiceberg: rename failed: %', SQLERRM;
    RETURN false;
END;
$$;

-- Public wrapper (#268) — admin-gated after #927 root cause fix (#933).
CREATE FUNCTION iceberg_rename_table(text, text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsiceberg_require_admin();
    RETURN _gsiceberg.iceberg_rename_table($1, $2);
END;
$$;

-- ============================================================
-- Namespace DDL (#689) — single-level Iceberg namespace registry.
-- Iceberg namespace = logical table grouping (_gsiceberg.tables.namespace).
-- v0.1.0: registry-only (create/drop/list). The namespace->PG-schema mapping
-- and the schema-privilege security migration are deferred to the full design
-- (docs/superpowers/specs/2026-07-10-iceberg-namespace-design.md, Status: Draft).
-- ============================================================

-- Create a namespace (admin-only).
CREATE FUNCTION iceberg_create_namespace(namespace text) RETURNS boolean
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsiceberg_require_admin();
    IF namespace = '' THEN
        RAISE EXCEPTION 'namespace name cannot be empty';
    END IF;
    INSERT INTO _gsiceberg.namespaces(name) VALUES (namespace);
    RETURN true;
EXCEPTION WHEN unique_violation THEN
    RAISE EXCEPTION 'namespace "%" already exists', namespace;
END;
$$;

-- Drop a namespace (admin-only). Refuses 'default' and non-empty namespaces.
CREATE FUNCTION iceberg_drop_namespace(namespace text) RETURNS boolean
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    n_tables int;
BEGIN
    PERFORM public.gsiceberg_require_admin();
    IF namespace = 'default' THEN
        RAISE EXCEPTION 'cannot drop the default namespace';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM _gsiceberg.namespaces ns WHERE ns.name = namespace) THEN
        RAISE EXCEPTION 'namespace "%" does not exist', namespace;
    END IF;
    SELECT count(*) INTO n_tables
        FROM _gsiceberg.tables t WHERE t.namespace = iceberg_drop_namespace.namespace;
    IF n_tables > 0 THEN
        RAISE EXCEPTION 'namespace "%" is not empty (% table(s))', namespace, n_tables;
    END IF;
    -- The count-then-DELETE is not atomic: a concurrent INSERT could add a
    -- table after the check. The live FK guarantees integrity regardless;
    -- translate its raw error into the same friendly message (#906).
    BEGIN
        DELETE FROM _gsiceberg.namespaces ns WHERE ns.name = namespace;
    EXCEPTION WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'namespace "%" is not empty', namespace;
    END;
    RETURN true;
END;
$$;

-- List all namespaces.
CREATE FUNCTION iceberg_list_namespaces() RETURNS SETOF text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT name FROM _gsiceberg.namespaces ORDER BY name;
$$;

-- List tables within a namespace.
CREATE FUNCTION iceberg_list_tables(namespace text) RETURNS SETOF text
LANGUAGE plpgsql STABLE STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM _gsiceberg.namespaces ns WHERE ns.name = iceberg_list_tables.namespace) THEN
        RAISE EXCEPTION 'namespace "%" does not exist', namespace;
    END IF;
    -- Filter by the connected caller. This function is SECURITY DEFINER (needed
    -- to read the REVOKE-ALL _gsiceberg schema), which makes current_user the
    -- definer, not the caller -- so RLS on _gsiceberg.tables is bypassed and we
    -- must reproduce tables_owner_policy explicitly against session_user (the
    -- login role). pg_has_role(session_user, role, 'MEMBER') is the 3-arg form
    -- that tests session_user; the 2-arg pg_has_role(role, 'MEMBER') form tests
    -- current_user (the definer superuser) and would defeat the filter (#906, #927).
    RETURN QUERY
        SELECT t.table_name FROM _gsiceberg.tables t
        WHERE t.namespace = iceberg_list_tables.namespace
          AND (t.owner = session_user OR t.owner = 'public'
               OR pg_has_role(session_user, 'gsiceberg_admin', 'MEMBER'))
        ORDER BY t.table_name;
END;
$$;

-- Reassign a table to a namespace (admin-only in Phase 1).
CREATE FUNCTION iceberg_set_table_namespace(table_name text, namespace text) RETURNS boolean
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    -- Admin-gated. Owner-based delegation (gsiceberg_require_table_owner) is
    -- unusable in Phase 1: tables.owner is populated with the definer at
    -- SECDEF mount time (never the real user), so an owner check can never
    -- match a real caller. Deferred to the owner-model fix (#928). See #927.
    PERFORM public.gsiceberg_require_admin();
    IF NOT EXISTS (SELECT 1 FROM _gsiceberg.namespaces ns WHERE ns.name = iceberg_set_table_namespace.namespace) THEN
        RAISE EXCEPTION 'namespace "%" does not exist', namespace;
    END IF;
    UPDATE _gsiceberg.tables t
        SET namespace = iceberg_set_table_namespace.namespace
        WHERE t.table_name = iceberg_set_table_namespace.table_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'table "%" not found', table_name;
    END IF;
    RETURN true;
END;
$$;
-- 02c-filesystem.sql — File lifecycle: gsiceberg-specific functions
-- NOTE: Schema _gsfs, tables (owned_files, whitelist, blacklist), and
-- all file management functions are created by the gsfilesystem
-- dependency extension (gsfilesystem--0.1.0.sql).
-- This file intentionally empty — gsiceberg-specific file operations
-- will be added here when they diverge from gsfilesystem.
-- 02d-index.sql — Index management: vector + scalar

-- ============================================================
-- _gsiceberg.index_physical — physical index catalog (#1025 Spec 2).
-- Each row is one version of a physical index (LSM level).
-- end_snapshot_id IS NULL means "alive" (replaces the old status
-- column).  Supports both scalar and vector index types.
-- ============================================================
DO $$ BEGIN
    CREATE SCHEMA _gsiceberg;
EXCEPTION WHEN duplicate_schema THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS _gsiceberg.index_physical (
    index_name        text NOT NULL,
    table_name        text NOT NULL,
    column_name       text NOT NULL,
    index_type        text NOT NULL DEFAULT 'vector'
                      CHECK (index_type IN ('scalar','vector')),
    version_path      text NOT NULL,
    version           int8 NOT NULL,
    level             int2 NOT NULL DEFAULT 1,
    begin_snapshot_id bigint NOT NULL,
    end_snapshot_id   bigint,          -- NULL = alive
    row_count         int8 NOT NULL DEFAULT 0,
    dim               int2,
    metric            text,
    field_id          int,
    schema_id         int,
    PRIMARY KEY (index_name, begin_snapshot_id)
);

CREATE TABLE IF NOT EXISTS _gsiceberg.index_proxy (
    index_name   text NOT NULL,
    table_name   text NOT NULL,
    column_name  text NOT NULL,
    index_type   text NOT NULL DEFAULT 'vector'
                 CHECK (index_type IN ('scalar','vector')),
    physical_ref text NOT NULL,  -- logical FK to index_physical.index_name;
                                   -- composite PK prevents DB-level FK
    snapshot_id  bigint NOT NULL,     -- proxy has one snapshot, one-to-one
    schema_id    int,
    field_id     int,
    PRIMARY KEY (index_name)
);

-- Retained for backward compatibility with gsvector-pg submodule
-- and existing installations.  All gsiceberg core code writes to
-- index_physical / index_proxy; the old table is a shadow copy
-- during the transition (#1025 Spec 2 migration step).
CREATE TABLE IF NOT EXISTS _gsiceberg.index_catalog (
    index_name      text NOT NULL,
    table_name      text NOT NULL,
    column_name     text NOT NULL,
    index_type      text NOT NULL DEFAULT 'vector',
    level           int2 NOT NULL DEFAULT 0,
    version         int8 NOT NULL,
    version_path    text NOT NULL,
    snapshot_id     bigint NOT NULL DEFAULT 0,
    parent_oid      bigint,
    lsm_oid         bigint,
    schema_id       int,
    field_id        int,
    status          text NOT NULL DEFAULT 'active',
    created_at      timestamptz NOT NULL DEFAULT now(),
    deleted_at      timestamptz,
    row_count       int8 NOT NULL DEFAULT 0,
    metric          text,
    dim             int2,
    PRIMARY KEY (index_name, snapshot_id, level)
);

-- Forward-compatibility: add columns on existing installations.
DO $$ BEGIN
    ALTER TABLE _gsiceberg.index_catalog ADD COLUMN schema_id int;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.index_catalog ADD COLUMN field_id int;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.index_catalog ADD COLUMN metric text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.index_catalog ADD COLUMN dim int2;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE _gsiceberg.index_catalog ADD COLUMN is_proxy boolean NOT NULL DEFAULT false;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- NOTE: iceberg_index_build / iceberg_index_drop are C functions defined
-- in gsvector-pg/src/index_build.c, declared in gsiceberg_vector extension.
-- iceberg_compact_index (vector LSM compaction) is also in gsiceberg_vector.

-- NOTE: iceberg_exec_delete and iceberg_exec_update are removed.
-- Use standard SQL DELETE FROM <view> and UPDATE <view> instead.
-- The INSTEAD OF triggers on the public VIEW handle tombstone insertion.

-- ============================================================
-- Scalar index delta functions (Task 04 — scalar incremental)
-- ============================================================

CREATE OR REPLACE FUNCTION scalar_index_append_delta(
    table_path text,
    index_name text,
    value bigint,
    row_id bigint,
    op text
) RETURNS boolean
AS 'MODULE_PATHNAME', 'scalar_index_append_delta'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION scalar_index_flush_delta(
    table_path text,
    index_name text
) RETURNS integer
AS 'MODULE_PATHNAME', 'scalar_index_flush_delta'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION scalar_index_flush_delta(
    table_path text,
    index_name text,
    table_name text
) RETURNS integer
AS 'MODULE_PATHNAME', 'scalar_index_flush_delta3'
LANGUAGE C STRICT;
-- 02e-util.sql — Cross-cutting utilities: guards, selectivity, type mapping

-- ============================================================
-- Security guard functions — enforce owner/admin checks
-- ============================================================

-- Check that current_user owns or has admin access to a table.
-- Called by SECURITY DEFINER functions that operate on user tables.
-- 2-arg (namespace, table) under the composite-PK model (#906).
CREATE OR REPLACE FUNCTION gsiceberg_require_table_owner(ns text, tbl text)
RETURNS void LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF pg_has_role(session_user, 'gsiceberg_admin', 'MEMBER') THEN RETURN; END IF;
    PERFORM 1 FROM _gsiceberg.tables
    WHERE tables.namespace = ns AND tables.table_name = tbl
      AND (tables.owner = session_user OR tables.owner = 'public');
    IF NOT FOUND THEN
        RAISE EXCEPTION 'permission denied for table "%"."%"', ns, tbl;
    END IF;
END;
$$;

-- Check that current_user is a member of gsiceberg_admin.

CREATE OR REPLACE FUNCTION gsiceberg_require_admin()
RETURNS void LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    -- This function is SECURITY DEFINER, so current_user is the definer (the
    -- installing superuser, an implicit member of every role). The 2-arg
    -- pg_has_role(role, priv) form tests current_user and would therefore
    -- always pass, letting any caller bypass the gate. Test the real caller
    -- with the 3-arg pg_has_role(session_user, role, priv) form (#927).
    IF NOT pg_has_role(session_user, 'gsiceberg_admin', 'MEMBER') THEN
        RAISE EXCEPTION 'only gsiceberg_admin can perform this operation';
    END IF;
END;
$$;

-- ============================================================
-- Scalar selectivity estimation (#394) — replaces PG default 0.5%/33%
-- ============================================================
CREATE OR REPLACE FUNCTION gsiceberg_estimate_selectivity(
    table_name text,
    col_name text,
    op text,
    val text
) RETURNS float8 LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    total_rows   bigint;
    matching     bigint;
    snap_id      bigint;
BEGIN
    SELECT MAX(snapshot_id) INTO snap_id
    FROM _gsiceberg.snapshots WHERE _gsiceberg.snapshots.table_name = $1;
    IF snap_id IS NULL THEN RETURN 0.5; END IF;

    SELECT COALESCE(SUM(record_count), 0) INTO total_rows
    FROM _gsiceberg.data_files
    WHERE data_files.table_name = $1 AND begin_snapshot_id <= snap_id AND (end_snapshot_id IS NULL OR end_snapshot_id > snap_id);
    IF total_rows = 0 THEN RETURN 0.5; END IF;

    IF op = '=' THEN
        SELECT COALESCE(SUM(record_count), 0) INTO matching
        FROM _gsiceberg.data_files
        WHERE data_files.table_name = $1 AND begin_snapshot_id <= snap_id AND (end_snapshot_id IS NULL OR end_snapshot_id > snap_id)
          AND lower_bounds IS NOT NULL AND upper_bounds IS NOT NULL
          AND lower_bounds->>$2 <= $4
          AND upper_bounds->>$2 >= $4;
    ELSIF op = '>' THEN
        SELECT COALESCE(SUM(record_count), 0) INTO matching
        FROM _gsiceberg.data_files
        WHERE data_files.table_name = $1 AND begin_snapshot_id <= snap_id AND (end_snapshot_id IS NULL OR end_snapshot_id > snap_id)
          AND upper_bounds IS NOT NULL
          AND upper_bounds->>$2 > $4;
    ELSIF op = '<' THEN
        SELECT COALESCE(SUM(record_count), 0) INTO matching
        FROM _gsiceberg.data_files
        WHERE data_files.table_name = $1 AND begin_snapshot_id <= snap_id AND (end_snapshot_id IS NULL OR end_snapshot_id > snap_id)
          AND lower_bounds IS NOT NULL
          AND lower_bounds->>$2 < $4;
    ELSIF op = '>=' THEN
        SELECT COALESCE(SUM(record_count), 0) INTO matching
        FROM _gsiceberg.data_files
        WHERE data_files.table_name = $1 AND begin_snapshot_id <= snap_id AND (end_snapshot_id IS NULL OR end_snapshot_id > snap_id)
          AND upper_bounds IS NOT NULL
          AND upper_bounds->>$2 >= $4;
    ELSIF op = '<=' THEN
        SELECT COALESCE(SUM(record_count), 0) INTO matching
        FROM _gsiceberg.data_files
        WHERE data_files.table_name = $1 AND begin_snapshot_id <= snap_id AND (end_snapshot_id IS NULL OR end_snapshot_id > snap_id)
          AND lower_bounds IS NOT NULL
          AND lower_bounds->>$2 <= $4;
    ELSE
        RETURN 0.5;
    END IF;

    IF total_rows = 0 THEN RETURN 0.5; END IF;
    RETURN matching::float8 / total_rows::float8;
END;
$$;


CREATE FUNCTION iceberg_type_mapping(iceberg_type_name text DEFAULT NULL)
RETURNS TABLE(iceberg_name text, pg_type text, pg_oid int)
LANGUAGE SQL AS $$
  SELECT * FROM (VALUES
    ('long', 'bigint', 20),
    ('int', 'integer', 23),
    ('double', 'double precision', 701),
    ('string', 'text', 25),
    ('boolean', 'boolean', 16),
    ('float', 'real', 700),
    ('timestamp', 'timestamp with time zone', 1184),
    ('vector(N)', 'vector', (SELECT oid FROM pg_type WHERE typname = 'vector')),
    ('map(K,V)', 'text', 3802),
    ('list(E)', 'text', 3802)
  ) AS t(iceberg_name, pg_type, pg_oid)
  WHERE $1 IS NULL OR t.iceberg_name = $1;
$$;

-- HA standby guard: blocks write operations on hot standby (#635).
CREATE OR REPLACE FUNCTION gsiceberg_require_primary()
RETURNS void LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF pg_is_in_recovery() THEN
        RAISE EXCEPTION 'gsiceberg: standby mode — '
            'write operations only available on primary node'
            USING HINT = 'Route write operations to the primary node. '
                         'SELECT queries work on standby with shared storage.';
    END IF;
END;
$$;

-- ============================================================
-- Cache statistics function (#797 Phase 2)
-- ============================================================
CREATE FUNCTION gsiceberg_cache_stats(
    OUT hits bigint,
    OUT misses bigint,
    OUT inserts bigint,
    OUT evictions bigint,
    OUT total_bytes_used bigint,
    OUT total_bytes_avail bigint
) RETURNS record
AS 'gsiceberg'
LANGUAGE C STABLE;

-- Force library load at extension install time so _PG_init fires
-- and ProcessUtility_hook is active before any user DDL.
-- Without this, hook may not trigger for CREATE FOREIGN TABLE
-- in a fresh connection.  gsiceberg_version() is a C function
-- whose execution guarantees the .so is loaded and _PG_init runs.
SELECT gsiceberg_version();

