-- gsfilesystem.sql — File ownership tracking extension.
-- Zero dependencies.  First extension in the gsiceberg family.

-- ============================================================
-- _gsfs schema
-- ============================================================
DO $$ BEGIN
    CREATE SCHEMA _gsfs;
EXCEPTION WHEN duplicate_schema THEN NULL;
END $$;

-- Schema permissions
REVOKE ALL ON SCHEMA _gsfs FROM PUBLIC;

-- Shared DBA role. gsfilesystem is installable standalone (no requires clause),
-- so it must not assume gsiceberg created this role. Idempotent create; both
-- extensions running the same CREATE ROLE is safe (#932).
DO $$ BEGIN
    CREATE ROLE gsiceberg_admin WITH NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Admin gate for gsfilesystem SECDEF functions (#932). SECURITY DEFINER makes
-- current_user the definer (installing superuser, member of every role), so the
-- 2-arg pg_has_role(role,priv) form always passes. Test the real caller via the
-- 3-arg pg_has_role(session_user, role, priv) form (#927 root cause).
CREATE OR REPLACE FUNCTION gsfile_require_admin()
RETURNS void LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF NOT pg_has_role(session_user, 'gsiceberg_admin', 'MEMBER') THEN
        RAISE EXCEPTION 'only gsiceberg_admin can perform this operation';
    END IF;
END;
$$;

-- ============================================================
-- Tables
-- ============================================================
CREATE TABLE IF NOT EXISTS _gsfs.owned_files (
    file_path   text PRIMARY KEY,
    table_name  text,
    refcount    integer NOT NULL DEFAULT 1,
    created_at  timestamptz DEFAULT now()
);

CREATE TABLE _gsfs.whitelist (
    dir_path    text PRIMARY KEY,
    created_at  timestamptz DEFAULT now()
);

CREATE TABLE _gsfs.blacklist (
    dir_path    text PRIMARY KEY,
    created_at  timestamptz DEFAULT now()
);

-- Directory-level refcount for proxy AM lifecycle (#1025 Spec 1).
-- When refcount > 0, gsfile_scan skips the entire directory tree.
DO $$ BEGIN
    ALTER TABLE _gsfs.blacklist ADD COLUMN refcount integer NOT NULL DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- ============================================================
-- C-level file operations
-- ============================================================
CREATE FUNCTION gsfile_scan_internal_c() RETURNS int
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION gsfile_cleanup_tmp(text) RETURNS int
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION gsfile_unregister_internal_c(text) RETURNS void
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT SECURITY DEFINER;

-- ============================================================
-- PL/pgSQL file management functions
-- ============================================================
CREATE OR REPLACE FUNCTION gsfile_register(path text, table_name text)
RETURNS int LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsfile_require_admin();
    UPDATE _gsfs.owned_files
        SET refcount = refcount + 1
        WHERE file_path = path;
    IF NOT FOUND THEN
        INSERT INTO _gsfs.owned_files (file_path, table_name, refcount)
        VALUES (path, table_name, 1);
    END IF;
    RETURN 0;
END;
$$;

CREATE OR REPLACE FUNCTION gsfile_unregister(path text)
RETURNS void LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsfile_require_admin();
    UPDATE _gsfs.owned_files
        SET refcount = refcount - 1 WHERE file_path = path;
    DELETE FROM _gsfs.owned_files
        WHERE file_path = path AND refcount <= 0;
END;
$$;

CREATE OR REPLACE FUNCTION gsfile_scan()
RETURNS int LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsfile_require_admin();
    RETURN public.gsfile_scan_internal_c();
END;
$$;

-- ============================================================
-- Whitelist/blacklist management
-- ============================================================
CREATE OR REPLACE FUNCTION gsfile_whitelist_add(dir_path text) RETURNS boolean
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsfile_require_admin();
    IF NOT EXISTS (SELECT 1 FROM _gsfs.whitelist WHERE dir_path = $1) THEN
        INSERT INTO _gsfs.whitelist (dir_path) VALUES (dir_path);
        RETURN true;
    END IF;
    RETURN false;
END;
$$;

CREATE OR REPLACE FUNCTION gsfile_whitelist_remove(dir_path text) RETURNS boolean
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsfile_require_admin();
    DELETE FROM _gsfs.whitelist WHERE _gsfs.whitelist.dir_path = $1;
    RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION gsfile_blacklist_add(dir_path text) RETURNS boolean
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsfile_require_admin();
    IF NOT EXISTS (SELECT 1 FROM _gsfs.blacklist WHERE dir_path = $1) THEN
        INSERT INTO _gsfs.blacklist (dir_path) VALUES (dir_path);
        RETURN true;
    END IF;
    RETURN false;
END;
$$;

CREATE OR REPLACE FUNCTION gsfile_blacklist_remove(dir_path text) RETURNS boolean
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.gsfile_require_admin();
    DELETE FROM _gsfs.blacklist WHERE _gsfs.blacklist.dir_path = $1;
    RETURN FOUND;
END;
$$;

-- Directory-level refcount: proxy AM calls on_flush to protect
-- physical index directories from GC (#1025 Spec 1).
CREATE OR REPLACE FUNCTION gsfile_blacklist_increment(dir_path text)
RETURNS int LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    cur int;
BEGIN
    UPDATE _gsfs.blacklist SET refcount = refcount + 1
    WHERE dir_path = $1
    RETURNING refcount INTO cur;
    IF NOT FOUND THEN
        INSERT INTO _gsfs.blacklist (dir_path, refcount) VALUES ($1, 1)
        RETURNING refcount INTO cur;
    END IF;
    RETURN cur;
END;
$$;

CREATE OR REPLACE FUNCTION gsfile_blacklist_decrement(dpath text)
RETURNS int LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    cur int;
BEGIN
    UPDATE _gsfs.blacklist SET refcount = refcount - 1
    WHERE dir_path = dpath
    RETURNING refcount INTO cur;
    IF cur IS NULL THEN
        RETURN 0;
    END IF;
    DELETE FROM _gsfs.blacklist
    WHERE dir_path = dpath AND refcount <= 0;
    RETURN cur;
END;
$$;

CREATE OR REPLACE FUNCTION iceberg_gc(table_name text)
RETURNS int LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    cleaned int := 0;
BEGIN
    PERFORM public.gsfile_require_admin();
    cleaned := cleaned + public.gsfile_cleanup_tmp(table_name);
    cleaned := cleaned + public.gsfile_scan();
    RETURN cleaned;
END;
$$;

-- #985 R4: refcount invariant guard.
ALTER TABLE _gsfs.owned_files
    ADD CONSTRAINT owned_files_refcount_check CHECK (refcount >= 0) NOT VALID;

DO $$ BEGIN
    ALTER TABLE _gsfs.owned_files ADD COLUMN namespace text NOT NULL DEFAULT 'default';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
