-- dev/garbage/oracle.sql — Obfuscated GARBAGE-profile fixture (Oracle Database 23ai+),
-- a table-for-table port of dev/garbage/mysql.sql. See dev/garbage/postgres.sql for
-- the rationale. Materialized as `ext_`-prefixed tables in the connecting user's
-- schema (run as rivet/rivet@//host/FREEPDB1). ZERO source identity, ZERO real data.
-- Deterministic + idempotent (DROP TABLE IF EXISTS … PURGE, 23ai syntax).
-- Runs under `sqlplus -s`; aborts on the first error (WHENEVER SQLERROR).
--
-- Deviations from dev/garbage/mysql.sql (Oracle has no exact equivalent):
--   * BIGINT → NUMBER(19), INT → NUMBER(10), SMALLINT → NUMBER(5). The signed
--     ranges of MySQL's fixed-width ints are NOT enforced (NUMBER(10) admits
--     9,999,999,999); only the digit count is.
--   * BIGINT UNSIGNED (ext_bigint_unsigned_pk.id) → NUMBER(20) + CHECK
--     (0 ..= 18446744073709551615). Oracle has no unsigned type; the three ids past
--     i64::MAX are stored exactly, so the "key wider than i64" hazard carries over.
--   * DECIMAL(p,s) → NUMBER(p,s).
--   * TEXT → VARCHAR2(4000) (every seeded value is < 100 chars; MySQL TEXT's 64 KiB
--     ceiling is not reproduced — CLOB would forbid keys/indexes and ordinary compare).
--   * MySQL TIMESTAMP (UTC-stored, rendered in the session time zone) →
--     TIMESTAMP(0) WITH LOCAL TIME ZONE, the Oracle type with the same semantics;
--     DEFAULT CURRENT_TIMESTAMP → DEFAULT SYSTIMESTAMP. NOW() - INTERVAL n X →
--     SYSTIMESTAMP - NUMTODSINTERVAL(n, 'X') (wall-clock-relative, as in MySQL).
--   * KEY ix_ref_id / UNIQUE KEY ux_order_id → CREATE INDEX / named UNIQUE constraint.
--   * Recursive CTE row generator → SELECT LEVEL FROM dual CONNECT BY LEVEL <= n.
--   * CONCAT/ELT/IF/DIV/% → ||, DECODE, CASE, TRUNC(a/b), MOD. REPEAT('x', k) →
--     RPAD('x', k, 'x'). MD5(n) → LOWER(RAWTOHEX(STANDARD_HASH(TO_CHAR(n), 'MD5')))
--     (same 32-char lowercase hex of the decimal string).
--   * Unquoted identifiers: Oracle stores every table/column name UPPER-case.

SET DEFINE OFF
SET SQLBLANKLINES ON
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

DROP TABLE IF EXISTS ext_bigint_pk_dual_ts PURGE;
CREATE TABLE ext_bigint_pk_dual_ts (
    id NUMBER(19) PRIMARY KEY,
    payload VARCHAR2(4000) NOT NULL,
    created_at TIMESTAMP(0) WITH LOCAL TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(0) WITH LOCAL TIME ZONE NULL
);
INSERT INTO ext_bigint_pk_dual_ts (id, payload, updated_at)
SELECT n, 'row' || n, SYSTIMESTAMP - NUMTODSINTERVAL(n, 'MINUTE')
FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);

DROP TABLE IF EXISTS ext_int_pk_dual_ts PURGE;
CREATE TABLE ext_int_pk_dual_ts (
    id NUMBER(10) PRIMARY KEY,
    payload VARCHAR2(4000) NOT NULL,
    created_at TIMESTAMP(0) WITH LOCAL TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(0) WITH LOCAL TIME ZONE NULL
);
INSERT INTO ext_int_pk_dual_ts (id, payload, updated_at)
SELECT n, 'row' || n, SYSTIMESTAMP - NUMTODSINTERVAL(n, 'MINUTE')
FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);

-- The field DB CASTs id AS UNSIGNED — a BIGINT UNSIGNED PK with ids PAST
-- i64::MAX. Keyset must read the high-water key past i64 (#bc512a3), or it
-- loses/duplicates the tail. Oracle: NUMBER(20) + CHECK stands in for UNSIGNED.
DROP TABLE IF EXISTS ext_bigint_unsigned_pk PURGE;
CREATE TABLE ext_bigint_unsigned_pk (
    id NUMBER(20) PRIMARY KEY CHECK (id BETWEEN 0 AND 18446744073709551615),
    payload NUMBER(10) NOT NULL
);
INSERT INTO ext_bigint_unsigned_pk (id, payload)
SELECT n, n FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);
-- Three ids past i64::MAX (9223372036854775807); u64::MAX = 18446744073709551615.
INSERT INTO ext_bigint_unsigned_pk (id, payload) VALUES
    (18446744073709551613, 100), (18446744073709551614, 101), (18446744073709551615, 102);

-- Sparse key: id span vastly exceeds the row count (the sparse-guard shape).
DROP TABLE IF EXISTS ext_sparse_key PURGE;
CREATE TABLE ext_sparse_key (id NUMBER(19) PRIMARY KEY, payload NUMBER(10) NOT NULL);
INSERT INTO ext_sparse_key (id, payload)
SELECT 1 + n * 1000000, n FROM (SELECT LEVEL - 1 n FROM dual CONNECT BY LEVEL <= 150000);

-- Scale-0 DECIMAL PK (Oracle/ERP shape) — an explicit range chunk_column on it
-- must LOUDLY bail (#103); `chunk_by_key: dkey` (keyset) IS accepted.
DROP TABLE IF EXISTS ext_decimal_key PURGE;
CREATE TABLE ext_decimal_key (dkey NUMBER(15,0) PRIMARY KEY, payload VARCHAR2(4000) NOT NULL);
INSERT INTO ext_decimal_key (dkey, payload)
SELECT n, 'row' || n FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);

-- Keyless, cursorless → full-mode fallback.
DROP TABLE IF EXISTS ext_no_pk_no_ts PURGE;
CREATE TABLE ext_no_pk_no_ts (label VARCHAR2(4000) NOT NULL, amount NUMBER(10) NOT NULL);
INSERT INTO ext_no_pk_no_ts (label, amount)
SELECT 'label' || n, MOD(n, 100) FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);

-- ── The MESSY reality (distilled from a real stuck run's state DB) ───────────
-- HISTORY/VERSION table: NO `id`, keyed by a non-PK integer `ref_id`. The planner
-- range-CHUNKS on ref_id. Non-unique index on ref_id, no PK.
DROP TABLE IF EXISTS ext_ref_id_history PURGE;
CREATE TABLE ext_ref_id_history (
    ref_id NUMBER(19) NOT NULL,
    version NUMBER(10) NOT NULL,
    field VARCHAR2(4000),
    field_cur VARCHAR2(4000),
    sign NUMBER(5) DEFAULT 1 NOT NULL,
    status VARCHAR2(4000),
    -- money as DECIMAL(11,4) VALUE columns — the DOMINANT field profile.
    cart NUMBER(11,4),
    earning NUMBER(11,4),
    subtotal NUMBER(11,4),
    total NUMBER(11,4),
    amount_cur VARCHAR2(4000),
    user_cur CHAR(3),
    purchase_type VARCHAR2(4000),
    created_at TIMESTAMP(0) WITH LOCAL TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(0) WITH LOCAL TIME ZONE NULL
);
CREATE INDEX ix_ref_id ON ext_ref_id_history (ref_id);
INSERT INTO ext_ref_id_history (ref_id, version, field, field_cur, sign, status, cart, earning, subtotal, total, amount_cur, user_cur, purchase_type, created_at, updated_at)
SELECT MOD(n, 400) + 1, TRUNC(n / 400) + 1, 'f' || n, 'EUR', CASE WHEN MOD(n, 2) = 0 THEN 1 ELSE -1 END,
       DECODE(MOD(n, 3), 0, 'pending', 1, 'done', 'void'),
       (MOD(n, 900000) + 1) / 100.0, (MOD(n, 500000) + 1) / 100.0, (MOD(n, 300000) + 1) / 100.0, (MOD(n, 1200000) + 1) / 100.0,
       'USD', DECODE(MOD(n, 3), 0, 'EUR', 1, 'USD', 'PLN'), DECODE(MOD(n, 2), 0, 'a', 'b'),
       SYSTIMESTAMP - NUMTODSINTERVAL(n, 'MINUTE'), SYSTIMESTAMP - NUMTODSINTERVAL(n, 'SECOND')
FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);

-- Keyed by `order_id` (NOT `id`): a unique key makes it keyset-able on a non-`id`
-- key. Wide-ish string columns (md5/currency/subid).
DROP TABLE IF EXISTS ext_order_keyed PURGE;
CREATE TABLE ext_order_keyed (
    order_id NUMBER(19) NOT NULL,
    md5 CHAR(32) NOT NULL,
    currency CHAR(3) NOT NULL,
    status VARCHAR2(4000) NOT NULL,
    subid VARCHAR2(4000),
    advcampaign_id NUMBER(10),
    CONSTRAINT ux_order_id UNIQUE (order_id)
);
INSERT INTO ext_order_keyed (order_id, md5, currency, status, subid, advcampaign_id)
SELECT n, LOWER(RAWTOHEX(STANDARD_HASH(TO_CHAR(n), 'MD5'))),
       DECODE(MOD(n, 3), 0, 'EUR', 1, 'USD', 'PLN'), DECODE(MOD(n, 2), 0, 'approved', 'declined'),
       'sub' || MOD(n, 50), MOD(n, 900) + 100
FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);

-- HEAP: no PK, no index → full mode only.
DROP TABLE IF EXISTS ext_heap_no_key PURGE;
CREATE TABLE ext_heap_no_key (payload VARCHAR2(4000) NOT NULL, n NUMBER(10) NOT NULL);
INSERT INTO ext_heap_no_key (payload, n)
SELECT RPAD('x', 20 + MOD(s, 50), 'x'), s FROM (SELECT LEVEL s FROM dual CONNECT BY LEVEL <= 150000);

-- NAME-TRAP: a column literally named `id` that is NOT a PK and NOT indexed.
-- init picks keyset ONLY for a catalog single-column PRIMARY KEY (never by the
-- name `id`), so this must NOT scaffold `chunk_by_key: id`.
DROP TABLE IF EXISTS ext_unindexed_id PURGE;
CREATE TABLE ext_unindexed_id (id NUMBER(19) NOT NULL, label VARCHAR2(4000) NOT NULL, amount NUMBER(10) NOT NULL);
INSERT INTO ext_unindexed_id (id, label, amount)
SELECT n, 'row' || n, MOD(n, 100) FROM (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 150000);

COMMIT;
