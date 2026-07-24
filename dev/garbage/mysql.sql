-- Obfuscated GARBAGE-profile fixture (MySQL) — see dev/garbage/postgres.sql for
-- the rationale. Materialized as `ext_`-prefixed tables in the `rivet` database (the rivet
-- MySQL user cannot CREATE a database; the non-default-schema hazard is PG-
-- specific anyway — #13 is n/a for MySQL, which resolves the db from the URL).
-- The MySQL-relevant hazard here is the BIGINT UNSIGNED key. ZERO source
-- identity, ZERO real data — only the profile that triggered the field bugs.
-- Deterministic + idempotent. Seeded via `make seed-garbage`.

DROP TABLE IF EXISTS ext_bigint_pk_dual_ts;
CREATE TABLE ext_bigint_pk_dual_ts (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL
);
SET SESSION cte_max_recursion_depth = 1000;
INSERT INTO ext_bigint_pk_dual_ts (id, payload, updated_at)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 500)
SELECT n, CONCAT('row', n), NOW() - INTERVAL n MINUTE FROM seq;

DROP TABLE IF EXISTS ext_int_pk_dual_ts;
CREATE TABLE ext_int_pk_dual_ts (
    id INT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL
);
INSERT INTO ext_int_pk_dual_ts (id, payload, updated_at)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 300)
SELECT n, CONCAT('row', n), NOW() - INTERVAL n MINUTE FROM seq;

-- The field DB CASTs id AS UNSIGNED — a BIGINT UNSIGNED PK with ids PAST
-- i64::MAX. Keyset must read the unsigned high-water key (#bc512a3), or it
-- loses/duplicates the tail. MySQL-only: PG/MSSQL have no unsigned integer type.
DROP TABLE IF EXISTS ext_bigint_unsigned_pk;
CREATE TABLE ext_bigint_unsigned_pk (
    id BIGINT UNSIGNED PRIMARY KEY,
    payload INT NOT NULL
);
INSERT INTO ext_bigint_unsigned_pk (id, payload)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 200)
SELECT n, n FROM seq;
-- Three ids past i64::MAX (9223372036854775807); u64::MAX = 18446744073709551615.
INSERT INTO ext_bigint_unsigned_pk (id, payload) VALUES
    (18446744073709551613, 100), (18446744073709551614, 101), (18446744073709551615, 102);

-- Sparse key: id span vastly exceeds the row count (the sparse-guard shape).
DROP TABLE IF EXISTS ext_sparse_key;
CREATE TABLE ext_sparse_key (id BIGINT PRIMARY KEY, payload INT NOT NULL);
INSERT INTO ext_sparse_key (id, payload)
WITH RECURSIVE seq AS (SELECT 0 n UNION ALL SELECT n+1 FROM seq WHERE n < 199)
SELECT 1 + n * 1000000, n FROM seq;

-- Scale-0 DECIMAL PK (Oracle/ERP shape) — an explicit range chunk_column on it
-- must LOUDLY bail (#103); `chunk_by_key: dkey` (keyset) IS accepted.
DROP TABLE IF EXISTS ext_decimal_key;
CREATE TABLE ext_decimal_key (dkey DECIMAL(15,0) PRIMARY KEY, payload TEXT NOT NULL);
INSERT INTO ext_decimal_key (dkey, payload)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 250)
SELECT n, CONCAT('row', n) FROM seq;

-- Keyless, cursorless → full-mode fallback.
DROP TABLE IF EXISTS ext_no_pk_no_ts;
CREATE TABLE ext_no_pk_no_ts (label TEXT NOT NULL, amount INT NOT NULL);
INSERT INTO ext_no_pk_no_ts (label, amount)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150)
SELECT CONCAT('label', n), n % 100 FROM seq;
