-- Obfuscated GARBAGE-profile fixture (PostgreSQL) — the anonymized SHAPE of a
-- real 200+-table field DB, materialized as persistent tables for manual runs
-- and exploration. ZERO source identity, ZERO real data — only the profile that
-- triggered the field bugs: heterogeneous PK types, dual created_at/updated_at,
-- a non-default schema, sparse key spans, a scale-0 decimal key, a keyless table.
--
-- Deterministic + idempotent (DROP + CREATE). Non-default schema `ext` (the field
-- fleet lives in its own schema, reached via search_path) is itself the profile —
-- it is the #13 chunked-probe-degrade shape. Seeded via `make seed-garbage`.
-- The sweep only drops `_<pid>_<counter>` fixtures, so `ext.*` persists.

DROP SCHEMA IF EXISTS ext CASCADE;
CREATE SCHEMA ext;

-- Fleet majority: bigint PK + BOTH timestamps. rivet keyset(id)s it (PK beats the
-- timestamp cursor); the field tool chose a size-tiered timestamp strategy.
CREATE TABLE ext.bigint_pk_dual_ts (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP
);
INSERT INTO ext.bigint_pk_dual_ts (id, payload, updated_at)
SELECT g, 'row' || g, now() - (g || ' minutes')::interval
FROM generate_series(1, 500) g;

-- Int-PK minority (the field fleet mixes bigint + int).
CREATE TABLE ext.int_pk_dual_ts (
    id INT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP
);
INSERT INTO ext.int_pk_dual_ts (id, payload, updated_at)
SELECT g, 'row' || g, now() - (g || ' minutes')::interval
FROM generate_series(1, 300) g;

-- Sparse key: id span vastly exceeds the row count (gappy field ids). Range
-- chunking would explode into near-empty windows — the sparse-guard shape.
CREATE TABLE ext.sparse_key (
    id BIGINT PRIMARY KEY,
    payload INT NOT NULL
);
INSERT INTO ext.sparse_key (id, payload)
SELECT 1 + g * 1000000, g FROM generate_series(0, 199) g;

-- Scale-0 DECIMAL PK (Oracle/ERP-migration shape). NOT integer-family, so an
-- explicit range chunk_column on it must LOUDLY bail (#103), not silently drop
-- fractional rows. `chunk_by_key: dkey` (keyset) IS accepted for it.
CREATE TABLE ext.decimal_key (
    dkey DECIMAL(15,0) PRIMARY KEY,
    payload TEXT NOT NULL
);
INSERT INTO ext.decimal_key (dkey, payload)
SELECT g, 'row' || g FROM generate_series(1, 250) g;

-- Keyless, cursorless table → full-mode fallback (no chunk column, no cursor).
CREATE TABLE ext.no_pk_no_ts (
    label TEXT NOT NULL,
    amount INT NOT NULL
);
INSERT INTO ext.no_pk_no_ts (label, amount)
SELECT 'label' || g, g % 100 FROM generate_series(1, 150) g;

ANALYZE ext.bigint_pk_dual_ts;
ANALYZE ext.int_pk_dual_ts;
ANALYZE ext.sparse_key;
ANALYZE ext.decimal_key;
ANALYZE ext.no_pk_no_ts;
