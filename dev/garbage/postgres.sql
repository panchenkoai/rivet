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
--
-- Scale: 150K rows/table — deliberately PAST `rivet init`'s 100K keyset/chunked
-- threshold, so init scaffolds the real per-table strategy (keyset on the int/
-- bigint PKs, range-chunk on the non-unique/keyless shapes) instead of collapsing
-- every table to `mode: full`. That is what exercises the diagnostic strategy
-- labels and the chunk-cost warnings end-to-end.

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
FROM generate_series(1, 150000) g;

-- Int-PK minority (the field fleet mixes bigint + int).
CREATE TABLE ext.int_pk_dual_ts (
    id INT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP
);
INSERT INTO ext.int_pk_dual_ts (id, payload, updated_at)
SELECT g, 'row' || g, now() - (g || ' minutes')::interval
FROM generate_series(1, 150000) g;

-- Sparse key: id span vastly exceeds the row count (gappy field ids). Range
-- chunking would explode into near-empty windows — the sparse-guard shape.
CREATE TABLE ext.sparse_key (
    id BIGINT PRIMARY KEY,
    payload INT NOT NULL
);
INSERT INTO ext.sparse_key (id, payload)
-- g::bigint: g*1000000 overflows INT past g≈2147; the key must stay BIGINT-wide.
SELECT 1 + g::bigint * 1000000, g FROM generate_series(0, 149999) g;

-- Scale-0 DECIMAL PK (Oracle/ERP-migration shape). NOT integer-family, so an
-- explicit range chunk_column on it must LOUDLY bail (#103), not silently drop
-- fractional rows. `chunk_by_key: dkey` (keyset) IS accepted for it.
CREATE TABLE ext.decimal_key (
    dkey DECIMAL(15,0) PRIMARY KEY,
    payload TEXT NOT NULL
);
INSERT INTO ext.decimal_key (dkey, payload)
SELECT g, 'row' || g FROM generate_series(1, 150000) g;

-- Keyless, cursorless table → full-mode fallback (no chunk column, no cursor).
CREATE TABLE ext.no_pk_no_ts (
    label TEXT NOT NULL,
    amount INT NOT NULL
);
INSERT INTO ext.no_pk_no_ts (label, amount)
SELECT 'label' || g, g % 100 FROM generate_series(1, 150000) g;

-- ── The MESSY reality (distilled from a real stuck run's state DB) ───────────
-- The field DB is NOT all clean `id` PKs. Real shapes below.

-- HISTORY/VERSION table: NO `id`, keyed by a non-unique, non-PK integer `ref_id`
-- (a foreign-key back to the parent). The planner range-CHUNKS on `ref_id` (not
-- keyset — ref_id is not a unique key). In the field these `*_version` tables
-- were the parallel-checkpoint failures (a heavy chunk hit the statement timeout).
-- Messy columns: a duplicated concept, `_cur` suffixes, mixed types — as found.
CREATE TABLE ext.ref_id_history (
    ref_id BIGINT NOT NULL,
    field TEXT,
    field_cur TEXT,
    sign SMALLINT NOT NULL DEFAULT 1,
    status TEXT,
    amount_cur TEXT,
    purchase_type TEXT
);
CREATE INDEX ix_ref_id_history_ref_id ON ext.ref_id_history (ref_id);
INSERT INTO ext.ref_id_history (ref_id, field, field_cur, sign, status, amount_cur, purchase_type)
SELECT (g % 400) + 1, 'f' || g, 'EUR', CASE WHEN g % 2 = 0 THEN 1 ELSE -1 END,
       (ARRAY['pending','done','void'])[1 + g % 3], 'USD', (ARRAY['a','b'])[1 + g % 2]
FROM generate_series(1, 150000) g;

-- Keyed by `order_id` (NOT `id`): a unique index makes it keyset-able on a
-- non-`id` key. Wide-ish string columns (md5, currency, subid) — the shape of a
-- full/keyset import table that has no column literally named `id`.
CREATE TABLE ext.order_keyed (
    order_id BIGINT NOT NULL,
    md5 CHAR(32) NOT NULL,
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL,
    subid TEXT,
    advcampaign_id INT
);
CREATE UNIQUE INDEX ux_order_keyed_order_id ON ext.order_keyed (order_id);
INSERT INTO ext.order_keyed (order_id, md5, currency, status, subid, advcampaign_id)
SELECT g, md5(g::text), (ARRAY['EUR','USD','PLN'])[1 + g % 3],
       (ARRAY['approved','declined'])[1 + g % 2], 'sub' || (g % 50), (g % 900) + 100
FROM generate_series(1, 150000) g;

-- HEAP: no PK, no index at all → full mode is the only safe strategy.
CREATE TABLE ext.heap_no_key (
    payload TEXT NOT NULL,
    n INT NOT NULL
);
INSERT INTO ext.heap_no_key (payload, n)
SELECT repeat('x', 20 + g % 50), g FROM generate_series(1, 150000) g;

ANALYZE ext.bigint_pk_dual_ts;
ANALYZE ext.int_pk_dual_ts;
ANALYZE ext.sparse_key;
ANALYZE ext.decimal_key;
ANALYZE ext.no_pk_no_ts;
ANALYZE ext.ref_id_history;
ANALYZE ext.order_keyed;
ANALYZE ext.heap_no_key;
