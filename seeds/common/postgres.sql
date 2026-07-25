-- seeds/common/postgres.sql — SELF-CONTAINED canonical seed (DDL + data + garbage).
-- Assembled from dev/postgres/init.sql (DDL) + a fast-profile fill + dev/garbage/postgres.sql.
-- Idempotent-ish: run against a fresh 'rivet' DB. RIVET_SEED_I_KNOW not required (plain SQL).

-- Idempotent: drop base tables (+ dependent views) before recreating. The
-- garbage section drops its own `ext` schema (CASCADE).
DROP VIEW IF EXISTS orders_sparse_for_export CASCADE;
DROP TABLE IF EXISTS content_items, page_views, events, orders, users,
                     orders_sparse, orders_coalesce,
                     rivet_type_matrix, rivet_type_matrix_full CASCADE;
DROP TYPE IF EXISTS rivet_status CASCADE;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT,
    balance NUMERIC(12,2),
    is_active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    notes TEXT,
    ordered_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    event_type VARCHAR(50) NOT NULL,
    payload JSONB,
    ip_address VARCHAR(45),
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_events_created_at ON events(created_at);
CREATE INDEX idx_events_user_id ON events(user_id);

-- Wide table, NO index on created_at -- intentionally degraded
CREATE TABLE page_views (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(36) NOT NULL,
    user_id INT,
    url TEXT NOT NULL,
    referrer TEXT,
    user_agent TEXT,
    ip_address VARCHAR(45),
    country_code CHAR(2),
    region VARCHAR(100),
    city VARCHAR(100),
    device_type VARCHAR(20),
    browser VARCHAR(50),
    os VARCHAR(50),
    screen_width INT,
    screen_height INT,
    viewport_width INT,
    viewport_height INT,
    page_load_ms INT,
    dom_ready_ms INT,
    time_on_page_ms INT,
    scroll_depth_pct SMALLINT,
    click_count SMALLINT,
    is_bounce BOOLEAN NOT NULL DEFAULT false,
    utm_source VARCHAR(100),
    utm_medium VARCHAR(100),
    utm_campaign VARCHAR(200),
    utm_term VARCHAR(200),
    utm_content VARCHAR(200),
    custom_props JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Heavy-text table, NO index on created_at -- worst case for memory
CREATE TABLE content_items (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    raw_html TEXT NOT NULL,
    metadata JSONB,
    tags TEXT,
    author_name VARCHAR(100) NOT NULL,
    author_email VARCHAR(200) NOT NULL,
    source_url TEXT,
    category VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    priority INT NOT NULL DEFAULT 0,
    view_count INT NOT NULL DEFAULT 0,
    comment_count INT NOT NULL DEFAULT 0,
    word_count INT NOT NULL DEFAULT 0,
    language CHAR(2) NOT NULL DEFAULT 'en',
    published_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    extra_data JSONB
);

-- Sparse BIGINT ids (large min..max gap, few rows) — for chunked mode / rivet check demos
CREATE TABLE orders_sparse (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);

CREATE OR REPLACE VIEW orders_sparse_for_export AS
SELECT
    id,
    payload,
    ROW_NUMBER() OVER (ORDER BY id) AS chunk_rownum
FROM orders_sparse;

-- Composite cursor fixture (ADR-0007): some rows have NULL in the primary
-- `updated_at` column, forcing `COALESCE(updated_at, created_at)` progression.
CREATE TABLE orders_coalesce (
    id BIGSERIAL PRIMARY KEY,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    updated_at TIMESTAMP,                     -- NULL-able: primary cursor column
    created_at TIMESTAMP NOT NULL DEFAULT now() -- never NULL: fallback
);

CREATE INDEX idx_orders_coalesce_updated_at ON orders_coalesce(updated_at);
CREATE INDEX idx_orders_coalesce_created_at ON orders_coalesce(created_at);

-- ─── Type-matrix demo: golden-style columns for parquet → warehouse checks ───
-- See dev/workbench/pg_type_matrix.yaml and dev/bigquery/type_matrix_bigquery.md
CREATE TABLE rivet_type_matrix (
    id BIGINT PRIMARY KEY,
    label TEXT NOT NULL,
    amount NUMERIC(18, 2),
    fee NUMERIC(18, 6),
    created_at TIMESTAMP NOT NULL,
    created_at_tz TIMESTAMPTZ NOT NULL,
    raw_bytes BYTEA NOT NULL,
    uid UUID NOT NULL,
    attrs JSONB
);

INSERT INTO rivet_type_matrix (
    id, label, amount, fee, created_at, created_at_tz, raw_bytes, uid, attrs
) VALUES
  (1, 'payments-like', 0.10, 0.000001,
      TIMESTAMP '2035-08-07 09:08:07.987654',
      TIMESTAMPTZ '2035-08-07 09:08:07.987654Z',
      '\x00ff012345'::bytea,
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011'::uuid,
      '{"tier":"gold","n":1}'::jsonb),
  (2, 'payments-like', 0.20, 0.000002,
      TIMESTAMP '2019-02-03 03:07:06.554433',
      TIMESTAMPTZ '2019-02-03 08:07:06.554433+05',
      '\xdeadbeef'::bytea,
      'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022'::uuid,
      '["a","b"]'::jsonb),
  (3, 'payments-like', 999999999999.99, 10.123456,
      TIMESTAMP '2020-01-15 00:00:00.000001',
      TIMESTAMPTZ '2020-01-15 00:00:00.000001+00',
      '\xcafe'::bytea,
      'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033'::uuid,
      '{"big":true}'::jsonb),
  (4, 'payments-like', -100.05, -0.123456,
      TIMESTAMP '2021-06-30 12:59:59.999999',
      TIMESTAMPTZ '2021-06-30 12:59:59.999999+00',
      '\x00'::bytea,
      'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380044'::uuid,
      '{}'::jsonb);

-- ─── Full type-matrix: covers every Rivet-mapped PG type ───────────────────
-- See dev/workbench/pg_type_matrix.yaml and tests/live_type_golden.rs
CREATE TYPE rivet_status AS ENUM ('active', 'inactive', 'pending');

CREATE TABLE rivet_type_matrix_full (
    id           BIGINT PRIMARY KEY,
    flag         BOOLEAN,
    int2_col     SMALLINT,
    int4_col     INTEGER,
    float4_col   REAL,
    date_col     DATE,
    time_col     TIME,
    interval_col INTERVAL,
    enum_col     rivet_status,
    tags         TEXT[],
    nums         INTEGER[]
);

INSERT INTO rivet_type_matrix_full VALUES
  (1, TRUE,  32767,       2147483647,  3.14::real, '2024-03-15', '14:30:00.123456',  INTERVAL '1 year 2 months 3 days', 'active',   ARRAY['alpha','beta'], ARRAY[1,2,3]),
  (2, FALSE, -32768,     -2147483648, -1.5::real,  '1970-01-01', '00:00:00',         INTERVAL '-1 year',                'inactive', ARRAY['gamma'],        ARRAY[42]),
  (3, NULL,  NULL,        0,           0.0::real,  '2000-02-29', '23:59:59.999999',  INTERVAL '0',                      NULL,       ARRAY[]::text[],       NULL);

-- ============================================================================
-- DATA FILL — fast-profile reproduction of the Rust seeder (src/bin/seed/fast.rs).
-- generate_series is the SQL analog of the seeder's `fast` path (deterministic,
-- no Rust binary needed). Row counts are dogfood-sized (enough for chunk_size
-- 1000 to make many windows); edit the N in each generate_series to scale toward
-- the seeder's default (100k users / 1M orders / 5M events / 2M page_views /
-- 200k content_items).
-- ============================================================================

INSERT INTO users (name, email, age, balance, is_active, bio, created_at, updated_at)
SELECT 'User '||i, 'user'||i||'@example.com', 18+(i%48), round((random()*200000)::numeric,2),
       (random()>0.1), CASE WHEN random()>0.3 THEN 'seed bio '||i ELSE NULL END,
       timestamp '2023-01-01'+((i%730)*interval '1 hour'),
       timestamp '2023-01-01'+((i%910)*interval '1 hour')
FROM generate_series(1, 2000) AS i;

INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at)
SELECT 1+((i-1)/10),
       (ARRAY['MacBook Pro 16"','Dell XPS 15','ThinkPad X1 Carbon','Surface Laptop','Ergonomic Chair',
              'Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'])[(i%10)+1],
       1+(i%10), round((5+(i%4995))::numeric,2),
       (ARRAY['pending','shipped','delivered','cancelled'])[(i%4)+1],
       CASE WHEN i%3=0 THEN 'note '||i ELSE NULL END,
       timestamp '2023-01-01'+((i%730)*interval '1 minute'),
       timestamp '2023-01-01'+((i%760)*interval '1 minute')
FROM generate_series(1, 20000) AS i;

INSERT INTO events (user_id, event_type, payload, ip_address, created_at)
SELECT 1+((i-1)/25),
       (ARRAY['login','logout','page_view','purchase','signup','settings_change',
              'password_reset','search','export','api_call'])[(i%10)+1],
       jsonb_build_object('seed',true,'i',i),
       '10.'||(i%255)::text||'.'||((i*7)%255)::text||'.1',
       timestamp '2023-01-01'+((i%730)*interval '1 minute')
FROM generate_series(1, 50000) AS i;

INSERT INTO page_views (
    session_id, user_id, url, referrer, user_agent, ip_address,
    country_code, region, city, device_type, browser, os,
    screen_width, screen_height, viewport_width, viewport_height,
    page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, click_count,
    is_bounce, utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    custom_props, created_at)
SELECT lpad(to_hex(i),32,'0'),
       CASE WHEN random()>0.3 THEN 1+(i%2000) ELSE NULL END,
       '/page/'||(i%26),
       CASE WHEN random()>0.4 THEN 'https://google.com' ELSE NULL END,
       'Mozilla/5.0 seed',
       '192.168.'||(i%255)::text||'.'||((i*3)%254+1)::text,
       (ARRAY['US','GB','DE','FR','CA'])[(i%5)+1],
       'Region '||(i%10), 'City '||(i%20),
       (ARRAY['desktop','mobile','tablet'])[(i%3)+1],
       (ARRAY['chrome','firefox','safari'])[(i%3)+1],
       (ARRAY['macOS','Windows','Linux'])[(i%3)+1],
       1280+(i%2560), 720+(i%1440), 800+(i%1200), 600+(i%900),
       100+(i%5000), 50+(i%2500), 500+(i%60000), (i%101)::smallint, (i%51)::smallint,
       (i%3=0),
       CASE WHEN i%4=0 THEN 'google' ELSE NULL END,
       CASE WHEN i%4=0 THEN 'cpc' ELSE NULL END,
       CASE WHEN i%8=0 THEN 'spring_sale' ELSE NULL END,
       NULL, NULL,
       CASE WHEN i%5=0 THEN jsonb_build_object('seed',true) ELSE NULL END,
       timestamp '2023-01-01'+((i%730)*interval '1 minute')
FROM generate_series(1, 20000) AS i;

INSERT INTO content_items (
    title, body, raw_html, metadata, tags, author_name, author_email,
    source_url, category, status, priority, view_count, comment_count,
    word_count, language, published_at, updated_at, created_at, extra_data)
SELECT 'Seed title '||i, repeat('lorem ipsum ',200), '<p>'||repeat('lorem ipsum ',200)||'</p>',
       jsonb_build_object('seed',true,'i',i), 'rust,postgres,data',
       'Author '||(i%1000), 'author'||(i%1000)||'@example.com',
       'https://blog.example.com/posts/'||i,
       (ARRAY['engineering','product','tutorial'])[(i%3)+1],
       (ARRAY['draft','review','published'])[(i%3)+1],
       (i%5), (i%100000), (i%500), 200, 'en',
       CASE WHEN i%3<>0 THEN timestamp '2024-01-01'+((i%365)*interval '1 day') ELSE NULL END,
       timestamp '2024-01-01'+((i%400)*interval '1 day'),
       timestamp '2023-01-01'+((i%730)*interval '1 day'),
       jsonb_build_object('revisions',1)
FROM generate_series(1, 5000) AS i;

-- orders_sparse: a huge min..max span over very few rows (id = 1 + i*gap) — the
-- sparse-chunk footgun fixture. gap 2,000,000.
INSERT INTO orders_sparse (id, payload)
SELECT 1 + i*2000000, 's'||i FROM generate_series(0, 2) AS i;

-- orders_coalesce: ~35% NULL updated_at (COALESCE composite-cursor fixture,
-- ADR-0007). created_at 2024-2025 < updated_at 2025-2026.
INSERT INTO orders_coalesce (product, quantity, price, updated_at, created_at)
SELECT (ARRAY['MacBook Pro 16"','Dell XPS 15','ThinkPad X1 Carbon','Surface Laptop','Ergonomic Chair'])[(i%5)+1],
       1+(i%10), round((5+(i%4995))::numeric,2),
       CASE WHEN random()<0.35 THEN NULL ELSE timestamp '2025-01-01'+((i%365)*interval '1 day') END,
       timestamp '2024-01-01'+((i%365)*interval '1 day')
FROM generate_series(1, 2000) AS i;

-- ============================================================================
-- GARBAGE PROFILE (ext.* schema) — the anonymized field-DB shapes.
-- ============================================================================
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
-- Also the DOMINANT field profile: money as DECIMAL(11,4) VALUE columns (the real
-- DB carried 177 of them across cart/earning/subtotal/total/amount) — init emits
-- an explicit `columns:` override for each, so this exercises that decimal path;
-- `version` + many rows per `ref_id` is the version-table heavy-chunk shape.
CREATE TABLE ext.ref_id_history (
    ref_id BIGINT NOT NULL,
    version INT NOT NULL,
    field TEXT,
    field_cur TEXT,
    sign SMALLINT NOT NULL DEFAULT 1,
    status TEXT,
    cart DECIMAL(11,4),
    earning DECIMAL(11,4),
    subtotal DECIMAL(11,4),
    total DECIMAL(11,4),
    amount_cur TEXT,
    user_cur CHAR(3),
    purchase_type TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP
);
CREATE INDEX ix_ref_id_history_ref_id ON ext.ref_id_history (ref_id);
INSERT INTO ext.ref_id_history (ref_id, version, field, field_cur, sign, status, cart, earning, subtotal, total, amount_cur, user_cur, purchase_type, created_at, updated_at)
SELECT (g % 400) + 1, (g / 400) + 1, 'f' || g, 'EUR', CASE WHEN g % 2 = 0 THEN 1 ELSE -1 END,
       (ARRAY['pending','done','void'])[1 + g % 3],
       (g % 900000 + 1) / 100.0, (g % 500000 + 1) / 100.0, (g % 300000 + 1) / 100.0, (g % 1200000 + 1) / 100.0,
       'USD', (ARRAY['EUR','USD','PLN'])[1 + g % 3], (ARRAY['a','b'])[1 + g % 2],
       now() - (g || ' minutes')::interval, now() - (g || ' seconds')::interval
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
