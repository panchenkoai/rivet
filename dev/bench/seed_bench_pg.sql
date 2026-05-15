-- Benchmark seed for PostgreSQL — Phase 2 stabilization harness.
--
-- Creates six dataset profiles covering the stabilization benchmark matrix
-- (roadmap §4.1).  Each table is self-contained; run this script once and
-- leave it in place across benchmark runs.
--
-- Usage:
--   psql "$POSTGRES_URL" -f dev/bench/seed_bench_pg.sql
--
-- Tables created:
--   bench_narrow  — 500 000 rows, numeric/date (baseline throughput)
--   bench_wide    — 100 000 rows, 10 text columns 200 B each (memory pressure)
--   bench_json    — 50 000 rows, JSON payload 1–4 KB (CPU/string pressure)
--   bench_hc      — 200 000 rows, UUID + email (quality uniqueness RAM risk)
--   bench_decimal — 200 000 rows, NUMERIC columns (type conversion pressure)
--   bench_sparse  — 10 000 rows, many nullable columns (null-ratio quality)

-- ── bench_narrow ─────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bench_narrow;
CREATE TABLE bench_narrow (
    id          BIGINT PRIMARY KEY,
    score       DOUBLE PRECISION NOT NULL,
    category    SMALLINT NOT NULL,
    flag        BOOLEAN NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

INSERT INTO bench_narrow (id, score, category, flag, created_at)
SELECT
    i,
    i * 1.23456,
    (i % 100)::SMALLINT,
    (i % 3 = 0),
    now() - (500000 - i) * interval '1 second'
FROM generate_series(1, 500000) i;

-- ── bench_wide ───────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bench_wide;
CREATE TABLE bench_wide (
    id         BIGINT PRIMARY KEY,
    col_a      TEXT NOT NULL,
    col_b      TEXT NOT NULL,
    col_c      TEXT NOT NULL,
    col_d      TEXT NOT NULL,
    col_e      TEXT NOT NULL,
    col_f      TEXT NOT NULL,
    col_g      TEXT NOT NULL,
    col_h      TEXT NOT NULL,
    col_i      TEXT NOT NULL,
    col_j      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO bench_wide (id, col_a, col_b, col_c, col_d, col_e,
                         col_f, col_g, col_h, col_i, col_j, updated_at)
SELECT
    i,
    repeat(chr(65 + (i % 26)), 200),
    repeat(chr(66 + (i % 26)), 200),
    repeat(chr(67 + (i % 26)), 200),
    repeat(chr(68 + (i % 26)), 200),
    repeat(chr(69 + (i % 26)), 200),
    repeat(chr(70 + (i % 26)), 200),
    repeat(chr(71 + (i % 26)), 200),
    repeat(chr(72 + (i % 26)), 200),
    repeat(chr(73 + (i % 26)), 200),
    repeat(chr(74 + (i % 26)), 200),
    now() - (100000 - i) * interval '1 second'
FROM generate_series(1, 100000) i;

-- ── bench_json ───────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bench_json;
CREATE TABLE bench_json (
    id         BIGINT PRIMARY KEY,
    payload    JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO bench_json (id, payload, updated_at)
SELECT
    i,
    jsonb_build_object(
        'id',       i,
        'name',     'user_' || i,
        'email',    'u' || i || '@example.com',
        'tags',     jsonb_build_array('tag_' || (i%10), 'cat_' || (i%5)),
        'meta',     jsonb_build_object(
                        'score',   round((random()*1000)::numeric, 2),
                        'active',  (i%3 <> 0),
                        'region',  (ARRAY['us-east','us-west','eu-west','ap-south'])[1 + i%4],
                        'payload', repeat('x', 200 + (i % 800))
                    )
    ),
    now() - (50000 - i) * interval '1 second'
FROM generate_series(1, 50000) i;

-- ── bench_hc — high cardinality (quality uniqueness RAM risk) ─────────────────

DROP TABLE IF EXISTS bench_hc;
CREATE TABLE bench_hc (
    id         BIGINT PRIMARY KEY,
    uuid_col   UUID NOT NULL DEFAULT gen_random_uuid(),
    email      TEXT NOT NULL,
    session_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO bench_hc (id, uuid_col, email, session_id, updated_at)
SELECT
    i,
    gen_random_uuid(),
    'user' || i || '@bench.example.com',
    md5(i::text || 'session'),
    now() - (200000 - i) * interval '1 second'
FROM generate_series(1, 200000) i;

-- ── bench_decimal — type conversion pressure ──────────────────────────────────

DROP TABLE IF EXISTS bench_decimal;
CREATE TABLE bench_decimal (
    id          BIGINT PRIMARY KEY,
    price       NUMERIC(18, 4) NOT NULL,
    qty         NUMERIC(10, 2) NOT NULL,
    discount    NUMERIC(5, 4) NOT NULL,
    total       NUMERIC(20, 4) NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

INSERT INTO bench_decimal (id, price, qty, discount, total, updated_at)
SELECT
    i,
    round((0.01 + (i % 99999) * 0.01)::numeric, 4),
    round((1 + (i % 9999) * 0.1)::numeric, 2),
    round((0.0001 + (i % 9999) * 0.0001)::numeric, 4),
    round((0.01 + (i % 99999) * 0.01 * (1 + (i % 9999) * 0.1))::numeric, 4),
    now() - (200000 - i) * interval '1 second'
FROM generate_series(1, 200000) i;

-- ── bench_sparse — null-ratio quality pressure ────────────────────────────────

DROP TABLE IF EXISTS bench_sparse;
CREATE TABLE bench_sparse (
    id          BIGINT PRIMARY KEY,
    val_a       TEXT,
    val_b       TEXT,
    val_c       TEXT,
    val_d       DOUBLE PRECISION,
    val_e       DOUBLE PRECISION,
    updated_at  TIMESTAMPTZ NOT NULL
);

INSERT INTO bench_sparse (id, val_a, val_b, val_c, val_d, val_e, updated_at)
SELECT
    i,
    CASE WHEN i % 10 = 0 THEN 'x' ELSE NULL END,
    CASE WHEN i % 5  = 0 THEN 'y' ELSE NULL END,
    CASE WHEN i % 3  = 0 THEN 'z' ELSE NULL END,
    CASE WHEN i % 4  = 0 THEN i * 1.1 ELSE NULL END,
    CASE WHEN i % 7  = 0 THEN i * 2.2 ELSE NULL END,
    now() - (10000 - i) * interval '1 second'
FROM generate_series(1, 10000) i;
