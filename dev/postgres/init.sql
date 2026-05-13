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
