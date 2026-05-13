CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT,
    balance DECIMAL(12,2),
    is_active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    notes TEXT,
    ordered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSON,
    ip_address VARCHAR(45),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_events_created_at ON events(created_at);
CREATE INDEX idx_events_user_id ON events(user_id);

-- Wide table, NO index on created_at -- intentionally degraded
CREATE TABLE page_views (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    custom_props JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Heavy-text table, NO index on created_at -- worst case for memory
CREATE TABLE content_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    title TEXT NOT NULL,
    body LONGTEXT NOT NULL,
    raw_html LONGTEXT NOT NULL,
    metadata JSON,
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
    published_at DATETIME,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    extra_data JSON
);

-- Sparse BIGINT ids — chunked mode demo (MySQL 8+ window functions)
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
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    updated_at DATETIME NULL,                                  -- primary
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP     -- fallback, never NULL
);

CREATE INDEX idx_orders_coalesce_updated_at ON orders_coalesce(updated_at);
CREATE INDEX idx_orders_coalesce_created_at ON orders_coalesce(created_at);

-- ─── Type-matrix demo (parquet → BigQuery / tooling checks) ───
-- See dev/workbench/mysql_type_matrix.yaml and dev/bigquery/type_matrix_bigquery.md
CREATE TABLE rivet_type_matrix (
    id BIGINT PRIMARY KEY,
    label VARCHAR(200) NOT NULL,
    amount DECIMAL(18, 2) NULL,
    fee DECIMAL(18, 6) NULL,
    created_at_dt DATETIME(6) NOT NULL,
    created_at_ts TIMESTAMP(6) NOT NULL,
    raw_bytes BINARY(4) NOT NULL,
    uid VARCHAR(36) NOT NULL,
    extras JSON NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO rivet_type_matrix (
    id, label, amount, fee, created_at_dt, created_at_ts, raw_bytes, uid, extras
) VALUES
  (1, 'payments-like', 0.10, 0.000001,
      '2035-08-07 09:08:07.987654',
      '2035-08-07 09:08:07.987654',
      UNHEX('00ff0123'),
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011',
      JSON_OBJECT('tier', 'gold', 'n', 1)),
  (2, 'payments-like', 0.20, 0.000002,
      '2019-02-03 03:07:06.554433',
      '2019-02-03 03:07:06.554433',
      UNHEX('deadbeef'),
      'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022',
      JSON_ARRAY('a', 'b')),
  (3, 'payments-like', 999999999999.99, 10.123456,
      '2020-01-15 00:00:00.000001',
      '2020-01-15 00:00:00.000001',
      UNHEX('cafe'),
      'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033',
      CAST('{"big":true}' AS JSON)),
  (4, 'payments-like', -100.05, -0.123456,
      '2021-06-30 12:59:59.999999',
      '2021-06-30 12:59:59.999999',
      UNHEX('00'),
      'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380044',
      CAST('{}' AS JSON));

-- ─── Full type-matrix: covers every Rivet-mapped MySQL type ────────────────
-- See dev/workbench/mysql_type_matrix.yaml and tests/live_type_golden.rs
CREATE TABLE rivet_type_matrix_full (
    id            BIGINT PRIMARY KEY,
    flag          BOOLEAN,                         -- TINYINT(1) → Bool
    bit1_col      BIT(1),                          -- BIT(1)     → Bool
    bit8_col      BIT(8),                          -- BIT(8)     → Int64
    tiny_col      TINYINT,                         -- TINYINT    → Int16
    date_col      DATE,                            -- DATE       → Date32
    time_col      TIME(6),                         -- TIME(6)    → Time64(µs)
    year_col      YEAR,                            -- YEAR       → Int16
    enum_col      ENUM('a', 'b', 'c'),             -- ENUM       → Utf8
    varbinary_col VARBINARY(4),                    -- VARBINARY  → Binary
    blob_col      BLOB                             -- BLOB       → Binary
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO rivet_type_matrix_full
    (id, flag, bit1_col, bit8_col, tiny_col, date_col, time_col, year_col, enum_col, varbinary_col, blob_col)
VALUES
  (1, TRUE,  b'1', b'10101010',  127, '2024-03-15', '14:30:00.123456', 2024, 'b', 0xDEADBEEF, 0x0102030405),
  (2, FALSE, b'0', b'00000001', -128, '1970-01-01', '00:00:00.000000', 2000, 'a', 0x00000000, 0xCAFE),
  (3, NULL,  NULL, NULL,           0, '2000-02-29', '23:59:59.999999', NULL, NULL, NULL,       NULL);
