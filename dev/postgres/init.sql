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
