-- Minimal E2E seed for PostgreSQL.
-- Populates only the tables required by run_e2e.sh (users, orders, events).
-- All other tables are left empty; `rivet init` only reads metadata, not rows.
--
-- Usage:
--   psql "$DATABASE_URL" -f dev/e2e/seed_pg.sql
--
-- Row counts (tuned so the split test produces >=2 files even at batch_size=500
-- + max_file_size=5KB, and chunked tests get at least one full chunk):
--   users  : 500
--   orders : 2 500  (5 per user)
--   events : 5 000  (10 per user)

TRUNCATE events, orders, users RESTART IDENTITY CASCADE;

-- ── users ────────────────────────────────────────────────────────────────────

INSERT INTO users (name, email, age, balance, is_active, created_at, updated_at)
SELECT
    'User ' || i,
    'user' || i || '@example.com',
    18 + (i % 60),
    round((100 + (i % 9000))::numeric, 2),
    (i % 5 <> 0),
    now() - ((500 - i) * interval '1 hour'),
    now() - ((500 - i) * interval '1 hour')
FROM generate_series(1, 500) AS i;

-- ── orders ───────────────────────────────────────────────────────────────────

INSERT INTO orders (user_id, product, quantity, price, status, ordered_at, updated_at)
SELECT
    1 + ((i - 1) / 5),
    (ARRAY['Widget','Gadget','Doohickey','Thingamajig','Whatchamacallit'])[(i % 5) + 1],
    1 + (i % 10),
    round((9.99 + (i % 990))::numeric, 2),
    (ARRAY['pending','paid','shipped','delivered','cancelled'])[(i % 5) + 1],
    now() - ((2500 - i) * interval '1 minute'),
    now() - ((2500 - i) * interval '1 minute')
FROM generate_series(1, 2500) AS i;

-- ── events ───────────────────────────────────────────────────────────────────

INSERT INTO events (user_id, event_type, ip_address, created_at)
SELECT
    1 + ((i - 1) / 10),
    (ARRAY['click','view','purchase','signup','logout'])[(i % 5) + 1],
    '10.' || (i % 255) || '.' || ((i * 7) % 255) || '.1',
    now() - ((5000 - i) * interval '1 minute')
FROM generate_series(1, 5000) AS i;
