//! Server-side bulk generation via `generate_series` (Postgres) or recursive CTE (MySQL).
//! Typically 10–50× faster than client-side INSERT loops at million-row scale.

use std::time::Instant;

use anyhow::{Context, Result};
use postgres::Client;

use crate::args::Args;
use crate::insert;

pub fn seed_postgres(args: &Args) -> Result<()> {
    let mut client = Client::connect(&args.pg_url, postgres::NoTls)
        .context("failed to connect to PostgreSQL")?;

    insert::ensure_orders_sparse_pg(&mut client)?;
    insert::ensure_orders_coalesce_pg(&mut client)?;

    client.batch_execute(
        "SET synchronous_commit = off;
         TRUNCATE orders_coalesce, orders_sparse, content_items, page_views, events, orders, users RESTART IDENTITY CASCADE",
    )?;

    let t = Instant::now();
    client.batch_execute(&pg_users_sql(args.users))?;
    println!(
        "  users:      {:>10} rows in {:.1}s",
        args.users,
        t.elapsed().as_secs_f64()
    );

    let total_orders = args.planned_orders();
    if total_orders > 0 {
        let t = Instant::now();
        client.batch_execute(&pg_orders_sql(total_orders, args.orders_per_user))?;
        println!(
            "  orders:     {:>10} rows in {:.1}s",
            total_orders,
            t.elapsed().as_secs_f64()
        );
    }

    let total_events = args.planned_events();
    if total_events > 0 {
        let t = Instant::now();
        client.batch_execute(&pg_events_sql(total_events, args.events_per_user))?;
        println!(
            "  events:     {:>10} rows in {:.1}s",
            total_events,
            t.elapsed().as_secs_f64()
        );
    }

    if args.page_views > 0 {
        let t = Instant::now();
        client.batch_execute(&pg_page_views_sql(args.page_views, args.users))?;
        println!(
            "  page_views: {:>10} rows in {:.1}s",
            args.page_views,
            t.elapsed().as_secs_f64()
        );
    }

    if args.content_items > 0 {
        let t = Instant::now();
        client.batch_execute(&pg_content_items_sql(args.content_items))?;
        println!(
            "  content:    {:>10} rows in {:.1}s",
            args.content_items,
            t.elapsed().as_secs_f64()
        );
    }

    if args.sparse_chunk_demo {
        let t = Instant::now();
        let n = insert::seed_pg_orders_sparse_fill(&mut client, args)?;
        println!(
            "  orders_sparse: {:>10} rows in {:.1}s",
            n,
            t.elapsed().as_secs_f64()
        );
    }

    if args.coalesce_rows > 0 {
        let t = Instant::now();
        let n = insert::seed_pg_orders_coalesce(&mut client, args)?;
        println!(
            "  orders_coalesce: {:>8} rows in {:.1}s (NULL ratio ~{:.0}%)",
            n,
            t.elapsed().as_secs_f64(),
            args.coalesce_null_ratio * 100.0,
        );
    }

    Ok(())
}

pub fn seed_mysql(args: &Args) -> Result<()> {
    use mysql::prelude::*;

    let pool = mysql::Pool::new(mysql::Opts::from_url(&args.mysql_url)?)?;
    let mut conn = pool.get_conn()?;

    insert::ensure_orders_sparse_mysql(&mut conn)?;
    insert::ensure_orders_coalesce_mysql(&mut conn)?;

    // Disable FK + uniqueness checks for the whole bulk load.  We generate
    // child rows from the same deterministic key space as parents, so the
    // data is consistent by construction.  Re-enabled at the end so the
    // session leaves the DB in normal state.
    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0")?;
    conn.query_drop("SET UNIQUE_CHECKS = 0")?;
    // Chunked CTE driver caps recursion at MYSQL_CHUNK_ROWS per statement, so
    // we only need to lift the default 1000-row limit modestly.
    conn.query_drop(format!(
        "SET SESSION cte_max_recursion_depth = {}",
        MYSQL_CHUNK_ROWS + 16
    ))?;
    conn.query_drop("TRUNCATE TABLE orders_coalesce")?;
    conn.query_drop("TRUNCATE TABLE orders_sparse")?;
    conn.query_drop("TRUNCATE TABLE content_items")?;
    conn.query_drop("TRUNCATE TABLE page_views")?;
    conn.query_drop("TRUNCATE TABLE events")?;
    conn.query_drop("TRUNCATE TABLE orders")?;
    conn.query_drop("TRUNCATE TABLE users")?;

    let t = Instant::now();
    run_chunked(&mut conn, args.users, |start, end| {
        mysql_users_sql(start, end)
    })?;
    println!(
        "  users:      {:>10} rows in {:.1}s",
        args.users,
        t.elapsed().as_secs_f64()
    );

    let total_orders = args.planned_orders();
    if total_orders > 0 {
        let t = Instant::now();
        let oppu = args.orders_per_user.max(1);
        let users_cap = args.users;
        run_chunked(&mut conn, total_orders, |start, end| {
            mysql_orders_sql(start, end, oppu, users_cap)
        })?;
        println!(
            "  orders:     {:>10} rows in {:.1}s",
            total_orders,
            t.elapsed().as_secs_f64()
        );
    }

    let total_events = args.planned_events();
    if total_events > 0 {
        let t = Instant::now();
        let eppu = args.events_per_user.max(1);
        let users_cap = args.users;
        run_chunked(&mut conn, total_events, |start, end| {
            mysql_events_sql(start, end, eppu, users_cap)
        })?;
        println!(
            "  events:     {:>10} rows in {:.1}s",
            total_events,
            t.elapsed().as_secs_f64()
        );
    }

    if args.page_views > 0 {
        let t = Instant::now();
        let users_cap = args.users.max(1);
        run_chunked(&mut conn, args.page_views, |start, end| {
            mysql_page_views_sql(start, end, users_cap)
        })?;
        println!(
            "  page_views: {:>10} rows in {:.1}s",
            args.page_views,
            t.elapsed().as_secs_f64()
        );
    }

    if args.content_items > 0 {
        let t = Instant::now();
        run_chunked(&mut conn, args.content_items, |start, end| {
            mysql_content_items_sql(start, end)
        })?;
        println!(
            "  content:    {:>10} rows in {:.1}s",
            args.content_items,
            t.elapsed().as_secs_f64()
        );
    }

    if args.sparse_chunk_demo {
        let t = Instant::now();
        let n = insert::seed_mysql_orders_sparse_fill(&mut conn, args)?;
        println!(
            "  orders_sparse: {:>10} rows in {:.1}s",
            n,
            t.elapsed().as_secs_f64()
        );
    }

    if args.coalesce_rows > 0 {
        let t = Instant::now();
        let n = insert::seed_mysql_orders_coalesce(&mut conn, args)?;
        println!(
            "  orders_coalesce: {:>8} rows in {:.1}s (NULL ratio ~{:.0}%)",
            n,
            t.elapsed().as_secs_f64(),
            args.coalesce_null_ratio * 100.0,
        );
    }

    // Restore safety checks for any follow-up queries on the same connection.
    conn.query_drop("SET FOREIGN_KEY_CHECKS = 1")?;
    conn.query_drop("SET UNIQUE_CHECKS = 1")?;

    Ok(())
}

fn pg_users_sql(users: usize) -> String {
    format!(
        r#"
INSERT INTO users (name, email, age, balance, is_active, bio, created_at, updated_at)
SELECT
    'User ' || i,
    'user' || i || '@example.com',
    18 + (i % 48),
    round((random() * 200000)::numeric, 2),
    (random() > 0.1),
    CASE WHEN random() > 0.3 THEN 'seed bio ' || i ELSE NULL END,
    timestamp '2023-01-01' + ((i % 730) * interval '1 hour'),
    timestamp '2023-01-01' + ((i % 910) * interval '1 hour')
FROM generate_series(1, {users}) AS i
"#
    )
}

fn pg_orders_sql(total: usize, orders_per_user: usize) -> String {
    let oppu = orders_per_user.max(1);
    format!(
        r#"
INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at)
SELECT
    1 + ((i - 1) / {oppu}),
    (ARRAY['MacBook Pro 16"','Dell XPS 15','ThinkPad X1 Carbon','Surface Laptop','Ergonomic Chair',
           'Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'])[(i % 10) + 1],
    1 + (i % 10),
    round((5 + (i % 4995))::numeric, 2),
    (ARRAY['pending','shipped','delivered','cancelled'])[(i % 4) + 1],
    CASE WHEN i % 3 = 0 THEN 'note ' || i ELSE NULL END,
    timestamp '2023-01-01' + ((i % 730) * interval '1 minute'),
    timestamp '2023-01-01' + ((i % 760) * interval '1 minute')
FROM generate_series(1, {total}) AS i
"#
    )
}

fn pg_events_sql(total: usize, events_per_user: usize) -> String {
    let eppu = events_per_user.max(1);
    format!(
        r#"
INSERT INTO events (user_id, event_type, payload, ip_address, created_at)
SELECT
    1 + ((i - 1) / {eppu}),
    (ARRAY['login','logout','page_view','purchase','signup','settings_change',
           'password_reset','search','export','api_call'])[(i % 10) + 1],
    jsonb_build_object('seed', true, 'i', i),
    '10.' || (i % 255)::text || '.' || ((i * 7) % 255)::text || '.1',
    timestamp '2023-01-01' + ((i % 730) * interval '1 minute')
FROM generate_series(1, {total}) AS i
"#
    )
}

fn pg_page_views_sql(page_views: usize, users: usize) -> String {
    format!(
        r#"
INSERT INTO page_views (
    session_id, user_id, url, referrer, user_agent, ip_address,
    country_code, region, city, device_type, browser, os,
    screen_width, screen_height, viewport_width, viewport_height,
    page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, click_count,
    is_bounce, utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    custom_props, created_at
)
SELECT
    lpad(to_hex(i), 32, '0'),
    CASE WHEN random() > 0.3 THEN 1 + (i % {users}) ELSE NULL END,
    '/page/' || (i % 26),
    CASE WHEN random() > 0.4 THEN 'https://google.com' ELSE NULL END,
    'Mozilla/5.0 seed',
    '192.168.' || (i % 255)::text || '.' || ((i * 3) % 254 + 1)::text,
    (ARRAY['US','GB','DE','FR','CA'])[(i % 5) + 1],
    'Region ' || (i % 10),
    'City ' || (i % 20),
    (ARRAY['desktop','mobile','tablet'])[(i % 3) + 1],
    (ARRAY['chrome','firefox','safari'])[(i % 3) + 1],
    (ARRAY['macOS','Windows','Linux'])[(i % 3) + 1],
    1280 + (i % 2560),
    720 + (i % 1440),
    800 + (i % 1200),
    600 + (i % 900),
    100 + (i % 5000),
    50 + (i % 2500),
    500 + (i % 60000),
    (i % 101)::smallint,
    (i % 51)::smallint,
    (i % 3 = 0),
    CASE WHEN i % 4 = 0 THEN 'google' ELSE NULL END,
    CASE WHEN i % 4 = 0 THEN 'cpc' ELSE NULL END,
    CASE WHEN i % 8 = 0 THEN 'spring_sale' ELSE NULL END,
    NULL,
    NULL,
    CASE WHEN i % 5 = 0 THEN jsonb_build_object('seed', true) ELSE NULL END,
    timestamp '2023-01-01' + ((i % 730) * interval '1 minute')
FROM generate_series(1, {page_views}) AS i
"#
    )
}

fn pg_content_items_sql(content_items: usize) -> String {
    format!(
        r#"
INSERT INTO content_items (
    title, body, raw_html, metadata, tags, author_name, author_email,
    source_url, category, status, priority, view_count, comment_count,
    word_count, language, published_at, updated_at, created_at, extra_data
)
SELECT
    'Seed title ' || i,
    repeat('lorem ipsum ', 200),
    '<p>' || repeat('lorem ipsum ', 200) || '</p>',
    jsonb_build_object('seed', true, 'i', i),
    'rust,postgres,data',
    'Author ' || (i % 1000),
    'author' || (i % 1000) || '@example.com',
    'https://blog.example.com/posts/' || i,
    (ARRAY['engineering','product','tutorial'])[(i % 3) + 1],
    (ARRAY['draft','review','published'])[(i % 3) + 1],
    (i % 5),
    (i % 100000),
    (i % 500),
    200,
    'en',
    CASE WHEN i % 3 <> 0 THEN timestamp '2024-01-01' + ((i % 365) * interval '1 day') ELSE NULL END,
    timestamp '2024-01-01' + ((i % 400) * interval '1 day'),
    timestamp '2023-01-01' + ((i % 730) * interval '1 day'),
    jsonb_build_object('revisions', 1)
FROM generate_series(1, {content_items}) AS i
"#
    )
}

/// Max rows generated per MySQL recursive-CTE statement.  Avoids
/// `cte_max_recursion_depth` blow-up and keeps each tmp table small.
const MYSQL_CHUNK_ROWS: usize = 500_000;

/// Iterate the global row range `[1, total]` in batches of `MYSQL_CHUNK_ROWS`
/// and execute `make_sql(start, end)` for each chunk inclusively.
fn run_chunked(
    conn: &mut mysql::PooledConn,
    total: usize,
    make_sql: impl Fn(usize, usize) -> String,
) -> Result<()> {
    use mysql::prelude::*;
    let mut start = 1usize;
    while start <= total {
        let end = (start + MYSQL_CHUNK_ROWS - 1).min(total);
        conn.query_drop(make_sql(start, end))?;
        start = end + 1;
    }
    Ok(())
}

fn mysql_users_sql(start: usize, end: usize) -> String {
    let count = end - start + 1;
    let offset = start - 1;
    format!(
        r#"
INSERT INTO users (name, email, age, balance, is_active, bio, created_at, updated_at)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {count}
)
SELECT
    CONCAT('User ', i + {offset}),
    CONCAT('user', i + {offset}, '@example.com'),
    18 + ((i + {offset}) % 48),
    ROUND(RAND(i + {offset}) * 200000, 2),
    ((i + {offset}) % 10 <> 0),
    IF((i + {offset}) % 3 = 0, CONCAT('seed bio ', i + {offset}), NULL),
    DATE_SUB('2023-01-01', INTERVAL -((i + {offset}) % 730) HOUR),
    DATE_SUB('2023-01-01', INTERVAL -((i + {offset}) % 910) HOUR)
FROM n
"#
    )
}

fn mysql_orders_sql(start: usize, end: usize, orders_per_user: usize, users: usize) -> String {
    let count = end - start + 1;
    let offset = start - 1;
    let oppu = orders_per_user.max(1);
    format!(
        r#"
INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {count}
)
SELECT
    LEAST(1 + FLOOR((i + {offset} - 1) / {oppu}), {users}),
    ELT(((i + {offset}) % 10) + 1, 'MacBook Pro','Dell XPS','ThinkPad','Surface','Ergonomic Chair',
        'Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'),
    1 + ((i + {offset}) % 10),
    ROUND(5 + ((i + {offset}) % 4995), 2),
    ELT(((i + {offset}) % 4) + 1, 'pending','shipped','delivered','cancelled'),
    IF((i + {offset}) % 3 = 0, CONCAT('note ', i + {offset}), NULL),
    DATE_SUB('2023-01-01', INTERVAL -((i + {offset}) % 730) MINUTE),
    DATE_SUB('2023-01-01', INTERVAL -((i + {offset}) % 760) MINUTE)
FROM n
"#
    )
}

fn mysql_events_sql(start: usize, end: usize, events_per_user: usize, users: usize) -> String {
    let count = end - start + 1;
    let offset = start - 1;
    let eppu = events_per_user.max(1);
    format!(
        r#"
INSERT INTO events (user_id, event_type, payload, ip_address, created_at)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {count}
)
SELECT
    LEAST(1 + FLOOR((i + {offset} - 1) / {eppu}), {users}),
    ELT(((i + {offset}) % 10) + 1, 'login','logout','page_view','purchase','signup',
        'settings_change','password_reset','search','export','api_call'),
    JSON_OBJECT('seed', TRUE, 'i', i + {offset}),
    CONCAT('10.', (i + {offset}) MOD 255, '.', ((i + {offset}) * 7) MOD 255, '.1'),
    DATE_SUB('2023-01-01', INTERVAL -((i + {offset}) % 730) MINUTE)
FROM n
"#
    )
}

fn mysql_page_views_sql(start: usize, end: usize, users: usize) -> String {
    let count = end - start + 1;
    let offset = start - 1;
    format!(
        r#"
INSERT INTO page_views (
    session_id, user_id, url, referrer, user_agent, ip_address,
    country_code, region, city, device_type, browser, os,
    screen_width, screen_height, viewport_width, viewport_height,
    page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, click_count,
    is_bounce, utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    custom_props, created_at
)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {count}
)
SELECT
    LPAD(HEX(i + {offset}), 32, '0'),
    IF((i + {offset}) % 3 = 0, 1 + ((i + {offset}) % {users}), NULL),
    CONCAT('/page/', (i + {offset}) MOD 26),
    IF((i + {offset}) % 4 = 0, 'https://google.com', NULL),
    'Mozilla/5.0 seed',
    CONCAT('192.168.', (i + {offset}) MOD 255, '.', ((i + {offset}) * 3) MOD 254 + 1),
    ELT(((i + {offset}) MOD 5) + 1, 'US','GB','DE','FR','CA'),
    CONCAT('Region ', (i + {offset}) MOD 10),
    CONCAT('City ', (i + {offset}) MOD 20),
    ELT(((i + {offset}) MOD 3) + 1, 'desktop','mobile','tablet'),
    ELT(((i + {offset}) MOD 3) + 1, 'chrome','firefox','safari'),
    ELT(((i + {offset}) MOD 3) + 1, 'macOS','Windows','Linux'),
    1280 + ((i + {offset}) MOD 2560),
    720 + ((i + {offset}) MOD 1440),
    800 + ((i + {offset}) MOD 1200),
    600 + ((i + {offset}) MOD 900),
    100 + ((i + {offset}) MOD 5000),
    50 + ((i + {offset}) MOD 2500),
    500 + ((i + {offset}) MOD 60000),
    (i + {offset}) MOD 101,
    (i + {offset}) MOD 51,
    ((i + {offset}) MOD 3 = 0),
    IF((i + {offset}) MOD 4 = 0, 'google', NULL),
    IF((i + {offset}) MOD 4 = 0, 'cpc', NULL),
    IF((i + {offset}) MOD 8 = 0, 'spring_sale', NULL),
    NULL,
    NULL,
    IF((i + {offset}) MOD 5 = 0, JSON_OBJECT('seed', TRUE), NULL),
    DATE_SUB('2023-01-01', INTERVAL -((i + {offset}) MOD 730) MINUTE)
FROM n
"#
    )
}

fn mysql_content_items_sql(start: usize, end: usize) -> String {
    let count = end - start + 1;
    let offset = start - 1;
    format!(
        r#"
INSERT INTO content_items (
    title, body, raw_html, metadata, tags, author_name, author_email,
    source_url, category, status, priority, view_count, comment_count,
    word_count, language, published_at, updated_at, created_at, extra_data
)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {count}
)
SELECT
    CONCAT('Seed title ', i + {offset}),
    REPEAT('lorem ipsum ', 200),
    CONCAT('<p>', REPEAT('lorem ipsum ', 200), '</p>'),
    JSON_OBJECT('seed', TRUE, 'i', i + {offset}),
    'rust,postgres,data',
    CONCAT('Author ', (i + {offset}) MOD 1000),
    CONCAT('author', (i + {offset}) MOD 1000, '@example.com'),
    CONCAT('https://blog.example.com/posts/', i + {offset}),
    ELT(((i + {offset}) MOD 3) + 1, 'engineering','product','tutorial'),
    ELT(((i + {offset}) MOD 3) + 1, 'draft','review','published'),
    (i + {offset}) MOD 5,
    (i + {offset}) MOD 100000,
    (i + {offset}) MOD 500,
    200,
    'en',
    IF((i + {offset}) MOD 3 <> 0, DATE_SUB('2024-01-01', INTERVAL -((i + {offset}) MOD 365) DAY), NULL),
    DATE_SUB('2024-01-01', INTERVAL -((i + {offset}) MOD 400) DAY),
    DATE_SUB('2023-01-01', INTERVAL -((i + {offset}) MOD 730) DAY),
    JSON_OBJECT('revisions', 1)
FROM n
"#
    )
}
