//! Server-side bulk generation for SQL Server via `GENERATE_SERIES` (2022+).
//!
//! The Postgres/MySQL paths (`fast.rs`) assume the rich app schema already
//! exists (from `dev/postgres/init.sql` / `dev/mysql/init.sql`) and only
//! `TRUNCATE`+fill. SQL Server's `dev/mssql/init.sql` ships only the
//! chunking-probe schema, so this path DROP+CREATEs the rich app tables first —
//! the parameterized twin of `dev/bench/seed_bench_mssql.sql`, driven by the
//! same `--users/--content-items/...` knobs as every other engine.
//!
//! tiberius is async; the seed binary is sync, so we drive it on a
//! `current_thread` runtime (the same shape `src/source/mssql/mod.rs` uses).

use std::time::Instant;

use anyhow::{Context, Result};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::args::Args;

type MssqlClient = Client<Compat<TcpStream>>;

/// Rows per `INSERT ... GENERATE_SERIES` statement. Bounds the log/tempdb
/// footprint of one batch the same way `fast.rs`'s MySQL CTE driver does, so a
/// 20M-row content_items seed doesn't grow the transaction log unbounded.
const MSSQL_CHUNK_ROWS: usize = 200_000;

pub fn seed_mssql(args: &Args) -> Result<()> {
    let rt = Runtime::new().context("mssql: tokio runtime build failed")?;
    let mut client = rt.block_on(connect(&args.mssql_url))?;

    rt.block_on(async {
        run(&mut client, SCHEMA_DDL)
            .await
            .context("mssql: (re)create app schema")?;

        let t = Instant::now();
        fill(&mut client, args.users, mssql_users_sql).await?;
        println!(
            "  users:      {:>10} rows in {:.1}s",
            args.users,
            t.elapsed().as_secs_f64()
        );

        let total_orders = args.planned_orders();
        if total_orders > 0 {
            let t = Instant::now();
            let oppu = args.orders_per_user.max(1);
            let users = args.users.max(1);
            fill(&mut client, total_orders, |s, e| {
                mssql_orders_sql(s, e, oppu, users)
            })
            .await?;
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
            let users = args.users.max(1);
            fill(&mut client, total_events, |s, e| {
                mssql_events_sql(s, e, eppu, users)
            })
            .await?;
            println!(
                "  events:     {:>10} rows in {:.1}s",
                total_events,
                t.elapsed().as_secs_f64()
            );
        }

        if args.page_views > 0 {
            let t = Instant::now();
            let users = args.users.max(1);
            fill(&mut client, args.page_views, |s, e| {
                mssql_page_views_sql(s, e, users)
            })
            .await?;
            println!(
                "  page_views: {:>10} rows in {:.1}s",
                args.page_views,
                t.elapsed().as_secs_f64()
            );
        }

        if args.content_items > 0 {
            let t = Instant::now();
            fill(&mut client, args.content_items, mssql_content_items_sql).await?;
            println!(
                "  content:    {:>10} rows in {:.1}s",
                args.content_items,
                t.elapsed().as_secs_f64()
            );
        }

        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}

/// Parse `sqlserver://user:pass@host:port/db` and log in (trust-cert, loopback
/// dev — same posture as the source engine on a loopback host).
// ponytail: inline parse (no percent-decoding) — dev creds only; reuse rivet's
// parse_mssql_url if this ever needs URL-encoded passwords.
async fn connect(url: &str) -> Result<MssqlClient> {
    let rest = url
        .strip_prefix("sqlserver://")
        .or_else(|| url.strip_prefix("mssql://"))
        .with_context(|| format!("mssql url must start with sqlserver://: {url}"))?;
    let (userinfo, hostportdb) = rest
        .rsplit_once('@')
        .with_context(|| format!("mssql url missing user:pass@: {url}"))?;
    let (user, pass) = userinfo
        .split_once(':')
        .with_context(|| format!("mssql url userinfo must be user:pass: {url}"))?;
    let (hostport, db) = hostportdb
        .split_once('/')
        .with_context(|| format!("mssql url must include /<database>: {url}"))?;
    let (host, port) = hostport.rsplit_once(':').unwrap_or((hostport, "1433"));
    let port: u16 = port
        .parse()
        .with_context(|| format!("mssql url port not a number: {port}"))?;

    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(db);
    config.authentication(AuthMethod::sql_server(user, pass));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .context("mssql: TCP connect failed")?;
    tcp.set_nodelay(true).ok();
    Client::connect(config, tcp.compat_write())
        .await
        .context("mssql: login failed")
}

/// Execute a single SQL batch, draining any result sets (`GO` is a sqlcmd
/// client directive, not T-SQL — the DDL below is one `;`-separated batch).
async fn run(client: &mut MssqlClient, sql: &str) -> Result<()> {
    client.simple_query(sql).await?.into_results().await?;
    Ok(())
}

/// Iterate `[1, total]` in `MSSQL_CHUNK_ROWS` batches, running
/// `make_sql(start, end)` (inclusive) for each — one bounded INSERT per chunk.
async fn fill(
    client: &mut MssqlClient,
    total: usize,
    make_sql: impl Fn(usize, usize) -> String,
) -> Result<()> {
    let mut start = 1usize;
    while start <= total {
        let end = (start + MSSQL_CHUNK_ROWS - 1).min(total);
        run(client, &make_sql(start, end)).await?;
        start = end + 1;
    }
    Ok(())
}

/// `GENERATE_SERIES(start, end)` as a BIGINT range; the series column is `value`.
fn series(start: usize, end: usize) -> String {
    format!("GENERATE_SERIES(CONVERT(BIGINT, {start}), CONVERT(BIGINT, {end}))")
}

/// The rich app schema — the same tables `dev/bench/seed_bench_mssql.sql`
/// creates, minus the DBA-harm `bench_*` probe tables (those are a separate
/// profile). One `;`-separated batch; no FKs (child rows key into the parent's
/// deterministic id space by construction).
const SCHEMA_DDL: &str = "\
DROP TABLE IF EXISTS events;\
DROP TABLE IF EXISTS orders;\
DROP TABLE IF EXISTS page_views;\
DROP TABLE IF EXISTS content_items;\
DROP TABLE IF EXISTS users;\
CREATE TABLE users (\
    id INT IDENTITY(1,1) PRIMARY KEY,\
    name NVARCHAR(100) NOT NULL,\
    email NVARCHAR(200) NOT NULL,\
    age INT NULL,\
    balance DECIMAL(12,2) NULL,\
    is_active BIT NOT NULL,\
    bio NVARCHAR(MAX) NULL,\
    created_at DATETIME2(6) NOT NULL,\
    updated_at DATETIME2(6) NOT NULL\
);\
CREATE TABLE orders (\
    id INT IDENTITY(1,1) PRIMARY KEY,\
    user_id INT NOT NULL,\
    product NVARCHAR(200) NOT NULL,\
    quantity INT NOT NULL,\
    price DECIMAL(10,2) NOT NULL,\
    status NVARCHAR(20) NOT NULL,\
    notes NVARCHAR(MAX) NULL,\
    ordered_at DATETIME2(6) NOT NULL,\
    updated_at DATETIME2(6) NOT NULL\
);\
CREATE TABLE events (\
    id BIGINT IDENTITY(1,1) PRIMARY KEY,\
    user_id INT NOT NULL,\
    event_type NVARCHAR(50) NOT NULL,\
    payload NVARCHAR(MAX) NULL,\
    ip_address NVARCHAR(45) NULL,\
    created_at DATETIME2(6) NOT NULL\
);\
CREATE TABLE page_views (\
    id BIGINT IDENTITY(1,1) PRIMARY KEY,\
    session_id NVARCHAR(36) NOT NULL,\
    user_id INT NULL,\
    url NVARCHAR(MAX) NOT NULL,\
    referrer NVARCHAR(MAX) NULL,\
    user_agent NVARCHAR(MAX) NULL,\
    ip_address NVARCHAR(45) NULL,\
    country_code CHAR(2) NULL,\
    region NVARCHAR(100) NULL,\
    city NVARCHAR(100) NULL,\
    device_type NVARCHAR(20) NULL,\
    browser NVARCHAR(50) NULL,\
    os NVARCHAR(50) NULL,\
    screen_width INT NULL,\
    screen_height INT NULL,\
    viewport_width INT NULL,\
    viewport_height INT NULL,\
    page_load_ms INT NULL,\
    dom_ready_ms INT NULL,\
    time_on_page_ms INT NULL,\
    scroll_depth_pct SMALLINT NULL,\
    click_count SMALLINT NULL,\
    is_bounce BIT NOT NULL,\
    utm_source NVARCHAR(100) NULL,\
    utm_medium NVARCHAR(100) NULL,\
    utm_campaign NVARCHAR(200) NULL,\
    utm_term NVARCHAR(200) NULL,\
    utm_content NVARCHAR(200) NULL,\
    custom_props NVARCHAR(MAX) NULL,\
    created_at DATETIME2(6) NOT NULL\
);\
CREATE TABLE content_items (\
    id BIGINT IDENTITY(1,1) PRIMARY KEY,\
    title NVARCHAR(MAX) NOT NULL,\
    body NVARCHAR(MAX) NOT NULL,\
    raw_html NVARCHAR(MAX) NOT NULL,\
    metadata NVARCHAR(MAX) NULL,\
    tags NVARCHAR(MAX) NULL,\
    author_name NVARCHAR(100) NOT NULL,\
    author_email NVARCHAR(200) NOT NULL,\
    source_url NVARCHAR(MAX) NULL,\
    category NVARCHAR(50) NULL,\
    status NVARCHAR(20) NOT NULL,\
    priority INT NOT NULL,\
    view_count INT NOT NULL,\
    comment_count INT NOT NULL,\
    word_count INT NOT NULL,\
    language CHAR(2) NOT NULL,\
    published_at DATETIME2(6) NULL,\
    updated_at DATETIME2(6) NOT NULL,\
    created_at DATETIME2(6) NOT NULL,\
    extra_data NVARCHAR(MAX) NULL\
);";

fn mssql_users_sql(start: usize, end: usize) -> String {
    format!(
        "INSERT INTO users (name, email, age, balance, is_active, bio, created_at, updated_at)
SELECT
    CONCAT(N'User ', value),
    CONCAT('user', value, '@example.com'),
    18 + (value % 48),
    ROUND((value % 200000) + 0.99, 2),
    CAST(CASE WHEN value % 10 <> 0 THEN 1 ELSE 0 END AS BIT),
    CASE WHEN value % 3 <> 0 THEN CONCAT(N'seed bio ', value) ELSE NULL END,
    DATEADD(HOUR, value % 730, CONVERT(DATETIME2(6), '2023-01-01')),
    DATEADD(HOUR, value % 910, CONVERT(DATETIME2(6), '2023-01-01'))
FROM {};",
        series(start, end)
    )
}

fn mssql_orders_sql(start: usize, end: usize, oppu: usize, users: usize) -> String {
    format!(
        "INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at)
SELECT
    CAST(LEAST(1 + (value - 1) / {oppu}, {users}) AS INT),
    CASE value % 10
        WHEN 0 THEN N'MacBook Pro 16\"' WHEN 1 THEN N'Dell XPS 15'
        WHEN 2 THEN N'ThinkPad X1 Carbon' WHEN 3 THEN N'Surface Laptop'
        WHEN 4 THEN N'Ergonomic Chair' WHEN 5 THEN N'Standing Desk'
        WHEN 6 THEN N'Monitor Arm' WHEN 7 THEN N'USB-C Hub'
        WHEN 8 THEN N'Mechanical Keyboard' ELSE N'Magic Mouse' END,
    1 + (value % 10),
    ROUND(5 + (value % 4995) + 0.0, 2),
    CASE value % 4 WHEN 0 THEN N'pending' WHEN 1 THEN N'shipped'
                   WHEN 2 THEN N'delivered' ELSE N'cancelled' END,
    CASE WHEN value % 3 = 0 THEN CONCAT(N'note ', value) ELSE NULL END,
    DATEADD(MINUTE, value % 730, CONVERT(DATETIME2(6), '2023-01-01')),
    DATEADD(MINUTE, value % 760, CONVERT(DATETIME2(6), '2023-01-01'))
FROM {};",
        series(start, end)
    )
}

fn mssql_events_sql(start: usize, end: usize, eppu: usize, users: usize) -> String {
    format!(
        "INSERT INTO events (user_id, event_type, payload, ip_address, created_at)
SELECT
    CAST(LEAST(1 + (value - 1) / {eppu}, {users}) AS INT),
    CASE value % 10
        WHEN 0 THEN N'login' WHEN 1 THEN N'logout' WHEN 2 THEN N'page_view'
        WHEN 3 THEN N'purchase' WHEN 4 THEN N'signup' WHEN 5 THEN N'settings_change'
        WHEN 6 THEN N'password_reset' WHEN 7 THEN N'search'
        WHEN 8 THEN N'export' ELSE N'api_call' END,
    CONCAT(N'{{\"seed\":true,\"i\":', value, N'}}'),
    CONCAT('10.', value % 255, '.', (value * 7) % 255, '.1'),
    DATEADD(MINUTE, value % 730, CONVERT(DATETIME2(6), '2023-01-01'))
FROM {};",
        series(start, end)
    )
}

fn mssql_page_views_sql(start: usize, end: usize, users: usize) -> String {
    format!(
        "INSERT INTO page_views (
    session_id, user_id, url, referrer, user_agent, ip_address,
    country_code, region, city, device_type, browser, os,
    screen_width, screen_height, viewport_width, viewport_height,
    page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, click_count,
    is_bounce, utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    custom_props, created_at)
SELECT
    LOWER(CONVERT(CHAR(36), NEWID())),
    CASE WHEN value % 3 = 0 THEN NULL ELSE CAST(1 + (value % {users}) AS INT) END,
    CONCAT('/page/', value % 26),
    CASE WHEN value % 4 = 0 THEN 'https://google.com' ELSE NULL END,
    N'Mozilla/5.0 seed',
    CONCAT('192.168.', value % 255, '.', (value * 3) % 254 + 1),
    CASE value % 5 WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE'
                   WHEN 3 THEN 'FR' ELSE 'CA' END,
    CONCAT(N'Region ', value % 10),
    CONCAT(N'City ', value % 20),
    CASE value % 3 WHEN 0 THEN N'desktop' WHEN 1 THEN N'mobile' ELSE N'tablet' END,
    CASE value % 3 WHEN 0 THEN N'chrome' WHEN 1 THEN N'firefox' ELSE N'safari' END,
    CASE value % 3 WHEN 0 THEN N'macOS' WHEN 1 THEN N'Windows' ELSE N'Linux' END,
    1280 + (value % 2560), 720 + (value % 1440),
    800 + (value % 1200), 600 + (value % 900),
    100 + (value % 5000), 50 + (value % 2500), 500 + (value % 60000),
    CAST(value % 101 AS SMALLINT), CAST(value % 51 AS SMALLINT),
    CAST(CASE WHEN value % 3 = 0 THEN 1 ELSE 0 END AS BIT),
    CASE WHEN value % 4 = 0 THEN N'google' ELSE NULL END,
    CASE WHEN value % 4 = 0 THEN N'cpc' ELSE NULL END,
    CASE WHEN value % 8 = 0 THEN N'spring_sale' ELSE NULL END,
    NULL, NULL,
    CASE WHEN value % 5 = 0 THEN N'{{\"seed\":true}}' ELSE NULL END,
    DATEADD(MINUTE, value % 730, CONVERT(DATETIME2(6), '2023-01-01'))
FROM {};",
        series(start, end)
    )
}

fn mssql_content_items_sql(start: usize, end: usize) -> String {
    format!(
        "INSERT INTO content_items (
    title, body, raw_html, metadata, tags, author_name, author_email,
    source_url, category, status, priority, view_count, comment_count,
    word_count, language, published_at, updated_at, created_at, extra_data)
SELECT
    CONCAT(N'Seed title ', value),
    REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)), 200),
    CONCAT(N'<p>', REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)), 200), N'</p>'),
    CONCAT(N'{{\"seed\":true,\"i\":', value, N'}}'),
    N'rust,postgres,data',
    CONCAT(N'Author ', value % 1000),
    CONCAT('author', value % 1000, '@example.com'),
    CONCAT('https://blog.example.com/posts/', value),
    CASE value % 3 WHEN 0 THEN N'engineering' WHEN 1 THEN N'product' ELSE N'tutorial' END,
    CASE value % 3 WHEN 0 THEN N'draft' WHEN 1 THEN N'review' ELSE N'published' END,
    value % 5, value % 100000, value % 500, 200, 'en',
    CASE WHEN value % 3 <> 0 THEN DATEADD(DAY, value % 365, CONVERT(DATETIME2(6), '2024-01-01')) ELSE NULL END,
    DATEADD(DAY, value % 400, CONVERT(DATETIME2(6), '2024-01-01')),
    DATEADD(DAY, value % 730, CONVERT(DATETIME2(6), '2023-01-01')),
    N'{{\"revisions\":1}}'
FROM {};",
        series(start, end)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builders_emit_parameterized_generate_series() {
        // Each builder must target its table and read a bounded GENERATE_SERIES
        // window — the chunk range, not a hard-coded count.
        let u = mssql_users_sql(1, 500);
        assert!(u.contains("INSERT INTO users"), "{u}");
        assert!(
            u.contains("GENERATE_SERIES(CONVERT(BIGINT, 1), CONVERT(BIGINT, 500))"),
            "{u}"
        );

        let o = mssql_orders_sql(501, 1000, 10, 2000);
        assert!(o.contains("INSERT INTO orders"), "{o}");
        // user_id maps into the parent id space and is capped at the user count.
        assert!(o.contains("/ 10, 2000)"), "{o}");
        assert!(
            o.contains("CONVERT(BIGINT, 501), CONVERT(BIGINT, 1000)"),
            "{o}"
        );

        let c = mssql_content_items_sql(1, 5000);
        assert!(c.contains("INSERT INTO content_items"), "{c}");
        assert!(c.contains("REPLICATE"), "{c}");
    }

    #[test]
    fn schema_creates_the_five_app_tables_without_fks() {
        for tbl in ["users", "orders", "events", "page_views", "content_items"] {
            assert!(
                SCHEMA_DDL.contains(&format!("CREATE TABLE {tbl} ")),
                "{tbl}"
            );
        }
        // No FK constraints — child rows key into the parent id space by
        // construction (mirrors the PG/MySQL bulk load disabling FK checks).
        assert!(
            !SCHEMA_DDL.contains("FOREIGN KEY"),
            "schema must be FK-free"
        );
    }
}
