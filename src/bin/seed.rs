use std::fmt::Write as FmtWrite;
use std::io::Write;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use rand::Rng;
use rand::RngExt;

#[derive(Parser)]
#[command(name = "seed", about = "Generate test data for rivet")]
struct Args {
    /// Target database: postgres, mysql, or both
    #[arg(short, long, default_value = "both")]
    target: String,

    /// Number of users to generate
    #[arg(long, default_value = "100000")]
    users: usize,

    /// Average orders per user
    #[arg(long, default_value = "10")]
    orders_per_user: usize,

    /// Average events per user
    #[arg(long, default_value = "50")]
    events_per_user: usize,

    /// Number of page_views to generate (wide table, degraded scenario)
    #[arg(long, default_value = "2000000")]
    page_views: usize,

    /// Number of content_items to generate (heavy text, worst case for memory)
    #[arg(long, default_value = "200000")]
    content_items: usize,

    /// PostgreSQL connection URL
    #[arg(long, default_value = "postgresql://rivet:rivet@localhost:5432/rivet")]
    pg_url: String,

    /// MySQL connection URL
    #[arg(long, default_value = "mysql://rivet:rivet@localhost:3306/rivet")]
    mysql_url: String,

    /// Batch size for inserts
    #[arg(long, default_value = "1000")]
    batch_size: usize,

    /// Fill `orders_sparse` with few rows and huge gaps in `id` (for chunked / sparse-key demos)
    #[arg(long)]
    sparse_chunk_demo: bool,

    /// Only create (if needed), truncate, and fill `orders_sparse` — skip users/orders/events/page_views/content
    #[arg(long)]
    only_sparse_chunk_demo: bool,

    /// Number of rows in `orders_sparse` (ids: 1, 1+gap, 1+2·gap, …). Use ≥3 to see sparse min..max vs row count
    #[arg(long, default_value = "3")]
    sparse_chunk_rows: usize,

    /// Gap between consecutive sparse ids (wider gap ⇒ more empty chunk windows for the same chunk_size)
    #[arg(long, default_value = "2000000")]
    sparse_chunk_id_gap: i64,

    /// Rows per INSERT for `orders_sparse` (keep ≤ few thousand if max_allowed_packet / statement size is tight)
    #[arg(long, default_value = "5000")]
    sparse_chunk_batch_size: usize,
}

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack",
    "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rachel", "Sam", "Tara",
    "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zach", "Anna", "Brian", "Clara", "Derek",
    "Elena", "Felix", "Gina", "Hugo", "Iris", "James", "Kate", "Liam", "Maya", "Nora",
    "Oscar", "Petra", "Ravi", "Sara", "Tom", "Ursula", "Vlad", "Wanda", "Xena", "Yuri",
];

const LAST_NAMES: &[&str] = &[
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson",
    "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson",
    "Robinson", "Clark", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "King", "Wright",
    "Scott", "Green", "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts",
    "Carter", "Phillips", "Evans", "Turner", "Torres", "Parker", "Collins", "Edwards", "Stewart",
    "Morris", "Murphy", "Rivera", "Cook",
];

const DOMAINS: &[&str] = &[
    "gmail.com", "yahoo.com", "outlook.com", "proton.me", "fastmail.com",
    "icloud.com", "hey.com", "mail.com", "zoho.com", "tutanota.com",
];

const PRODUCTS: &[&str] = &[
    "MacBook Pro 16\"", "Dell XPS 15", "ThinkPad X1 Carbon", "Surface Laptop",
    "Ergonomic Chair", "Standing Desk", "Monitor Arm", "USB-C Hub",
    "Mechanical Keyboard", "Magic Mouse", "Webcam HD", "Noise Cancelling Headphones",
    "GPU RTX 4090", "NVMe SSD 2TB", "RAM DDR5 64GB", "UPS Battery Backup",
    "Cable Management Kit", "Desk Lamp LED", "Monitor 27\" 4K", "Docking Station",
    "Wireless Mouse", "Keyboard Wrist Rest", "Screen Protector", "Laptop Stand",
    "External SSD 1TB", "Smart Power Strip", "Blue Light Glasses", "Desk Mat XL",
    "Portable Charger", "Lightning Cable 3-pack",
];

const STATUSES: &[&str] = &["pending", "shipped", "delivered", "cancelled"];

const EVENT_TYPES: &[&str] = &[
    "login", "logout", "page_view", "purchase", "signup",
    "settings_change", "password_reset", "search", "export", "api_call",
];

const BIOS: &[&str] = &[
    "Software engineer", "Product manager", "Data scientist", "DevOps lead",
    "CTO at startup", "Junior developer", "ML engineer", "Full-stack dev",
    "Backend engineer", "Frontend specialist", "SRE", "Platform engineer",
    "Tech lead", "VP of Engineering", "Security researcher", "QA engineer",
    "Mobile developer", "Cloud architect", "Solutions architect", "DBA",
];

const BROWSERS: &[&str] = &["chrome", "firefox", "safari", "arc", "edge", "brave"];
const DEVICES: &[&str] = &[
    "macbook", "macbook_pro", "windows_pc", "linux_desktop",
    "linux_laptop", "iphone", "android", "ipad",
];
const PAGES: &[&str] = &[
    "/dashboard", "/products", "/settings", "/profile", "/orders",
    "/analytics", "/reports", "/admin", "/search", "/help",
];

fn main() -> Result<()> {
    let args = Args::parse();

    if args.only_sparse_chunk_demo {
        println!(
            "Sparse chunk demo: {} row(s), id gap {}",
            args.sparse_chunk_rows, args.sparse_chunk_id_gap
        );
        if args.target == "postgres" || args.target == "both" {
            println!("\n=== PostgreSQL (orders_sparse only) ===");
            seed_pg_sparse_only(&args)?;
        }
        if args.target == "mysql" || args.target == "both" {
            println!("\n=== MySQL (orders_sparse only) ===");
            seed_mysql_sparse_only(&args)?;
        }
        println!("\nDone!");
        return Ok(());
    }

    let total_orders = args.users * args.orders_per_user;
    let total_events = args.users * args.events_per_user;
    println!(
        "Generating: {} users, {} orders, {} events, {} page_views, {} content_items{}",
        args.users,
        total_orders,
        total_events,
        args.page_views,
        args.content_items,
        if args.sparse_chunk_demo {
            " + orders_sparse demo"
        } else {
            ""
        }
    );

    if args.target == "postgres" || args.target == "both" {
        println!("\n=== PostgreSQL ===");
        seed_postgres(&args)?;
    }
    if args.target == "mysql" || args.target == "both" {
        println!("\n=== MySQL ===");
        seed_mysql(&args)?;
    }

    println!("\nDone!");
    Ok(())
}

// ─── PostgreSQL ──────────────────────────────────────────────

fn seed_postgres(args: &Args) -> Result<()> {
    let mut client = postgres::Client::connect(&args.pg_url, postgres::NoTls)
        .context("failed to connect to PostgreSQL")?;

    ensure_orders_sparse_pg(&mut client)?;

    client.execute(
        "TRUNCATE orders_sparse, content_items, page_views, events, orders, users RESTART IDENTITY CASCADE",
        &[],
    )?;

    let t = Instant::now();
    seed_pg_users(&mut client, args)?;
    println!("  users:      {:>10} rows in {:.1}s", args.users, t.elapsed().as_secs_f64());

    let t = Instant::now();
    let total_orders = seed_pg_orders(&mut client, args)?;
    println!("  orders:     {:>10} rows in {:.1}s", total_orders, t.elapsed().as_secs_f64());

    let t = Instant::now();
    let total_events = seed_pg_events(&mut client, args)?;
    println!("  events:     {:>10} rows in {:.1}s", total_events, t.elapsed().as_secs_f64());

    let t = Instant::now();
    seed_pg_page_views(&mut client, args)?;
    println!("  page_views: {:>10} rows in {:.1}s", args.page_views, t.elapsed().as_secs_f64());

    let t = Instant::now();
    seed_pg_content_items(&mut client, args)?;
    println!("  content:    {:>10} rows in {:.1}s", args.content_items, t.elapsed().as_secs_f64());

    if args.sparse_chunk_demo {
        let t = Instant::now();
        let n = seed_pg_orders_sparse_fill(&mut client, args)?;
        println!("  orders_sparse: {:>10} rows in {:.1}s", n, t.elapsed().as_secs_f64());
    }

    Ok(())
}

fn seed_pg_users(client: &mut postgres::Client, args: &Args) -> Result<()> {
    let mut rng = rand::rng();
    let mut tx = client.transaction()?;

    let mut batch = String::new();
    let mut count = 0;

    for i in 0..args.users {
        let (name, email) = gen_user(&mut rng, i);
        let age = rng.random_range(18..=65);
        let balance = rng.random_range(0.0..200_000.0_f64);
        let is_active = rng.random_bool(0.9);
        let bio = if rng.random_bool(0.7) {
            format!("'{}'", BIOS[rng.random_range(0..BIOS.len())])
        } else {
            "NULL".to_string()
        };
        let created = gen_timestamp(&mut rng, 2023, 2024);
        let updated = gen_timestamp_after(&mut rng, &created, 180);

        if count == 0 {
            batch.clear();
            batch.push_str(
                "INSERT INTO users (name, email, age, balance, is_active, bio, created_at, updated_at) VALUES ",
            );
        } else {
            batch.push(',');
        }

        write!(
            batch,
            "('{name}', '{email}', {age}, {balance:.2}, {is_active}, {bio}, '{created}', '{updated}')"
        )?;
        count += 1;

        if count >= args.batch_size || i == args.users - 1 {
            tx.batch_execute(&batch)?;
            count = 0;
            if (i + 1) % 50_000 == 0 {
                eprint!("    users {}/{}...\r", i + 1, args.users);
                std::io::stderr().flush().ok();
            }
        }
    }

    tx.commit()?;
    Ok(())
}

fn seed_pg_orders(client: &mut postgres::Client, args: &Args) -> Result<usize> {
    let mut rng = rand::rng();
    let mut tx = client.transaction()?;

    let mut batch = String::new();
    let mut count = 0;
    let mut total = 0;

    for user_id in 1..=args.users {
        let n_orders = poisson_sample(&mut rng, args.orders_per_user as f64);

        for _ in 0..n_orders {
            let product = PRODUCTS[rng.random_range(0..PRODUCTS.len())].replace('\"', "\\\"");
            let quantity = rng.random_range(1..=10);
            let price = rng.random_range(5.0..5000.0_f64);
            let status = STATUSES[rng.random_range(0..STATUSES.len())];
            let notes = if rng.random_bool(0.4) {
                format!("'{}'", gen_note(&mut rng))
            } else {
                "NULL".to_string()
            };
            let ordered = gen_timestamp(&mut rng, 2023, 2024);
            let updated = gen_timestamp_after(&mut rng, &ordered, 30);

            if count == 0 {
                batch.clear();
                batch.push_str(
                    "INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at) VALUES ",
                );
            } else {
                batch.push(',');
            }

            write!(
                batch,
                "({user_id}, '{product}', {quantity}, {price:.2}, '{status}', {notes}, '{ordered}', '{updated}')"
            )?;
            count += 1;
            total += 1;

            if count >= args.batch_size {
                tx.batch_execute(&batch)?;
                count = 0;
            }
        }

        if user_id % 50_000 == 0 {
            eprint!("    orders: user {}/{}...\r", user_id, args.users);
            std::io::stderr().flush().ok();
        }
    }

    if count > 0 {
        tx.batch_execute(&batch)?;
    }
    tx.commit()?;
    Ok(total)
}

fn seed_pg_events(client: &mut postgres::Client, args: &Args) -> Result<usize> {
    let mut rng = rand::rng();
    let mut tx = client.transaction()?;

    let mut batch = String::new();
    let mut count = 0;
    let mut total = 0;

    for user_id in 1..=args.users {
        let n_events = poisson_sample(&mut rng, args.events_per_user as f64);

        for _ in 0..n_events {
            let event_type = EVENT_TYPES[rng.random_range(0..EVENT_TYPES.len())];
            let payload = gen_event_payload(&mut rng, event_type);
            let ip = gen_ip(&mut rng);
            let created = gen_timestamp(&mut rng, 2023, 2024);

            if count == 0 {
                batch.clear();
                batch.push_str(
                    "INSERT INTO events (user_id, event_type, payload, ip_address, created_at) VALUES ",
                );
            } else {
                batch.push(',');
            }

            write!(
                batch,
                "({user_id}, '{event_type}', '{payload}', '{ip}', '{created}')"
            )?;
            count += 1;
            total += 1;

            if count >= args.batch_size {
                tx.batch_execute(&batch)?;
                count = 0;
            }
        }

        if user_id % 50_000 == 0 {
            eprint!("    events: user {}/{}...\r", user_id, args.users);
            std::io::stderr().flush().ok();
        }
    }

    if count > 0 {
        tx.batch_execute(&batch)?;
    }
    tx.commit()?;
    Ok(total)
}

// ─── MySQL ───────────────────────────────────────────────────

fn seed_mysql(args: &Args) -> Result<()> {
    use mysql::prelude::*;

    let pool = mysql::Pool::new(mysql::Opts::from_url(&args.mysql_url)?)?;
    let mut conn = pool.get_conn()?;

    ensure_orders_sparse_mysql(&mut conn)?;

    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0")?;
    conn.query_drop("TRUNCATE TABLE orders_sparse")?;
    conn.query_drop("TRUNCATE TABLE content_items")?;
    conn.query_drop("TRUNCATE TABLE page_views")?;
    conn.query_drop("TRUNCATE TABLE events")?;
    conn.query_drop("TRUNCATE TABLE orders")?;
    conn.query_drop("TRUNCATE TABLE users")?;
    conn.query_drop("SET FOREIGN_KEY_CHECKS = 1")?;

    let t = Instant::now();
    seed_mysql_users(&mut conn, args)?;
    println!("  users:      {:>10} rows in {:.1}s", args.users, t.elapsed().as_secs_f64());

    let t = Instant::now();
    let total_orders = seed_mysql_orders(&mut conn, args)?;
    println!("  orders:     {:>10} rows in {:.1}s", total_orders, t.elapsed().as_secs_f64());

    let t = Instant::now();
    let total_events = seed_mysql_events(&mut conn, args)?;
    println!("  events:     {:>10} rows in {:.1}s", total_events, t.elapsed().as_secs_f64());

    let t = Instant::now();
    seed_mysql_page_views(&mut conn, args)?;
    println!("  page_views: {:>10} rows in {:.1}s", args.page_views, t.elapsed().as_secs_f64());

    let t = Instant::now();
    seed_mysql_content_items(&mut conn, args)?;
    println!("  content:    {:>10} rows in {:.1}s", args.content_items, t.elapsed().as_secs_f64());

    if args.sparse_chunk_demo {
        let t = Instant::now();
        let n = seed_mysql_orders_sparse_fill(&mut conn, args)?;
        println!("  orders_sparse: {:>10} rows in {:.1}s", n, t.elapsed().as_secs_f64());
    }

    Ok(())
}

fn seed_mysql_users(conn: &mut mysql::PooledConn, args: &Args) -> Result<()> {
    use mysql::prelude::*;
    let mut rng = rand::rng();

    let mut batch = String::new();
    let mut count = 0;

    for i in 0..args.users {
        let (name, email) = gen_user(&mut rng, i);
        let age = rng.random_range(18..=65);
        let balance = rng.random_range(0.0..200_000.0_f64);
        let is_active = rng.random_bool(0.9) as u8;
        let bio = if rng.random_bool(0.7) {
            format!("'{}'", BIOS[rng.random_range(0..BIOS.len())])
        } else {
            "NULL".to_string()
        };
        let created = gen_timestamp(&mut rng, 2023, 2024);
        let updated = gen_timestamp_after(&mut rng, &created, 180);

        if count == 0 {
            batch.clear();
            batch.push_str(
                "INSERT INTO users (name, email, age, balance, is_active, bio, created_at, updated_at) VALUES ",
            );
        } else {
            batch.push(',');
        }

        write!(
            batch,
            "('{name}', '{email}', {age}, {balance:.2}, {is_active}, {bio}, '{created}', '{updated}')"
        )?;
        count += 1;

        if count >= args.batch_size || i == args.users - 1 {
            conn.query_drop(&batch)?;
            count = 0;
            if (i + 1) % 50_000 == 0 {
                eprint!("    users {}/{}...\r", i + 1, args.users);
                std::io::stderr().flush().ok();
            }
        }
    }

    Ok(())
}

fn seed_mysql_orders(conn: &mut mysql::PooledConn, args: &Args) -> Result<usize> {
    use mysql::prelude::*;
    let mut rng = rand::rng();

    let mut batch = String::new();
    let mut count = 0;
    let mut total = 0;

    for user_id in 1..=args.users {
        let n_orders = poisson_sample(&mut rng, args.orders_per_user as f64);

        for _ in 0..n_orders {
            let product = PRODUCTS[rng.random_range(0..PRODUCTS.len())].replace('\"', "\\\"");
            let quantity = rng.random_range(1..=10);
            let price = rng.random_range(5.0..5000.0_f64);
            let status = STATUSES[rng.random_range(0..STATUSES.len())];
            let notes = if rng.random_bool(0.4) {
                format!("'{}'", gen_note(&mut rng))
            } else {
                "NULL".to_string()
            };
            let ordered = gen_timestamp(&mut rng, 2023, 2024);
            let updated = gen_timestamp_after(&mut rng, &ordered, 30);

            if count == 0 {
                batch.clear();
                batch.push_str(
                    "INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at) VALUES ",
                );
            } else {
                batch.push(',');
            }

            write!(
                batch,
                "({user_id}, '{product}', {quantity}, {price:.2}, '{status}', {notes}, '{ordered}', '{updated}')"
            )?;
            count += 1;
            total += 1;

            if count >= args.batch_size {
                conn.query_drop(&batch)?;
                count = 0;
            }
        }

        if user_id % 50_000 == 0 {
            eprint!("    orders: user {}/{}...\r", user_id, args.users);
            std::io::stderr().flush().ok();
        }
    }

    if count > 0 {
        conn.query_drop(&batch)?;
    }
    Ok(total)
}

fn seed_mysql_events(conn: &mut mysql::PooledConn, args: &Args) -> Result<usize> {
    use mysql::prelude::*;
    let mut rng = rand::rng();

    let mut batch = String::new();
    let mut count = 0;
    let mut total = 0;

    for user_id in 1..=args.users {
        let n_events = poisson_sample(&mut rng, args.events_per_user as f64);

        for _ in 0..n_events {
            let event_type = EVENT_TYPES[rng.random_range(0..EVENT_TYPES.len())];
            let payload = gen_event_payload(&mut rng, event_type);
            let ip = gen_ip(&mut rng);
            let created = gen_timestamp(&mut rng, 2023, 2024);

            if count == 0 {
                batch.clear();
                batch.push_str(
                    "INSERT INTO events (user_id, event_type, payload, ip_address, created_at) VALUES ",
                );
            } else {
                batch.push(',');
            }

            write!(
                batch,
                "({user_id}, '{event_type}', '{payload}', '{ip}', '{created}')"
            )?;
            count += 1;
            total += 1;

            if count >= args.batch_size {
                conn.query_drop(&batch)?;
                count = 0;
            }
        }

        if user_id % 50_000 == 0 {
            eprint!("    events: user {}/{}...\r", user_id, args.users);
            std::io::stderr().flush().ok();
        }
    }

    if count > 0 {
        conn.query_drop(&batch)?;
    }
    Ok(total)
}

// ─── Data generators ─────────────────────────────────────────

fn gen_user(rng: &mut impl Rng, idx: usize) -> (String, String) {
    let first = FIRST_NAMES[rng.random_range(0..FIRST_NAMES.len())];
    let last = LAST_NAMES[rng.random_range(0..LAST_NAMES.len())];
    let domain = DOMAINS[rng.random_range(0..DOMAINS.len())];
    let name = format!("{} {}", first, last);
    let email = format!("{}.{}{}@{}", first.to_lowercase(), last.to_lowercase(), idx, domain);
    (name, email)
}

fn gen_timestamp(rng: &mut impl Rng, year_start: i32, year_end: i32) -> String {
    let start = chrono::NaiveDate::from_ymd_opt(year_start, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();
    let end = chrono::NaiveDate::from_ymd_opt(year_end, 12, 31)
        .unwrap()
        .and_hms_opt(23, 59, 59)
        .unwrap()
        .and_utc()
        .timestamp();
    let ts = rng.random_range(start..=end);
    chrono::DateTime::from_timestamp(ts, 0)
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn gen_timestamp_after(rng: &mut impl Rng, base: &str, max_days_after: u32) -> String {
    let base_dt = chrono::NaiveDateTime::parse_from_str(base, "%Y-%m-%d %H:%M:%S").unwrap();
    let offset_secs = rng.random_range(0..max_days_after as i64 * 86400);
    let result = base_dt + chrono::Duration::seconds(offset_secs);
    result.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn gen_ip(rng: &mut impl Rng) -> String {
    if rng.random_bool(0.5) {
        format!("192.168.{}.{}", rng.random_range(0..=255), rng.random_range(1..=254))
    } else if rng.random_bool(0.5) {
        format!("10.{}.{}.{}", rng.random_range(0..=255), rng.random_range(0..=255), rng.random_range(1..=254))
    } else {
        format!("172.{}.{}.{}", rng.random_range(16..=31), rng.random_range(0..=255), rng.random_range(1..=254))
    }
}

fn gen_event_payload(rng: &mut impl Rng, event_type: &str) -> String {
    match event_type {
        "login" | "logout" => {
            let device = DEVICES[rng.random_range(0..DEVICES.len())];
            let browser = BROWSERS[rng.random_range(0..BROWSERS.len())];
            format!(r#"{{"device": "{device}", "browser": "{browser}"}}"#)
        }
        "page_view" => {
            let page = PAGES[rng.random_range(0..PAGES.len())];
            let duration = rng.random_range(500..60000);
            format!(r#"{{"page": "{page}", "duration_ms": {duration}}}"#)
        }
        "purchase" => {
            let order_id = rng.random_range(1..1_000_000);
            let amount = rng.random_range(5.0..5000.0_f64);
            format!(r#"{{"order_id": {order_id}, "amount": {amount:.2}}}"#)
        }
        "signup" => {
            let plans = ["free", "pro", "enterprise"];
            let plan = plans[rng.random_range(0..plans.len())];
            format!(r#"{{"plan": "{plan}"}}"#)
        }
        "search" => {
            let terms = ["laptop", "keyboard", "monitor", "desk", "cable", "ssd", "gpu"];
            let term = terms[rng.random_range(0..terms.len())];
            let results = rng.random_range(0..500);
            format!(r#"{{"query": "{term}", "results": {results}}}"#)
        }
        _ => {
            format!(r#"{{"action": "{event_type}"}}"#)
        }
    }
}

fn gen_note(rng: &mut impl Rng) -> String {
    let notes = [
        "Rush delivery", "Gift wrap requested", "Leave at door",
        "Fragile item", "Call before delivery", "No substitutions",
        "Company purchase", "Tax exempt", "Bulk order", "Repeat customer",
    ];
    notes[rng.random_range(0..notes.len())].to_string()
}

/// Simple Poisson-like sample for natural variation around the mean.
fn poisson_sample(rng: &mut impl Rng, lambda: f64) -> usize {
    let l = (-lambda).exp();
    let mut k = 0usize;
    let mut p = 1.0_f64;
    loop {
        k += 1;
        p *= rng.random::<f64>();
        if p <= l {
            break;
        }
    }
    k.saturating_sub(1)
}

// ─── Page Views (wide table, degraded scenario) ──────────────

const URLS: &[&str] = &[
    "/", "/products", "/products/123", "/products/456/reviews", "/cart",
    "/checkout", "/checkout/payment", "/checkout/confirm", "/account",
    "/account/orders", "/account/settings", "/blog", "/blog/rust-is-fast",
    "/blog/postgres-tips", "/docs", "/docs/getting-started", "/pricing",
    "/about", "/contact", "/help", "/search?q=keyboard", "/search?q=monitor",
    "/api/v1/health", "/login", "/signup", "/forgot-password",
];

const REFERRERS: &[&str] = &[
    "https://google.com", "https://bing.com", "https://duckduckgo.com",
    "https://twitter.com", "https://reddit.com/r/programming",
    "https://news.ycombinator.com", "https://github.com",
    "(direct)", "(direct)", "(direct)",
];

const USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) AppleWebKit/605.1.15 Mobile Safari",
    "Mozilla/5.0 (iPad; CPU OS 17_0) AppleWebKit/605.1.15 Mobile Safari",
    "Mozilla/5.0 (Android 14; Mobile) AppleWebKit/537.36 Chrome/120.0",
    "Mozilla/5.0 (Macintosh) AppleWebKit/605.1.15 Safari/17.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
];

const COUNTRIES: &[&str] = &["US", "GB", "DE", "FR", "CA", "AU", "JP", "BR", "IN", "NL"];
const REGIONS: &[&str] = &[
    "California", "Texas", "New York", "London", "Bavaria",
    "Ile-de-France", "Ontario", "New South Wales", "Tokyo", "Sao Paulo",
];
const CITIES: &[&str] = &[
    "San Francisco", "Austin", "New York", "London", "Munich",
    "Paris", "Toronto", "Sydney", "Tokyo", "Sao Paulo",
];
const DEVICE_TYPES: &[&str] = &["desktop", "mobile", "tablet"];
const OS_NAMES: &[&str] = &["macOS", "Windows", "Linux", "iOS", "Android"];
const UTM_SOURCES: &[&str] = &["google", "facebook", "twitter", "newsletter", "reddit", "direct"];
const UTM_MEDIUMS: &[&str] = &["cpc", "organic", "social", "email", "referral"];
const UTM_CAMPAIGNS: &[&str] = &[
    "spring_sale", "black_friday", "product_launch", "retarget_q4", "brand_awareness",
];

fn gen_page_view_row(rng: &mut impl Rng, user_count: usize) -> String {
    let session_id = format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        rng.random_range(0u32..u32::MAX),
        rng.random_range(0u16..u16::MAX),
        rng.random_range(0u16..u16::MAX),
        rng.random_range(0u16..u16::MAX),
        rng.random_range(0u64..0xFFFFFFFFFFFF),
    );

    let user_id = if rng.random_bool(0.7) {
        format!("{}", rng.random_range(1..=user_count))
    } else {
        "NULL".to_string()
    };

    let url = URLS[rng.random_range(0..URLS.len())];
    let referrer = if rng.random_bool(0.6) {
        format!("'{}'", REFERRERS[rng.random_range(0..REFERRERS.len())])
    } else {
        "NULL".to_string()
    };
    let ua = USER_AGENTS[rng.random_range(0..USER_AGENTS.len())].replace('\'', "''");
    let ip = gen_ip(rng);
    let country = COUNTRIES[rng.random_range(0..COUNTRIES.len())];
    let region = REGIONS[rng.random_range(0..REGIONS.len())];
    let city = CITIES[rng.random_range(0..CITIES.len())];
    let device = DEVICE_TYPES[rng.random_range(0..DEVICE_TYPES.len())];
    let browser = BROWSERS[rng.random_range(0..BROWSERS.len())];
    let os = OS_NAMES[rng.random_range(0..OS_NAMES.len())];
    let sw = rng.random_range(320..3840);
    let sh = rng.random_range(568..2160);
    let vw = rng.random_range(320..sw + 1);
    let vh = rng.random_range(400..sh + 1);
    let page_load = rng.random_range(100..15000);
    let dom_ready = rng.random_range(50..page_load + 1);
    let time_on_page = rng.random_range(500..300000);
    let scroll_depth = rng.random_range(0..=100i16);
    let clicks = rng.random_range(0..=50i16);
    let is_bounce = rng.random_bool(0.35);

    let utm_source = if rng.random_bool(0.4) {
        format!("'{}'", UTM_SOURCES[rng.random_range(0..UTM_SOURCES.len())])
    } else {
        "NULL".to_string()
    };
    let utm_medium = if utm_source != "NULL" {
        format!("'{}'", UTM_MEDIUMS[rng.random_range(0..UTM_MEDIUMS.len())])
    } else {
        "NULL".to_string()
    };
    let utm_campaign = if utm_source != "NULL" && rng.random_bool(0.6) {
        format!("'{}'", UTM_CAMPAIGNS[rng.random_range(0..UTM_CAMPAIGNS.len())])
    } else {
        "NULL".to_string()
    };

    let custom = if rng.random_bool(0.3) {
        let ab = ["control", "variant_a", "variant_b"];
        let variant = ab[rng.random_range(0..ab.len())];
        format!("'{{\"ab_test\": \"{variant}\"}}'")
    } else {
        "NULL".to_string()
    };

    let created = gen_timestamp(rng, 2023, 2024);

    format!(
        "('{session_id}', {user_id}, '{url}', {referrer}, '{ua}', '{ip}', \
         '{country}', '{region}', '{city}', '{device}', '{browser}', '{os}', \
         {sw}, {sh}, {vw}, {vh}, {page_load}, {dom_ready}, {time_on_page}, \
         {scroll_depth}, {clicks}, {is_bounce}, {utm_source}, {utm_medium}, \
         {utm_campaign}, NULL, NULL, {custom}, '{created}')"
    )
}

const PV_COLS: &str = "(session_id, user_id, url, referrer, user_agent, ip_address, \
    country_code, region, city, device_type, browser, os, \
    screen_width, screen_height, viewport_width, viewport_height, \
    page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, \
    click_count, is_bounce, utm_source, utm_medium, utm_campaign, \
    utm_term, utm_content, custom_props, created_at)";

fn seed_pg_page_views(client: &mut postgres::Client, args: &Args) -> Result<()> {
    let mut rng = rand::rng();
    let mut tx = client.transaction()?;
    let mut batch = String::new();
    let mut count = 0;

    for i in 0..args.page_views {
        if count == 0 {
            batch.clear();
            write!(batch, "INSERT INTO page_views {} VALUES ", PV_COLS)?;
        } else {
            batch.push(',');
        }

        let row = gen_page_view_row(&mut rng, args.users);
        batch.push_str(&row);
        count += 1;

        if count >= args.batch_size || i == args.page_views - 1 {
            tx.batch_execute(&batch)?;
            count = 0;
            if (i + 1) % 500_000 == 0 {
                eprint!("    page_views {}/{}...\r", i + 1, args.page_views);
                std::io::stderr().flush().ok();
            }
        }
    }

    tx.commit()?;
    Ok(())
}

fn seed_mysql_page_views(conn: &mut mysql::PooledConn, args: &Args) -> Result<()> {
    use mysql::prelude::*;
    let mut rng = rand::rng();
    let mut batch = String::new();
    let mut count = 0;

    for i in 0..args.page_views {
        if count == 0 {
            batch.clear();
            write!(batch, "INSERT INTO page_views {} VALUES ", PV_COLS)?;
        } else {
            batch.push(',');
        }

        let row = gen_page_view_row(&mut rng, args.users);
        batch.push_str(&row);
        count += 1;

        if count >= args.batch_size || i == args.page_views - 1 {
            conn.query_drop(&batch)?;
            count = 0;
            if (i + 1) % 500_000 == 0 {
                eprint!("    page_views {}/{}...\r", i + 1, args.page_views);
                std::io::stderr().flush().ok();
            }
        }
    }

    Ok(())
}

// ─── Content Items (heavy text, worst-case memory) ───────────

const LOREM_SENTENCES: &[&str] = &[
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.",
    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum.",
    "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia.",
    "Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit.",
    "Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet consectetur.",
    "Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit.",
    "Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil.",
    "At vero eos et accusamus et iusto odio dignissimos ducimus qui blanditiis praesentium.",
    "Nam libero tempore, cum soluta nobis est eligendi optio cumque nihil impedit.",
    "Temporibus autem quibusdam et aut officiis debitis aut rerum necessitatibus saepe eveniet.",
    "Itaque earum rerum hic tenetur a sapiente delectus, ut aut reiciendis voluptatibus.",
    "Nulla facilisi. Mauris sollicitudin fermentum libero. Praesent nonummy mi in odio.",
    "Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis.",
    "Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis.",
];

const CONTENT_CATEGORIES: &[&str] = &[
    "engineering", "product", "design", "marketing", "support",
    "tutorial", "announcement", "changelog", "case-study", "opinion",
];

const CONTENT_STATUSES: &[&str] = &["draft", "review", "published", "archived"];

const CONTENT_TITLES: &[&str] = &[
    "Getting Started with Rust for Backend Development",
    "How We Scaled Our Database to Handle 1M QPS",
    "Best Practices for PostgreSQL Index Tuning",
    "Understanding Memory Management in Modern Systems",
    "A Deep Dive into Apache Arrow Columnar Format",
    "Building Reliable Data Pipelines at Scale",
    "Monitoring Production Databases Without Killing Them",
    "The Complete Guide to MySQL Replication",
    "Why We Migrated from MongoDB to PostgreSQL",
    "Optimizing Parquet File Sizes for Cloud Storage",
    "Handling Schema Evolution in Data Export Systems",
    "Real-time CDC vs Batch Export: A Practical Comparison",
    "How to Design a Source-Aware ETL Tool",
    "Performance Profiling Rust Applications in Production",
    "Building a Config-Driven Data Platform",
];

fn gen_lorem_text(rng: &mut impl Rng, target_bytes: usize) -> String {
    let mut text = String::with_capacity(target_bytes + 200);
    while text.len() < target_bytes {
        let sentence = LOREM_SENTENCES[rng.random_range(0..LOREM_SENTENCES.len())];
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(sentence);
    }
    text
}

fn gen_html_wrapper(body: &str) -> String {
    let mut html = String::with_capacity(body.len() * 2 + 500);
    html.push_str("<article><header><h1>Title</h1><meta charset=\"utf-8\"/></header><div class=\"content\">");
    for paragraph in body.split(". ") {
        html.push_str("<p>");
        html.push_str(paragraph);
        html.push_str(".</p>\n");
    }
    html.push_str("</div><footer><nav><a href=\"/prev\">Previous</a><a href=\"/next\">Next</a></nav></footer></article>");
    html
}

fn gen_content_metadata(rng: &mut impl Rng) -> String {
    let reading_time = rng.random_range(1..30);
    let version = rng.random_range(1..20);
    let seo_score = rng.random_range(0..100);
    format!(
        "{{\"reading_time_min\": {reading_time}, \"version\": {version}, \
         \"seo_score\": {seo_score}, \"featured\": {featured}, \
         \"allow_comments\": {comments}, \
         \"og_image\": \"https://cdn.example.com/images/{img}.jpg\", \
         \"canonical_url\": \"https://blog.example.com/posts/{slug}\"}}",
        featured = rng.random_bool(0.2),
        comments = rng.random_bool(0.8),
        img = rng.random_range(1000..9999),
        slug = rng.random_range(10000..99999),
    )
}

fn gen_extra_data(rng: &mut impl Rng) -> String {
    let revisions = rng.random_range(1..10);
    let mut editors = String::from("[");
    for i in 0..rng.random_range(1..4) {
        if i > 0 { editors.push_str(", "); }
        write!(editors, "\"editor_{}@example.com\"", rng.random_range(1..100)).ok();
    }
    editors.push(']');
    format!(
        "{{\"revisions\": {revisions}, \"editors\": {editors}, \
         \"source_system\": \"cms-v3\", \"import_batch\": \"{batch}\"}}",
        batch = rng.random_range(100..999),
    )
}

const CI_COLS: &str = "(title, body, raw_html, metadata, tags, author_name, author_email, \
    source_url, category, status, priority, view_count, comment_count, \
    word_count, language, published_at, updated_at, created_at, extra_data)";

fn gen_content_item_row(rng: &mut impl Rng) -> String {
    let title = CONTENT_TITLES[rng.random_range(0..CONTENT_TITLES.len())];
    let body_size = rng.random_range(2000..8000);
    let body = gen_lorem_text(rng, body_size).replace('\'', "''");
    let html = gen_html_wrapper(&body).replace('\'', "''");
    let metadata = gen_content_metadata(rng);
    let word_count = body.split_whitespace().count();

    let tags_list = ["rust", "postgres", "mysql", "data", "etl", "arrow", "parquet", "performance", "tutorial"];
    let n_tags = rng.random_range(1..5);
    let tags: Vec<&str> = (0..n_tags).map(|_| tags_list[rng.random_range(0..tags_list.len())]).collect();
    let tags_str = tags.join(",");

    let first = FIRST_NAMES[rng.random_range(0..FIRST_NAMES.len())];
    let last = LAST_NAMES[rng.random_range(0..LAST_NAMES.len())];
    let domain = DOMAINS[rng.random_range(0..DOMAINS.len())];
    let category = CONTENT_CATEGORIES[rng.random_range(0..CONTENT_CATEGORIES.len())];
    let status = CONTENT_STATUSES[rng.random_range(0..CONTENT_STATUSES.len())];
    let priority = rng.random_range(0..5);
    let views = rng.random_range(0..100000);
    let comments = rng.random_range(0..500);
    let lang = ["en", "de", "fr", "es", "ja"][rng.random_range(0..5)];

    let created = gen_timestamp(rng, 2022, 2024);
    let updated = gen_timestamp_after(rng, &created, 365);
    let published = if status == "published" || status == "archived" {
        format!("'{}'", gen_timestamp_after(rng, &created, 30))
    } else {
        "NULL".to_string()
    };

    let extra = gen_extra_data(rng);

    format!(
        "('{title}', '{body}', '{html}', '{metadata}', '{tags_str}', \
         '{first} {last}', '{first}.{last}@{domain}', \
         'https://blog.example.com/posts/{slug}', '{category}', '{status}', \
         {priority}, {views}, {comments}, {word_count}, '{lang}', \
         {published}, '{updated}', '{created}', '{extra}')",
        slug = rng.random_range(10000..99999),
    )
}

fn seed_pg_content_items(client: &mut postgres::Client, args: &Args) -> Result<()> {
    let mut rng = rand::rng();
    let mut tx = client.transaction()?;
    let mut batch = String::new();
    let mut count = 0;
    let ci_batch = 200; // smaller batch size for large rows

    for i in 0..args.content_items {
        if count == 0 {
            batch.clear();
            write!(batch, "INSERT INTO content_items {} VALUES ", CI_COLS)?;
        } else {
            batch.push(',');
        }

        batch.push_str(&gen_content_item_row(&mut rng));
        count += 1;

        if count >= ci_batch || i == args.content_items - 1 {
            tx.batch_execute(&batch)?;
            count = 0;
            if (i + 1) % 50_000 == 0 {
                eprint!("    content_items {}/{}...\r", i + 1, args.content_items);
                std::io::stderr().flush().ok();
            }
        }
    }

    tx.commit()?;
    Ok(())
}

fn seed_mysql_content_items(conn: &mut mysql::PooledConn, args: &Args) -> Result<()> {
    use mysql::prelude::*;
    let mut rng = rand::rng();
    let mut batch = String::new();
    let mut count = 0;
    let ci_batch = 200;

    for i in 0..args.content_items {
        if count == 0 {
            batch.clear();
            write!(batch, "INSERT INTO content_items {} VALUES ", CI_COLS)?;
        } else {
            batch.push(',');
        }

        batch.push_str(&gen_content_item_row(&mut rng));
        count += 1;

        if count >= ci_batch || i == args.content_items - 1 {
            conn.query_drop(&batch)?;
            count = 0;
            if (i + 1) % 50_000 == 0 {
                eprint!("    content_items {}/{}...\r", i + 1, args.content_items);
                std::io::stderr().flush().ok();
            }
        }
    }

    Ok(())
}

// ─── Sparse chunk demo: orders_sparse (wide id gaps) ─────────

fn ensure_orders_sparse_pg(client: &mut postgres::Client) -> Result<()> {
    client.batch_execute(
        r#"
CREATE TABLE IF NOT EXISTS orders_sparse (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE OR REPLACE VIEW orders_sparse_for_export AS
SELECT
    id,
    payload,
    ROW_NUMBER() OVER (ORDER BY id) AS chunk_rownum
FROM orders_sparse;
"#,
    )?;
    Ok(())
}

fn sparse_insert_batch_size(args: &Args) -> usize {
    args.sparse_chunk_batch_size.max(1)
}

fn insert_pg_orders_sparse(client: &mut postgres::Client, args: &Args) -> Result<usize> {
    if args.sparse_chunk_rows == 0 {
        return Ok(0);
    }
    let total = args.sparse_chunk_rows;
    let bs = sparse_insert_batch_size(args);
    let mut offset = 0usize;
    let progress_every = 100_000usize;
    while offset < total {
        let take = (total - offset).min(bs);
        let mut batch = String::from("INSERT INTO orders_sparse (id, payload) VALUES ");
        for j in 0..take {
            let i = offset + j;
            let id = 1_i64.saturating_add(i as i64 * args.sparse_chunk_id_gap);
            if j > 0 {
                batch.push(',');
            }
            // Short payload keeps multi-row statements small for huge `--sparse-chunk-rows`
            write!(batch, "({}, 's{}')", id, i)?;
        }
        client.execute(&batch, &[])?;
        offset += take;
        if total > 50_000 && (offset == total || offset % progress_every == 0) {
            eprint!("    orders_sparse {}/{}\r", offset, total);
            std::io::stderr().flush().ok();
        }
    }
    if total > 50_000 {
        eprintln!();
    }
    Ok(total)
}

fn seed_pg_orders_sparse_fill(client: &mut postgres::Client, args: &Args) -> Result<usize> {
    ensure_orders_sparse_pg(client)?;
    insert_pg_orders_sparse(client, args)
}

fn seed_pg_sparse_only(args: &Args) -> Result<()> {
    let mut client = postgres::Client::connect(&args.pg_url, postgres::NoTls)
        .context("failed to connect to PostgreSQL")?;
    ensure_orders_sparse_pg(&mut client)?;
    client
        .execute("TRUNCATE orders_sparse", &[])
        .context("truncate orders_sparse")?;
    let n = insert_pg_orders_sparse(&mut client, args)?;
    println!("  inserted {} row(s) into orders_sparse", n);
    if n > 0 {
        let row = client.query_one(
            "SELECT COUNT(*)::bigint, MIN(id), MAX(id) FROM orders_sparse",
            &[],
        )?;
        let cnt: i64 = row.get(0);
        let lo: Option<i64> = row.get(1);
        let hi: Option<i64> = row.get(2);
        println!("  COUNT / MIN(id) / MAX(id): {} / {:?} / {:?}", cnt, lo, hi);
    }
    Ok(())
}

fn ensure_orders_sparse_mysql(conn: &mut mysql::PooledConn) -> Result<()> {
    use mysql::prelude::*;
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS orders_sparse (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
)",
    )?;
    conn.query_drop(
        "CREATE OR REPLACE VIEW orders_sparse_for_export AS
SELECT id, payload, ROW_NUMBER() OVER (ORDER BY id) AS chunk_rownum FROM orders_sparse",
    )?;
    Ok(())
}

fn insert_mysql_orders_sparse(conn: &mut mysql::PooledConn, args: &Args) -> Result<usize> {
    use mysql::prelude::*;
    if args.sparse_chunk_rows == 0 {
        return Ok(0);
    }
    let total = args.sparse_chunk_rows;
    let bs = sparse_insert_batch_size(args);
    let mut offset = 0usize;
    let progress_every = 100_000usize;
    while offset < total {
        let take = (total - offset).min(bs);
        let mut batch = String::from("INSERT INTO orders_sparse (id, payload) VALUES ");
        for j in 0..take {
            let i = offset + j;
            let id = 1_i64.saturating_add(i as i64 * args.sparse_chunk_id_gap);
            if j > 0 {
                batch.push(',');
            }
            write!(batch, "({}, 's{}')", id, i)?;
        }
        conn.query_drop(&batch)?;
        offset += take;
        if total > 50_000 && (offset == total || offset % progress_every == 0) {
            eprint!("    orders_sparse {}/{}\r", offset, total);
            std::io::stderr().flush().ok();
        }
    }
    if total > 50_000 {
        eprintln!();
    }
    Ok(total)
}

fn seed_mysql_orders_sparse_fill(conn: &mut mysql::PooledConn, args: &Args) -> Result<usize> {
    ensure_orders_sparse_mysql(conn)?;
    insert_mysql_orders_sparse(conn, args)
}

fn seed_mysql_sparse_only(args: &Args) -> Result<()> {
    use mysql::prelude::*;
    let pool = mysql::Pool::new(mysql::Opts::from_url(&args.mysql_url)?)?;
    let mut conn = pool.get_conn()?;
    ensure_orders_sparse_mysql(&mut conn)?;
    conn.query_drop("TRUNCATE TABLE orders_sparse")?;
    let n = insert_mysql_orders_sparse(&mut conn, args)?;
    println!("  inserted {} row(s) into orders_sparse", n);
    if n > 0 {
        let (cnt, lo, hi): (i64, Option<i64>, Option<i64>) = conn
            .query_first("SELECT COUNT(*), MIN(id), MAX(id) FROM orders_sparse")?
            .expect("aggregate on non-empty table");
        println!("  COUNT / MIN(id) / MAX(id): {} / {:?} / {:?}", cnt, lo, hi);
    }
    Ok(())
}
