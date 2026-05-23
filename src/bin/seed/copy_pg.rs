//! Realistic Postgres seeding via `COPY FROM STDIN` with optional parallel workers.

use std::io::Write as IoWrite;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use anyhow::{Context, Result};
use postgres::Client;
use rand::RngExt;

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
    copy_pg_users(&mut client, args)?;
    println!(
        "  users:      {:>10} rows in {:.1}s (COPY)",
        args.users,
        t.elapsed().as_secs_f64()
    );

    let t = Instant::now();
    let total_orders = copy_pg_orders_parallel(args)?;
    println!(
        "  orders:     {:>10} rows in {:.1}s (COPY, {} workers)",
        total_orders,
        t.elapsed().as_secs_f64(),
        args.workers
    );

    let t = Instant::now();
    let total_events = copy_pg_events_parallel(args)?;
    println!(
        "  events:     {:>10} rows in {:.1}s (COPY, {} workers)",
        total_events,
        t.elapsed().as_secs_f64(),
        args.workers
    );

    if args.page_views > 0 {
        let t = Instant::now();
        copy_pg_page_views_parallel(args)?;
        println!(
            "  page_views: {:>10} rows in {:.1}s (COPY, {} workers)",
            args.page_views,
            t.elapsed().as_secs_f64(),
            args.workers
        );
    }

    if args.content_items > 0 {
        let t = Instant::now();
        copy_pg_content_items(&mut client, args)?;
        println!(
            "  content:    {:>10} rows in {:.1}s (COPY)",
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

fn copy_pg_users(client: &mut Client, args: &Args) -> Result<()> {
    let columns = "name, email, age, balance, is_active, bio, created_at, updated_at";
    pg_copy(client, "users", columns, |w| {
        let mut rng = rand::rng();
        let mut buf = Vec::with_capacity(args.batch_size * 256);
        for i in 0..args.users {
            let (name, email) = insert::gen_user(&mut rng, i);
            let age = rng.random_range(18..=65);
            let balance = rng.random_range(0.0..200_000.0_f64);
            let is_active = rng.random_bool(0.9);
            let bio = if rng.random_bool(0.7) {
                Some(insert::BIOS[rng.random_range(0..insert::BIOS.len())].to_string())
            } else {
                None
            };
            let created = insert::gen_timestamp(&mut rng, 2023, 2024);
            let updated = insert::gen_timestamp_after(&mut rng, &created, 180);
            write_copy_row(
                &mut buf,
                &[
                    copy_text(&name),
                    copy_text(&email),
                    copy_text(&age.to_string()),
                    copy_text(&format!("{balance:.2}")),
                    copy_bool(is_active),
                    copy_opt_text(bio.as_deref()),
                    copy_text(&created),
                    copy_text(&updated),
                ],
            );
            if buf.len() >= args.batch_size * 256 || i + 1 == args.users {
                w.write_all(&buf)?;
                buf.clear();
            }
        }
        Ok(args.users)
    })?;
    Ok(())
}

fn copy_pg_orders_parallel(args: &Args) -> Result<usize> {
    let workers = args.workers.max(1);
    let err: Arc<Mutex<Option<anyhow::Error>>> = Arc::new(Mutex::new(None));
    let total = Arc::new(Mutex::new(0usize));
    let chunk = args.users.div_ceil(workers);

    thread::scope(|scope| {
        for worker in 0..workers {
            let start_user = worker * chunk + 1;
            if start_user > args.users {
                continue;
            }
            let end_user = ((worker + 1) * chunk).min(args.users);
            let url = args.pg_url.clone();
            let orders_per_user = args.orders_per_user;
            let batch_size = args.batch_size;
            let err = Arc::clone(&err);
            let total = Arc::clone(&total);

            scope.spawn(move || {
                match copy_pg_orders_worker(&url, start_user, end_user, orders_per_user, batch_size)
                {
                    Ok(n) => *total.lock().expect("total lock") += n,
                    Err(e) => *err.lock().expect("err lock") = Some(e),
                }
            });
        }
    });

    if let Some(e) = err.lock().expect("err lock").take() {
        return Err(e);
    }
    Ok(*total.lock().expect("total lock"))
}

fn copy_pg_orders_worker(
    url: &str,
    start_user: usize,
    end_user: usize,
    orders_per_user: usize,
    batch_size: usize,
) -> Result<usize> {
    let mut client = Client::connect(url, postgres::NoTls)?;
    let columns = "user_id, product, quantity, price, status, notes, ordered_at, updated_at";
    pg_copy(&mut client, "orders", columns, |w| {
        let mut rng = rand::rng();
        let mut buf = Vec::with_capacity(batch_size * 512);
        let mut count = 0usize;
        for user_id in start_user..=end_user {
            let n_orders = insert::poisson_sample(&mut rng, orders_per_user as f64);
            for _ in 0..n_orders {
                let product = insert::PRODUCTS[rng.random_range(0..insert::PRODUCTS.len())]
                    .replace('\"', "\\\"");
                let quantity = rng.random_range(1..=10);
                let price = rng.random_range(5.0..5000.0_f64);
                let status = insert::STATUSES[rng.random_range(0..insert::STATUSES.len())];
                let notes = if rng.random_bool(0.4) {
                    Some(insert::gen_note(&mut rng))
                } else {
                    None
                };
                let ordered = insert::gen_timestamp(&mut rng, 2023, 2024);
                let updated = insert::gen_timestamp_after(&mut rng, &ordered, 30);
                write_copy_row(
                    &mut buf,
                    &[
                        copy_text(&user_id.to_string()),
                        copy_text(&product),
                        copy_text(&quantity.to_string()),
                        copy_text(&format!("{price:.2}")),
                        copy_text(status),
                        copy_opt_text(notes.as_deref()),
                        copy_text(&ordered),
                        copy_text(&updated),
                    ],
                );
                count += 1;
                if count.is_multiple_of(batch_size) {
                    w.write_all(&buf)?;
                    buf.clear();
                }
            }
        }
        if !buf.is_empty() {
            w.write_all(&buf)?;
        }
        Ok(count)
    })
}

fn copy_pg_events_parallel(args: &Args) -> Result<usize> {
    let workers = args.workers.max(1);
    let err: Arc<Mutex<Option<anyhow::Error>>> = Arc::new(Mutex::new(None));
    let total = Arc::new(Mutex::new(0usize));
    let chunk = args.users.div_ceil(workers);

    thread::scope(|scope| {
        for worker in 0..workers {
            let start_user = worker * chunk + 1;
            if start_user > args.users {
                continue;
            }
            let end_user = ((worker + 1) * chunk).min(args.users);
            let url = args.pg_url.clone();
            let events_per_user = args.events_per_user;
            let batch_size = args.batch_size;
            let err = Arc::clone(&err);
            let total = Arc::clone(&total);

            scope.spawn(move || {
                match copy_pg_events_worker(&url, start_user, end_user, events_per_user, batch_size)
                {
                    Ok(n) => *total.lock().expect("total lock") += n,
                    Err(e) => *err.lock().expect("err lock") = Some(e),
                }
            });
        }
    });

    if let Some(e) = err.lock().expect("err lock").take() {
        return Err(e);
    }
    Ok(*total.lock().expect("total lock"))
}

fn copy_pg_events_worker(
    url: &str,
    start_user: usize,
    end_user: usize,
    events_per_user: usize,
    batch_size: usize,
) -> Result<usize> {
    let mut client = Client::connect(url, postgres::NoTls)?;
    let columns = "user_id, event_type, payload, ip_address, created_at";
    pg_copy(&mut client, "events", columns, |w| {
        let mut rng = rand::rng();
        let mut buf = Vec::with_capacity(batch_size * 512);
        let mut count = 0usize;
        for user_id in start_user..=end_user {
            let n_events = insert::poisson_sample(&mut rng, events_per_user as f64);
            for _ in 0..n_events {
                let event_type =
                    insert::EVENT_TYPES[rng.random_range(0..insert::EVENT_TYPES.len())];
                let payload = insert::gen_event_payload(&mut rng, event_type);
                let ip = insert::gen_ip(&mut rng);
                let created = insert::gen_timestamp(&mut rng, 2023, 2024);
                write_copy_row(
                    &mut buf,
                    &[
                        copy_text(&user_id.to_string()),
                        copy_text(event_type),
                        copy_text(&payload),
                        copy_text(&ip),
                        copy_text(&created),
                    ],
                );
                count += 1;
                if count.is_multiple_of(batch_size) {
                    w.write_all(&buf)?;
                    buf.clear();
                }
            }
        }
        if !buf.is_empty() {
            w.write_all(&buf)?;
        }
        Ok(count)
    })
}

fn copy_pg_page_views_parallel(args: &Args) -> Result<()> {
    let workers = args.workers.max(1);
    let err: Arc<Mutex<Option<anyhow::Error>>> = Arc::new(Mutex::new(None));
    let chunk = args.page_views.div_ceil(workers);

    thread::scope(|scope| {
        for worker in 0..workers {
            let start = worker * chunk;
            if start >= args.page_views {
                continue;
            }
            let end = ((worker + 1) * chunk).min(args.page_views);
            let url = args.pg_url.clone();
            let users = args.users;
            let batch_size = args.batch_size;
            let err = Arc::clone(&err);

            scope.spawn(move || {
                if let Err(e) = copy_pg_page_views_worker(&url, start, end, users, batch_size) {
                    *err.lock().expect("err lock") = Some(e);
                }
            });
        }
    });

    if let Some(e) = err.lock().expect("err lock").take() {
        return Err(e);
    }
    Ok(())
}

fn copy_pg_page_views_worker(
    url: &str,
    start: usize,
    end: usize,
    users: usize,
    batch_size: usize,
) -> Result<()> {
    let mut client = Client::connect(url, postgres::NoTls)?;
    let columns = insert::PV_COLS
        .trim_start_matches('(')
        .trim_end_matches(')');
    pg_copy(&mut client, "page_views", columns, |w| {
        let mut rng = rand::rng();
        let mut buf = Vec::with_capacity(batch_size * 1024);
        let mut count = 0usize;
        for _i in start..end {
            let row = insert::gen_page_view_row(&mut rng, users);
            // gen_page_view_row returns SQL VALUES tuple — convert to COPY tab row
            let copy_row = sql_values_tuple_to_copy_row(&row);
            buf.extend_from_slice(copy_row.as_bytes());
            buf.push(b'\n');
            count += 1;
            if count.is_multiple_of(batch_size) {
                w.write_all(&buf)?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            w.write_all(&buf)?;
        }
        Ok(count)
    })?;
    Ok(())
}

fn copy_pg_content_items(client: &mut Client, args: &Args) -> Result<()> {
    let columns = insert::CI_COLS
        .trim_start_matches('(')
        .trim_end_matches(')');
    pg_copy(client, "content_items", columns, |w| {
        let mut rng = rand::rng();
        let mut buf = Vec::with_capacity(512 * 1024);
        let batch = 200usize;
        for i in 0..args.content_items {
            let row = insert::gen_content_item_row(&mut rng);
            let copy_row = sql_values_tuple_to_copy_row(&row);
            buf.extend_from_slice(copy_row.as_bytes());
            buf.push(b'\n');
            if (i + 1) % batch == 0 || i + 1 == args.content_items {
                w.write_all(&buf)?;
                buf.clear();
            }
        }
        Ok(args.content_items)
    })?;
    Ok(())
}

fn pg_copy(
    client: &mut Client,
    table: &str,
    columns: &str,
    mut write_rows: impl FnMut(&mut dyn IoWrite) -> Result<usize>,
) -> Result<usize> {
    let sql = format!("COPY {table} ({columns}) FROM STDIN WITH (FORMAT text)");
    let mut writer = client.copy_in(&sql).context("COPY start")?;
    let count = write_rows(&mut writer)?;
    writer.finish().context("COPY finish")?;
    Ok(count)
}

fn write_copy_row(buf: &mut Vec<u8>, fields: &[String]) {
    for (idx, field) in fields.iter().enumerate() {
        if idx > 0 {
            buf.push(b'\t');
        }
        buf.extend_from_slice(field.as_bytes());
    }
    buf.push(b'\n');
}

fn copy_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

fn copy_opt_text(value: Option<&str>) -> String {
    match value {
        None => "\\N".to_string(),
        Some(v) => copy_text(v),
    }
}

fn copy_bool(value: bool) -> String {
    if value {
        "t".to_string()
    } else {
        "f".to_string()
    }
}

/// Convert `('a', 1, NULL, ...)` SQL fragment from legacy generators to COPY text.
fn sql_values_tuple_to_copy_row(tuple: &str) -> String {
    let inner = tuple.trim().trim_start_matches('(').trim_end_matches(')');
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();
    let mut in_quote = false;
    let mut token = String::new();

    let flush = |out: &mut String, token: &str| {
        let trimmed = token.trim();
        if trimmed.eq_ignore_ascii_case("NULL") {
            if !out.is_empty() {
                out.push('\t');
            }
            out.push_str("\\N");
        } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
            if !out.is_empty() {
                out.push('\t');
            }
            let unquoted = &trimmed[1..trimmed.len() - 1];
            out.push_str(&copy_text(unquoted));
        } else {
            if !out.is_empty() {
                out.push('\t');
            }
            out.push_str(trimmed);
        }
    };

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                token.push(ch);
            }
            '\'' if in_quote => {
                if chars.peek() == Some(&'\'') {
                    token.push(ch);
                    token.push(chars.next().expect("escaped quote"));
                } else {
                    in_quote = false;
                    token.push(ch);
                }
            }
            ',' if !in_quote => {
                flush(&mut out, &token);
                token.clear();
            }
            _ => token.push(ch),
        }
    }
    if !token.is_empty() {
        flush(&mut out, &token);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::sql_values_tuple_to_copy_row;

    #[test]
    fn converts_sql_tuple_to_copy_row() {
        let row = "('Alice', 42, NULL, '2024-01-01 00:00:00')";
        assert_eq!(
            sql_values_tuple_to_copy_row(row),
            "Alice\t42\t\\N\t2024-01-01 00:00:00"
        );
    }
}
