mod args;
mod copy_pg;
mod fast;
mod insert;
mod mssql;

use anyhow::{Result, bail};
use clap::Parser;

use args::{Args, SeedProfile};

/// Env var that must equal `1` to authorise the destructive seed run.
const CONFIRM_ENV: &str = "RIVET_SEED_I_KNOW";

/// Tables every seed profile unconditionally `TRUNCATE ... CASCADE`s before
/// loading fixtures (see fast.rs / copy_pg.rs / insert.rs).
const TRUNCATED_TABLES: &str =
    "orders_coalesce, orders_sparse, content_items, page_views, events, orders, users";

/// Returns `Ok(())` only when the operator has explicitly opted in to the
/// destructive run via `RIVET_SEED_I_KNOW=1`. This is a safety gate, not a
/// security boundary: the binary is dev-only (built behind the `dev-seed`
/// feature) and connects to a local fixture DB, but its default run wipes
/// real tables named `users` / `orders` with no prompt, so we refuse unless
/// the caller confirms they know what will be truncated.
fn confirm_destructive(
    confirmed: bool,
    target: &str,
    pg_url: &str,
    mysql_url: &str,
    mssql_url: &str,
) -> Result<()> {
    if confirmed {
        return Ok(());
    }
    let urls = match target {
        "postgres" => format!("\n  postgres: {pg_url}"),
        "mysql" => format!("\n  mysql:    {mysql_url}"),
        "sqlserver" | "mssql" => format!("\n  sqlserver: {mssql_url}"),
        "all" => {
            format!("\n  postgres: {pg_url}\n  mysql:    {mysql_url}\n  sqlserver: {mssql_url}")
        }
        _ => format!("\n  postgres: {pg_url}\n  mysql:    {mysql_url}"),
    };
    bail!(
        "refusing to seed: this will TRUNCATE ... CASCADE (SQL Server: DROP+CREATE) the following tables{urls}\n  tables:   {TRUNCATED_TABLES}\n\nThis is destructive and not reversible. Re-run with {CONFIRM_ENV}=1 to proceed."
    );
}

fn main() -> Result<()> {
    let args = Args::parse();

    let confirmed = std::env::var(CONFIRM_ENV).as_deref() == Ok("1");
    confirm_destructive(
        confirmed,
        &args.target,
        &args.pg_url,
        &args.mysql_url,
        &args.mssql_url,
    )?;

    if args.only_sparse_chunk_demo {
        println!(
            "Sparse chunk demo: {} row(s), id gap {}",
            args.sparse_chunk_rows, args.sparse_chunk_id_gap
        );
        if args.target == "postgres" || args.target == "both" {
            println!("\n=== PostgreSQL (orders_sparse only) ===");
            insert::seed_pg_sparse_only(&args)?;
        }
        if args.target == "mysql" || args.target == "both" {
            println!("\n=== MySQL (orders_sparse only) ===");
            insert::seed_mysql_sparse_only(&args)?;
        }
        println!("\nDone!");
        return Ok(());
    }

    let profile_label = match args.profile {
        SeedProfile::Fast => "fast (SQL-side)",
        SeedProfile::Realistic => "realistic (Rust + COPY on PG)",
        SeedProfile::Insert => "legacy INSERT (v1)",
    };
    println!(
        "Profile: {} | workers: {} | Generating: {} users, {} orders, {} events, {} page_views, {} content_items{}",
        profile_label,
        args.workers,
        args.users,
        args.planned_orders(),
        args.planned_events(),
        args.page_views,
        args.content_items,
        if args.sparse_chunk_demo {
            " + orders_sparse demo"
        } else {
            ""
        }
    );

    if matches!(args.target.as_str(), "postgres" | "both" | "all") {
        println!("\n=== PostgreSQL ===");
        match args.profile {
            SeedProfile::Fast => fast::seed_postgres(&args)?,
            SeedProfile::Realistic => copy_pg::seed_postgres(&args)?,
            SeedProfile::Insert => insert::seed_postgres(&args)?,
        }
    }
    if matches!(args.target.as_str(), "mysql" | "both" | "all") {
        println!("\n=== MySQL ===");
        match args.profile {
            SeedProfile::Fast => fast::seed_mysql(&args)?,
            SeedProfile::Realistic | SeedProfile::Insert => insert::seed_mysql(&args)?,
        }
    }
    if matches!(args.target.as_str(), "sqlserver" | "mssql" | "all") {
        // SQL Server generates rows server-side via GENERATE_SERIES (2022+);
        // there is no COPY/INSERT split, so every profile takes the one path.
        println!("\n=== SQL Server ===");
        mssql::seed_mssql(&args)?;
    }

    println!("\nDone!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PG: &str = "postgresql://rivet:rivet@localhost:5432/rivet";
    const MY: &str = "mysql://rivet:rivet@localhost:3306/rivet";
    const MS: &str = "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet";

    #[test]
    fn refuses_destructive_run_without_confirmation() {
        let err = confirm_destructive(false, "both", PG, MY, MS)
            .unwrap_err()
            .to_string();
        assert!(err.contains("refusing to seed"), "got: {err}");
        assert!(err.contains("TRUNCATE"), "got: {err}");
        // Must name the tables it will wipe and how to opt in.
        assert!(err.contains("users"), "got: {err}");
        assert!(err.contains(CONFIRM_ENV), "got: {err}");
    }

    #[test]
    fn allows_destructive_run_when_confirmed() {
        assert!(confirm_destructive(true, "both", PG, MY, MS).is_ok());
    }

    #[test]
    fn sqlserver_target_names_only_the_mssql_url() {
        // A SQL Server run must show the mssql url (and not silently point the
        // operator at the postgres/mysql fixtures it is not touching).
        let err = confirm_destructive(false, "sqlserver", PG, MY, MS)
            .unwrap_err()
            .to_string();
        assert!(err.contains(MS), "got: {err}");
        assert!(!err.contains("5432"), "got: {err}");
        // `all` fans out to every engine, so it must list all three.
        let all = confirm_destructive(false, "all", PG, MY, MS)
            .unwrap_err()
            .to_string();
        assert!(
            all.contains("5432") && all.contains("3306") && all.contains("1433"),
            "got: {all}"
        );
    }
}
