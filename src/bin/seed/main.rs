mod args;
mod copy_pg;
mod fast;
mod insert;

use anyhow::Result;
use clap::Parser;

use args::{Args, SeedProfile};

fn main() -> Result<()> {
    let args = Args::parse();

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

    if args.target == "postgres" || args.target == "both" {
        println!("\n=== PostgreSQL ===");
        match args.profile {
            SeedProfile::Fast => fast::seed_postgres(&args)?,
            SeedProfile::Realistic => copy_pg::seed_postgres(&args)?,
            SeedProfile::Insert => insert::seed_postgres(&args)?,
        }
    }
    if args.target == "mysql" || args.target == "both" {
        println!("\n=== MySQL ===");
        match args.profile {
            SeedProfile::Fast => fast::seed_mysql(&args)?,
            SeedProfile::Realistic | SeedProfile::Insert => insert::seed_mysql(&args)?,
        }
    }

    println!("\nDone!");
    Ok(())
}
