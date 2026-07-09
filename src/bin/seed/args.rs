use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum SeedProfile {
    /// Server-side SQL (`generate_series` / recursive CTE). Best for 100k+ rows.
    #[default]
    Fast,
    /// Realistic names, JSON payloads, and lorem text. Postgres uses COPY + workers.
    Realistic,
    /// Original v1 multi-row INSERT from Rust (slow; backward compatibility).
    Insert,
}

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(1, 8))
        .unwrap_or(4)
}

#[derive(Parser)]
#[command(
    name = "seed",
    about = "Generate test data for rivet (v2: fast SQL profile + COPY bulk load)"
)]
pub struct Args {
    /// Target database: postgres, mysql, sqlserver, both (pg+mysql), or all
    #[arg(short, long, default_value = "both")]
    pub target: String,

    /// Load strategy: `fast` (SQL-side generation) or `realistic` (Rust + COPY on PG)
    #[arg(long, value_enum, default_value_t = SeedProfile::Fast)]
    pub profile: SeedProfile,

    /// Parallel COPY/insert workers (realistic Postgres orders/events/page_views)
    #[arg(long, default_value_t = default_workers())]
    pub workers: usize,

    /// Number of users to generate
    #[arg(long, default_value = "100000")]
    pub users: usize,

    /// Average orders per user (fast profile uses exact count; realistic uses Poisson)
    #[arg(long, default_value = "10")]
    pub orders_per_user: usize,

    /// Average events per user (fast profile uses exact count; realistic uses Poisson)
    #[arg(long, default_value = "50")]
    pub events_per_user: usize,

    /// Number of page_views to generate (wide table, degraded scenario)
    #[arg(long, default_value = "2000000")]
    pub page_views: usize,

    /// Number of content_items to generate (heavy text, worst case for memory)
    #[arg(long, default_value = "200000")]
    pub content_items: usize,

    /// PostgreSQL connection URL
    #[arg(long, default_value = "postgresql://rivet:rivet@localhost:5432/rivet")]
    pub pg_url: String,

    /// MySQL connection URL
    #[arg(long, default_value = "mysql://rivet:rivet@localhost:3306/rivet")]
    pub mysql_url: String,

    /// SQL Server connection URL (sqlserver://user:pass@host:port/db)
    #[arg(
        long,
        default_value = "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet"
    )]
    pub mssql_url: String,

    /// Batch size for realistic INSERT path (Postgres realistic uses COPY instead)
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Fill `orders_sparse` with few rows and huge gaps in `id` (for chunked / sparse-key demos)
    #[arg(long)]
    pub sparse_chunk_demo: bool,

    /// Only create (if needed), truncate, and fill `orders_sparse` — skip users/orders/events/page_views/content
    #[arg(long)]
    pub only_sparse_chunk_demo: bool,

    /// Number of rows in `orders_sparse` (ids: 1, 1+gap, 1+2·gap, …). Use ≥3 to see sparse min..max vs row count
    #[arg(long, default_value = "3")]
    pub sparse_chunk_rows: usize,

    /// Gap between consecutive sparse ids (wider gap ⇒ more empty chunk windows for the same chunk_size)
    #[arg(long, default_value = "2000000")]
    pub sparse_chunk_id_gap: i64,

    /// Rows per INSERT for `orders_sparse` (keep ≤ few thousand if max_allowed_packet / statement size is tight)
    #[arg(long, default_value = "5000")]
    pub sparse_chunk_batch_size: usize,

    /// Number of rows in `orders_coalesce` (composite-cursor demo with NULL updated_at)
    #[arg(long, default_value = "2000")]
    pub coalesce_rows: usize,

    /// Fraction of `orders_coalesce` rows with NULL `updated_at` (range 0.0..1.0)
    #[arg(long, default_value = "0.35")]
    pub coalesce_null_ratio: f64,
}

impl Args {
    pub fn planned_orders(&self) -> usize {
        self.users.saturating_mul(self.orders_per_user)
    }

    pub fn planned_events(&self) -> usize {
        self.users.saturating_mul(self.events_per_user)
    }
}
