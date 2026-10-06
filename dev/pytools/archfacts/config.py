"""Tunable tables and thresholds of archfacts; bump SCHEMA whenever a fact's meaning or shape changes."""

from __future__ import annotations

SCHEMA = 1

FEATURE_SETS = {
    "default": ("jemalloc", "oracle"),
    "no-default-jemalloc": ("jemalloc",),
}

# Engine-specific code: the repo paths that are the engine's home, and the driver crates only that home should name.
ENGINES = {
    "postgres": {
        "paths": ["src/source/postgres", "src/source/pg_numeric_wire.rs", "src/init/postgres.rs", "src/preflight/postgres.rs"],
        "crates": ["postgres", "postgres_types", "postgres_protocol", "postgres_native_tls", "tokio_postgres"],
    },
    "mysql": {
        "paths": ["src/source/mysql", "src/init/mysql.rs", "src/preflight/mysql.rs"],
        "crates": ["mysql", "mysql_common"],
    },
    "mssql": {
        "paths": ["src/source/mssql", "src/init/mssql.rs", "src/preflight/mssql.rs"],
        "crates": ["tiberius"],
    },
    "mongo": {
        "paths": ["src/source/mongo", "src/init/mongo.rs", "src/preflight/mongo.rs", "src/pipeline/mongo_parallel.rs"],
        "crates": ["mongodb", "bson"],
    },
    "oracle": {
        "paths": ["src/source/oracle", "src/init/oracle.rs", "src/preflight/oracle.rs"],
        "crates": ["oracledb"],
    },
}

SOURCE_DIRS = ("src", "tests", "benches", "examples")
# What the index is built from: only a change under these makes the tree `dirty` for the cache key.
INDEX_INPUTS = ("src", "tests", "benches", "examples", "Cargo.toml", "Cargo.lock", "build.rs")
TEST_DIRS = ("tests", "benches", "examples")
STD_CRATES = frozenset({"core", "std", "alloc", "proc_macro", "test"})

GIT_WINDOW = 200
COCHANGE_MAX_FILES = 30
COCHANGE_MIN_TOGETHER = 3

DUP_MIN_TOKENS = 40
DUP_SHINGLE = 8
DUP_NEAR_JACCARD = 0.75
DUP_COMMON_SHINGLE = 30

# Share of identifiers in indexed production code that rust-analyzer left without a symbol; above it the run is `degraded`.
DEGRADED_UNRESOLVED_RATIO = 0.02

CALLERS_KEPT = 200
SITES_KEPT = 40
