# oracle-driver spike

Standalone crate (not a rivet workspace member): reads an Oracle table through the
pure-Rust `oracledb` beta, builds Arrow itself, writes `/tmp/oracle-spike.parquet`,
and dumps Oracle's own rendering to `/tmp/oracle-spike-truth.tsv`. `cargo run --
logminer` mines one insert through `DBMS_LOGMNR`. Results: `dev/research/oracle-source.md` §7a.

    docker run -d --name rivet-oracle-spike -p 15210:1521 -e ORACLE_PASSWORD=rivet gvenzl/oracle-free:23-slim-faststart
    # as system in FREEPDB1: CREATE USER spike IDENTIFIED BY rivet DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
    #                        GRANT CREATE SESSION, CREATE TABLE TO spike;
    CARGO_TARGET_DIR=/tmp/oracle-spike-target cargo run
