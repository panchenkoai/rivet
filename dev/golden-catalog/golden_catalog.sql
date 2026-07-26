-- Golden-seed metadata catalog (versioned source → SQLite `golden_catalog.db`).
--
-- ONE authoritative record per (seed, engine) of what each golden / garbage
-- fixture IS: its shape, the strategy rivet should pick for it, and — for the
-- keyset-parallel golden — the EXACT expected part-file count. The golden tests
-- and the pre-release gate read this instead of hard-coding numbers per test:
-- a seed's expected outcome lives here, next to what makes it that shape.
--
--   build:  bash dev/golden-catalog/build.sh   (regenerates golden_catalog.db)
--   query:  SELECT * FROM golden_seed WHERE category='garbage' AND engine='postgres';
--
-- `category`: 'normal' = a clean golden whose exact counts are pinned; 'garbage'
-- = an anonymized field-DB profile that TRIGGERS a specific strategy / bug guard
-- (dev/garbage/*.sql, `make seed-garbage`). Both feed the pre-release dogfood.

DROP TABLE IF EXISTS golden_seed;
CREATE TABLE golden_seed (
    name          TEXT    NOT NULL,  -- fixture / table name
    category      TEXT    NOT NULL CHECK (category IN ('normal', 'garbage')),
    engine        TEXT    NOT NULL,  -- postgres | mysql | mssql
    schema_table  TEXT    NOT NULL,  -- fully-qualified table as rivet reads it
    key_column    TEXT,              -- the keyset/chunk key (NULL = keyless)
    key_type      TEXT,              -- bigint | bigint unsigned | decimal(p,0) | none | ...
    row_count     INTEGER NOT NULL,
    key_min       TEXT,
    key_max       TEXT,
    span_per_row  REAL,              -- (max-min)/rows — the sparsity signal
    strategy      TEXT    NOT NULL,  -- keyset | keyset-parallel | range | full
    parallel      INTEGER,           -- parallel workers for the pinned golden (NULL = n/a)
    chunk_size    INTEGER,
    expected_rows INTEGER,           -- golden row count (NULL = not a pinned golden)
    expected_files INTEGER,          -- golden part-file count under (parallel, chunk_size)
    guards        TEXT    NOT NULL,  -- the behaviour / bug this profile pins
    sql_file      TEXT    NOT NULL,
    PRIMARY KEY (name, engine)
);

-- ── NORMAL: the keyset-parallel sparse GOLDEN (10M, id=n*997, span≈997) ───────
-- Cross-engine, EMPIRICALLY measured (21 not the naive 20 — the OFFSET boundary is
-- 0-indexed + range 0 is inclusive, so the first range holds total/N+1 rows → 6
-- files, the others 5 → 6/5/5/5). Validated live on the devbox at 10M.
INSERT INTO golden_seed VALUES
 ('keyset_sparse','normal','postgres','public.keyset_sparse','id','bigint',10000000,'997','9970000000',997.0,'keyset-parallel',4,500000,10000000,21,'parallel keyset fan-out on a very-sparse key: 4 ROW-percentile ranges, sparse-key immune (range chunking would explode)','dev/parallel_keyset/golden/fixture_postgres.sql'),
 ('keyset_sparse','normal','mysql','keyset_sparse','id','bigint',10000000,'997','9970000000',997.0,'keyset-parallel',4,500000,10000000,21,'parallel keyset fan-out on a very-sparse key','dev/parallel_keyset/golden/fixture_mysql.sql'),
 ('keyset_sparse','normal','mssql','dbo.keyset_sparse','id','bigint',10000000,'997','9970000000',997.0,'keyset-parallel',4,500000,10000000,21,'parallel keyset fan-out on a very-sparse key (MSSQL OFFSET..FETCH percentile sampling)','dev/parallel_keyset/golden/fixture_mssql.sql'),
 ('keyset_sparse_unsigned','normal','mysql','keyset_sparse_unsigned','id','bigint unsigned',10000000,'10000000100000000','10001000000000000',100000000.0,'keyset-parallel',4,500000,10000000,21,'unsigned key ENTIRELY above i64::MAX — boundary literals + paging cursor must round-trip as u64 (the field unsigned-keyset bug regime)','dev/parallel_keyset/golden/fixture_mysql_unsigned.sql');

-- ── GARBAGE: anonymized field-DB profiles (dev/garbage/*.sql, `ext` schema) ───
-- Each triggers a specific strategy / diagnostic / bug guard. 150K rows/table
-- (past init's 100K keyset threshold, so init scaffolds the real strategy).
INSERT INTO golden_seed VALUES
 ('bigint_pk_dual_ts','garbage','postgres','ext.bigint_pk_dual_ts','id','bigint',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'fleet-majority shape: bigint PK + dual created_at/updated_at → keyset(id) (PK beats the timestamp cursor)','dev/garbage/postgres.sql'),
 ('int_pk_dual_ts','garbage','postgres','ext.int_pk_dual_ts','id','integer',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'int-PK minority (fleet mixes bigint + int)','dev/garbage/postgres.sql'),
 ('sparse_key','garbage','postgres','ext.sparse_key','id','bigint',150000,'1','149999000001',1000000.0,'keyset',NULL,NULL,NULL,NULL,'sparse key: id span vastly exceeds row count → range chunking explodes into near-empty windows (the sparse-guard warn shape); keyset is immune','dev/garbage/postgres.sql'),
 ('decimal_key','garbage','postgres','ext.decimal_key','dkey','decimal(20,0)',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'scale-0 DECIMAL PK (ERP-migration shape): NOT integer-family → an explicit range chunk_column must LOUDLY bail (#103); chunk_by_key (keyset) IS accepted','dev/garbage/postgres.sql'),
 ('no_pk_no_ts','garbage','postgres','ext.no_pk_no_ts',NULL,'none',150000,NULL,NULL,NULL,'full','full',NULL,NULL,NULL,'keyless + cursorless → mode:full fallback (no chunk column, no cursor)','dev/garbage/postgres.sql'),
 ('ref_id_version','garbage','postgres','ext.ref_id_version','ref_id','integer (non-unique)',150000,'1','30000',NULL,'range',NULL,NULL,NULL,NULL,'history/version table: non-unique non-PK ref_id → range-CHUNK (not keyset); the field parallel-checkpoint statement-timeout failure shape','dev/garbage/postgres.sql'),
 ('bigint_unsigned_pk','garbage','mysql','ext_bigint_unsigned_pk','id','bigint unsigned',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'BIGINT UNSIGNED key (MySQL-specific field hazard) → keyset must handle u64 keys past i64::MAX','dev/garbage/mysql.sql');
