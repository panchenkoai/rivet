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
--
-- Seed count is PER ENGINE (the PRIMARY KEY is (name, engine)): the garbage set
-- exists in all three SQL engines (PG `ext.` schema, MySQL `ext_` prefix, MSSQL
-- `ext.` schema), plus two engine-specific hazards — MySQL `bigint_unsigned_pk`
-- and MSSQL `wide_cols`. Mongo has no garbage SQL fixtures (document store).

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
 ('keyset_sparse_unsigned','normal','mysql','keyset_sparse_unsigned','id','bigint unsigned',10000000,'10000000000100000000','10001000000000000000',100000000.0,'keyset-parallel',4,500000,10000000,21,'unsigned key ENTIRELY above i64::MAX — boundary literals + paging cursor must round-trip as u64 (the field unsigned-keyset bug regime)','dev/parallel_keyset/golden/fixture_mysql_unsigned.sql');

-- ── GARBAGE: anonymized field-DB profiles (dev/garbage/*.sql, `make seed-garbage`) ─
-- Each triggers a specific strategy / diagnostic / bug guard. 150K rows/table
-- (past init's 100K keyset threshold, so init scaffolds the real strategy). The
-- shared 9 profiles exist in all three engines; MySQL adds bigint_unsigned_pk and
-- MSSQL adds wide_cols → 9×3 + 2 = 29 garbage records.
INSERT INTO golden_seed VALUES
 ('bigint_pk_dual_ts','garbage','postgres','ext.bigint_pk_dual_ts','id','bigint',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'fleet-majority shape: bigint PK + dual created_at/updated_at -> keyset(id) (PK beats the timestamp cursor)','dev/garbage/postgres.sql'),
 ('int_pk_dual_ts','garbage','postgres','ext.int_pk_dual_ts','id','integer',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'int-PK minority (fleet mixes bigint + int)','dev/garbage/postgres.sql'),
 ('sparse_key','garbage','postgres','ext.sparse_key','id','bigint',150000,'1','149999000001',1000000.0,'keyset',NULL,NULL,NULL,NULL,'sparse key: id span vastly exceeds row count -> range chunking explodes into near-empty windows (the sparse-guard warn shape); keyset is immune','dev/garbage/postgres.sql'),
 ('decimal_key','garbage','postgres','ext.decimal_key','dkey','decimal(15,0)',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'scale-0 DECIMAL PK (ERP-migration shape): NOT integer-family -> an explicit range chunk_column must LOUDLY bail (#103); chunk_by_key (keyset) IS accepted','dev/garbage/postgres.sql'),
 ('no_pk_no_ts','garbage','postgres','ext.no_pk_no_ts',NULL,'none',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'keyless + cursorless -> mode:full fallback (no chunk column, no cursor)','dev/garbage/postgres.sql'),
 ('ref_id_history','garbage','postgres','ext.ref_id_history','ref_id','bigint (non-unique, indexed)',150000,NULL,NULL,NULL,'range',NULL,NULL,NULL,NULL,'history/version table: NO id, keyed by a non-unique indexed ref_id -> range-CHUNK (not keyset); the field version-table heavy-chunk / parallel-checkpoint statement-timeout shape','dev/garbage/postgres.sql'),
 ('order_keyed','garbage','postgres','ext.order_keyed','order_id','bigint',150000,NULL,NULL,NULL,'keyset',NULL,NULL,NULL,NULL,'NO column literally named id (name-trap): keyset on order_id via explicit chunk_by_key, not a name heuristic','dev/garbage/postgres.sql'),
 ('heap_no_key','garbage','postgres','ext.heap_no_key',NULL,'none',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'HEAP: no PK, no index at all -> mode:full is the only safe strategy','dev/garbage/postgres.sql'),
 ('unindexed_id','garbage','postgres','ext.unindexed_id','id','bigint (not unique)',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'name-trap: a column NAMED id that is NOT a unique/indexed key -> must NOT keyset it (verify by construction, not the column name)','dev/garbage/postgres.sql'),
 ('bigint_pk_dual_ts','garbage','mysql','ext_bigint_pk_dual_ts','id','bigint',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'fleet-majority shape: bigint PK + dual created_at/updated_at -> keyset(id) (PK beats the timestamp cursor)','dev/garbage/mysql.sql'),
 ('int_pk_dual_ts','garbage','mysql','ext_int_pk_dual_ts','id','integer',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'int-PK minority (fleet mixes bigint + int)','dev/garbage/mysql.sql'),
 ('sparse_key','garbage','mysql','ext_sparse_key','id','bigint',150000,'1','149999000001',1000000.0,'keyset',NULL,NULL,NULL,NULL,'sparse key: id span vastly exceeds row count -> range chunking explodes into near-empty windows (the sparse-guard warn shape); keyset is immune','dev/garbage/mysql.sql'),
 ('decimal_key','garbage','mysql','ext_decimal_key','dkey','decimal(15,0)',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'scale-0 DECIMAL PK (ERP-migration shape): NOT integer-family -> an explicit range chunk_column must LOUDLY bail (#103); chunk_by_key (keyset) IS accepted','dev/garbage/mysql.sql'),
 ('no_pk_no_ts','garbage','mysql','ext_no_pk_no_ts',NULL,'none',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'keyless + cursorless -> mode:full fallback (no chunk column, no cursor)','dev/garbage/mysql.sql'),
 ('ref_id_history','garbage','mysql','ext_ref_id_history','ref_id','bigint (non-unique, indexed)',150000,NULL,NULL,NULL,'range',NULL,NULL,NULL,NULL,'history/version table: NO id, keyed by a non-unique indexed ref_id -> range-CHUNK (not keyset); the field version-table heavy-chunk / parallel-checkpoint statement-timeout shape','dev/garbage/mysql.sql'),
 ('order_keyed','garbage','mysql','ext_order_keyed','order_id','bigint',150000,NULL,NULL,NULL,'keyset',NULL,NULL,NULL,NULL,'NO column literally named id (name-trap): keyset on order_id via explicit chunk_by_key, not a name heuristic','dev/garbage/mysql.sql'),
 ('heap_no_key','garbage','mysql','ext_heap_no_key',NULL,'none',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'HEAP: no PK, no index at all -> mode:full is the only safe strategy','dev/garbage/mysql.sql'),
 ('unindexed_id','garbage','mysql','ext_unindexed_id','id','bigint (not unique)',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'name-trap: a column NAMED id that is NOT a unique/indexed key -> must NOT keyset it (verify by construction, not the column name)','dev/garbage/mysql.sql'),
 ('bigint_unsigned_pk','garbage','mysql','ext_bigint_unsigned_pk','id','bigint unsigned',150003,'1','18446744073709551615',NULL,'keyset',NULL,NULL,NULL,NULL,'BIGINT UNSIGNED key (MySQL-specific field hazard): 150000 sequential + 3 ids at the u64 ceiling (...613/614/615, key_max = u64::MAX) -> keyset boundary literals + paging cursor must round-trip as u64 past i64::MAX','dev/garbage/mysql.sql'),
 ('bigint_pk_dual_ts','garbage','mssql','ext.bigint_pk_dual_ts','id','bigint',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'fleet-majority shape: bigint PK + dual created_at/updated_at -> keyset(id) (PK beats the timestamp cursor)','dev/garbage/mssql.sql'),
 ('int_pk_dual_ts','garbage','mssql','ext.int_pk_dual_ts','id','integer',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'int-PK minority (fleet mixes bigint + int)','dev/garbage/mssql.sql'),
 ('sparse_key','garbage','mssql','ext.sparse_key','id','bigint',150000,'1','149999000001',1000000.0,'keyset',NULL,NULL,NULL,NULL,'sparse key: id span vastly exceeds row count -> range chunking explodes into near-empty windows (the sparse-guard warn shape); keyset is immune','dev/garbage/mssql.sql'),
 ('decimal_key','garbage','mssql','ext.decimal_key','dkey','decimal(15,0)',150000,'1','150000',1.0,'keyset',NULL,NULL,NULL,NULL,'scale-0 DECIMAL PK (ERP-migration shape): NOT integer-family -> an explicit range chunk_column must LOUDLY bail (#103); chunk_by_key (keyset) IS accepted','dev/garbage/mssql.sql'),
 ('no_pk_no_ts','garbage','mssql','ext.no_pk_no_ts',NULL,'none',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'keyless + cursorless -> mode:full fallback (no chunk column, no cursor)','dev/garbage/mssql.sql'),
 ('ref_id_history','garbage','mssql','ext.ref_id_history','ref_id','bigint (non-unique, indexed)',150000,NULL,NULL,NULL,'range',NULL,NULL,NULL,NULL,'history/version table: NO id, keyed by a non-unique indexed ref_id -> range-CHUNK (not keyset); the field version-table heavy-chunk / parallel-checkpoint statement-timeout shape','dev/garbage/mssql.sql'),
 ('order_keyed','garbage','mssql','ext.order_keyed','order_id','bigint',150000,NULL,NULL,NULL,'keyset',NULL,NULL,NULL,NULL,'NO column literally named id (name-trap): keyset on order_id via explicit chunk_by_key, not a name heuristic','dev/garbage/mssql.sql'),
 ('heap_no_key','garbage','mssql','ext.heap_no_key',NULL,'none',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'HEAP: no PK, no index at all -> mode:full is the only safe strategy','dev/garbage/mssql.sql'),
 ('unindexed_id','garbage','mssql','ext.unindexed_id','id','bigint (not unique)',150000,NULL,NULL,NULL,'full',NULL,NULL,NULL,NULL,'name-trap: a column NAMED id that is NOT a unique/indexed key -> must NOT keyset it (verify by construction, not the column name)','dev/garbage/mssql.sql'),
 ('wide_cols','garbage','mssql','ext.wide_cols','id','bigint',5,NULL,NULL,NULL,'keyset',NULL,NULL,NULL,NULL,'wide-column table (many columns, few rows) -> keyset(id) on a wide row','dev/garbage/mssql.sql');
