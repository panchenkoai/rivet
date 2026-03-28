-- Example migration: sparse primary keys (gaps in ID space) with Rivet chunked export.
--
-- Rivet discovers chunk boundaries with MIN/MAX on your export query's chunk_column,
-- then runs BETWEEN start AND end for each chunk. If physical ids are sparse
-- (large min..max span, few rows), most chunks scan empty ranges.
--
-- Fix: chunk on a dense surrogate (e.g. ROW_NUMBER() OVER (ORDER BY id)) so
-- MIN/MAX match row count, not physical id span. Point `chunk_column` at that alias.
-- Note: ORDER BY id can mean a full sort or a large index-ordered scan; for huge/hot
-- tables consider incremental mode, a materialized view, or a precomputed dense key.

BEGIN;

CREATE TABLE IF NOT EXISTS orders_sparse (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);

-- Three rows with ids millions apart (deleted history, shard offsets, snowflake ids, etc.)
INSERT INTO orders_sparse (id, payload) VALUES
    (1, 'a'),
    (2000000, 'b'),
    (4000000, 'c')
ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE VIEW orders_sparse_for_export AS
SELECT
    id,
    payload,
    ROW_NUMBER() OVER (ORDER BY id) AS chunk_rownum
FROM orders_sparse;

COMMIT;

-- Example rivet export (chunk on the surrogate, not on id):
--
-- exports:
--   - name: orders_sparse
--     query: "SELECT id, payload, chunk_rownum FROM orders_sparse_for_export"
--     mode: chunked
--     chunk_column: chunk_rownum
--     chunk_size: 100000
--     format: parquet
--     destination:
--       type: local
--       path: ./out
