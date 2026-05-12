-- Replay into docker-compose Postgres when the DB already exists (volume created
-- before rivet_type_matrix landed in dev/postgres/init.sql). From repo root:
--
--   docker compose exec -T postgres psql -U rivet -d rivet -v ON_ERROR_STOP=1 \
--     < dev/sql/rivet_type_matrix_postgres.sql
--
BEGIN;
DROP TABLE IF EXISTS rivet_type_matrix CASCADE;
CREATE TABLE rivet_type_matrix (
    id BIGINT PRIMARY KEY,
    label TEXT NOT NULL,
    amount NUMERIC(18, 2),
    fee NUMERIC(18, 6),
    created_at TIMESTAMP NOT NULL,
    created_at_tz TIMESTAMPTZ NOT NULL,
    raw_bytes BYTEA NOT NULL,
    uid UUID NOT NULL,
    attrs JSONB
);

INSERT INTO rivet_type_matrix (
    id, label, amount, fee, created_at, created_at_tz, raw_bytes, uid, attrs
) VALUES
  (1, 'payments-like', 0.10, 0.000001,
      TIMESTAMP '2035-08-07 09:08:07.987654',
      TIMESTAMPTZ '2035-08-07 09:08:07.987654Z',
      '\x00ff012345'::bytea,
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011'::uuid,
      '{"tier":"gold","n":1}'::jsonb),
  (2, 'payments-like', 0.20, 0.000002,
      TIMESTAMP '2019-02-03 03:07:06.554433',
      TIMESTAMPTZ '2019-02-03 08:07:06.554433+05',
      '\xdeadbeef'::bytea,
      'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022'::uuid,
      '["a","b"]'::jsonb),
  (3, 'payments-like', 999999999999.99, 10.123456,
      TIMESTAMP '2020-01-15 00:00:00.000001',
      TIMESTAMPTZ '2020-01-15 00:00:00.000001+00',
      '\xcafe'::bytea,
      'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033'::uuid,
      '{"big":true}'::jsonb),
  (4, 'payments-like', -100.05, -0.123456,
      TIMESTAMP '2021-06-30 12:59:59.999999',
      TIMESTAMPTZ '2021-06-30 12:59:59.999999+00',
      '\x00'::bytea,
      'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380044'::uuid,
      '{}'::jsonb);
COMMIT;
