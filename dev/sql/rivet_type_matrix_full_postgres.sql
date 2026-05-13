-- Full type-matrix for Postgres — replay into existing volumes.
--
--   docker compose exec -T postgres psql -U rivet -d rivet -v ON_ERROR_STOP=1 \
--     < dev/sql/rivet_type_matrix_full_postgres.sql
--
BEGIN;
DROP TABLE IF EXISTS rivet_type_matrix_full CASCADE;
DROP TYPE IF EXISTS rivet_status CASCADE;

CREATE TYPE rivet_status AS ENUM ('active', 'inactive', 'pending');

CREATE TABLE rivet_type_matrix_full (
    id           BIGINT PRIMARY KEY,
    flag         BOOLEAN,
    int2_col     SMALLINT,
    int4_col     INTEGER,
    float4_col   REAL,
    date_col     DATE,
    time_col     TIME,
    interval_col INTERVAL,
    enum_col     rivet_status,
    tags         TEXT[],
    nums         INTEGER[]
);

INSERT INTO rivet_type_matrix_full VALUES
  (1, TRUE,  32767,       2147483647,  3.14::real, '2024-03-15', '14:30:00.123456',  INTERVAL '1 year 2 months 3 days', 'active',   ARRAY['alpha','beta'], ARRAY[1,2,3]),
  (2, FALSE, -32768,     -2147483648, -1.5::real,  '1970-01-01', '00:00:00',         INTERVAL '-1 year',                'inactive', ARRAY['gamma'],        ARRAY[42]),
  (3, NULL,  NULL,        0,           0.0::real,  '2000-02-29', '23:59:59.999999',  INTERVAL '0',                      NULL,       ARRAY[]::text[],       NULL);

COMMIT;
