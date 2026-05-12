-- Replay into docker-compose MySQL for existing volumes.
--
-- From repo root (stdin is interpreted on the host, then streamed into mysql):
--
--   docker compose exec -T mysql mysql -urivet -privet rivet \
--     < dev/sql/rivet_type_matrix_mysql.sql
--
DROP TABLE IF EXISTS rivet_type_matrix;

CREATE TABLE rivet_type_matrix (
    id BIGINT PRIMARY KEY,
    label VARCHAR(200) NOT NULL,
    amount DECIMAL(18, 2) NULL,
    fee DECIMAL(18, 6) NULL,
    created_at_dt DATETIME(6) NOT NULL,
    created_at_ts TIMESTAMP(6) NOT NULL,
    raw_bytes BINARY(4) NOT NULL,
    uid VARCHAR(36) NOT NULL,
    extras JSON NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO rivet_type_matrix (
    id, label, amount, fee, created_at_dt, created_at_ts, raw_bytes, uid, extras
) VALUES
  (1, 'payments-like', 0.10, 0.000001,
      '2035-08-07 09:08:07.987654',
      '2035-08-07 09:08:07.987654',
      UNHEX('00ff0123'),
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011',
      JSON_OBJECT('tier', 'gold', 'n', 1)),
  (2, 'payments-like', 0.20, 0.000002,
      '2019-02-03 03:07:06.554433',
      '2019-02-03 03:07:06.554433',
      UNHEX('deadbeef'),
      'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022',
      JSON_ARRAY('a', 'b')),
  (3, 'payments-like', 999999999999.99, 10.123456,
      '2020-01-15 00:00:00.000001',
      '2020-01-15 00:00:00.000001',
      UNHEX('cafe'),
      'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033',
      CAST('{"big":true}' AS JSON)),
  (4, 'payments-like', -100.05, -0.123456,
      '2021-06-30 12:59:59.999999',
      '2021-06-30 12:59:59.999999',
      UNHEX('00'),
      'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380044',
      CAST('{}' AS JSON));
