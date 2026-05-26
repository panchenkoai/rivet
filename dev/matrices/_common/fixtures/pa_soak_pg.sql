-- 10_000-row table for soak / perf regression guards. ROWS substituted by seed script.
DROP TABLE IF EXISTS pa_soak;
CREATE TABLE pa_soak (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  payload TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO pa_soak (id, name, payload)
  SELECT i, 'row_' || i, repeat('x', 200) FROM generate_series(1, __ROWS__) i;
ANALYZE pa_soak;
