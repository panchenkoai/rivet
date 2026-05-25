-- 30-row fixture table shared by CLI, cfg, path, query, and cross-version matrices.
DROP TABLE IF EXISTS pa_audit;
CREATE TABLE pa_audit (
  id INT PRIMARY KEY,
  name TEXT,
  updated_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO pa_audit (id, name)
  SELECT i, 'row_' || i FROM generate_series(1, 30) i;
