-- 30-row fixture table (MySQL). Run INSERTs separately — MySQL lacks generate_series in all versions.
DROP TABLE IF EXISTS pa_audit;
CREATE TABLE pa_audit (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
