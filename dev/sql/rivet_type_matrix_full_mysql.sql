-- Full type-matrix for MySQL — replay into existing volumes.
--
--   docker compose exec -T mysql mysql -urivet -privet rivet \
--     < dev/sql/rivet_type_matrix_full_mysql.sql
--
DROP TABLE IF EXISTS rivet_type_matrix_full;

CREATE TABLE rivet_type_matrix_full (
    id            BIGINT PRIMARY KEY,
    flag          BOOLEAN,                         -- TINYINT(1) → Bool
    bit1_col      BIT(1),                          -- BIT(1)     → Bool
    bit8_col      BIT(8),                          -- BIT(8)     → Int64
    tiny_col      TINYINT,                         -- TINYINT    → Int16
    date_col      DATE,                            -- DATE       → Date32
    time_col      TIME(6),                         -- TIME(6)    → Time64(µs)
    year_col      YEAR,                            -- YEAR       → Int16
    enum_col      ENUM('a', 'b', 'c'),             -- ENUM       → Utf8
    varbinary_col VARBINARY(4),                    -- VARBINARY  → Binary
    blob_col      BLOB                             -- BLOB       → Binary
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO rivet_type_matrix_full
    (id, flag, bit1_col, bit8_col, tiny_col, date_col, time_col, year_col, enum_col, varbinary_col, blob_col)
VALUES
  (1, TRUE,  b'1', b'10101010',  127, '2024-03-15', '14:30:00.123456', 2024, 'b', 0xDEADBEEF, 0x0102030405),
  (2, FALSE, b'0', b'00000001', -128, '1970-01-01', '00:00:00.000000', 2000, 'a', 0x00000000, 0xCAFE),
  (3, NULL,  NULL, NULL,           0, '2000-02-29', '23:59:59.999999', NULL, NULL, NULL,       NULL);
