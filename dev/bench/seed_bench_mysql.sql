-- Benchmark seed for MySQL — Phase 2 stabilization harness.
--
-- Mirrors dev/bench/seed_bench_pg.sql for MySQL equivalents.
--
-- Usage:
--   mysql -h 127.0.0.1 -u rivet -privet rivet < dev/bench/seed_bench_mysql.sql
--
-- Note: MySQL lacks generate_series; a stored procedure loop is used instead.

-- ── bench_narrow ─────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bench_narrow;
CREATE TABLE bench_narrow (
    id          BIGINT NOT NULL PRIMARY KEY,
    score       DOUBLE NOT NULL,
    category    SMALLINT NOT NULL,
    flag        TINYINT(1) NOT NULL,
    created_at  DATETIME(6) NOT NULL
);

DROP PROCEDURE IF EXISTS seed_bench_narrow;
DELIMITER $$
CREATE PROCEDURE seed_bench_narrow()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 500000 DO
        INSERT INTO bench_narrow VALUES (
            i,
            i * 1.23456,
            i MOD 100,
            (i MOD 3 = 0),
            DATE_SUB(NOW(6), INTERVAL (500000 - i) SECOND)
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_bench_narrow();
DROP PROCEDURE seed_bench_narrow;

-- ── bench_wide ───────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bench_wide;
CREATE TABLE bench_wide (
    id         BIGINT NOT NULL PRIMARY KEY,
    col_a      TEXT NOT NULL,
    col_b      TEXT NOT NULL,
    col_c      TEXT NOT NULL,
    col_d      TEXT NOT NULL,
    col_e      TEXT NOT NULL,
    col_f      TEXT NOT NULL,
    col_g      TEXT NOT NULL,
    col_h      TEXT NOT NULL,
    col_i      TEXT NOT NULL,
    col_j      TEXT NOT NULL,
    updated_at DATETIME(6) NOT NULL
);

DROP PROCEDURE IF EXISTS seed_bench_wide;
DELIMITER $$
CREATE PROCEDURE seed_bench_wide()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 100000 DO
        INSERT INTO bench_wide VALUES (
            i,
            REPEAT(CHAR(65 + (i MOD 26)), 200),
            REPEAT(CHAR(66 + (i MOD 26)), 200),
            REPEAT(CHAR(67 + (i MOD 26)), 200),
            REPEAT(CHAR(68 + (i MOD 26)), 200),
            REPEAT(CHAR(69 + (i MOD 26)), 200),
            REPEAT(CHAR(70 + (i MOD 26)), 200),
            REPEAT(CHAR(71 + (i MOD 26)), 200),
            REPEAT(CHAR(72 + (i MOD 26)), 200),
            REPEAT(CHAR(73 + (i MOD 26)), 200),
            REPEAT(CHAR(74 + (i MOD 26)), 200),
            DATE_SUB(NOW(6), INTERVAL (100000 - i) SECOND)
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_bench_wide();
DROP PROCEDURE seed_bench_wide;

-- ── bench_hc — high cardinality ───────────────────────────────────────────────

DROP TABLE IF EXISTS bench_hc;
CREATE TABLE bench_hc (
    id         BIGINT NOT NULL PRIMARY KEY,
    uuid_col   CHAR(36) NOT NULL,
    email      VARCHAR(255) NOT NULL,
    session_id CHAR(32) NOT NULL,
    updated_at DATETIME(6) NOT NULL
);

DROP PROCEDURE IF EXISTS seed_bench_hc;
DELIMITER $$
CREATE PROCEDURE seed_bench_hc()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 200000 DO
        INSERT INTO bench_hc VALUES (
            i,
            UUID(),
            CONCAT('user', i, '@bench.example.com'),
            MD5(CONCAT(i, 'session')),
            DATE_SUB(NOW(6), INTERVAL (200000 - i) SECOND)
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_bench_hc();
DROP PROCEDURE seed_bench_hc;

-- ── bench_decimal ─────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bench_decimal;
CREATE TABLE bench_decimal (
    id         BIGINT NOT NULL PRIMARY KEY,
    price      DECIMAL(18, 4) NOT NULL,
    qty        DECIMAL(10, 2) NOT NULL,
    discount   DECIMAL(5, 4) NOT NULL,
    total      DECIMAL(20, 4) NOT NULL,
    updated_at DATETIME(6) NOT NULL
);

DROP PROCEDURE IF EXISTS seed_bench_decimal;
DELIMITER $$
CREATE PROCEDURE seed_bench_decimal()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 200000 DO
        INSERT INTO bench_decimal VALUES (
            i,
            ROUND(0.01 + (i MOD 99999) * 0.01, 4),
            ROUND(1 + (i MOD 9999) * 0.1, 2),
            ROUND(0.0001 + (i MOD 9999) * 0.0001, 4),
            ROUND((0.01 + (i MOD 99999) * 0.01) * (1 + (i MOD 9999) * 0.1), 4),
            DATE_SUB(NOW(6), INTERVAL (200000 - i) SECOND)
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_bench_decimal();
DROP PROCEDURE seed_bench_decimal;

-- ── bench_sparse ─────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bench_sparse;
CREATE TABLE bench_sparse (
    id         BIGINT NOT NULL PRIMARY KEY,
    val_a      TEXT,
    val_b      TEXT,
    val_c      TEXT,
    val_d      DOUBLE,
    val_e      DOUBLE,
    updated_at DATETIME(6) NOT NULL
);

DROP PROCEDURE IF EXISTS seed_bench_sparse;
DELIMITER $$
CREATE PROCEDURE seed_bench_sparse()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 10000 DO
        INSERT INTO bench_sparse VALUES (
            i,
            IF(i MOD 10 = 0, 'x', NULL),
            IF(i MOD 5  = 0, 'y', NULL),
            IF(i MOD 3  = 0, 'z', NULL),
            IF(i MOD 4  = 0, i * 1.1, NULL),
            IF(i MOD 7  = 0, i * 2.2, NULL),
            DATE_SUB(NOW(6), INTERVAL (10000 - i) SECOND)
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_bench_sparse();
DROP PROCEDURE seed_bench_sparse;
