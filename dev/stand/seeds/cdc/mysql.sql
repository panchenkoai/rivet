-- LIGHT CDC seed — MySQL (5.7 and 8.0). Same schema + formulas as the common
-- seed, 10 rows per table, plus the replication grants the binlog reader
-- (COM_BINLOG_DUMP) needs.

GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivet'@'%';
FLUSH PRIVILEGES;

CREATE TABLE users (
    id         INT PRIMARY KEY,
    email      VARCHAR(64)   NOT NULL,
    name       VARCHAR(64)   NOT NULL,
    country    VARCHAR(2)    NOT NULL,
    active     TINYINT(1)    NOT NULL,
    balance    DECIMAL(12,2) NOT NULL,
    created_at DATETIME      NOT NULL
);

CREATE TABLE orders (
    id         INT PRIMARY KEY,
    user_id    INT           NOT NULL,
    amount     DECIMAL(12,2) NOT NULL,
    status     VARCHAR(16)   NOT NULL,
    order_date DATE          NOT NULL,
    created_at DATETIME      NOT NULL
);

CREATE TABLE events (
    id          BIGINT PRIMARY KEY,
    user_id     INT          NOT NULL,
    event_type  VARCHAR(16)  NOT NULL,
    payload     VARCHAR(64)  NOT NULL,
    occurred_at DATETIME     NOT NULL
);

CREATE TABLE seed_d (d INT NOT NULL);
INSERT INTO seed_d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

INSERT INTO users
SELECT n, CONCAT('user', n, '@example.com'), CONCAT('User ', n),
       ELT((n % 10) + 1, 'US','DE','FR','GB','UA','PL','ES','IT','NL','SE'),
       (n % 7) <> 0,
       CAST((n * 37) % 1000000 AS DECIMAL(14,2)) / 100,
       DATE_ADD(DATE_ADD('2020-01-01 00:00:00', INTERVAL (n % 1000) DAY), INTERVAL (n % 86400) SECOND)
FROM (SELECT 1 + d AS n FROM seed_d) t
WHERE n <= 10;

INSERT INTO orders
SELECT n, (n % 10) + 1,
       CAST((n * 17) % 500000 AS DECIMAL(14,2)) / 100,
       ELT((n % 5) + 1, 'new','paid','shipped','done','cancelled'),
       DATE_ADD('2021-01-01', INTERVAL (n % 730) DAY),
       DATE_ADD(DATE_ADD('2021-01-01 00:00:00', INTERVAL (n % 730) DAY), INTERVAL (n % 86400) SECOND)
FROM (SELECT 1 + d AS n FROM seed_d) t
WHERE n <= 10;

INSERT INTO events
SELECT n, (n % 10) + 1,
       ELT((n % 8) + 1, 'view','click','signup','login','logout','purchase','refund','error'),
       CONCAT('{"seq":', n, ',"grp":', n % 100, '}'),
       DATE_ADD('2022-01-01 00:00:00', INTERVAL (n % 31536000) SECOND)
FROM (SELECT 1 + d AS n FROM seed_d) t
WHERE n <= 10;

DROP TABLE seed_d;
