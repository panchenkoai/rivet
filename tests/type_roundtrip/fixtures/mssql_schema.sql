-- SQL Server type matrix (the natively-supported subset — no json/enum/
-- interval/array, which T-SQL has no first-class type for). Mirrors the PG/
-- MySQL matrices so the same DuckDB/ClickHouse/BigQuery oracles can validate it.
CREATE TABLE {table_name} (
    id            BIGINT          NOT NULL PRIMARY KEY,
    c_smallint    SMALLINT        NOT NULL,
    c_int         INT             NOT NULL,
    c_bigint      BIGINT          NOT NULL,
    c_tinyint     TINYINT         NOT NULL,
    c_bit         BIT             NOT NULL,
    amount        DECIMAL(18,2)   NULL,
    fee           DECIMAL(20,6)   NULL,
    price         DECIMAL(10,2)   NULL,
    c_real        REAL            NOT NULL,
    c_float       FLOAT           NOT NULL,
    c_date        DATE            NOT NULL,
    c_time        TIME(6)         NOT NULL,
    created_at    DATETIME2       NOT NULL,
    created_at_tz DATETIMEOFFSET  NULL,
    label         NVARCHAR(200)   NOT NULL,
    c_varchar     VARCHAR(50)     NOT NULL,
    c_char        CHAR(10)        NOT NULL,
    raw_bytes     VARBINARY(8)    NOT NULL,
    uid           UNIQUEIDENTIFIER NOT NULL,
    c_nvarchar    NVARCHAR(100)   NOT NULL,
    note_nullable NVARCHAR(500)   NULL,
    note_all_null NVARCHAR(500)   NULL
);
