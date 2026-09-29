"""Sentinel fidelity: boundary values must round-trip exactly, or be refused loudly.

A value the product cannot represent faithfully may make the run FAIL; it may never
make the run SUCCEED with a different value. The class this grades is the one counts
and sums cannot see: a zero date that becomes 1970-01-01, a NULL that becomes epoch, a
BC date that becomes AD, a 24:00 that wraps to 00:00, an unreadable cell written as NULL.

Every table is created at run time in the gate's own engine container (never in the
shared seed, which the verdicts golden grades) and exported through the full path and
the keyset path. The expected value is the SOURCE row, read by DuckDB — never rivet.

  OK tables     every row is representable: the run must succeed and match exactly.
  RISKY tables  one boundary value (+ a NULL row): the run must either match exactly
                or fail; success with a changed value is the defect.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import engines
from .core import Ledger

SCEN = "sentinels"


@dataclass(frozen=True)
class Table:
    name: str
    ddl: str
    rows: tuple[str, ...]
    risky: bool = False
    prelude: str = ""
    bc_text: bool = False


def _risky(engine: str, name: str, coltype: str, literal: str, prelude: str = "", bc_text: bool = False) -> Table:
    """One boundary value and a NULL, in a table of its own so a refusal hides nothing else."""
    return Table(name, f"CREATE TABLE {name} (id INT PRIMARY KEY, v {coltype})",
                 (f"(1, {literal})", "(2, NULL)"), risky=True, prelude=prelude, bc_text=bc_text)


PG = [
    Table(
        "rivet_sent_ok",
        "CREATE TABLE rivet_sent_ok (id INT PRIMARY KEY, ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, "
        "t TIME, n NUMERIC(38,10), f8 DOUBLE PRECISION, f4 REAL, b BOOLEAN, txt TEXT, u UUID, "
        "by BYTEA, i8 BIGINT, i2 SMALLINT)",
        (
            "(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            "(2, '1970-01-01 00:00:00', '1970-01-01 00:00:00+00', '1970-01-01', '00:00:00', 0, 0, 0, "
            "false, '', '00000000-0000-0000-0000-000000000000', '\\x', 0, 0)",
            "(3, '1969-12-31 23:59:59.999999', '1969-12-31 23:59:59.999999+00', '1969-12-31', "
            "'23:59:59.999999', -0.0000000001, -1e-300, -1.5, true, 'NULL', "
            "'ffffffff-ffff-ffff-ffff-ffffffffffff', '\\x00', -1, -32768)",
            "(4, '0001-01-01 00:00:00', '0001-01-02 00:00:00+00', '0001-01-01', '00:00:00.000001', "
            "9999999999999999999999999999.9999999999, 1e308, 3.4e38, true, '  ', "
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\xdeadbeef', 9223372036854775807, 32767)",
            "(5, '9999-12-31 23:59:59.999999', '9999-12-30 23:59:59.999999+00', '9999-12-31', "
            "'12:00:00', -9999999999999999999999999999.9999999999, -1e308, -3.4e38, false, "
            "'unicode ✓ ñ', '12345678-1234-1234-1234-123456789abc', '\\xff', "
            "-9223372036854775808, 1)",
            "(6, '2024-02-29 12:00:00', '2024-02-29 12:00:00+05:30', '2000-02-29', '23:59:59', "
            "0.5, 0.1, 0.1, true, 'x', '00000000-0000-0000-0000-000000000001', '\\x01', 1, -1)",
        ),
    ),
    _risky("postgres", "rivet_sent_ts_inf", "TIMESTAMP", "'infinity'"),
    _risky("postgres", "rivet_sent_tstz_ninf", "TIMESTAMPTZ", "'-infinity'"),
    _risky("postgres", "rivet_sent_date_inf", "DATE", "'infinity'"),
    _risky("postgres", "rivet_sent_time_24", "TIME", "'24:00:00'"),
    _risky("postgres", "rivet_sent_date_bc", "DATE", "'0044-03-15 BC'"),
    _risky("postgres", "rivet_sent_ts_bc", "TIMESTAMP", "'0044-03-15 12:00:00 BC'"),
    _risky("postgres", "rivet_sent_num_nan", "NUMERIC", "'NaN'"),
    _risky("postgres", "rivet_sent_f8_nan", "DOUBLE PRECISION", "'NaN'"),
    _risky("postgres", "rivet_sent_f8_inf", "DOUBLE PRECISION", "'Infinity'"),
]

MYSQL_UTF8 = "SET NAMES utf8mb4;"
MYSQL_ZERO = MYSQL_UTF8 + "SET SESSION sql_mode = '';"
MYSQL = [
    Table(
        "rivet_sent_ok",
        "CREATE TABLE rivet_sent_ok (id INT PRIMARY KEY, dt DATETIME(6), ts TIMESTAMP(6) NULL, "
        "d DATE, t TIME(6), y YEAR, n DECIMAL(38,10), f DOUBLE, fl FLOAT, b TINYINT(1), "
        "txt VARCHAR(50), bin VARBINARY(16), i BIGINT, e ENUM('a','b')) CHARACTER SET utf8mb4",
        (
            "(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            "(2, '1970-01-01 00:00:00', '1970-01-01 00:00:01', '1970-01-01', '00:00:00', 1970, 0, 0, 0, "
            "0, '', '', 0, 'a')",
            "(3, '1000-01-01 00:00:00', '1970-01-01 00:00:01.000001', '1000-01-01', '00:00:00.000001', 1901, "
            "-0.0000000001, -1e-300, -1.5, 1, 'NULL', X'00', -1, 'b')",
            "(4, '9999-12-31 23:59:59.999999', '2038-01-19 03:14:07.999999', '9999-12-31', "
            "'23:59:59.999999', 2155, 9999999999999999999999999999.9999999999, 1e308, 3.4e38, 1, '  ', "
            "X'414243', 9223372036854775807, 'a')",
            "(5, '2024-02-29 12:00:00', '2024-02-29 12:00:00', '2000-02-29', '23:59:59.999999', 2000, "
            "0.5, 0.1, 0.1, 0, 'unicode ✓ ñ', X'7F', -9223372036854775808, 'b')",
        ),
        prelude=MYSQL_UTF8,
    ),
    _risky("mysql", "rivet_sent_time_838", "TIME", "'838:59:59'", MYSQL_UTF8),
    _risky("mysql", "rivet_sent_time_neg", "TIME", "'-838:59:59'", MYSQL_UTF8),
    _risky("mysql", "rivet_sent_time_25h", "TIME", "'25:00:00'", MYSQL_UTF8),
    _risky("mysql", "rivet_sent_dt_zero", "DATETIME", "'0000-00-00 00:00:00'", MYSQL_ZERO),
    _risky("mysql", "rivet_sent_ts_zero", "TIMESTAMP NULL", "'0000-00-00 00:00:00'", MYSQL_ZERO),
    _risky("mysql", "rivet_sent_date_zero", "DATE", "'0000-00-00'", MYSQL_ZERO),
    _risky("mysql", "rivet_sent_year_zero", "YEAR", "0000", MYSQL_ZERO),
]

MSSQL = [
    Table(
        "rivet_sent_ok",
        "CREATE TABLE rivet_sent_ok (id INT PRIMARY KEY, dt DATETIME, dt2 DATETIME2(6), "
        "sdt SMALLDATETIME, d DATE, t TIME(6), dto DATETIMEOFFSET(6), n DECIMAL(38,10), "
        "f FLOAT, r REAL, bt BIT, nv NVARCHAR(50), vb VARBINARY(16), u UNIQUEIDENTIFIER, i BIGINT)",
        (
            "(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            "(2, '1970-01-01', '1970-01-01', '1970-01-01', '1970-01-01', '00:00:00', "
            "'1970-01-01 00:00:00 +00:00', 0, 0, 0, 0, N'', 0x, '00000000-0000-0000-0000-000000000000', 0)",
            "(3, '1753-01-01', '0001-01-01', '1900-01-01', '0001-01-01', '23:59:59.999999', "
            "'0001-01-02 00:00:00 +14:00', -0.0000000001, -1e-300, -1.5, 1, N'NULL', 0x00, "
            "'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF', -1)",
            "(4, '9999-12-31 23:59:59.990', '9999-12-31 23:59:59.999999', '2079-06-06 23:59', "
            "'9999-12-31', '12:00:00', '9999-12-30 00:00:00 -14:00', "
            "9999999999999999999999999999.9999999999, 1e308, 3.4e38, 0, N'  ', 0xDEADBEEF, "
            "'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 9223372036854775807)",
            "(5, '2024-02-29 12:00:00.003', '2024-02-29 12:00:00', '2024-02-29 12:00', '2000-02-29', "
            "'23:59:59', '2024-02-29 12:00:00 +05:30', 0.5, 0.1, 0.1, 1, N'unicode ✓ ñ', 0xFF, "
            "'12345678-1234-1234-1234-123456789ABC', -9223372036854775808)",
        ),
    ),
    _risky("mssql", "rivet_sent_dt2_100ns", "DATETIME2(7)", "'2024-01-01 00:00:00.0000001'"),
    _risky("mssql", "rivet_sent_xml", "XML", "'<a/>'"),
    _risky("mssql", "rivet_sent_variant", "SQL_VARIANT", "CAST(1 AS INT)"),
]

ORACLE = [
    Table(
        "rivet_sent_ok",
        "CREATE TABLE rivet_sent_ok (id NUMBER(10) PRIMARY KEY, d DATE, ts TIMESTAMP(6), "
        "tstz TIMESTAMP(6) WITH TIME ZONE, n NUMBER(38,10), bd BINARY_DOUBLE, txt VARCHAR2(50), "
        "rw RAW(16), i NUMBER(19))",
        (
            "(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            "(2, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', "
            "TIMESTAMP '1970-01-01 00:00:00 +00:00', 0, 0d, 'x', HEXTORAW('00'), 0)",
            "(3, DATE '0001-01-01', TIMESTAMP '0001-01-01 00:00:00.000001', "
            "TIMESTAMP '0001-01-02 00:00:00 +14:00', -0.0000000001, -1e-300d, 'NULL', HEXTORAW('FF'), -1)",
            "(4, TO_DATE('9999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'), "
            "TIMESTAMP '9999-12-31 23:59:59.999999', TIMESTAMP '9999-12-30 00:00:00 -12:00', "
            "9999999999999999999999999999.9999999999, 1e308d, '  ', HEXTORAW('DEADBEEF'), "
            "9223372036854775807)",
            "(5, DATE '2024-02-29', TIMESTAMP '2024-02-29 12:00:00', "
            "TIMESTAMP '2024-02-29 12:00:00 +05:30', 0.5, 0.1d, 'unicode ✓ ñ', HEXTORAW('01'), "
            "-9223372036854775808)",
        ),
    ),
    _risky("oracle", "rivet_sent_date_bc", "DATE", "DATE '-0044-03-15'", bc_text=True),
    _risky("oracle", "rivet_sent_ts9", "TIMESTAMP(9)", "TIMESTAMP '2024-01-01 00:00:00.000000001'"),
]

TABLES = {"postgres": PG, "mysql": MYSQL, "mssql": MSSQL, "oracle": ORACLE}


def _oracle_exec(url: str, statements: list[str]) -> str:
    """Run DDL/DML on Oracle through python-oracledb; '' on success, else the error."""
    from urllib.parse import unquote, urlparse

    import oracledb

    u = urlparse(url)
    try:
        with oracledb.connect(
            user=unquote(u.username or ""), password=unquote(u.password or ""),
            dsn=f"{u.hostname}:{u.port or 1521}/{u.path.lstrip('/')}",
        ) as con, con.cursor() as cur:
            for s in statements:
                try:
                    cur.execute(s)
                except oracledb.DatabaseError as e:
                    if not s.startswith("DROP"):
                        raise RuntimeError(f"{s[:80]}: {e}") from e
            con.commit()
    except Exception as e:  # noqa: BLE001 — a seed that cannot be written is the finding
        return str(e)
    return ""


def create(engine: str, url: str, t: Table) -> str:
    """Create and fill one sentinel table; '' on success, else the error text."""
    if engine == "oracle":
        return _oracle_exec(url, [f"DROP TABLE {t.name} PURGE", t.ddl,
                                  *(f"INSERT INTO {t.name} VALUES {r}" for r in t.rows)])
    drop = f"DROP TABLE IF EXISTS {t.name};"
    inserts = "".join(f"INSERT INTO {t.name} VALUES {r};" for r in t.rows)
    p = engines.sql(engine, url, f"{t.prelude}{drop}{t.ddl};{inserts}")
    return "" if p.ok else (p.stderr or p.stdout).strip()[:300]


def drop(engine: str, url: str, t: Table) -> None:
    """Remove one sentinel table (best effort)."""
    if engine == "oracle":
        _oracle_exec(url, [f"DROP TABLE {t.name} PURGE"])
    else:
        engines.sql(engine, url, f"DROP TABLE IF EXISTS {t.name};")


def bc_dates_differ(url: str, table: str, parquet: str) -> list[str]:
    """Oracle only: `v` compared as BC-aware date TEXT, because python-oracledb cannot hold a BC year."""
    from .duck import Oracle
    from .value_diff import oracle_rows

    src = {r["ID"]: r["V"] for r in oracle_rows(url, f"SELECT id, TO_CHAR(v, 'SYYYY-MM-DD') AS v FROM {table}")}
    with Oracle() as ora:
        dst = dict(ora.db.sql(f"SELECT id, CAST(CAST(v AS DATE) AS VARCHAR) FROM {parquet}").fetchall())

    def norm(v: object) -> object:
        if v is None:
            return None
        t = str(v).strip()
        return ("BC " + t[1:]) if t.startswith("-") else ("BC " + t.replace(" (BC)", "")) if "(BC)" in t else t

    return [f"`v` differs at ID={k}: source {src[k]!r} vs warehouse {dst.get(int(k))!r}"
            for k in src if norm(src[k]) != norm(dst.get(int(k)))]


PANIC_EXIT = 101


def verdict(t: Table, exported_ok: bool, diffs: list[str] | None, oracle_error: str,
            returncode: int = 1) -> tuple[bool, str]:
    """(pass, message) for one table × path: exact, or a loud failure only where the value is risky."""
    if not exported_ok and returncode == PANIC_EXIT:
        return False, "PANICKED instead of refusing"
    if not exported_ok:
        if t.risky:
            return True, "refused loudly"
        return False, "REFUSED a representable value"
    if oracle_error:
        return False, f"oracle could not read the source to compare: {oracle_error[:160]}"
    if diffs:
        return False, "SUCCEEDED WITH A CHANGED VALUE: " + "; ".join(diffs[:2])
    return True, "round-tripped exactly"


def sc_sentinels(led: Ledger, engine: str, tag: str, url: str) -> None:
    """Every sentinel table through the full and keyset paths, graded against the source."""
    from .scenarios import Scope, _declared_read, _export_local, _failed, _passed, _skipped
    from .value_diff import compare_rows_to_parquet

    tables = TABLES.get(engine)
    if tables is None:
        _skipped(led, engine, tag, SCEN, "-", f"sentinels: no sentinel set for {engine}", "no set")
        return
    out = Scope(engine, tag).dir("sentinels")
    for t in tables:
        err = create(engine, url, t)
        if err:
            _failed(led, engine, tag, SCEN, t.name, f"sentinels[{t.name}]: could not seed: {err}", "seed")
            continue
        for mode, label in (("full", "full"), ("chunked", "keyset")):
            dest = out / t.name / label
            p = _export_local(engine, url, t.name, dest, mode)
            diffs: list[str] | None = None
            oracle_error = ""
            if p.ok:
                src = _declared_read(dest, ".parquet")
                if not src:
                    oracle_error = "no declared parts"
                else:
                    try:
                        if t.bc_text:
                            diffs = bc_dates_differ(url, t.name, f"read_parquet({src})")
                        else:
                            _, diffs = compare_rows_to_parquet(engine, url, t.name, f"read_parquet({src})")
                    except Exception as e:  # noqa: BLE001 — an oracle that cannot read is not a pass
                        oracle_error = str(e)
            ok, msg = verdict(t, p.ok, diffs, oracle_error, p.returncode)
            if not p.ok:
                msg += f" (exit {p.returncode})"
            store = f"{t.name}/{label}"
            if ok:
                _passed(led, engine, tag, SCEN, store, f"sentinels[{store}]: {msg}")
            else:
                _failed(led, engine, tag, SCEN, store, f"sentinels[{store}]: {msg}", msg[:200])
        drop(engine, url, t)


def _self_test() -> None:
    """The verdict table, without an engine."""
    ok = Table("t", "", ())
    risky = Table("r", "", (), risky=True)
    assert verdict(ok, True, [], "") == (True, "round-tripped exactly")
    assert verdict(ok, False, None, "")[0] is False, "refusing a representable value fails"
    assert verdict(risky, False, None, "")[0] is True, "refusing a risky value passes"
    assert verdict(risky, True, ["`v` differs"], "")[0] is False, "a changed risky value fails"
    assert verdict(ok, True, None, "boom")[0] is False, "an unreadable oracle is not a pass"
    assert verdict(risky, True, [], "")[0] is True, "a risky value that round-trips passes"
    assert verdict(risky, False, None, "", PANIC_EXIT)[0] is False, "a panic is a crash, not a refusal"


if __name__ == "__main__":
    _self_test()
    print("sentinels self-test ok")

