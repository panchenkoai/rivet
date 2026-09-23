"""Source vs warehouse, value by value, through one DuckDB session.

The expected value is the SOURCE row, never a golden rivet wrote: a golden blessed
from rivet's output grades change, not correctness (it froze 16 raw UUID bytes as
"expected" for months). DuckDB reads both sides — the source through its own
scanner, BigQuery through the community extension — and each cell is reduced to a
canonical form that two correct renderings of one value share.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import re
import uuid

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def canon(v: object) -> object:
    """One canonical form per value, so a correct round trip compares equal across readers."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, uuid.UUID):
        return v.hex
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if isinstance(v, dt.datetime):
        aware = v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)
        return ("ts", (aware - _EPOCH) // dt.timedelta(microseconds=1))
    if isinstance(v, dt.date):
        return ("date", v.isoformat())
    if isinstance(v, dt.time):
        return ("dur", 0, 0, v.hour * 3_600_000_000 + v.minute * 60_000_000 + v.second * 1_000_000 + v.microsecond)
    if isinstance(v, dt.timedelta):
        return ("dur", 0, 0, v // dt.timedelta(microseconds=1))
    if isinstance(v, decimal.Decimal):
        return ("num", format(v.normalize(), "f"))
    if isinstance(v, int):
        return ("num", str(v))
    if isinstance(v, float):
        return ("num", repr(v))
    if isinstance(v, (list, tuple)):
        # rivet lands an array as ARRAY<STRUCT<item T>> (docs/type-mapping.md): same values, one wrapper deeper.
        return [canon(x["item"]) if isinstance(x, dict) and set(x) == {"item"} else canon(x) for x in v]
    if isinstance(v, dict):
        return {str(k): canon(x) for k, x in sorted(v.items())}
    if isinstance(v, str):
        clock = re.fullmatch(r"(-?)(\d{1,3}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?", v)
        if clock:
            sign, h, m, sec, frac = clock.groups()
            micros = ((int(h) * 60 + int(m)) * 60 + int(sec)) * 1_000_000 + int((frac or "0").ljust(6, "0"))
            return ("dur", 0, 0, -micros if sign else micros)
        interval = _interval(v)
        if interval is not None:
            return ("dur", *interval)
        s = v.strip()
        if s[:1] in "{[":
            try:
                return canon(json.loads(s))
            except ValueError:
                pass
        return v
    return str(v)


def _interval(s: str) -> tuple[int, int, int] | None:
    """(months, days, microseconds) of an ISO-8601 duration (`P1Y2M3D`) or DuckDB interval text (`1 year 2 months 3 days`)."""
    iso = re.fullmatch(
        r"P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?[\d.]+)S)?)?", s
    )
    if iso and s != "P":
        y, mo, d, h, mi, sec = (g or "0" for g in iso.groups())
        micros = (int(h) * 3600 + int(mi) * 60) * 1_000_000 + round(float(sec) * 1_000_000)
        return (int(y) * 12 + int(mo), int(d), micros)
    parts = re.fullmatch(
        r"(?:(-?\d+) years? ?)?(?:(-?\d+) mons?(?:ths?)? ?)?(?:(-?\d+) days? ?)?(?:(-?\d+):(\d+):([\d.]+))?", s.strip()
    )
    if parts and any(parts.groups()):
        y, mo, d, h, mi, sec = (g or "0" for g in parts.groups())
        micros = (int(h) * 3600 + int(mi) * 60) * 1_000_000 + round(float(sec) * 1_000_000)
        return (int(y) * 12 + int(mo), int(d), micros)
    return None


def diff_rows(
    source: list[dict],
    dest: list[dict],
    key: str = "id",
    padded: frozenset = frozenset(),
    bits: frozenset = frozenset(),
) -> list[str]:
    """Every column whose values differ between source and destination, keyed by `key`, with one example each; `padded` names CHAR(n) columns (trailing blanks insignificant), `bits` names BIT(n) columns (bytes read as an unsigned integer)."""
    src = {canon(r[key]): r for r in source}
    dst = {canon(r[key]): r for r in dest}
    out: list[str] = []
    missing = sorted(map(str, set(src) - set(dst)))
    extra = sorted(map(str, set(dst) - set(src)))
    if missing:
        out.append(f"rows missing from the warehouse: {missing[:5]}")
    if extra:
        out.append(f"rows only in the warehouse: {extra[:5]}")
    columns = sorted(set().union(*(r.keys() for r in source)) if source else set())
    for col in columns:
        for k in sorted(set(src) & set(dst), key=str):
            if col not in dst[k]:
                out.append(f"column `{col}` is not in the warehouse table")
                break
            a, b = src[k].get(col), dst[k].get(col)
            if col in padded and isinstance(a, str) and isinstance(b, str):
                a, b = a.rstrip(" "), b.rstrip(" ")
            if col in bits and isinstance(a, (bytes, bytearray)):
                a = int.from_bytes(a, "big")
            a, b = canon(a), canon(b)
            if a != b:
                out.append(f"`{col}` differs at {key}={k}: source {src[k].get(col)!r} vs warehouse {dst[k].get(col)!r}")
                break
    return out


def fetch(ora, sql: str) -> list[dict]:
    """Rows of `sql` over the oracle's DuckDB session, as dicts."""
    rel = ora.db.sql(sql)
    names = rel.columns
    return [dict(zip(names, row)) for row in rel.fetchall()]


def invalid_text_columns(ora, relation: str) -> list[str]:
    """The text columns of `relation` holding bytes that are not UTF-8 — a STRING the warehouse cannot render."""
    bad = []
    for name, typ, *_ in ora.db.sql(f"DESCRIBE {relation}").fetchall():
        if str(typ) != "VARCHAR":
            continue
        try:
            ora.db.sql(f"SELECT {name} FROM {relation}").fetchall()
        except Exception as e:  # noqa: BLE001 — the decode failure IS the finding
            if "unicode" in str(e).lower():
                bad.append(name)
    return bad


def source_select(ora, relation: str) -> str:
    """`SELECT *` over `relation`, with INTERVAL columns read as text (DuckDB's Python conversion folds months into days)."""
    described = ora.db.sql(f"DESCRIBE {relation}").fetchall()
    intervals = [row[0] for row in described if str(row[1]).startswith("INTERVAL")]
    replace = ", ".join(f"CAST({c} AS VARCHAR) AS {c}" for c in intervals)
    replace_clause = f"REPLACE ({replace}) " if replace else ""
    return f"SELECT * {replace_clause}FROM {relation} ORDER BY id"


def pg_padded_columns(ora, table: str) -> frozenset:
    """The CHAR(n) columns of a PostgreSQL table attached as `pg`, from its own catalog."""
    inner = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' AND data_type = 'character'"
    ).replace("'", "''")
    rows = ora.db.sql(f"SELECT * FROM postgres_query('pg', '{inner}')").fetchall()
    return frozenset(r[0] for r in rows)


def mysql_bit_columns(ora, database: str, table: str) -> frozenset:
    """The BIT(n) columns of a MySQL table attached as `my`, from its own catalog."""
    inner = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{database}' AND table_name = '{table}' AND data_type = 'bit'"
    ).replace("'", "''")
    rows = ora.db.sql(f"SELECT * FROM mysql_query('my', '{inner}')").fetchall()
    return frozenset(r[0] for r in rows)


def source_attach(engine: str, url: str) -> tuple[dict, str]:
    """The `Oracle` keyword that attaches `url`, and the DuckDB schema prefix its tables live under."""
    from urllib.parse import urlparse

    u = urlparse(url)
    db = u.path.lstrip("/")
    if engine == "postgres":
        return {"postgres": url}, "pg.public"
    if engine == "mysql":
        dsn = f"host={u.hostname} port={u.port} user={u.username} password={u.password} database={db}"
        return {"mysql": dsn}, f"my.{db}"
    if engine == "mssql":
        return {"mssql": "mssql://" + url.split("://", 1)[1]}, "ms.dbo"
    raise ValueError(f"no DuckDB source attach for {engine}")


def compare_to_bigquery(engine: str, url: str, dataset: str, table: str) -> tuple[int, list[str]]:
    """(warehouse rows, differences) between the source table and its BigQuery load, read by DuckDB on both sides."""
    from .duck import Oracle

    attach, prefix = source_attach(engine, url)
    warehouse = f"bq.{dataset}.{table}"
    with Oracle(bigquery=True, **attach) as ora:
        bad = invalid_text_columns(ora, warehouse)
        if bad:
            return 0, [f"STRING column(s) {bad} hold bytes that are not UTF-8 — unreadable as text"]
        src = fetch(ora, source_select(ora, f"{prefix}.{table}"))
        dst = fetch(ora, f"SELECT * FROM {warehouse} ORDER BY id")
        padded = pg_padded_columns(ora, table) if engine == "postgres" else frozenset()
        bits = mysql_bit_columns(ora, prefix.split(".", 1)[1], table) if engine == "mysql" else frozenset()
        return len(dst), diff_rows(src, dst, padded=padded, bits=bits)


def _self_test() -> None:
    u = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011")
    assert canon(u) == canon(u.bytes), "a UUID and its 16 bytes are one value"
    t = dt.datetime(2035, 8, 7, 9, 8, 7, 987654)
    assert canon(t) == canon(t.replace(tzinfo=dt.timezone.utc))
    assert canon(t) != canon(t.replace(microsecond=0)), "microseconds must count"
    assert canon(decimal.Decimal("1.50")) == canon(decimal.Decimal("1.5"))
    assert canon(decimal.Decimal("100")) == canon(100)
    assert canon('{"b":1,"a":2}') == canon({"a": 2, "b": 1})
    assert canon(dt.time(4, 0)) != canon(dt.timedelta(hours=100)), "a wrapped TIME is a difference"
    assert diff_rows([{"id": 1, "u": u}], [{"id": 1, "u": b"\x00" * 16}]), "garbage bytes are a finding"
    assert not diff_rows([{"id": 1, "u": u}], [{"id": 1, "u": u.bytes}])
    assert canon("P1Y2M3D") == canon("1 year 2 months 3 days")
    assert canon("P1Y2M3D") != canon("P1Y2M4D")
    assert canon([1, 2]) == canon([{"item": 1}, {"item": 2}])
    assert not diff_rows([{"id": 1, "c": "a"}], [{"id": 1, "c": "a  "}], padded=frozenset({"c"}))
    assert diff_rows([{"id": 1, "c": "a"}], [{"id": 1, "c": "a  "}]), "varchar blanks count"
    assert canon("09:08:07.987654") == canon(dt.time(9, 8, 7, 987654))
    assert canon("100:00:00") != canon(dt.time(4, 0)), "a MySQL TIME past 24h must not match its wrap"
    assert canon("-01:30:00") == canon(-dt.timedelta(hours=1, minutes=30))
    assert not diff_rows([{"id": 1, "b": b"\xff"}], [{"id": 1, "b": 255}], bits=frozenset({"b"}))
    assert canon("00:00:00") == canon("PT0S"), "a zero interval read as clock text or as ISO is one value"
    assert canon("P1D") != canon("24:00:00"), "one day and 24 hours differ in PostgreSQL interval semantics"
    print("value_diff self-test ok")


if __name__ == "__main__":
    _self_test()
