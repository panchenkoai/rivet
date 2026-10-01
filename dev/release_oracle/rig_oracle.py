"""The live suite's default oracle: one DuckDB session grades one run's declared output.

Every `rivet run|load|compact --config` a live test starts through the `Rig` or a shared
`run_rivet*` helper reaches it; a hand-built spawn of the binary is not graded (counted by
tests/offline/rig_oracle_ratchet.rs).

tests/common/verify.rs only gathers facts from the config file — engine, source URL, table
or query, the export's own filter, the manifests the run wrote, the state DB — and hands
them here as JSON on stdin. This module owns every check: it ATTACHes the source and
rivet's state DB READ_ONLY through `duck.Oracle`, reads only the parts the Success
manifests declare, and grades per column

  * TYPE: the delivered type is not narrower than the source's (the type ledger's TEXT
    deliveries are required where a ledger row exists),
  * VALUES: every row, as a multiset, through `value_diff.canon` (full precision),
  * COUNT(*), COUNT(col) and COUNT(DISTINCT col), source vs delivered,
  * rivet's counters for THIS run: manifest `row_count`, `export_metrics.total_rows`
    (success rows), `file_log.row_count` of the declared parts vs the rows they hold.

Per mode (values and counts): a snapshot run (full, chunked, keyset) is its own new
Success manifests against the whole source; a delta run (incremental, keyset-incremental,
Mongo resume) is every Success manifest in the destination, latest version per key,
against the source inside the covered window (`cursor_low`, `cursor_high`]; a CDC run is
the latest after-image per key (by `__pos`, `__seq`; snapshot leg first, deletes removed)
against the source's current rows — only the keys the stream touched unless a snapshot
leg exists. Mongo grades `_id` only (the scanner's inferred schema shares nothing else
with the document blob). Oracle is read through python-oracledb (DuckDB has no scanner),
so its TYPE check sees text. The CDC checkpoint is not graded.
"""

from __future__ import annotations

import datetime as dt
import itertools
import json
import os
import re
import sys
from collections import Counter

META = ("__op", "__pos", "__seq")

INT_BITS = {
    "TINYINT": (8, True), "SMALLINT": (16, True), "INTEGER": (32, True), "BIGINT": (64, True),
    "HUGEINT": (128, True), "UTINYINT": (8, False), "USMALLINT": (16, False),
    "UINTEGER": (32, False), "UBIGINT": (64, False), "UHUGEINT": (128, False),
}
INT_DIGITS = {8: 3, 16: 5, 32: 10, 64: 20, 128: 39}
#: (native source type, delivered DuckDB type) pairs the scanner's wider reading would misgrade: MySQL YEAR is 1901..2155.
NATIVE_FITS = {("YEAR", "SMALLINT"), ("BINARY_FLOAT", "FLOAT")}
#: Scanner settings that make the source read faithful (the same values tests/live/live_cdc_type_parity.rs sets in its own session):
#: PostgreSQL's binary COPY drops char(n) padding; MySQL's scanner reads TINYINT(1) as BOOLEAN and its session zone moves TIMESTAMPs.
SCANNER_SETTINGS = {
    "postgres": {"pg_use_text_protocol": True},
    "mysql": {"mysql_tinyint1_as_boolean": False, "mysql_session_time_zone": "+00:00"},
}
#: PostgreSQL's own output-function text of `{c}` (`::text` would add an inet's netmask).
PG_TEXT = "CASE WHEN {c} IS NOT NULL THEN format('%s', {c}) END"
#: The ledger's `render.canon` names this module grades; any other fails loudly.
CANONS = ("number", "timestamp", "float32", "float64", "interval", "round_micros")
#: Oracle catalog types whose python-oracledb value is a number (read as exact text, compared by value).
ORACLE_NUMERIC = ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE")
TS_RANK = {
    "DATE": 0, "TIMESTAMP_S": 1, "TIMESTAMP_MS": 2, "TIMESTAMP": 3,
    "TIMESTAMP WITH TIME ZONE": 3, "TIMESTAMP_NS": 4,
}


def _decimal(t: str) -> tuple[int, int] | None:
    """(precision, scale) of a DuckDB `DECIMAL(p,s)`, else `None`."""
    if not (t.startswith("DECIMAL(") and t.endswith(")")):
        return None
    p, s = t[8:-1].split(",")
    return int(p), int(s)


def type_loss(src: str, dst: str) -> str | None:
    """Why delivering a DuckDB-typed source column as `dst` narrows it, or `None` when the mapping is faithful (text keeps every digit)."""
    if src == dst or dst == "VARCHAR":
        return None
    if src in INT_BITS:
        bits, signed = INT_BITS[src]
        if dst in INT_BITS:
            b2, s2 = INT_BITS[dst]
            fits = b2 >= bits if signed == s2 else (s2 and b2 > bits)
            return None if fits else "a narrower integer"
        d = _decimal(dst)
        if d:
            return None if d[0] - d[1] >= INT_DIGITS[bits] else "too few integer digits"
        if (dst == "DOUBLE" and bits <= 32) or (dst == "FLOAT" and bits <= 16):
            return None
        return "an integer delivered in a type that cannot hold every value"
    d = _decimal(src)
    if d:
        p, s = d
        d2 = _decimal(dst)
        if d2:
            return None if d2[1] >= s and d2[0] - d2[1] >= p - s else "fewer digits"
        if dst in INT_BITS:
            return None if s == 0 and p < INT_DIGITS[INT_BITS[dst][0]] else "fractional or too-wide digits"
        if (dst == "DOUBLE" and p <= 15) or (dst == "FLOAT" and p <= 6):
            return None
        return "a decimal delivered in a type that cannot hold every value"
    if src == "FLOAT":
        return None if dst == "DOUBLE" else "a float delivered as a non-float"
    if src == "DOUBLE":
        return "a double delivered as a narrower type"
    if src in TS_RANK:
        if dst not in TS_RANK:
            return "a temporal delivered as a non-temporal"
        return None if TS_RANK[dst] >= TS_RANK[src] else "coarser temporal precision"
    if src.startswith("TIME"):
        return None if dst.startswith("TIME") else "a time of day delivered as a non-time"
    return None


def norm_native(t: str | bytes) -> str:
    """A declared source type in the ledger's spelling: upper case, single spaces, PostgreSQL's long names shortened (MySQL's catalog answers bytes)."""
    t = t.decode() if isinstance(t, bytes) else t
    t = " ".join(t.upper().split()).replace(", ", ",")
    t = re.sub(r"^TIMESTAMP(\(\d+\))? WITHOUT TIME ZONE", r"TIMESTAMP\1", t)
    t = re.sub(r"^TIMESTAMP(\(\d+\))? WITH TIME ZONE", r"TIMESTAMPTZ\1", t)
    t = re.sub(r"^TIME(\(\d+\))? WITHOUT TIME ZONE", r"TIME\1", t)
    t = re.sub(r"^CHARACTER VARYING", "VARCHAR", t)
    return re.sub(r"^CHARACTER\b", "CHAR", t)


def arrow_to_duck(delivery: str, text_forms: set[str]) -> str | None:
    """The DuckDB type a parquet column of the ledger's `delivery` reads as; `None` when the ledger names no Arrow type DuckDB maps one way."""
    if delivery == "arrow.json":
        return "JSON"
    if delivery in text_forms:
        return "VARCHAR"
    fixed = {
        "Int8": "TINYINT", "Int16": "SMALLINT", "Int32": "INTEGER", "Int64": "BIGINT",
        "UInt8": "UTINYINT", "UInt16": "USMALLINT", "UInt32": "UINTEGER", "UInt64": "UBIGINT",
        "Float32": "FLOAT", "Float64": "DOUBLE", "Boolean": "BOOLEAN", "Utf8": "VARCHAR",
        "LargeUtf8": "VARCHAR", "Binary": "BLOB", "LargeBinary": "BLOB", "Date32": "DATE",
        "Time64(µs)": "TIME", "Time64(ns)": "TIME_NS", "Timestamp(µs)": "TIMESTAMP", "Timestamp(ns)": "TIMESTAMP_NS",
        "Timestamp(ms)": "TIMESTAMP_MS", "Timestamp(s)": "TIMESTAMP_S", "arrow.uuid": "UUID",
    }
    if delivery in fixed:
        return fixed[delivery]
    if re.fullmatch(r'Timestamp\((s|ms|µs|ns), ".+"\)', delivery):
        return "TIMESTAMP WITH TIME ZONE"
    m = re.fullmatch(r"Decimal128\((\d+), (\d+)\)", delivery)
    if m:
        return f"DECIMAL({m.group(1)},{m.group(2)})"
    m = re.fullmatch(r"List\((.+)\)", delivery)
    inner = arrow_to_duck(m.group(1), text_forms) if m else None
    return f"{inner}[]" if inner else None


def ledger(engine: str, mode: str) -> tuple[dict, set[str]]:
    """The type ledger's rows for `engine` x `mode` keyed by normalized native type, and its TEXT form names."""
    import yaml

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    with open(os.path.join(root, "docs", "type-capability-matrix.yaml")) as f:
        doc = yaml.safe_load(f)
    forms = set(doc.get("forms") or {})
    # One `rows:` list serves both modes; its `clickhouse:` map is read under the flat keys below, and `renders:` (by delivery) under each row's own render.
    eng = (doc.get("engines") or {}).get(engine) or {}
    rows, by_delivery = eng.get("rows") or [], eng.get("renders") or {}
    ch_keys = {"type": "clickhouse", "today": "today_clickhouse", "defect": "clickhouse_defect", "defect_samples": "clickhouse_defect_samples"}
    rows = [{**r, "render": {**by_delivery.get(r.get("delivery"), {}), **(r.get("render") or {})},
             **{ch_keys[k]: v for k, v in (r.get("clickhouse") or {}).items()}} if isinstance(r, dict) else r for r in rows]
    out = {norm_native(r["native_type"]): r for r in rows if isinstance(r, dict) and r.get("native_type")}
    for n, r in out.items():
        for render in (r.get("render") or {}, r.get("today_render") or {}):
            if render.get("canon") not in (None, *CANONS):
                raise ValueError(f"type ledger {engine}/{mode} {n}: unknown render.canon {render['canon']!r}")
    return out, forms


def pos_order(engine: str) -> str:
    """A text expression over `__pos` whose lexical order is the engine's change order."""
    return {
        "postgres": "lpad(split_part(json_extract_string(__pos, '$.lsn'), '/', 1), 8, '0') || "
        "lpad(split_part(json_extract_string(__pos, '$.lsn'), '/', 2), 8, '0')",
        "mysql": "lpad(regexp_extract(json_extract_string(__pos, '$.file'), '([0-9]+)$', 1), 12, '0') || "
        "lpad(json_extract_string(__pos, '$.pos'), 20, '0')",
        "mssql": "json_extract_string(__pos, '$.lsn')",
        "oracle": "lpad(json_extract_string(__pos, '$.commit_scn'), 30, '0')",
    }.get(engine, "__pos")


def _qi(c: str) -> str:
    """A quoted identifier."""
    return '"' + c.replace('"', '""') + '"'


def _lit(s: str) -> str:
    """A single-quoted SQL literal."""
    return "'" + s.replace("'", "''") + "'"


def _plist(files: list[str]) -> str:
    """A DuckDB list literal of file paths."""
    return "[" + ", ".join(_lit(f) for f in files) + "]"


def _load(root: str, name: str) -> dict:
    """One manifest, parsed."""
    with open(os.path.join(root, name)) as f:
        return json.load(f)


def declared_parts(root: str, manifests: list[str]) -> list[str]:
    """Paths of the committed parts the named Success manifests under `root` declare."""
    from .scenarios import success_part_names

    out = set()
    for name in manifests:
        for part in success_part_names(_load(root, name)):
            p = part if os.path.isabs(part) else os.path.join(root, part)
            if os.path.isfile(p):
                out.add(p)
    return sorted(out)


def in_run_order(root: str, manifests: list[str]) -> list[str]:
    """The named manifests ordered by when their run finished."""
    return sorted(manifests, key=lambda n: (str(_load(root, n).get("finished_at") or ""), n))


def watermark(root: str, manifests: list[str], coalesced: str | None = None) -> str | None:
    """The window a delta export has covered in THIS destination: up to the latest manifest's `cursor_high`, from the first manifest's `cursor_low` (exclusive, unless that row was delivered); a first run (no `cursor_low`) also read the NULL-cursor rows."""
    ordered = in_run_order(root, manifests)
    windows = [(_load(root, n).get("source") or {}).get("extraction") or {} for n in ordered]
    windows = [w for w in windows if w.get("cursor_column") and w.get("cursor_high") is not None]
    if not windows:
        return None
    col = windows[-1]["cursor_column"]
    c = coalesced if col == "_rivet_coalesced_cursor" and coalesced else _qi(col)
    high = f"{c} <= {_lit(str(windows[-1]['cursor_high']))}"
    low = windows[0].get("cursor_low")
    if low is None:
        return f"({c} IS NULL OR {high})"
    return f"{c} IS NOT NULL AND {high} AND ({c} > {_lit(str(low))} OR CAST({c} AS VARCHAR) IN (SELECT CAST({c} AS VARCHAR) FROM got))"


def manifest_facts(root: str, manifests: list[str]) -> tuple[list[str], int]:
    """(run ids, summed `row_count`) of the named Success manifests under `root`."""
    ids, rows = [], 0
    for name in manifests:
        doc = _load(root, name)
        if doc.get("run_id"):
            ids.append(doc["run_id"])
        rows += int(doc.get("row_count") or 0)
    return ids, rows


def _render(row: dict) -> dict:
    """The render that grades what rivet delivers today: `today_render` beside a known_defect, else `render`."""
    return (row.get("today_render") if row.get("known_defect") else None) or row.get("render") or {}


def _pg_projection(table: str, native: dict, renders: dict) -> str:
    """A server-side SELECT rendering each column the ledger renders server-side, and wide or unbounded numerics, with PostgreSQL's own text (the scanner reads those as DOUBLE)."""
    cols = []
    for name, nat in native.items():
        q = _qi(name)
        wide = nat.startswith("NUMERIC(") and int(nat[8:].split(",")[0].rstrip(")")) > 38
        expr = renders.get(nat) or (PG_TEXT if nat in ("NUMERIC", "NUMERIC[]") or wide else "{c}")
        cols.append(f"{expr.replace('{c}', q)} AS {q}")
    return f"postgres_query('pg', {_lit('SELECT ' + ', '.join(cols) + ' FROM ' + table)})"


def oracle_ds_iso(td: "dt.timedelta") -> str:
    """An INTERVAL DAY TO SECOND (python-oracledb's timedelta) as ISO text with Oracle's own day field, every part carrying the sign."""
    import decimal

    total = td // dt.timedelta(microseconds=1)
    days, rest = divmod(abs(total), 86_400_000_000)
    sign = "-" if total < 0 else ""
    secs = format((decimal.Decimal(rest).scaleb(-6)).normalize(), "f") if rest else "0"
    return f"P{sign}{days}DT{sign}{secs}S"


def _oracle_register(ora, url: str, sql: str, json_cols: frozenset = frozenset()) -> dict:
    """Rows of an Oracle SELECT (read by python-oracledb) registered as `ora_src`; NUMBER as exact text, INTERVAL YEAR TO MONTH as ISO text, VECTOR as a list, a JSON column as JSON text. Returns `{column: "NUMBER"}` for the NUMBER columns."""
    import array
    import decimal

    import pyarrow as pa

    from .value_diff import oracle_result

    numeric: dict = {}

    def cell(name: str, v: object) -> object:
        if isinstance(v, decimal.Decimal):
            numeric[name] = "NUMBER"
            return str(v)
        if type(v).__name__ == "IntervalYM":
            return f"P{v.years}Y{v.months}M"
        if isinstance(v, dt.timedelta):
            return oracle_ds_iso(v)
        if isinstance(v, array.array):
            return list(v)
        whole = lambda d: int(d) if d == d.to_integral_value() else float(d)  # noqa: E731 — JSON numbers as JSON reads them
        as_json = v is not None and (name in json_cols or isinstance(v, dict))
        return json.dumps(v, default=lambda d: whole(d) if isinstance(d, decimal.Decimal) else str(d)) if as_json else v

    names, rows = oracle_result(url, sql)
    cols = list(zip(*[[cell(n, v) for n, v in zip(names, r)] for r in rows])) or [[] for _ in names]

    def column(c: list) -> pa.Array:
        try:
            a = pa.array(c)
        except (pa.ArrowInvalid, pa.ArrowTypeError):  # a column of mixed python types: compared as text
            return pa.array([None if v is None else (v.hex() if isinstance(v, bytes) else str(v)) for v in c])
        return pa.array(c, pa.string()) if pa.types.is_null(a.type) else a

    ora.db.register("ora_src", pa.table({n: column(list(c)) for n, c in zip(names, cols)}))
    return numeric


def _source(ora, spec: dict, renders: dict) -> tuple[str, list[str], dict]:
    """(source relation, primary key columns, declared type per column in the ledger's spelling) for the spec's engine."""
    from .value_diff import oracle_rows

    engine, table, query = spec["engine"], spec.get("table") or "", spec.get("query")
    schema, leaf = table.split(".", 1) if "." in table and engine != "mongo" else (None, table)
    native: dict = {}
    if engine == "oracle":
        from .value_diff import oracle_table_select

        owner = f"owner = {_lit(schema.upper())}" if schema else "owner = USER"
        declared = {} if query else {r["COLUMN_NAME"]: norm_native(r["T"]) for r in oracle_rows(
            spec["url"],
            "SELECT column_name, CASE WHEN data_type = 'NUMBER' AND data_precision IS NOT NULL THEN "
            "'NUMBER(' || data_precision || CASE WHEN data_scale > 0 THEN ',' || data_scale END || ')' "
            "ELSE data_type END AS t FROM all_tab_columns "
            f"WHERE {owner} AND table_name = {_lit(leaf.upper())}",
        )}
        by_col = {c: renders.get(f"source:{n}") for c, n in declared.items()}
        sql = query.rstrip().rstrip(";") if query else oracle_table_select(spec["url"], table, {c: e for c, e in by_col.items() if e})
        native = _oracle_register(ora, spec["url"], sql, frozenset(c for c, t in declared.items() if t == "JSON"))
        native.update(declared)
        rel = "ora_src"
        key = [r["COLUMN_NAME"] for r in oracle_rows(
            spec["url"],
            "SELECT cc.column_name FROM all_constraints c JOIN all_cons_columns cc "
            "ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name "
            f"WHERE c.constraint_type = 'P' AND c.{owner} AND c.table_name = {_lit(leaf.upper())} "
            "ORDER BY cc.position",
        )]
        return rel, key, native
    if engine == "postgres":
        if query:
            return f"postgres_query('pg', {_lit(query)})", [], native
        reg = _lit(table)
        key = [r[0] for r in ora.rows(
            "SELECT * FROM postgres_query('pg', " + _lit(
                "SELECT a.attname::text FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                f"AND a.attnum = ANY(i.indkey) WHERE i.indrelid = {reg}::regclass AND i.indisprimary"
            ) + ")"
        )]
        native = {c: norm_native(t) for c, t in ora.rows(
            "SELECT * FROM postgres_query('pg', " + _lit(
                "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
                f"WHERE a.attrelid = {reg}::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
            ) + ")"
        )}
        return _pg_projection(table, native, renders), key, native
    if engine == "mysql":
        db = spec["database"]
        if query:
            return f"mysql_query('my', {_lit(query)})", [], native
        key = [r[0] for r in ora.rows(
            "SELECT * FROM mysql_query('my', " + _lit(
                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = "
                f"{_lit(schema or db)} AND TABLE_NAME = {_lit(leaf)} AND CONSTRAINT_NAME = 'PRIMARY' "
                "ORDER BY ORDINAL_POSITION"
            ) + ")"
        )]
        native = {c: "BOOLEAN" if norm_native(t) == "TINYINT(1)" else norm_native(t) for c, t in ora.rows(
            "SELECT * FROM mysql_query('my', " + _lit(
                f"SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {_lit(schema or db)} "
                f"AND TABLE_NAME = {_lit(leaf)}"
            ) + ")"
        )}
        return f"my.{schema or db}.{leaf}", key, native
    if engine == "mssql":
        sch = schema or "dbo"
        if query:
            return f"mssql_scan('ms', {_lit(query)})", [], native
        oid = f"OBJECT_ID({_lit(sch + '.' + leaf)})"
        if spec.get("capture_instance"):
            # A CDC export captures the relation its capture instance names, as rivet resolves it.
            hit = ora.rows(
                "SELECT * FROM mssql_scan('ms', " + _lit(
                    "SELECT OBJECT_SCHEMA_NAME(ct.source_object_id) AS sch, OBJECT_NAME(ct.source_object_id) AS tbl, "
                    "ct.source_object_id AS oid FROM cdc.change_tables ct WHERE ct.capture_instance = "
                    f"N{_lit(spec['capture_instance'])}"
                ) + ")"
            )
            if hit and hit[0][0] is None:
                raise Unreachable(f"capture instance `{spec['capture_instance']}`: its source table was dropped, the change table remains")
            if hit:
                sch, leaf, oid = hit[0][0], hit[0][1], str(hit[0][2])
        key = [r[0] for r in ora.rows(
            "SELECT * FROM mssql_scan('ms', " + _lit(
                "SELECT c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id "
                "AND ic.index_id = i.index_id JOIN sys.columns c ON c.object_id = ic.object_id AND "
                f"c.column_id = ic.column_id WHERE i.is_primary_key = 1 AND i.object_id = {oid} "
                "ORDER BY ic.key_ordinal"
            ) + ")"
        )]
        native = {c: norm_native(t) for c, t in ora.rows(
            "SELECT * FROM mssql_scan('ms', " + _lit(
                "SELECT COLUMN_NAME, UPPER(DATA_TYPE) + CASE WHEN DATA_TYPE IN ('decimal', 'numeric') THEN "
                "'(' + CAST(NUMERIC_PRECISION AS varchar) + ',' + CAST(NUMERIC_SCALE AS varchar) + ')' "
                "WHEN CHARACTER_MAXIMUM_LENGTH > 0 THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS varchar) + ')' "
                "ELSE '' END FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = {_lit(sch)} AND TABLE_NAME = {_lit(leaf)}"
            ) + ")"
        )}
        # The scanner trims CHAR/NCHAR padding; the server's own cast keeps it.
        renders = {**{n: "CAST({c} AS NVARCHAR(MAX))" for n in native.values() if n.startswith(("CHAR(", "NCHAR("))}, **renders}
        if not any(n in renders for n in native.values()):
            return f"ms.{sch}.{leaf}", key, native
        cols = ", ".join(renders.get(n, "{c}").replace("{c}", f"[{c}]") + f" AS [{c}]" for c, n in native.items())
        return f"mssql_scan('ms', {_lit(f'SELECT {cols} FROM [{sch}].[{leaf}]')})", key, native
    if engine == "mongo":
        # The scanner infers a schema (flattening nested keys); only `_id` is shared with the document blob.
        return f'(SELECT "_id" FROM mg.{spec["database"]}.{_qi(leaf)})', ["_id"], native
    raise ValueError(f"no source reader for engine {engine!r}")


class Unreachable(Exception):
    """The source relation cannot be read for a stated reason: a named SKIP, never a pass."""


def _attach(spec: dict) -> dict:
    """The `duck.Oracle` keywords that attach the spec's source."""
    from urllib.parse import urlparse

    from .value_diff import source_attach

    engine, url = spec["engine"], spec["url"]
    if engine == "oracle":
        return {}
    if engine == "mongo":
        u = urlparse(url)
        return {"mongo": f"mongodb://{u.netloc}/" + (f"?{u.query}" if u.query else "")}
    if engine == "postgres":
        from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

        # The config's own `options` (a search_path) is kept; the scanner's text settings are appended to it.
        styles = "-c DateStyle=ISO,MDY -c IntervalStyle=postgres -c TimeZone=UTC -c bytea_output=hex"
        parts = urlsplit(url)
        q = dict(parse_qsl(parts.query))
        q["options"] = f"{q['options']} {styles}" if q.get("options") else styles
        url = urlunsplit(parts._replace(query=urlencode(q, quote_via=lambda s, *_: quote(s, safe=""))))
    return source_attach(engine, url)[0]


def _columns(ora, rel: str) -> list[tuple[str, str]]:
    """(name, DuckDB type) of every column of `rel`."""
    return [(r[0], str(r[1])) for r in ora.rows(f"DESCRIBE SELECT * FROM {rel}")]


def _is_num(t: str) -> bool:
    """Whether a DuckDB type is numeric."""
    return t in INT_BITS or t.startswith("DECIMAL") or t in ("FLOAT", "DOUBLE")


def _proj(col: str, st: str, dt: str, own: str) -> str:
    """How one side (`own` is its type) projects a column: temporal/interval values and text-vs-number pairs as full-precision text, a TIMESTAMP_NS as epoch nanoseconds (DuckDB cannot render int64-min as text), the rest native."""
    if own == "TIMESTAMP_NS":
        return f"epoch_ns({_qi(col)})"
    temporal = any(k in st or k in dt for k in ("TIME", "INTERVAL"))
    mixed = (st == "VARCHAR") != (dt == "VARCHAR") and (_is_num(st) or _is_num(dt))
    return f"CAST({_qi(col)} AS VARCHAR)" if temporal or mixed else _qi(col)


def _numtext(v: object) -> object:
    """A numeric text as an exact Decimal (so `1.50` and `1.5` are one value); anything else unchanged."""
    import decimal

    if isinstance(v, str):
        try:
            return decimal.Decimal(v.strip())
        except decimal.InvalidOperation:
            return v
    return v


def compare(
    ora,
    src: str,
    dst: str | None,
    bits: frozenset = frozenset(),
    numbers: frozenset = frozenset(),
    defects: dict | None = None,
    duck: dict | None = None,
    canons: dict | None = None,
    duck_source: bool = True,
) -> dict:
    """Column pairing, per-column counts and the canon multiset difference between `src` and `dst`. `bits`: MySQL BIT(n) columns (bytes as an unsigned integer); `numbers`: source columns read as numeric text; `defects`: known_defect column -> its `defect_samples`, the only source values that leave the value multiset; `duck`: the ledger's DuckDB render per column, applied alike to both sides; `canons`: the ledger's canon per column."""
    import decimal
    import struct

    from .value_diff import _secs, canon

    defects, duck, canons = defects or {}, duck or {}, canons or {}
    scols = _columns(ora, src)
    dcols = [c for c in _columns(ora, dst) if c[0] not in META] if dst else []
    exact, folded = dict(dcols), {n.lower(): (n, t) for n, t in dcols}
    pairs, missing = [], []
    for name, st in scols:
        hit = (name, exact[name]) if name in exact else folded.get(name.lower())
        if hit:
            pairs.append((name, st, hit[0], hit[1]))
        else:
            missing.append(name)

    def proj(col: str, key: str, st: str, dt: str, own: str, side: str) -> str:
        r = duck.get(key) if side == "d" or duck_source else None
        return r.replace("{c}", _qi(col)) if r and r != "arrow" else _proj(col, st, dt, own)

    sp = ", ".join(f"{proj(s, s, st, dt, st, "s")} AS c{i}" for i, (s, st, _, dt) in enumerate(pairs)) or "1 AS c0"
    dp = ", ".join(f"{proj(d, s, st, dt, dt, "d")} AS c{i}" for i, (s, st, d, dt) in enumerate(pairs)) or "1 AS c0"
    ora.db.sql(f"CREATE OR REPLACE TEMP TABLE s AS SELECT {sp} FROM {src}")
    if dst:
        ora.db.sql(f"CREATE OR REPLACE TEMP TABLE d AS SELECT {dp} FROM {dst}")
    else:
        ora.db.sql("CREATE OR REPLACE TEMP TABLE d AS SELECT * FROM s LIMIT 0")
    agg = "".join(f", count(c{i}), count(DISTINCT c{i})" for i in range(len(pairs)))

    def stats(t: str) -> tuple[int, list[list[int]]]:
        r = ora.rows(f"SELECT count(*){agg} FROM {t}")[0]
        return r[0], [[r[1 + 2 * i], r[2 + 2 * i]] for i in range(len(pairs))]

    out = {"pairs": pairs, "missing": missing, "dst_cols": dcols}
    out["src_count"], out["src_stats"] = stats("s")
    out["dst_count"], out["dst_stats"] = stats("d")
    numeric = {i for i, (s, st, _, dt) in enumerate(pairs) if _is_num(st) or _is_num(dt) or s in numbers}
    bit = {i for i, p in enumerate(pairs) if p[0] in bits}
    nanos = {i for i, (_, st, _, dt) in enumerate(pairs) if "TIMESTAMP_NS" in (st, dt)}
    how = {i: canons[p[0]] for i, p in enumerate(pairs) if p[0] in canons}

    def cell(i: int, v: object) -> object:
        if v is None:
            return None
        if how.get(i) == "round_micros":
            c = canon(v)
            if isinstance(c, tuple) and c[0] == "ts":
                return ("ts", _secs(decimal.Decimal(c[1]).quantize(decimal.Decimal("0.000001"), decimal.ROUND_HALF_UP)))
            return c
        if how.get(i) == "seconds":
            c = canon(v)
            if isinstance(c, tuple) and c[0] == "dur" and c[1] == c[2] == 0:
                return ("num", c[3])
            return canon(_numtext(v))
        if how.get(i) == "float32":
            return struct.unpack("f", struct.pack("f", float(v)))[0]
        if i in bit and isinstance(v, (bytes, bytearray)):
            return int.from_bytes(v, "big")
        if i in nanos and (isinstance(v, int) or (isinstance(v, str) and v.lstrip("-").isdigit())):
            return ("ts", _secs(decimal.Decimal(v).scaleb(-9)))
        return _numtext(v) if i in numeric or how.get(i) in ("number", "float64") else v

    def text(i: int, v: object) -> str:
        return json.dumps(canon(cell(i, v)), default=str, sort_keys=True)

    # A column with defect samples is graded on its own; the row multiset is over every other column.
    split = {i for i, p in enumerate(pairs) if defects.get(p[0])}
    keep = [i for i in range(len(pairs)) if i not in split]
    cols = ", ".join(f"c{i}" for i in keep) or "1"
    try:
        rs = ora.rows(f"SELECT {cols} FROM s EXCEPT ALL SELECT {cols} FROM d")
        rd = ora.rows(f"SELECT {cols} FROM d EXCEPT ALL SELECT {cols} FROM s")
    except Exception:  # noqa: BLE001 — incomparable native types: every row goes through canon
        rs, rd = ora.rows(f"SELECT {cols} FROM s"), ora.rows(f"SELECT {cols} FROM d")

    def key(r: tuple) -> str:
        return "[" + ", ".join(text(i, v) for i, v in zip(keep, r)) + "]"

    cs, cd = Counter(map(key, rs)), Counter(map(key, rd))
    only_s, only_d = cs - cd, cd - cs
    diff = []
    if only_s or only_d:
        col = lambda rows, j: Counter(text(keep[j], r[j]) for r in rows)  # noqa: E731
        diff = [pairs[keep[j]][0] for j in range(len(keep)) if col(rs, j) != col(rd, j)]
    for i in sorted(split):
        name = pairs[i][0]
        samples = {text(i, x) for x in defects[name]}
        sv = Counter(text(i, r[0]) for r in ora.rows(f"SELECT c{i} FROM s"))
        dv = Counter(text(i, r[0]) for r in ora.rows(f"SELECT c{i} FROM d"))
        lost = Counter({k: n for k, n in (sv - dv).items() if k not in samples})
        if lost:
            diff.append(name)
            only_s = only_s + lost
    out["diff_columns"] = diff
    out["only_src"], out["only_dst"] = sum(only_s.values()), sum(only_d.values())
    out["only_src_sample"] = [s[:600] for s in sorted(only_s)[:5]]
    out["only_dst_sample"] = [s[:600] for s in sorted(only_d)[:5]]
    return out


def grade_findings(
    f: dict,
    rows: dict,
    text_forms: set[str],
    native: dict,
    check_types: bool,
    overrides: frozenset = frozenset(),
    collapse: frozenset = frozenset(),
    null_class: frozenset = frozenset(),
) -> list[str]:
    """Every disagreement in the findings `f`, one line each; empty means the run is sound. A column with a ledger row is graded against the row's delivery (a `known_defect` row is an expected divergence); one without is graded as not narrower than the source; a `columns:` override is the export's own declaration."""
    bad = []
    if f.get("dst_cols"):
        bad += [f"TYPE: source column `{m}` is absent from the delivered parquet" for m in f["missing"]]
    for col, st, _, dt in f.get("pairs", []) if check_types else []:
        nat = native.get(col)
        row = rows.get(nat) if nat else None
        if col in overrides or (nat, dt) in NATIVE_FITS or (row and row.get("known_defect")):
            continue
        want = arrow_to_duck(row["delivery"], text_forms) if row else None
        if want is not None and want != dt:
            bad.append(f"TYPE: `{col}` ({nat}): the type ledger delivers {row['delivery']} ({want}), delivered {dt}")
        elif want is None and (why := type_loss(st, dt)) is not None:
            bad.append(f"TYPE: `{col}` source {st} delivered as {dt}: {why}")
    if "src_count" in f:
        if f["src_count"] != f["dst_count"]:
            bad.append(f"COUNT(*): source {f['src_count']}, delivered {f['dst_count']}")
        for (col, *_), a, b in zip(f["pairs"], f["src_stats"], f["dst_stats"]):
            if col in null_class:  # a declared NULL-class defect (a NULL array loads as [])
                continue
            if a[0] != b[0]:
                bad.append(f"COUNT(`{col}`) (non-null): source {a[0]}, delivered {b[0]}")
            if a[1] != b[1] and col not in collapse:
                bad.append(f"COUNT(DISTINCT `{col}`): source {a[1]}, delivered {b[1]}")
        if f["only_src"] or f["only_dst"]:
            bad.append(
                f"VALUES: {f['only_src']} source row(s) not delivered, {f['only_dst']} delivered row(s) not in the "
                f"source; differing column(s) {f.get('diff_columns')} of {[p[0] for p in f['pairs']]}; "
                f"source-only {f['only_src_sample']}; "
                f"delivered-only {f['only_dst_sample']}"
            )
    parts, declared = f.get("part_rows", 0), f.get("manifest_rows", 0)
    if declared != parts:
        bad.append(f"COUNTER: this run's Success manifests declare {declared} rows, their parts hold {parts}")
    if "metrics_runs" in f:
        if f["metrics_runs"] == 0:
            bad.append("COUNTER: export_metrics has no success row for this run")
        elif f["metrics_rows"] != parts:
            bad.append(f"COUNTER: export_metrics.total_rows {f['metrics_rows']}, this run's declared parts hold {parts}")
        if f["file_log_parts"] != f["declared_parts"]:
            bad.append(f"COUNTER: file_log records {f['file_log_parts']} of this run's {f['declared_parts']} declared part(s)")
        elif f["file_log_rows"] != parts:
            bad.append(f"COUNTER: file_log.row_count of the declared parts sums to {f['file_log_rows']}, they hold {parts}")
    return bad


def _text_form_mismatches(parts: list[str], row_of: dict, forms: set[str]) -> list[str]:
    """A column the ledger delivers as a TEXT form must carry that form in its `rivet.text_form` field metadata."""
    import pyarrow.parquet as pq

    if not parts:
        return []
    schema = pq.read_schema(parts[0])
    bad = []
    for col, r in row_of.items():
        if r.get("known_defect") or r.get("delivery") not in forms or col not in schema.names:
            continue
        got = (schema.field(col).metadata or {}).get(b"rivet.text_form", b"").decode()
        if got != r["delivery"]:
            bad.append(f"TYPE: `{col}`: the ledger delivers text form {r['delivery']}, the part labels it {got or 'nothing'}")
    return bad


def _state_table(ora, spec: dict, name: str) -> str:
    """`st.<schema>.<name>` wherever the state DB keeps it (a least-privilege role keeps its own schema); asked of the state DB alone, since `duckdb_tables()` would enumerate every attached catalog (mongoc opened ~800 connections doing so)."""
    if not str(spec.get("state") or "").startswith("postgres"):
        return f"st.{name}"
    rows = ora.rows(
        "SELECT * FROM postgres_query('st', "
        + _lit(f"SELECT table_schema::text FROM information_schema.tables WHERE table_name = {_lit(name)}")
        + ")"
    )
    return f"st.{rows[0][0]}.{name}" if rows else f"st.{name}"


def _counters(ora, spec: dict, new_parts: list[str], run_ids: list[str]) -> dict:
    """rivet's own ledger for this run: export_metrics success rows and the file_log rows of the declared parts."""
    if not (spec.get("state") and run_ids):
        return {}
    ids = ", ".join(_lit(r) for r in run_ids)
    names = "[" + ", ".join(_lit(os.path.basename(p)) for p in new_parts) + "]::VARCHAR[]"
    runs, rows = ora.rows(
        f"SELECT count(*), coalesce(sum(total_rows), 0) FROM {_state_table(ora, spec, 'export_metrics')} "
        f"WHERE run_id IN ({ids}) AND status = 'success'"
    )[0]
    fl_parts, fl_rows = ora.rows(
        f"SELECT count(DISTINCT regexp_extract(file_name, '[^/]+$')), coalesce(sum(row_count), 0) "
        f"FROM {_state_table(ora, spec, 'file_log')} WHERE run_id IN ({ids}) "
        f"AND list_contains({names}, regexp_extract(file_name, '[^/]+$'))"
    )[0]
    return {"metrics_runs": runs, "metrics_rows": rows, "file_log_parts": fl_parts,
            "file_log_rows": fl_rows, "declared_parts": len(new_parts)}


def _duck_blind(t) -> bool:
    """A parquet type DuckDB 1.5.5 reads short: Decimal256 (as DOUBLE), Time64(ns) and a zoned Timestamp(ns) (both at microseconds)."""
    import pyarrow as pa

    return (
        pa.types.is_decimal256(t)
        or (pa.types.is_time64(t) and t.unit == "ns")
        or (pa.types.is_timestamp(t) and t.unit == "ns" and t.tz is not None)
    )


def exact_text(table):
    """`table` with every column DuckDB would read short cast by pyarrow to its exact text."""
    import pyarrow as pa

    for i, f in enumerate(table.schema):
        if _duck_blind(f.type):
            table = table.set_column(i, f.name, table[f.name].cast(pa.string()))
    return table


def _parts(ora, files: list[str]) -> str:
    """A relation over parquet `files`; a column DuckDB reads short is read by pyarrow as exact text."""
    import pyarrow.parquet as pq

    if not any(_duck_blind(f.type) for f in pq.read_schema(files[0])):
        return f"read_parquet({_plist(files)}, union_by_name = true)"
    import pyarrow as pa

    table = exact_text(pa.concat_tables([pq.read_table(f) for f in files], promote_options="default"))
    view = f"parts_{next(_VIEWS)}"
    ora.db.register(view, table)
    return view


_VIEWS = itertools.count()


def _meta_leg(ora, files: list[str], engine: str, snapshot: bool) -> str:
    """One SELECT over `files` carrying `__op`, `__pos`, `__seq` and the change order `__ord` (a snapshot leg sorts first)."""
    rel = _parts(ora, files)
    have = {c for c, _ in _columns(ora, rel)}
    add = [] if "__op" in have else ["'snapshot' AS __op"]
    add += [] if "__pos" in have else ["'' AS __pos"]
    add += [] if "__seq" in have else ["-1::BIGINT AS __seq"]
    add.append("'' AS __ord" if snapshot else f"{pos_order(engine)} AS __ord")
    return f"SELECT *, {', '.join(add)} FROM {rel}"


def grade(spec: dict) -> dict:
    """Grade one run: `{failures, notes, facts}`."""
    from .duck import Oracle
    from .value_diff import mongo_document_columns

    notes: list[str] = []
    engine, cdc, cumulative = spec["engine"], spec["mode"] == "cdc", spec.get("cumulative", False)
    out_dir, snap_dir = spec["out_dir"], spec["snapshot_dir"]
    graded = in_run_order(out_dir, spec["manifests"])
    snaps = declared_parts(snap_dir, spec["snapshot_manifests"])
    new_parts = declared_parts(out_dir, spec["new_manifests"]) + declared_parts(snap_dir, spec["new_snapshot_manifests"])
    run_ids, manifest_rows = manifest_facts(out_dir, spec["new_manifests"])
    ids, rows = manifest_facts(snap_dir, spec["new_snapshot_manifests"])
    run_ids, manifest_rows = run_ids + ids, manifest_rows + rows
    rows, forms, renders, config = _prep(engine, cdc)
    kw = {"state": spec["state"]} if spec.get("state") else {}
    with Oracle(config=config, **kw, **_attach(spec)) as ora:
        ora.db.sql("SET TimeZone = 'UTC'")
        src, key, native = _source(ora, spec, renders)
        key = spec.get("key") or key
        # One read of the source: each later DESCRIBE or scan would open fresh scanner connections (mongoc opened ~2k per test).
        # A CDC pin run can precede the table; its empty stream never reads the source.
        if not cdc or snaps or declared_parts(out_dir, graded):
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE source_rows AS SELECT * FROM {src}")
            src = "source_rows"
        filt = watermark(out_dir, graded, spec.get("cursor_expr")) if cumulative and not cdc else None
        if cumulative and not cdc and spec.get("state") and spec.get("export"):
            first = (_load(out_dir, graded[0]).get("source") or {}).get("extraction") or {} if graded else {}
            mine = ", ".join(_lit(r) for r in manifest_facts(out_dir, graded)[0]) or "NULL"
            earlier = ora.scalar(
                f"SELECT count(*) FROM {_state_table(ora, spec, 'export_metrics')} WHERE export_name = {_lit(spec['export'])} "
                f"AND status = 'success' AND run_id NOT IN ({mine})"
            )
            if earlier and first.get("cursor_low") is None:
                raise Unreachable(f"delta export: {earlier} earlier run(s) delivered outside this destination, which records no lower bound")
        if filt:
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE got AS SELECT *, 0 AS __mseq FROM {src} LIMIT 0")
            src = f"(SELECT * FROM {src} WHERE {filt})"
        dst: str | None = None
        if cdc:
            changes = declared_parts(out_dir, graded)
            legs = [_meta_leg(ora, changes, engine, False)] if changes else []
            legs += [_meta_leg(ora, snaps, engine, True)] if snaps else []
            if legs and key:
                ora.db.sql("CREATE OR REPLACE TEMP TABLE ev AS " + " UNION ALL BY NAME ".join(f"({x})" for x in legs))
                kl = ", ".join(_qi(k) for k in key)
                ora.db.sql(
                    "CREATE OR REPLACE TEMP TABLE dst AS SELECT * EXCLUDE (__op, __pos, __seq, __ord, __rn) FROM "
                    f"(SELECT *, row_number() OVER (PARTITION BY {kl} ORDER BY __ord DESC, __seq DESC) AS __rn FROM ev) "
                    "WHERE __rn = 1 AND __op <> 'delete'"
                )
                dst = "dst"
                if not snaps:
                    on = " AND ".join(f"CAST(s.{_qi(k)} AS VARCHAR) = CAST(e.{_qi(k)} AS VARCHAR)" for k in key)
                    src = f"(SELECT s.* FROM {src} s SEMI JOIN (SELECT DISTINCT {kl} FROM ev) e ON {on})"
                    notes.append("no snapshot leg: only the keys the stream touched are graded")
            elif legs:
                notes.append("no primary key found: CDC values not graded")
        else:
            legs = [
                f"SELECT *, {i} AS __mseq FROM {_parts(ora, ps)}"
                for i, ps in enumerate(declared_parts(out_dir, [m]) for m in graded) if ps
            ]
            if legs:
                ora.db.sql("CREATE OR REPLACE TEMP TABLE got AS " + " UNION ALL BY NAME ".join(f"({x})" for x in legs))
                if cumulative and key:
                    kl = ", ".join(_qi(k) for k in key)
                    dst = (f"(SELECT * EXCLUDE (__mseq, __rn) FROM (SELECT *, row_number() OVER "
                           f"(PARTITION BY {kl} ORDER BY __mseq DESC) AS __rn FROM got) WHERE __rn = 1)")
                else:
                    dst = "(SELECT * EXCLUDE (__mseq) FROM got)"
        if engine == "mongo" and dst:
            dst = mongo_document_columns(ora, src, dst)
        bits = frozenset(c for c, n in native.items() if n.startswith("BIT")) if engine == "mysql" else frozenset()
        numbers = frozenset(c for c, n in native.items() if n.startswith(ORACLE_NUMERIC)) if engine == "oracle" else frozenset()
        row_of = {c: rows[n] for c, n in native.items() if n in rows}
        defects = {c: [x.strip("'") for x in r.get("defect_samples") or []] for c, r in row_of.items() if r.get("known_defect")}
        # Oracle: its source leg is the client-side `source` render; a DuckDB render (strftime: astronomical years) would grade a different calendar.
        duck = {} if engine == "oracle" else {c: _render(r)["duck"] for c, r in row_of.items() if _render(r).get("duck")}
        canons = {c: _render(r)["canon"] for c, r in row_of.items() if _render(r).get("canon")}
        f = compare(ora, src, dst, bits, numbers, defects, duck, canons, engine != "oracle") if dst or not cdc else {}
        delivered = {d: t for _, _, d, t in f.get("pairs", [])}
        collapse = frozenset(
            c for c in defects if defects[c] and delivered.get(c) == "BOOLEAN"
        )
        f["text_form"] = _text_form_mismatches(new_parts, row_of, forms)
        part_rows = ora.scalar(f"SELECT count(*) FROM {_parts(ora, new_parts)}") if new_parts else 0
        f.update(part_rows=part_rows, manifest_rows=manifest_rows, **_counters(ora, spec, new_parts, run_ids))
    overrides = frozenset(spec.get("overrides") or [])
    failures = grade_findings(f, rows, forms, native, engine != "mongo", overrides, collapse if dst or not cdc else frozenset())
    failures += f.get("text_form") or []
    facts = {k: v for k, v in f.items() if k not in ("pairs", "dst_cols", "src_stats", "dst_stats")}
    return {"failures": failures, "notes": notes, "facts": facts, "key": key}


def _prep(engine: str, cdc: bool) -> tuple[dict, set[str], dict, dict]:
    """(ledger rows, TEXT forms, source-side renders, DuckDB config) for one session over `engine`."""
    rows, forms = ledger(engine, "cdc" if cdc else "batch")
    renders = {
        n: _render(r).get("server")
        or (PG_TEXT if engine == "postgres" and (r.get("delivery") in forms or r.get("delivery") == "server_text" or r.get("batch_refuses")) else None)
        for n, r in rows.items()
    }
    renders = {n: e for n, e in renders.items() if e}
    renders.update({f"source:{n}": _render(r)["source"] for n, r in rows.items() if _render(r).get("source")})
    # Two threads: every live test runs this, and a scanner opens a connection per thread.
    return rows, forms, renders, {"threads": 2, **SCANNER_SETTINGS.get(engine, {})}


#: Columns a warehouse load adds beside the source's.
WAREHOUSE_META = ("__is_deleted", "__op", "__pos", "__seq", "_rivet_")


def _clickhouse(load: dict, password: str, sql: str) -> str:
    """A DuckDB relation over ClickHouse's own answer to `sql`, fetched as Parquet over HTTP."""
    from urllib.parse import quote, urlsplit

    u = urlsplit(load.get("url") or "http://127.0.0.1:8123")
    auth = f"user={quote(str(load.get('user') or 'default'))}&password={quote(password)}"
    return f"read_parquet('{u.scheme}://{u.netloc}/?{auth}&query={quote(sql + ' FORMAT Parquet')}')"


def grade_load(spec: dict) -> dict:
    """Grade the warehouse table a `rivet load` (or `compact`) left: its live rows against the source, per column like a run."""
    from .duck import Oracle

    engine, cdc = spec["engine"], spec["mode"] == "cdc"
    load = spec["load"]
    target = str(load.get("target"))
    if target not in ("clickhouse", "bigquery"):
        return {"skip": f"load target `{target}`: the rig oracle grades clickhouse and bigquery"}
    rows, forms, renders, config = _prep(engine, cdc)
    kw = {"state": spec["state"]} if spec.get("state") else {}
    if target == "bigquery":
        os.environ["BQ_ORACLE_PROJECT"] = str(load.get("project") or "")
        os.environ["BQ_ORACLE_DATASET"] = str(load.get("dataset") or "")
        kw.update(bigquery=True, bq_dataset=str(load.get("dataset")))
    notes: list[str] = []
    try:
        ora = Oracle(config=config, **kw, **_attach(spec))
    except Exception as e:  # noqa: BLE001 — an absent warehouse credential is a named skip, never a pass
        if target == "bigquery" and any(k in str(e).lower() for k in ("credential", "permission", "unauthenticated", "default credentials")):
            return {"skip": f"BigQuery unreachable ({str(e)[:160]}): set BIGQUERY_TEST_PROJECT, RIVET_TEST_GCS_BUCKET and gcloud ADC"}
        raise
    with ora:
        ora.db.sql("SET TimeZone = 'UTC'")
        loaded = ora.rows(
            f"SELECT target_table FROM {_state_table(ora, spec, 'load_run')} "
            f"WHERE export_name = {_lit(spec['export'])} AND status = 'success' ORDER BY finished_at DESC LIMIT 1"
        )
        if not loaded:
            return {"failures": [f"WAREHOUSE: no successful load_run row for export `{spec['export']}`"], "notes": notes}
        fq = loaded[0][0]
        leaf = fq.split(".")[-1]
        src, key, native = _source(ora, spec, renders)
        ora.db.sql(f"CREATE OR REPLACE TEMP TABLE source_rows AS SELECT * FROM {src}")
        src = "source_rows"
        if target == "clickhouse":
            db = str(load.get("database") or "default")
            password = spec.get("password") or ""
            cols = ora.rows(f"SELECT * FROM {_clickhouse(load, password, f'SELECT name, type FROM system.columns WHERE database = {_lit(db)} AND table = {_lit(leaf)}')}")
            wh_types = {n: t for n, t in cols}
            # ClickHouse ships a binary String as invalid UTF-8; read it as the hex the source canon uses.
            blobs = [c for c, t in _columns(ora, "source_rows") if t == "BLOB" and "String" in wh_types.get(c, "")]
            # A Decimal wider than 38 digits reaches DuckDB as DOUBLE; read it as ClickHouse's exact text.
            wide = {c for c, t in wh_types.items() if (m := re.search(r"Decimal\((\d+)", t)) and int(m.group(1)) > 38}
            sel = ", ".join(f"hex(`{c}`) AS `{c}`" if c in blobs else f"toString(`{c}`) AS `{c}`" if c in wide else f"`{c}`" for c in wh_types)
            rel = _clickhouse(load, password, f"SELECT {sel} FROM `{db}`.`{leaf}`")
            if blobs:
                rel = f"(SELECT * REPLACE ({', '.join(f'unhex(CAST({_qi(c)} AS VARCHAR)) AS {_qi(c)}' for c in blobs)}) FROM {rel})"
            buffered = 0
        else:
            ds = str(load.get("dataset"))
            # One catalog listing (seconds); a BigQuery query job costs ~10 s, a storage read ~2 s and reads tables only.
            tables = {r[0] for r in ora.rows("SELECT table_name FROM duckdb_tables() WHERE database_name = 'bq'")}
            rel = f"bq.{ds}.{leaf}" if leaf in tables else f"bigquery_query('bq', {_lit(f'SELECT * FROM `{fq}`')})"
            wh_types = {}
            buffered = f"{leaf}__changes" in tables
        if not wh_types and target == "clickhouse":
            return {"failures": [f"WAREHOUSE: `{fq}` does not exist in ClickHouse database `{db}`"], "notes": notes}
        if buffered:
            return {"skip": f"`{fq}__changes` holds an uncompacted buffer: the base is current only after `rivet compact`"}
        try:
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh_all AS SELECT * FROM {rel}")
        except Exception as e:  # noqa: BLE001 — a table that requires a partition filter is read with an all-partitions one
            m = re.search(r"filter over column\(s\) '([^']+)'", str(e))
            if not m:
                raise
            c = m.group(1)
            sql = f"SELECT * FROM `{fq}` WHERE `{c}` IS NULL OR `{c}` >= TIMESTAMP('0001-01-01')"
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh_all AS SELECT * FROM bigquery_query('bq', {_lit(sql)})")
        have = [c for c, _ in _columns(ora, "wh_all")]
        keep = ", ".join(_qi(c) for c in have if not c.startswith(WAREHOUSE_META)) or "1"
        if "__pos" in have and key:
            # A change log: its live state is the latest image per key, deletes removed.
            kl = ", ".join(_qi(k) for k in key)
            seq = ", __seq DESC" if "__seq" in have else ""
            folded = (f"(SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY {kl} ORDER BY {pos_order(engine)} DESC{seq}) AS __rn "
                      f"FROM wh_all) WHERE __rn = 1 AND coalesce(__op, '') <> 'delete')")
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh AS SELECT {keep} FROM {folded}")
            notes.append("change-log layout: graded at the latest image per key")
        else:
            live = "WHERE NOT __is_deleted" if "__is_deleted" in have else ""
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh AS SELECT {keep} FROM wh_all {live}")
        if (cdc or "__pos" in have) and key:
            on = " AND ".join(f"CAST(s.{_qi(k)} AS VARCHAR) = CAST(w.{_qi(k)} AS VARCHAR)" for k in key)
            if not spec.get("snapshot"):
                src = f"(SELECT s.* FROM {src} s SEMI JOIN wh_all w ON {on})"
                notes.append("no snapshot leg: only the keys the warehouse holds are graded")
            if "__is_deleted" in have:
                gone = ora.scalar(f"SELECT count(*) FROM source_rows s SEMI JOIN (SELECT * FROM wh_all WHERE __is_deleted) w ON {on}")
                if gone:
                    notes.append(f"{gone} key(s) flagged __is_deleted still exist in the source")
        bits = frozenset(c for c, n in native.items() if n.startswith("BIT")) if engine == "mysql" else frozenset()
        numbers = frozenset(c for c, n in native.items() if n.startswith(ORACLE_NUMERIC)) if engine == "oracle" else frozenset()
        row_of = {c: rows[n] for c, n in native.items() if n in rows}
        # The ledger names the ClickHouse load on its cdc rows only; a batch load lands in the same types.
        ch_rows = ledger(engine, "cdc")[0] if target == "clickhouse" else {}
        ch_of = {c: ch_rows[n] for c, n in native.items() if n in ch_rows}
        defects = {
            c: [None if x.upper() == "NULL" else x.strip("'") for x in (r.get("defect_samples") or [])]
            for c, r in row_of.items() if r.get("known_defect")
        }
        defects.update({
            c: [None if x.upper() == "NULL" else x.strip("'") for x in (r.get("clickhouse_defect_samples") or [])]
            for c, r in ch_of.items() if r.get("clickhouse_defect")
        })
        canons = {c: _render(r)["canon"] for c, r in row_of.items() if _render(r).get("canon")}
        # A TIME the ledger loads into ClickHouse as Decimal seconds is compared in seconds.
        canons.update({c: "seconds" for c, r in ch_of.items()
                       if target == "clickhouse" and str(r.get("clickhouse", "")).startswith(("Decimal", "Nullable(Decimal")) and native[c].startswith("TIME")})
        duck = {} if engine == "oracle" else {c: _render(r)["duck"] for c, r in row_of.items() if _render(r).get("duck")}
        f = compare(ora, src, "wh", bits, numbers, defects, duck, canons, engine != "oracle")
        delivered = {d: t for _, _, d, t in f.get("pairs", [])}
        collapse = frozenset(c for c in defects if defects[c] and delivered.get(c) == "BOOLEAN")
    # The ClickHouse type the ledger names is graded below; the not-narrower rule covers the rest.
    ledgered = frozenset(c for c, r in ch_of.items() if r.get("clickhouse"))
    null_class = frozenset(c for c, xs in defects.items() if None in xs)
    bad = grade_findings(f, {}, forms, native, engine != "mongo", frozenset(spec.get("overrides") or []) | ledgered,
                         collapse=collapse, null_class=null_class)
    for col, r in ch_of.items():
        want, got = r.get("clickhouse"), wh_types.get(col)
        # A known_defect delivery drives the ClickHouse type too: the marker excuses it.
        excused = r.get("clickhouse_defect") or r.get("known_defect") or row_of.get(col, {}).get("known_defect")
        if target != "clickhouse" or not want or excused or got is None:
            continue
        # A key column cannot be Nullable in ClickHouse.
        if got != want and not (col in key and want == f"Nullable({got})"):
            bad.append(f"TYPE: `{col}` ({native[col]}): the ledger loads ClickHouse {want}, the table has {got}")
    bad += [f"WAREHOUSE: {n}" for n in notes if "still exist" in n]
    return {"failures": [f"WAREHOUSE {target} `{fq}`: {b}" for b in bad], "notes": notes,
            "facts": {k: v for k, v in f.items() if k not in ("pairs", "dst_cols", "src_stats", "dst_stats")}}


def _self_test() -> None:
    assert type_loss("INTEGER", "BIGINT") is None
    assert type_loss("BIGINT", "INTEGER")
    assert type_loss("UBIGINT", "BIGINT")
    assert type_loss("UINTEGER", "BIGINT") is None
    assert type_loss("DECIMAL(38,10)", "DOUBLE")
    assert type_loss("DECIMAL(10,2)", "DECIMAL(12,2)") is None
    assert type_loss("DECIMAL(10,4)", "DECIMAL(10,2)")
    assert type_loss("TIMESTAMP", "DATE")
    assert type_loss("TIMESTAMP_NS", "TIMESTAMP")
    assert type_loss("TIMESTAMP WITH TIME ZONE", "TIMESTAMP") is None
    assert type_loss("DOUBLE", "FLOAT")
    assert type_loss("BLOB", "VARCHAR") is None
    f = {
        "missing": ["gone"], "dst_cols": [["id", "INTEGER"]], "pairs": [["id", "BIGINT", "id", "INTEGER"]],
        "src_count": 3, "dst_count": 2, "src_stats": [[3, 3]], "dst_stats": [[2, 1]],
        "only_src": 1, "only_dst": 0, "only_src_sample": [], "only_dst_sample": [],
        "part_rows": 2, "manifest_rows": 2, "metrics_runs": 1, "metrics_rows": 5, "file_log_rows": 2,
        "file_log_parts": 1, "declared_parts": 1,
    }
    bad = "\n".join(grade_findings(f, {}, set(), {}, True))
    for cls in ("column `gone`", "TYPE: `id`", "COUNT(*)", "COUNT(`id`)", "COUNT(DISTINCT `id`)", "VALUES",
                "export_metrics.total_rows 5"):
        assert cls in bad, f"{cls} missing from {bad}"
    clean = {**f, "missing": [], "pairs": [["id", "BIGINT", "id", "BIGINT"]], "src_count": 2,
             "src_stats": [[2, 2]], "dst_stats": [[2, 2]], "only_src": 0, "metrics_rows": 2}
    assert not grade_findings(clean, {}, set(), {}, True), grade_findings(clean, {}, set(), {}, True)
    assert grade_findings({**clean, "file_log_rows": 3}, {}, set(), {}, True), "a wrong file_log is a finding"
    assert grade_findings({**clean, "file_log_parts": 0}, {}, set(), {}, True), "a declared part file_log lacks is a finding"
    assert _numtext("1.50") == _numtext("1.5000"), "a numeric text compares by value"
    assert grade_findings({**clean, "manifest_rows": 3}, {}, set(), {}, True), "a wrong manifest row_count is a finding"
    led = {**clean, "pairs": [["n", "DOUBLE", "n", "DOUBLE"]]}
    rows = {"NUMERIC": {"delivery": "decimal_plain"}, "BIGINT": {"delivery": "Int64", "known_defect": "x"}}
    assert any("type ledger delivers decimal_plain" in b for b in grade_findings(led, rows, {"decimal_plain"}, {"n": "NUMERIC"}, True))
    xfail = {**clean, "pairs": [["b", "BIGINT", "b", "INTEGER"]]}
    assert not grade_findings(xfail, rows, set(), {"b": "BIGINT"}, True), "a known_defect excuses its TYPE"
    nulled = {**xfail, "dst_stats": [[1, 1]]}
    assert any("COUNT(`b`)" in b for b in grade_findings(nulled, rows, set(), {"b": "BIGINT"}, True)), \
        "a known_defect column whose non-null count drops is still reported"
    collapsed = {**xfail, "dst_stats": [[2, 1]]}
    assert not grade_findings(collapsed, rows, set(), {"b": "BIGINT"}, True, collapse=frozenset({"b"}))
    assert grade_findings(collapsed, rows, set(), {"b": "BIGINT"}, True), "DISTINCT relaxes only for a collapse column"
    assert norm_native("timestamp(6) without time zone[]") == "TIMESTAMP(6)[]"
    assert norm_native("character varying(50)") == "VARCHAR(50)"
    assert arrow_to_duck('Timestamp(µs, "UTC")', set()) == "TIMESTAMP WITH TIME ZONE"
    assert arrow_to_duck("List(Decimal128(18, 2))", set()) == "DECIMAL(18,2)[]"
    from .value_diff import canon

    one_day = dt.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=5)
    assert oracle_ds_iso(one_day) == "P1DT7384.000005S", oracle_ds_iso(one_day)
    assert canon(oracle_ds_iso(one_day)) == canon("P1DT2H3M4.000005S"), "Oracle's day is its own field"
    assert canon(oracle_ds_iso(-dt.timedelta(days=1, hours=2))) == canon("P-1DT-2H"), "the sign rides every part"
    assert canon(oracle_ds_iso(dt.timedelta(0))) == canon("PT0S")
    assert canon(oracle_ds_iso(one_day)) != canon("PT93784.000005S"), "a day is not folded into seconds"
    _ns_self_test()
    print("rig_oracle self-test ok")


def _ns_self_test() -> None:
    """A 9th fractional digit survives the read where DuckDB alone would drop it."""
    import tempfile

    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from .value_diff import canon

    def written(ns: int) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        pq.write_table(pa.table({
            "t": pa.array([45_296_123_456_789 + ns], pa.time64("ns")),
            "z": pa.array([1_700_000_000_123_456_789 + ns], pa.timestamp("ns", tz="UTC")),
        }), f)
        return f

    full, cut = written(0), written(-789)
    via_duck = lambda f: duckdb.sql(f"SELECT t::VARCHAR, z::VARCHAR FROM '{f}'").fetchall()  # noqa: E731
    assert via_duck(full) == via_duck(cut), "DuckDB alone reads both shapes at microseconds"
    ours = lambda f: [canon(v) for v in exact_text(pq.read_table(f)).to_pylist()[0].values()]  # noqa: E731
    assert ours(full) != ours(cut), "the oracle must see the 9th digit a delivery truncated"
    assert ours(full) == [("dur", 0, 0, "45296.123456789"), ("ts", "1700000000.123456789")], ours(full)


if __name__ == "__main__":
    if sys.argv[1:] in (["grade"], ["grade-load"]):
        spec = json.load(sys.stdin)
        run = grade if sys.argv[1] == "grade" else grade_load
        try:
            verdict = run(spec)
        except Unreachable as e:
            verdict = {"skip": str(e)}
        except Exception as e:  # noqa: BLE001 — a SQL Server catalog deadlock victim is retried once
            if "deadlock" not in str(e):
                raise
            verdict = run(spec)
        sys.stdout.write(json.dumps(verdict, default=str))
    else:
        _self_test()
