"""The live suite's default oracle: one DuckDB session grades one run's declared output.

Every `rivet run|load|compact --config`, `rivet apply <config.yaml | plan.json>` and reaped
`Rig::spawn_args_env` child a live test starts through the `Rig` or a shared `run_rivet*`
helper reaches it; a hand-built spawn of the binary is not graded (counted by
tests/offline/rig_oracle_ratchet.rs). An exception in here fails the test as an oracle
error, never a SKIP.

tests/common/verify.rs only gathers facts from the config file — engine, source URL, table
or query, the export's own filter, the manifests the run wrote, the state DB — and hands
them here as JSON on stdin; a cloud destination (MinIO, fake-gcs or real GCS, Azurite) is
pulled whole through the store's own API first, a multi-table capture is graded one table
per sub-prefix, and destination placeholders are resolved independently. Parts are read as
parquet (never a hive path's value in place of the file's column) or CSV (as text). This module owns every check: it ATTACHes the source and
rivet's state DB READ_ONLY through `duck.Oracle`, reads only the parts the Success
manifests declare, and grades per column

  * TYPE: the delivered type is not narrower than the source's catalog type (the type
    ledger's delivery is required where a ledger row exists; a known_defect row may
    deliver only its declared `today_delivery`; a `columns:` override its declared type),
  * VALUES: every row, as a multiset, through `value_diff.canon` (full precision; a text
    column with no ledger canon is compared as its exact text),
  * COUNT(*), COUNT(col) and COUNT(DISTINCT col), source vs delivered,
  * rivet's counters for THIS run: manifest `row_count`, `export_metrics.total_rows`
    (success rows), `file_log.row_count` of the declared parts vs the rows they hold.

Per mode (values and counts): a snapshot run (full, chunked, keyset) is its own new
Success manifests against the whole source (no new manifest over a non-empty source is a
failure; a `--resume` run is graded on the rows it delivered, reported as `partial`); a
delta run (incremental, keyset-incremental, Mongo resume) is every Success manifest in the
destination, latest version per key, against every source row past the cursor where this
stream's previous graded run ended (the oracle's own record; rivet's `cursor_low` and
`cursor_high` never bound it); a CDC run is the latest after-image per key (by `__pos`,
`__seq`; snapshot leg first, deletes removed) against the source's current rows: all of
them when a snapshot leg exists or `initial: snapshot` is declared, else every row changed
between source images the oracle took before the stream's previous successful run and
before this one, plus the keys the stream touched. A stream's first successful run is graded
from its ANCHOR (recorded before a run while the stream's slot or checkpoint did not exist
yet: that run's own image, all of SQL Server's capture instance; a PostgreSQL slot found
existing adds the keys its pending changes name); a stream anchored before any run the
oracle saw is `partial`, never a plain pass; a keyless CDC relation is a SKIP. Mongo
grades `_id` only, reported as `partial`. Oracle is read through python-oracledb (DuckDB has
no scanner), so its TYPE check sees text and grades a NUMBER from its catalog type. The CDC
checkpoint is not graded.
"""

from __future__ import annotations

import datetime as dt
import itertools
import json
import os
import re
import sys
import time
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
    if src == "VARCHAR":
        return None if dst == "JSON" else "text delivered as a non-text type"
    if src == "TIMESTAMP WITH TIME ZONE" and dst in TS_RANK:
        return "a zoned timestamp delivered naive"
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


#: A declared source type that is text in the source itself (not text only because the oracle projected it).
TEXT_NATIVE = re.compile(r"^(N?VARCHAR2?|N?CHAR|N?TEXT|CITEXT|N?CLOB|(TINY|MEDIUM|LONG)TEXT|STRING|NAME|BPCHAR)\b")


def catalog_duck(native: str | None) -> str | None:
    """The DuckDB type a numeric the oracle reads as text declares in its catalog (`None` for any other): `DECIMAL(p,s)`, unbounded as a 1000-digit sentinel."""
    m = re.fullmatch(r"(?:NUMERIC|DECIMAL|NUMBER)(?:\((\d+)(?:,(-?\d+))?\))?(?: UNSIGNED)?", native or "")
    if not m:
        return {"BINARY_DOUBLE": "DOUBLE", "BINARY_FLOAT": "FLOAT"}.get(native or "")
    if m.group(1) is None:
        return "DECIMAL(1000,500)"
    return f"DECIMAL({m.group(1)},{max(int(m.group(2) or 0), 0)})"


def override_duck(declared: str) -> str | None:
    """The DuckDB type a parquet column of a `columns:` override type (src/types/override_type.rs) reads as; `None` when the oracle has no exact mapping."""
    t = " ".join(declared.lower().split())
    m = re.fullmatch(r"(?:decimal|numeric)\((\d+), ?(\d+)\)", t)
    if m:
        p, sc = int(m.group(1)), int(m.group(2))
        return f"DECIMAL({p},{sc})" if p <= 38 else "VARCHAR"
    for names, duck in (
        (("bool", "boolean"), "BOOLEAN"), (("int2", "smallint", "int16"), "SMALLINT"),
        (("int4", "int", "integer", "int32"), "INTEGER"), (("int8", "bigint", "int64"), "BIGINT"),
        (("float4", "real", "float32"), "FLOAT"), (("float8", "double", "double precision", "float64"), "DOUBLE"),
        (("text", "varchar", "string", "char", "bpchar", "name"), "VARCHAR"),
        (("binary", "bytea", "blob", "varbinary"), "BLOB"), (("date",), "DATE"), (("json", "jsonb"), "JSON"),
        (("uuid",), "UUID"), (("timestamp", "timestamp without time zone"), "TIMESTAMP"), (("timestamp_ns",), "TIMESTAMP_NS"),
        (("timestamp_tz", "timestamptz", "timestamp with time zone", "timestamp_utc"), "TIMESTAMP WITH TIME ZONE"),
        (("timestamp_tz_ns", "timestamptz_ns"), "VARCHAR"),
    ):
        if t in names:
            return duck
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
        base = os.path.dirname(os.path.join(root, name))
        for part in success_part_names(_load(root, name)):
            p = part if os.path.isabs(part) else os.path.join(base, part)
            if os.path.isfile(p):
                out.add(p)
    return sorted(out)


def in_run_order(root: str, manifests: list[str]) -> list[str]:
    """The named manifests ordered by when their run finished."""
    return sorted(manifests, key=lambda n: (str(_load(root, n).get("finished_at") or ""), n))


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
    """A server-side SELECT rendering each column the ledger renders server-side, and every numeric, with PostgreSQL's own text (the scanner reads a wide one as DOUBLE, and a NaN has no DECIMAL)."""
    cols = []
    for name, nat in native.items():
        q = _qi(name)
        expr = renders.get(nat) or (PG_TEXT if nat == "NUMERIC[]" or re.fullmatch(r"NUMERIC(\(.*\))?", nat) else "{c}")
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


def json_text(v: object) -> str:
    """A python-oracledb JSON value as JSON text, every Decimal number written with its exact digits (never through a float)."""
    import decimal

    if isinstance(v, dict):
        return "{" + ",".join(f"{json.dumps(str(k))}:{json_text(x)}" for k, x in v.items()) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(json_text(x) for x in v) + "]"
    if isinstance(v, decimal.Decimal):
        return str(v)
    try:
        return json.dumps(v)
    except TypeError:
        return json.dumps(str(v))


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
        as_json = v is not None and (name in json_cols or isinstance(v, dict))
        return json_text(v) if as_json else v

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
    whole = re.fullmatch(r"\s*select\s+\*\s+from\s+([\w.]+)(\s+order\s+by\s+[\w\s,.]+)?\s*;?\s*", query or "", re.I)
    if whole and engine in ("postgres", "mysql", "mssql", "oracle"):
        # The same rows as the table: read through the table path, so its catalog types and key apply.
        table, query = whole.group(1), None
    schema, leaf = table.split(".", 1) if "." in table and engine != "mongo" else (None, table)
    native: dict = {}
    if engine == "oracle":
        from .value_diff import oracle_table_select

        owner = f"owner = {_lit(schema.upper())}" if schema else "owner = USER"
        declared = {} if query else {r["COLUMN_NAME"]: norm_native(r["T"]) for r in oracle_rows(
            spec["url"],
            "SELECT column_name, CASE WHEN data_type = 'NUMBER' AND data_precision IS NOT NULL THEN "
            "'NUMBER(' || data_precision || CASE WHEN data_scale > 0 THEN ',' || data_scale END || ')' "
            "WHEN data_type = 'NUMBER' AND data_scale = 0 THEN 'NUMBER(38)' "
            "ELSE data_type END AS t FROM all_tab_columns "
            f"WHERE {owner} AND table_name = {_lit(leaf.upper())}",
        )}
        if not query and not declared:
            raise LookupError(f"table {table} does not exist (no column in all_tab_columns)")
        by_col = {c: renders.get(f"source:{n}") for c, n in declared.items()}
        sql = query.rstrip().rstrip(";") if query else oracle_table_select(spec["url"], table, {c: e for c, e in by_col.items() if e})
        native = {c: "NUMBER (by value)" for c in _oracle_register(ora, spec["url"], sql, frozenset(c for c, t in declared.items() if t == "JSON"))}
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
        wide = [c for c, n in native.items() if (d := re.match(r"DECIMAL\((\d+)", n)) and int(d.group(1)) > 38]
        if not wide:
            return f"my.{schema or db}.{leaf}", key, native
        # The scanner reads a DECIMAL wider than DuckDB's 38 digits as DOUBLE; MySQL's own text keeps every digit.
        cols = ", ".join(f"CAST(`{c}` AS CHAR) AS `{c}`" if c in wide else f"`{c}`" for c in native)
        return f"mysql_query('my', {_lit(f'SELECT {cols} FROM `{schema or db}`.`{leaf}`')})", key, native
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


#: Engine messages that say the relation is not there (yet), the only source error a CDC stream may precede.
ABSENT = ("does not exist", "not found", "invalid object name", "doesn't exist", "cross-database references are not implemented")


def absent(e: Exception) -> bool:
    """Whether `e` says the source relation does not exist."""
    return any(k in str(e).lower() for k in ABSENT)


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
        if "://" not in url:
            # A libpq keyword/value string takes the settings as one more pair.
            return source_attach(engine, f"{url} options='{styles}'")[0]
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
    """How one side (`own` is its type) projects a column: temporal/interval values, text-vs-number, decimal-vs-float and same-typed float pairs (-0.0 keeps its sign) as full-precision text, a TIMESTAMP_NS as epoch nanoseconds (DuckDB cannot render int64-min as text), the rest native."""
    if own == "TIMESTAMP_NS":
        return f"epoch_ns({_qi(col)})"
    temporal = any(k in st or k in dt for k in ("TIME", "INTERVAL"))
    mixed = (st == "VARCHAR") != (dt == "VARCHAR") and (_is_num(st) or _is_num(dt))
    floats = st == dt and st in ("FLOAT", "DOUBLE")
    decimal_vs_float = {st[:7], dt[:7]} in ({"DECIMAL", "DOUBLE"}, {"DECIMAL", "FLOAT"})
    return f"CAST({_qi(col)} AS VARCHAR)" if temporal or mixed or floats or decimal_vs_float else _qi(col)


def key_match(ora, left: str, la: str, right: str, ra: str, key: list[str]) -> str:
    """`la.k = ra.k` per key column: by value when either side is numeric (an Oracle NUMBER reaches DuckDB as text from the source and as DECIMAL(38,9) from BigQuery), else as text."""
    lt, rt = dict(_columns(ora, left)), dict(_columns(ora, right))

    def one(k: str) -> str:
        a, b = f"{la}.{_qi(k)}", f"{ra}.{_qi(k)}"
        text = f"CAST({a} AS VARCHAR) = CAST({b} AS VARCHAR)"
        if not (_is_num(lt.get(k, "")) or _is_num(rt.get(k, ""))):
            return text
        return f"coalesce(TRY_CAST({a} AS DECIMAL(38,9)) = TRY_CAST({b} AS DECIMAL(38,9)), {text})"

    return " AND ".join(one(k) for k in key)


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
    verbatim: frozenset = frozenset(),
    key: list | None = None,
) -> dict:
    """Column pairing, per-column counts and the canon multiset difference between `src` and `dst`. `bits`: MySQL BIT(n) columns (bytes as an unsigned integer); `numbers`: source columns read as numeric text; `defects`: known_defect column -> its `defect_samples`, the only source values that may differ (per `key` row when one is known); `duck`: the ledger's DuckDB render per column, applied alike to both sides; `canons`: the ledger's canon per column; `verbatim`: text columns compared as their exact text, never through a canon."""
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
    # A number's text compares by value only when the SOURCE is a number: '02134' text is not 2134.
    numeric = {i for i, (s, st, _, dt) in enumerate(pairs) if _is_num(st) or s in numbers}
    exact = {i for i, (s, st, _, dt) in enumerate(pairs) if s in verbatim and st == dt == "VARCHAR"}
    bit = {i for i, p in enumerate(pairs) if p[0] in bits or (p[1] == "BLOB" and p[3] in INT_BITS)}
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
        if i in exact:
            return json.dumps(v)
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

    def rowkey(r: tuple) -> str:
        return "[" + ", ".join(text(i, v) for i, v in zip(keep, r)) + "]"

    cs, cd = Counter(map(rowkey, rs)), Counter(map(rowkey, rd))
    only_s, only_d = cs - cd, cd - cs
    diff = []
    if only_s or only_d:
        col = lambda rows, j: Counter(text(keep[j], r[j]) for r in rows)  # noqa: E731
        diff = [pairs[keep[j]][0] for j in range(len(keep)) if col(rs, j) != col(rd, j)]
    kidx = [j for k in key or [] for j, p in enumerate(pairs) if p[0] == k]
    for i in sorted(split):
        name = pairs[i][0]
        samples = {text(i, x) for x in defects[name]}
        if key and len(kidx) == len(key):
            # Per row: a cell may differ only where the source holds a declared sample.
            on = " AND ".join(f"CAST(s.c{j} AS VARCHAR) IS NOT DISTINCT FROM CAST(d.c{j} AS VARCHAR)" for j in kidx)
            both = [(text(i, a), text(i, b)) for a, b in ora.rows(f"SELECT s.c{i}, d.c{i} FROM s JOIN d ON {on}")]
            lost = Counter(a for a, b in both if a != b and a not in samples)
        else:
            sv = Counter(text(i, r[0]) for r in ora.rows(f"SELECT c{i} FROM s"))
            dv = Counter(text(i, r[0]) for r in ora.rows(f"SELECT c{i} FROM d"))
            lost = Counter({k: n for k, n in (sv - dv).items() if k not in samples})
            # Without a key, each excused sample may account for one delivered value the source lacks, no more.
            spare = sum((dv - sv).values()) - sum(n for k, n in (sv - dv).items() if k in samples)
            if spare > 0:
                lost[f"{spare} delivered value(s) no declared sample explains"] += spare
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
    overrides: dict | None = None,
    collapse: frozenset = frozenset(),
    null_class: frozenset = frozenset(),
) -> list[str]:
    """Every disagreement in the findings `f`, one line each; empty means the run is sound. A column with a ledger row is graded against the row's delivery (a `known_defect` row may deliver its `today_delivery` instead, nothing else); one without is graded as not narrower than its catalog type; a `columns:` override against the type it declares (`None`: not graded here)."""
    bad = []
    overrides = overrides or {}
    if f.get("dst_cols"):
        bad += [f"TYPE: source column `{m}` is absent from the delivered parquet" for m in f["missing"]]
    for col, st, _, dt in f.get("pairs", []) if check_types else []:
        nat = native.get(col)
        row = rows.get(nat) if nat else None
        if col in overrides:
            want = override_duck(overrides[col]) if overrides[col] else None
            if want is not None and want != dt:
                bad.append(f"TYPE: `{col}`: the `columns:` override declares {overrides[col]} ({want}), delivered {dt}")
            continue
        if (nat, dt) in NATIVE_FITS:
            continue
        if row and row.get("known_defect"):
            today = {arrow_to_duck(row.get(k), text_forms) for k in ("delivery", "today_delivery") if row.get(k)}
            if dt not in today:
                bad.append(f"TYPE: `{col}` ({nat}): its known_defect excuses only {sorted(t for t in today if t)}, delivered {dt}")
            continue
        want = arrow_to_duck(row["delivery"], text_forms) if row else None
        # A source the oracle reads as text (a wide numeric) is graded from its catalog type; other text is graded only when the catalog says text.
        src_t = (catalog_duck(nat) or (st if nat and TEXT_NATIVE.match(nat) else None)) if st == "VARCHAR" else st
        if want is not None and want != dt:
            bad.append(f"TYPE: `{col}` ({nat}): the type ledger delivers {row['delivery']} ({want}), delivered {dt}")
        elif want is None and src_t and (why := type_loss(src_t, dt)) is not None:
            bad.append(f"TYPE: `{col}` source {src_t} delivered as {dt}: {why}")
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
        elif f["metrics_rows"] != f.get("stream_rows", parts):
            bad.append(f"COUNTER: export_metrics.total_rows {f['metrics_rows']}, this run's declared parts hold {f.get('stream_rows', parts)}")
        if f["file_log_parts"] != f["declared_parts"]:
            bad.append(f"COUNTER: file_log records {f['file_log_parts']} of this run's {f['declared_parts']} declared part(s)")
        elif f["file_log_rows"] != parts:
            bad.append(f"COUNTER: file_log.row_count of the declared parts sums to {f['file_log_rows']}, they hold {parts}")
    return bad


#: The failure classes a test's `oracle_known_defect` may name: a predicate over one failure line.
KNOWN_DEFECT_CLASSES = {
    "delivered-only rows": lambda line: bool(re.match(r"VALUES: 0 source row\(s\) not delivered, [1-9]", line)) or bool(
        (m := re.match(r"COUNT\(.*: source (\d+), delivered (\d+)$", line)) and int(m.group(2)) > int(m.group(1))
    ),
    "undelivered rows": lambda line: bool(re.match(r"VALUES: [1-9]\d* source row\(s\) not delivered, 0 ", line)) or bool(
        (m := re.match(r"COUNT\(.*: source (\d+), delivered (\d+)$", line)) and int(m.group(2)) < int(m.group(1))
    ),
}


def known_defect_covers(cls: str, failures: list[str]) -> bool:
    """Whether every failure line is of the named known-defect class (an unknown class is a harness error)."""
    if cls not in KNOWN_DEFECT_CLASSES:
        raise ValueError(f"unknown known-defect class {cls!r}; one of {sorted(KNOWN_DEFECT_CLASSES)}")
    return bool(failures) and all(KNOWN_DEFECT_CLASSES[cls](f) for f in failures)


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


def stream_parts(stream: dict, run_ids: list[str]) -> list[str]:
    """Every part a multi-table capture's run declared, across all its tables' destinations (and their `snapshot/` legs)."""
    out = []
    for d in stream["dirs"]:
        for root in (d, os.path.join(d, "snapshot")):
            names = [n for n in (os.listdir(root) if os.path.isdir(root) else []) if n.startswith("manifest-") and n.endswith(".json")]
            out += declared_parts(root, [n for n in names if _load(root, n).get("run_id") in run_ids])
    return out


def _counters(ora, spec: dict, new_parts: list[str], run_ids: list[str]) -> dict:
    """rivet's own ledger for this run: export_metrics success rows and the file_log rows of the declared parts (one table of a multi-table capture: its ledger names are `<table>/<part>`, its metrics row counts the whole stream)."""
    if not (spec.get("state") and run_ids):
        return {}
    ids = ", ".join(_lit(r) for r in run_ids)
    stream = spec.get("stream")
    # A capture's own parts are ledgered as `<table>/<part>`; its baseline (snapshot) parts under their own unique names.
    ledger = (lambda p: os.path.basename(p) if "snapshot" in p.split(os.sep) else f"{stream['table']}/{os.path.basename(p)}") if stream else os.path.basename
    names = "[" + ", ".join(_lit(ledger(p)) for p in new_parts) + "]::VARCHAR[]"
    match = "file_name" if stream else "regexp_extract(file_name, '[^/]+$')"
    runs, rows = ora.rows(
        f"SELECT count(*), coalesce(sum(total_rows), 0) FROM {_state_table(ora, spec, 'export_metrics')} "
        f"WHERE run_id IN ({ids}) AND status = 'success'"
    )[0]
    fl_parts, fl_rows = ora.rows(
        f"SELECT count(DISTINCT {match}), coalesce(sum(row_count), 0) "
        f"FROM {_state_table(ora, spec, 'file_log')} WHERE run_id IN ({ids}) "
        f"AND list_contains({names}, {match})"
    )[0]
    out = {"metrics_runs": runs, "metrics_rows": rows, "file_log_parts": fl_parts,
           "file_log_rows": fl_rows, "declared_parts": len(new_parts)}
    if stream:
        every = stream_parts(stream, run_ids)
        out["stream_rows"] = ora.scalar(f"SELECT count(*) FROM {_parts(ora, every, spec.get('format') or 'parquet')}") if every else 0
    return out


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


def _parts(ora, files: list[str], fmt: str = "parquet") -> str:
    """A relation over the delivered `files`: parquet as written (never a hive path's value in place of the file's column; a column DuckDB reads short read by pyarrow as exact text), CSV as the text of its own header and cells (`""` is an empty string, an empty cell NULL)."""
    import pyarrow.parquet as pq

    if fmt == "csv":
        return (f"read_csv({_plist(files)}, header = true, all_varchar = true, delim = ',', quote = '\"', "
                "escape = '\"', allow_quoted_nulls = false, union_by_name = true)")
    if not any(_duck_blind(f.type) for f in pq.read_schema(files[0])):
        return f"read_parquet({_plist(files)}, union_by_name = true, hive_partitioning = false)"
    import pyarrow as pa

    table = exact_text(pa.concat_tables([pq.read_table(f) for f in files], promote_options="default"))
    view = f"parts_{next(_VIEWS)}"
    ora.db.register(view, table)
    return view


_VIEWS = itertools.count()


def _ndjson(ora, files: list[str], engine: str) -> str:
    """The change lines a `rivet cdc` run printed: each image's positions named by the CDC row's columns (Mongo `_id`, `document`; else the source's, in order) as their JSON text, with `__op`, `__pos`, `__seq`."""
    cols = ["_id", "document"] if engine == "mongo" else [c for c, _ in _columns(ora, "source_rows")]
    img = "CASE WHEN json_extract_string(json, '$.op') = 'delete' THEN 'before' ELSE 'after' END"
    sel = ", ".join(f"json_extract_string(json, '$.' || {img} || '[{i}]') AS {_qi(c)}" for i, c in enumerate(cols))
    return (f"(SELECT {sel}, json_extract_string(json, '$.op') AS __op, CAST(json_extract(json, '$.pos') AS VARCHAR) AS __pos, "
            f"CAST(json_extract(json, '$.seq') AS BIGINT) AS __seq FROM read_ndjson_objects({_plist(files)}))")

#: The directory label rivet gives a partition_by bucket of NULL values.
HIVE_NULL = "__HIVE_DEFAULT_PARTITION__"


def csv_text(cols: list[tuple[str, str]]) -> str:
    """A projection of typed source columns to the text rivet's CSV writer documents for them (true/false, lower-case hex, hyphenated UUID); every other column as it is, compared by value."""
    def one(c: str, t: str) -> str:
        q = _qi(c)
        expr = {"BOOLEAN": f"CASE WHEN {q} THEN 'true' WHEN NOT {q} THEN 'false' END",
                "BLOB": f"lower(hex({q}))", "UUID": f"CAST({q} AS VARCHAR)"}.get(t, q)
        return f"{expr} AS {q}"

    return ", ".join(one(c, t) for c, t in cols) or "1"


def misfiled(ora, parts: list[str], col: str) -> list[str]:
    """A partition_by part whose `<col>=<label>` directory does not label its rows' values (the label is a prefix of the value's text; the NULL bucket holds NULLs only)."""
    label = f"regexp_extract(filename, {_lit('/' + re.escape(col) + '=([^/]+)/')}, 1)"
    n = ora.scalar(
        f"SELECT count(*) FROM (SELECT CAST({_qi(col)} AS VARCHAR) AS v, {label} AS b FROM "
        f"read_parquet({_plist(parts)}, filename = true, hive_partitioning = false, union_by_name = true)) "
        f"WHERE b = '' OR CASE WHEN b = {_lit(HIVE_NULL)} THEN v IS NOT NULL ELSE v IS NULL OR NOT starts_with(v, b) END"
    )
    return [f"PARTITION: {n} row(s) sit under a `{col}=` directory whose label does not match their value"] if n else []


def _meta_leg(ora, files: list[str], engine: str, snapshot: bool, fmt: str = "parquet") -> str:
    """One SELECT over `files` carrying `__op`, `__pos`, `__seq` and the change order `__ord` (a snapshot leg sorts first)."""
    rel = _ndjson(ora, files, engine) if fmt == "ndjson" else _parts(ora, files, fmt)
    have = {c for c, _ in _columns(ora, rel)}
    add = [] if "__op" in have else ["'snapshot' AS __op"]
    add += [] if "__pos" in have else ["'' AS __pos"]
    add += [] if "__seq" in have else ["-1::BIGINT AS __seq"]
    add.append("'' AS __ord" if snapshot else f"{pos_order(engine)} AS __ord")
    return f"SELECT *, {', '.join(add)} FROM {rel}"


def _image(rel: str) -> str:
    """`rel` with every column as text: the form a source image is kept in."""
    return f"(SELECT CAST(COLUMNS(*) AS VARCHAR) FROM {rel})"


def write_image(ora, rel: str, path: str) -> None:
    """Write `rel` as a source image at `path` (a temp file renamed, so a reader never sees half of it)."""
    tmp = path + ".tmp.parquet"
    ora.db.sql(f"COPY {_image(rel)} TO {_lit(tmp)} (FORMAT parquet)")
    os.replace(tmp, path)
    if os.path.exists(path + ".missing"):
        os.remove(path + ".missing")


def changes_between(ora, base: str | None, upper: str | None, key: list[str], first_owes_all: bool = False) -> str | None:
    """The key texts of the rows changed between the source images `base` and `upper`; `None` when either image is unknown (with `first_owes_all`, a missing `base` owes every row of `upper`)."""
    b, u = image_state(base), image_state(upper)
    b = b or ("absent" if first_owes_all else None)
    if not (b and u):
        return None
    if u == "absent":
        return f"(SELECT {', '.join(f'NULL::VARCHAR AS {_qi(k)}' for k in key)} LIMIT 0)"
    return changed_keys(ora, base if b == "image" else None, f"read_parquet({_lit(upper)})", key)


def image_state(path: str | None) -> str | None:
    """`image` when `path` holds a source image, `absent` when the table did not exist when it was taken, else `None`."""
    if path and os.path.isfile(path):
        return "image"
    if path and os.path.isfile(path + ".missing"):
        return "absent"
    return None


def changed_keys(ora, image: str | None, cur: str, key: list[str]) -> str:
    """The key texts of the rows of `cur` (a relation or a later image) inserted or changed since the source image at `image` (over the columns both hold); every row when `image` is `None` (no table then)."""
    kl = ", ".join(_qi(k) for k in key)
    if image is None:
        return f"(SELECT {kl} FROM {_image(cur)})"
    old = f"read_parquet({_lit(image)})"
    have = {c for c, _ in _columns(ora, old)}
    common = ", ".join(_qi(c) for c, _ in _columns(ora, cur) if c in have)
    return f"(SELECT {kl} FROM (SELECT {common} FROM {_image(cur)} EXCEPT SELECT {common} FROM {old}))"


def delta_window(spec: dict, out_dir: str, graded: list[str]) -> tuple[dict, str | None]:
    """(this stream's delta record, the exclusive lower cursor bound for `out_dir`): where the stream's previous graded run ended when this destination was first graded; `None` for a stream with no graded run (every row is owed)."""
    path = spec.get("cursor_record")
    record = {"high": None, "col": None, "dest": {}}
    if path and os.path.isfile(path):
        with open(path) as f:
            record = json.load(f)
    low, col = record["dest"].setdefault(out_dir, [record["high"], record["col"]])
    if spec.get("consumed"):
        # The load deletes what it staged (`cleanup_source`): a run's own parts are all that remain, so it owes what came after the previous graded run.
        low, col = record["high"], record["col"]
    # A bound on another cursor column says nothing about this one (a column switch needs a state reset).
    latest = [w for w in ((_load(out_dir, n).get("source") or {}).get("extraction") or {} for n in graded) if w.get("cursor_column")]
    return record, low if not latest or latest[-1]["cursor_column"] == col else None


def save_delta_window(spec: dict, record: dict, graded: list[str]) -> None:
    """Record the cursor_high of this destination's latest graded manifest as where the stream's next run starts."""
    path = spec.get("cursor_record")
    for name in reversed(in_run_order(spec["out_dir"], graded)):
        w = (_load(spec["out_dir"], name).get("source") or {}).get("extraction") or {}
        if w.get("cursor_column") and w.get("cursor_high") is not None:
            record.update(high=str(w["cursor_high"]), col=w["cursor_column"])
            break
    if path:
        with open(path, "w") as f:
            json.dump(record, f)


def _mssql_captured(spec: dict, key: list[str]) -> str:
    """SQL Server's own change table, per key its latest captured LSN: a run owes what the capture job had harvested before it opened, not what the base table holds."""
    schema, leaf = (spec.get("table") or "").split(".", 1) if "." in (spec.get("table") or "") else ("dbo", spec.get("table") or "")
    ci = spec.get("capture_instance") or f"{schema}_{leaf}"
    keys = ", ".join(f"[{k}]" for k in key)
    sql = f"SELECT {keys}, CONVERT(varchar(max), MAX(__$start_lsn), 2) AS __lsn FROM cdc.[{ci}_CT] GROUP BY {keys}"
    return f"mssql_scan('ms', {_lit(sql)})"


def take_image(spec: dict) -> dict:
    """Write the source image a CDC export without a snapshot leg is graded from: the captured table as it stands before a run opens its stream (and, when `spec["anchor"]` is set, the stream's anchor)."""
    from .duck import Oracle

    path = spec["image"]
    _, _, renders, config = _prep(spec["engine"], True)
    with Oracle(config=config, **_attach(spec)) as ora:
        try:
            src, key, _ = _source(ora, spec, renders)
            if spec["engine"] == "mssql" and key:
                src = _mssql_captured(spec, key)
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE source_rows AS SELECT * FROM {src}")
        except Exception as e:  # noqa: BLE001 — the captured table may not exist yet; every later row is then new
            if not absent(e):
                raise
            _mark_absent(path)
            return {"image": "absent", "anchor": record_anchor(ora, spec, path, [])}
        write_image(ora, "source_rows", path)
        return {"image": "image", "anchor": record_anchor(ora, spec, path, key)}


def _clear_image(base: str) -> None:
    """Remove the image at `base` in either form (parquet, or the absent-table marker)."""
    for p in (base, base + ".missing"):
        if os.path.exists(p):
            os.remove(p)


def _mark_absent(base: str) -> None:
    """Record at `base` that the table did not exist (nothing was there to owe)."""
    _clear_image(base)
    open(base + ".missing", "w").close()


def anchor_action(engine: str, held: bool | None, recorded: bool) -> str | None:
    """What the begin image says about the stream's anchor: `begin` (this run anchors at its open), `owe_all` (SQL Server reads its whole capture instance), `slot` (an existing PostgreSQL slot: this image plus the keys it holds), or `None` (keep what is recorded, or nothing is knowable)."""
    if held is None:
        return None
    if not held:
        return "owe_all" if engine == "mssql" else "begin"
    if recorded:
        return None
    return "slot" if engine == "postgres" else None


def record_anchor(ora, spec: dict, begin: str, key: list[str]) -> str | None:
    """Record the stream's anchor at `spec["anchor"]` per `anchor_action`: held is whether the slot (PostgreSQL) or the checkpoint file exists before this run."""
    import shutil

    anchor, keys = spec.get("anchor"), spec.get("anchor_keys")
    if not anchor:
        return None
    engine, slot, ckpt = spec["engine"], spec.get("slot"), spec.get("checkpoint")
    if engine == "postgres":
        held = slot_exists(ora, slot) if slot else None
    else:
        held = os.path.isfile(ckpt) if ckpt else None
    act = anchor_action(engine, held, image_state(anchor) is not None)
    if act is None:
        return None
    pending = None
    if act == "slot" and key and keys:
        try:
            pending = slot_keys(ora, slot, spec.get("table") or "", key)
        except Exception as e:  # noqa: BLE001 — a process holding the slot hides its pending changes: the anchor stays unknown
            if "is active for PID" not in str(e):
                raise
            return None
    if keys and os.path.exists(keys):
        os.remove(keys)
    if act == "owe_all" or image_state(begin) == "absent":
        _mark_absent(anchor)
    else:
        _clear_image(anchor)
        shutil.copyfile(begin, anchor)
    if pending is not None:
        write_keys(pending, key, keys)
    return act


def slot_exists(ora, slot: str) -> bool:
    """Whether PostgreSQL holds the replication slot `slot`."""
    sql = f"SELECT count(*) FROM pg_replication_slots WHERE slot_name = {_lit(slot)}"
    return bool(ora.scalar(f"SELECT * FROM postgres_query('pg', {_lit(sql)})"))


def slot_keys(ora, slot: str, table: str, key: list[str]) -> list[tuple[str, ...]]:
    """The key texts of `table`'s rows the slot holds changes for, from the server's own `test_decoding` text, peeked (never consumed)."""
    sql = f"SELECT n.nspname::text, c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = {_lit(table)}::regclass"
    rel = ora.rows(f"SELECT * FROM postgres_query('pg', {_lit(sql)})")[0]
    peek = f"SELECT data FROM pg_logical_slot_peek_changes({_lit(slot)}, NULL, NULL)"
    return test_decoding_keys([r[0] for r in ora.rows(f"SELECT * FROM postgres_query('pg', {_lit(peek)})")], tuple(rel), key)


_TD_IDENT = r'"(?:[^"]|"")*"|[^\s".:\[]+'
_TD_ROW = re.compile(rf"^table ({_TD_IDENT})\.({_TD_IDENT}): (?:INSERT|UPDATE|DELETE): (.*)$", re.S)
_TD_COL = re.compile(rf"(old-key:|new-tuple:)|({_TD_IDENT})\[[^\]]*(?:\[\])*\]:('(?:[^']|'')*'|\S+)")


def _td_unquote(s: str, q: str) -> str:
    """A `test_decoding` identifier (`"`) or literal (`'`) as its text."""
    return s[1:-1].replace(q + q, q) if s.startswith(q) else s


def test_decoding_keys(lines: list[str], rel: tuple[str, str], key: list[str]) -> list[tuple[str, ...]]:
    """Every key (as text) `test_decoding` change lines name for relation `(schema, name)`: an UPDATE's old key and new tuple both."""
    out: set[tuple[str, ...]] = set()
    for line in lines:
        m = _TD_ROW.match(line)
        if not m or (_td_unquote(m.group(1), '"'), _td_unquote(m.group(2), '"')) != rel:
            continue
        groups: list[dict] = [{}]
        for t in _TD_COL.finditer(m.group(3)):
            if t.group(1):
                groups.append({})
            else:
                groups[-1][_td_unquote(t.group(2), '"')] = t.group(3)
        for g in groups:
            vals = [g.get(k) for k in key]
            if all(v is not None and v != "null" for v in vals):
                out.add(tuple(_td_unquote(v, "'") for v in vals))
    return sorted(out)


def write_keys(keys: list[tuple[str, ...]], cols: list[str], path: str) -> None:
    """Write key texts as a parquet of VARCHAR `cols` (a temp file renamed)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    tmp = path + ".tmp.parquet"
    pq.write_table(pa.table({c: pa.array([k[i] for k in keys], pa.string()) for i, c in enumerate(cols)}), tmp)
    os.replace(tmp, path)


def _value_findings(
    ora, engine: str, fmt: str, native: dict, rows: dict, src: str, dst: str | None, key: list[str], partial: list[str]
) -> tuple[dict, dict, frozenset]:
    """The value leg every graded delivery shares: (`compare`'s findings of `src` against `dst`, the type-ledger row per column, the columns a known defect collapses to BOOLEAN); what it could not cover is appended to `partial`."""
    from .value_diff import mongo_document_columns

    if engine == "mongo":
        partial.append("Mongo: only `_id` is graded (the scanner's inferred schema shares nothing else with the document blob)")
        if dst:
            dst = mongo_document_columns(ora, src, dst)
    bits = frozenset(c for c, n in native.items() if n.startswith("BIT")) if engine == "mysql" else frozenset()
    numbers = frozenset(
        c for c, n in native.items() if (n.startswith(ORACLE_NUMERIC) if engine == "oracle" else catalog_duck(n))
    )
    row_of = {c: rows[n] for c, n in native.items() if n in rows}
    defects = {c: [x.strip("'") for x in r.get("defect_samples") or []] for c, r in row_of.items() if r.get("known_defect")}
    # Oracle: its source leg is the client-side `source` render; a DuckDB render (strftime: astronomical years) would grade a different calendar.
    # A CSV holds text: no DuckDB render applies to it; the source is rendered as the CSV writer documents its text.
    duck = {} if engine == "oracle" or fmt in ("csv", "ndjson") else {c: _render(r)["duck"] for c, r in row_of.items() if _render(r).get("duck")}
    canons = {c: _render(r)["canon"] for c, r in row_of.items() if _render(r).get("canon")}
    verbatim = frozenset(
        c for c, n in native.items()
        if (engine == "postgres" and n == "JSON") or (TEXT_NATIVE.match(n) and c not in duck and c not in canons and c not in numbers)
    )
    if fmt in ("csv", "ndjson"):
        src = f"(SELECT {csv_text(_columns(ora, 'source_rows'))} FROM {src})"
    f = compare(ora, src, dst, bits, numbers, defects, duck, canons, engine != "oracle", verbatim, key)
    delivered = {d: t for _, _, d, t in f.get("pairs", [])}
    collapse = frozenset(c for c in defects if defects[c] and delivered.get(c) == "BOOLEAN")
    return f, row_of, collapse


def stdout_ledger(ora, spec: dict, printed: int) -> list[str]:
    """A stdout run writes no manifest and no part: its whole ledger is the one `export_metrics` success row it recorded since it began, which must count the rows it printed."""
    if not spec.get("state"):
        return []
    got = [r[0] for r in ora.rows(
        f"SELECT total_rows FROM {_state_table(ora, spec, 'export_metrics')} WHERE export_name = {_lit(spec['export'])} "
        f"AND status = 'success' AND CAST(run_at AS TIMESTAMPTZ) >= CAST({_lit(spec['since'])} AS TIMESTAMPTZ)"
    )]
    return [] if got == [printed] else [
        f"COUNTER: export_metrics records total_rows {got} in the success row(s) of this run, its stdout holds {printed} row(s)"]


def grade_stdout(spec: dict) -> dict:
    """Grade one `destination: stdout` run: the bytes it printed (kept by the harness at `spec["stdout"]`) against the whole source, and its `export_metrics` row against the rows they hold."""
    from .duck import Oracle

    partial: list[str] = []
    engine, fmt, printed = spec["engine"], spec.get("format") or "parquet", spec["stdout"]
    rows, forms, renders, config = _prep(engine, False)
    kw = {"state": spec["state"]} if spec.get("state") else {}
    with Oracle(config=config, **kw, **_attach(spec)) as ora:
        ora.db.sql("SET TimeZone = 'UTC'")
        src, key, native = _source(ora, spec, renders)
        ora.db.sql(f"CREATE OR REPLACE TEMP TABLE source_rows AS SELECT * FROM {src}")
        key = spec.get("key") or key
        # No byte at all is a run that printed nothing: graded as an empty delivery, never as an unreadable file.
        dst = _parts(ora, [printed], fmt) if os.path.getsize(printed) else None
        n = ora.scalar(f"SELECT count(*) FROM {dst}") if dst else 0
        f, row_of, collapse = _value_findings(ora, engine, fmt, native, rows, "source_rows", dst, key, partial)
        extra = _text_form_mismatches([printed], row_of, forms) if dst and fmt == "parquet" else []
        extra += stdout_ledger(ora, spec, n)
    failures = grade_findings(f, rows, forms, native, engine != "mongo" and fmt == "parquet", spec.get("overrides") or {}, collapse)
    facts = {k: v for k, v in f.items() if k not in ("pairs", "dst_cols", "src_stats", "dst_stats")}
    out = {"failures": failures + extra, "notes": ["stdout: no manifest or part exists to compare"], "facts": facts, "key": key}
    if partial:
        out["partial"] = "; ".join(partial)
    return out


def grade(spec: dict) -> dict:
    """Grade one run: `{failures, notes, facts}` (plus `partial`: what a PASS did not cover)."""
    from .duck import Oracle

    notes: list[str] = []
    extra: list[str] = []
    partial: list[str] = []
    engine, cdc, cumulative = spec["engine"], spec["mode"] == "cdc", spec.get("cumulative", False)
    fmt = spec.get("format") or "parquet"
    out_dir, snap_dir = spec["out_dir"], spec["snapshot_dir"]
    low = None
    graded = in_run_order(out_dir, spec["manifests"])
    snaps = declared_parts(snap_dir, spec["snapshot_manifests"])
    others = spec.get("stream_dirs") or []
    snaps += [p for d in others for p in declared_parts(d["snapshot_dir"], d["snapshot_manifests"])]
    new_parts = declared_parts(out_dir, spec["new_manifests"]) + declared_parts(snap_dir, spec["new_snapshot_manifests"])
    run_ids, manifest_rows = manifest_facts(out_dir, spec["new_manifests"])
    ids, rows = manifest_facts(snap_dir, spec["new_snapshot_manifests"])
    run_ids, manifest_rows = run_ids + ids, manifest_rows + rows
    rows, forms, renders, config = _prep(engine, cdc)
    kw = {"state": spec["state"]} if spec.get("state") else {}
    with Oracle(config=config, **kw, **_attach(spec)) as ora:
        ora.db.sql("SET TimeZone = 'UTC'")
        changes = declared_parts(out_dir, graded) + [p for d in others for p in declared_parts(d["dir"], d["manifests"])] if cdc else []
        if fmt == "ndjson":
            # `rivet cdc` without `--output` prints its changes: every line this stream printed, kept by the harness.
            changes = [spec["ndjson"]] if os.path.isfile(spec["ndjson"]) and os.path.getsize(spec["ndjson"]) else []
        try:
            src, key, native = _source(ora, spec, renders)
            # One read of the source: each later DESCRIBE or scan would open fresh scanner connections (mongoc opened ~2k per test).
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE source_rows AS SELECT * FROM {src}")
        except Exception as e:  # noqa: BLE001 — a CDC pin run can precede the table it captures
            if not (cdc and not changes and not snaps and absent(e)):
                raise
            raise Unreachable(f"CDC: an empty stream over a table the source cannot read yet ({str(e)[:120]})") from e
        src = "source_rows"
        key = spec.get("key") or key
        dst: str | None = None
        if cdc:
            if not key:
                raise Unreachable("CDC on a relation with no primary key (and no census key): its values cannot be folded per row")
            legs = [_meta_leg(ora, changes, engine, False, fmt)] if changes else []
            legs += [_meta_leg(ora, snaps, engine, True, fmt)] if snaps else []
            kl = ", ".join(_qi(k) for k in key)
            if legs:
                ora.db.sql("CREATE OR REPLACE TEMP TABLE ev AS " + " UNION ALL BY NAME ".join(f"({x})" for x in legs))
                ora.db.sql(
                    "CREATE OR REPLACE TEMP TABLE dst AS SELECT * EXCLUDE (__op, __pos, __seq, __ord, __rn) FROM "
                    f"(SELECT *, row_number() OVER (PARTITION BY {kl} ORDER BY __ord DESC, __seq DESC) AS __rn FROM ev) "
                    "WHERE __rn = 1 AND __op <> 'delete'"
                )
                dst = "dst"
            # A stream owes the whole source when it delivered a snapshot leg, or declares one and this is its first run.
            if not snaps and not (spec.get("snapshot") and not image_state(spec.get("base"))):
                as_text = ", ".join(f"CAST({_qi(k)} AS VARCHAR) AS {_qi(k)}" for k in key)
                touched = [f"SELECT DISTINCT {as_text} FROM ev"] if legs else []
                # Without a baseline the stream must hold every row changed between its previous successful run's start (else its anchor) and this run's.
                changed = changes_between(ora, spec.get("base"), spec.get("upper"), key)
                if changed is None and not image_state(spec.get("base")):
                    changed = changes_between(ora, spec.get("anchor"), spec.get("upper"), key)
                    if changed and os.path.isfile(spec.get("anchor_keys") or ""):
                        touched.append(f"SELECT {kl} FROM read_parquet({_lit(spec['anchor_keys'])})")
                if changed:
                    touched.append(f"SELECT {kl} FROM {changed}")
                elif legs:
                    partial.append("the stream's first graded run (no source image before an earlier run): only the keys it touched are graded")
                else:
                    raise Unreachable("CDC: the stream's first run delivered nothing, and no earlier source image says what it owed")
                on = " AND ".join(f"CAST(s.{_qi(k)} AS VARCHAR) = e.{_qi(k)}" for k in key)
                src = f"(SELECT s.* FROM {src} s SEMI JOIN ({' UNION '.join(touched)}) e ON {on})"
                notes.append(f"no snapshot leg: graded the keys the stream touched{' and every row changed since its previous run' if changed else ''}")
        else:
            if cumulative:
                record, low = delta_window(spec, out_dir, graded)
                windows = [(_load(out_dir, n).get("source") or {}).get("extraction") or {} for n in in_run_order(out_dir, graded)]
                windows = [w for w in windows if w.get("cursor_column") and w.get("cursor_high") is not None]
                if low is not None:
                    # The cursor this destination's runs read by now (a mode switch may change it), else the recorded one.
                    col = windows[-1]["cursor_column"] if windows else record["col"]
                    c = spec["cursor_expr"] if col == "_rivet_coalesced_cursor" and spec.get("cursor_expr") else _qi(col)
                    ora.db.sql(f"CREATE OR REPLACE TEMP TABLE got AS SELECT *, 0 AS __mseq FROM {src} LIMIT 0")
                    # A row at or below the bound is owed only if delivered anyway: matched by key (a cursor's text differs per reader), else by cursor.
                    ident = f"concat_ws(chr(31), {', '.join(f'CAST({_qi(k)} AS VARCHAR)' for k in key)})" if key else f"CAST({c} AS VARCHAR)"
                    src = (f"(SELECT * FROM {src} WHERE ({c} IS NOT NULL AND {c} > {_lit(low)}) "
                           f"OR {ident} IN (SELECT {ident} FROM got))")
                if spec.get("settle") and windows:
                    # `settle:` holds young rows back by rivet's own clock: the upper edge is rivet's cursor_high, a named partial.
                    w = windows[-1]
                    c = spec["cursor_expr"] if w["cursor_column"] == "_rivet_coalesced_cursor" and spec.get("cursor_expr") else _qi(w["cursor_column"])
                    src = f"(SELECT * FROM {src} WHERE {c} <= {_lit(str(w['cursor_high']))})"
                    partial.append("`settle:` holds young rows back by rivet's clock: rows past rivet's cursor_high are not graded")
            legs = [
                f"SELECT *, {i} AS __mseq FROM {_parts(ora, ps, fmt)}"
                for i, ps in enumerate(declared_parts(out_dir, [m]) for m in graded) if ps
            ]
            if legs:
                ora.db.sql("CREATE OR REPLACE TEMP TABLE got AS " + " UNION ALL BY NAME ".join(f"({x})" for x in legs))
                if cumulative and key:
                    kl = ", ".join(_qi(k) for k in key)
                    # A delta cannot express a delete: a key an EARLIER run delivered (graded then) that the source no longer holds is not owed.
                    fresh = [i for i, m in enumerate(graded) if m in spec["new_manifests"]] or [-1]
                    on = " AND ".join(f"CAST(s.{_qi(k)} AS VARCHAR) = CAST(g.{_qi(k)} AS VARCHAR)" for k in key)
                    dst = (f"(SELECT * EXCLUDE (__mseq, __rn) FROM (SELECT *, row_number() OVER "
                           f"(PARTITION BY {kl} ORDER BY __mseq DESC) AS __rn FROM got) g WHERE __rn = 1 AND "
                           f"(__mseq IN ({', '.join(map(str, fresh))}) OR EXISTS (SELECT 1 FROM source_rows s WHERE {on})))")
                else:
                    dst = "(SELECT * EXCLUDE (__mseq) FROM got)"
        rc = spec.get("range_column")
        if spec.get("replay") and rc and rc in {c for c, _ in _columns(ora, "source_rows")}:
            # A sealed plan owes exactly the source rows inside the chunk ranges it carries (inclusive, as planned).
            within = " OR ".join(f"{_qi(rc)} BETWEEN {int(lo)} AND {int(hi)}" for lo, hi in spec["ranges"])
            src = f"(SELECT * FROM {src} WHERE {within})"
        elif (spec.get("resume") or spec.get("replay")) and not cumulative and dst and key:
            # A resume (or a sealed plan's ranges) completes a plan made before this invocation; the source may have moved since.
            kl = ", ".join(_qi(k) for k in key)
            on = " AND ".join(f"CAST(s.{_qi(k)} AS VARCHAR) = CAST(e.{_qi(k)} AS VARCHAR)" for k in key)
            src = f"(SELECT s.* FROM {src} s SEMI JOIN (SELECT DISTINCT {kl} FROM got) e ON {on})"
            partial.append("a `--resume` run completes a plan made before it: the delivered rows are graded, completeness against that plan is not"
                           if spec.get("resume") else
                           "a sealed plan replays the chunk ranges it was planned with: the delivered rows are graded, completeness against the live source is not")
        f, row_of, collapse = _value_findings(ora, engine, fmt, native, rows, src, dst, key, partial)
        if spec.get("nothing_new") and not cumulative:
            if spec.get("resume"):
                raise Unreachable("a `--resume` run wrote no new manifest: it skipped an export a prior run completed")
            if f["src_count"] == 0:
                raise Unreachable("the run wrote no new Success manifest, and the source holds no row")
            extra.append(f"DELIVERY: the run exited 0 and wrote no new Success manifest while the source holds {f['src_count']} row(s)")
        first = (_load(out_dir, graded[0]).get("source") or {}).get("extraction") or {} if graded and cumulative and not cdc else {}
        if first.get("cursor_low") is not None and f["only_src"] and low is None:
            extra.append(f"DELTA: the first run in this destination resumed from cursor_low {first['cursor_low']!r}, a cursor no graded run of this stream ended at")
        if cumulative and not cdc:
            save_delta_window(spec, record, graded)
        f["text_form"] = _text_form_mismatches(new_parts, row_of, forms) if fmt == "parquet" else []
        if spec.get("partition_by") and new_parts:
            extra += misfiled(ora, new_parts, spec["partition_by"])
        part_rows = ora.scalar(f"SELECT count(*) FROM {_parts(ora, new_parts, fmt)}") if new_parts else 0
        f.update(part_rows=part_rows, manifest_rows=manifest_rows, **_counters(ora, spec, new_parts, run_ids))
    failures = grade_findings(f, rows, forms, native, engine != "mongo" and fmt == "parquet", spec.get("overrides") or {}, collapse)
    failures += (f.get("text_form") or []) + extra
    facts = {k: v for k, v in f.items() if k not in ("pairs", "dst_cols", "src_stats", "dst_stats")}
    out = {"failures": failures, "notes": notes, "facts": facts, "key": key}
    if partial:
        out["partial"] = "; ".join(partial)
    return out


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


def load_run_filter(spec: dict) -> str:
    """This export's `load_run` rows: rivet keys them by the table it loads (the source table, else the export
    name, a schema dot folded to `_`) and by `<dataset>.<table>`, never by the export name."""
    load = spec["load"]
    leaf = str(spec.get("table") or spec["export"]).replace(".", "_")
    # BigQuery writes `<project>.<dataset>.<table>`, ClickHouse `<database>.<table>`.
    fq = f"{load.get('database') or 'default'}.{leaf}" if load.get("target") == "clickhouse" else f".{load.get('dataset')}.{leaf}"
    return f"export_name = {_lit(leaf)} AND ends_with(target_table, {_lit(fq)})"


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
        kw.update(bigquery=True, bq_project=str(load.get("project") or ""), bq_dataset=str(load.get("dataset") or ""))
    notes: list[str] = []
    partial: list[str] = []
    try:
        ora = Oracle(config=config, **kw, **_attach(spec))
    except Exception as e:  # noqa: BLE001 — an absent warehouse credential is a named skip, never a pass
        if target == "bigquery" and any(k in str(e).lower() for k in ("credential", "permission", "unauthenticated", "default credentials")):
            return {"skip": f"BigQuery unreachable ({str(e)[:160]}): set BIGQUERY_TEST_PROJECT, RIVET_TEST_GCS_BUCKET and gcloud ADC"}
        raise
    with ora:
        ora.db.sql("SET TimeZone = 'UTC'")
        mine = load_run_filter(spec)
        loaded = ora.rows(
            f"SELECT target_table FROM {_state_table(ora, spec, 'load_run')} "
            f"WHERE {mine} AND status = 'success' ORDER BY finished_at DESC LIMIT 1"
        )
        if not loaded:
            return {"failures": [f"WAREHOUSE: no successful load_run row for export `{spec['export']}` ({mine})"], "notes": notes}
        fq = loaded[0][0]
        leaf = fq.split(".")[-1]
        src, key, native = _source(ora, spec, renders)
        key = spec.get("key") or key
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
            if leaf not in tables and not ora.scalar(
                f"SELECT count(*) FROM {_state_table(ora, spec, 'load_run')} "
                f"WHERE {mine} AND status = 'success' AND rows_loaded > 0"
            ):
                # Every success row is a skip ("up to date", 0 rows): nothing was ever loaded, so there is no table to grade.
                return {"skip": f"no load has written `{fq}` yet (every load row for `{spec['export']}` loaded 0 rows)", "notes": notes}
            rel = f"bq.{ds}.{leaf}" if leaf in tables else f"bigquery_query('bq', {_lit(f'SELECT * FROM `{fq}`')})"
            wh_types = {}
            buffered = f"{leaf}__changes" in tables
            if buffered:
                # `<table>__changes` beside a VIEW is the changelog+view layout (the view is the current state);
                # beside a base TABLE (or no base yet) it is base_buffer's uncompacted buffer.
                proj, ds_ = fq.split(".")[:2]
                sql = f"SELECT table_type FROM `{proj}.{ds_}`.INFORMATION_SCHEMA.TABLES WHERE table_name = '{leaf}'"
                kind = [r[0] for r in ora.rows(f"SELECT * FROM bigquery_query('bq', {_lit(sql)})")]
                buffered = kind != ["VIEW"]
                base = bool(kind)
                if buffered and not base:
                    return {"skip": f"`{fq}__changes` holds a base_buffer buffer beside no base table: a base dropped from under the stream cannot be told from rows never loaded"}
                if not buffered:
                    rel = f"bigquery_query('bq', {_lit(f'SELECT * FROM `{fq}`')})"  # a view has no storage-API read
        if not wh_types and target == "clickhouse":
            return {"failures": [f"WAREHOUSE: `{fq}` does not exist in ClickHouse database `{db}`"], "notes": notes}
        if buffered and spec.get("verb") == "compact":
            return {"failures": [f"WAREHOUSE bigquery `{fq}`: `rivet compact` exited 0 and left `{leaf}__changes` behind"], "notes": notes}
        if buffered and not key:
            return {"skip": f"`{fq}__changes` holds an uncompacted buffer and the relation names no key to fold it into the base by"}
        try:
            if buffered:
                # base ∪ buffer, folded per key below the way `rivet compact` merges them: a buffer row beats the base.
                legs = ([f"(SELECT *, 0 AS _rivet_src FROM {rel})"] if base else []) + [f"(SELECT *, 1 AS _rivet_src FROM bq.{ds}.{leaf}__changes)"]
                ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh_all AS {' UNION ALL BY NAME '.join(legs)}")
                partial.append("base_buffer before `rivet compact`: graded base ∪ buffer folded per key, not the merge compact performs")
            else:
                ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh_all AS SELECT * FROM {rel}")
        except Exception as e:  # noqa: BLE001 — a table that requires a partition filter is read with an all-partitions one
            m = re.search(r"filter over column\(s\) '([^']+)'", str(e))
            if storage_schema_lags(e) and not buffered:
                # The Storage API still serves the schema before an ALTER; a query job reads the table as it is now.
                ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh_all AS SELECT * FROM bigquery_query('bq', {_lit(f'SELECT * FROM `{fq}`')})")
                m = None
            elif not m or buffered:
                raise
            if m:
                c = m.group(1)
                sql = f"SELECT * FROM `{fq}` WHERE `{c}` IS NULL OR `{c}` >= TIMESTAMP('0001-01-01')"
                ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh_all AS SELECT * FROM bigquery_query('bq', {_lit(sql)})")
        have = [c for c, _ in _columns(ora, "wh_all")]
        keep = ", ".join(_qi(c) for c in have if not c.startswith(WAREHOUSE_META)) or "1"
        if ("__pos" in have or "_rivet_src" in have) and key:
            # A change log (or base ∪ buffer): its live state is the latest image per key, deletes removed.
            kl = ", ".join(_qi(k) for k in key)
            seq = ", __seq DESC" if "__seq" in have else ""
            # A buffer row beats the base, and a change beats a snapshot row (NULL `__pos`), as rivet's own current-state view orders them.
            order = ", ".join(([ "_rivet_src DESC"] if "_rivet_src" in have else [])
                              + ([f"__pos IS NOT NULL DESC, {pos_order(engine)} DESC"] if "__pos" in have else [])) + seq
            gone = " AND NOT coalesce(__is_deleted, false)" if "__is_deleted" in have else ""
            live_op = " AND coalesce(__op, '') <> 'delete'" if "__op" in have else ""
            folded = (f"(SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY {kl} ORDER BY {order}) AS __rn "
                      f"FROM wh_all) WHERE __rn = 1{live_op}{gone})")
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh AS SELECT {keep} FROM {folded}")
            notes.append("change-log layout: graded at the latest image per key")
        else:
            live = "WHERE NOT __is_deleted" if "__is_deleted" in have else ""
            ora.db.sql(f"CREATE OR REPLACE TEMP TABLE wh AS SELECT {keep} FROM wh_all {live}")
        if spec.get("delta") and not cdc and key:
            # A delta load cannot express a delete: a key the source no longer holds stays in the warehouse by design.
            on = key_match(ora, "source_rows", "s", "wh", "w", key)
            stale = ora.scalar(f"SELECT count(*) FROM wh w WHERE NOT EXISTS (SELECT 1 FROM source_rows s WHERE {on})")
            if stale:
                ora.db.sql(f"DELETE FROM wh w WHERE NOT EXISTS (SELECT 1 FROM source_rows s WHERE {on})")
                partial.append(f"{stale} warehouse key(s) the source no longer holds are not graded: a delta load cannot express a delete")
        if (cdc or "__pos" in have) and key:
            on = key_match(ora, "source_rows", "s", "wh_all", "w", key)
            if not spec.get("snapshot"):
                # The rows the load must hold: every row changed between the stream's first and latest successful runs, plus the keys it holds.
                kl = ", ".join(_qi(k) for k in key)
                as_text = ", ".join(f"CAST({_qi(k)} AS VARCHAR) AS {_qi(k)}" for k in key)
                held = [f"SELECT DISTINCT {as_text} FROM wh_all"]
                state = changes_between(ora, spec.get("base"), spec.get("upper"), key)
                if state:
                    held.append(f"SELECT {kl} FROM {state}")
                    if os.path.isfile(spec.get("anchor_keys") or ""):
                        held.append(f"SELECT {kl} FROM read_parquet({_lit(spec['anchor_keys'])})")
                else:
                    partial.append("CDC load without a snapshot leg and no source images of its stream: only the keys the warehouse holds are graded")
                ek = key_match(ora, "source_rows", "s", "wh_all", "e", key)
                src = f"(SELECT s.* FROM {src} s SEMI JOIN ({' UNION '.join(held)}) e ON {ek})"
                notes.append(f"no snapshot leg: graded the keys the warehouse holds{' and every row the stream owed' if state else ''}")
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
        verbatim = frozenset(
            c for c, n in native.items()
            if (engine == "postgres" and n == "JSON") or (TEXT_NATIVE.match(n) and c not in duck and c not in canons and c not in numbers)
        )
        f = compare(ora, src, "wh", bits, numbers, defects, duck, canons, engine != "oracle", verbatim, key)
        if target == "bigquery" and any(st == "TIMESTAMP WITH TIME ZONE" and dt == "TIMESTAMP" for _, st, _, dt in f["pairs"]):
            # DuckDB's BigQuery reader types TIMESTAMP and DATETIME alike; BigQuery's own catalog tells them apart.
            proj, ds_, tbl = fq.split(".")
            sql = f"SELECT column_name, data_type FROM `{proj}.{ds_}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{tbl}'"
            try:
                info = ora.rows(f"SELECT * FROM bigquery_query('bq', {_lit(sql)})")
            except Exception:  # noqa: BLE001 — one retry: a BigQuery query job can time out transiently
                info = ora.rows(f"SELECT * FROM bigquery_query('bq', {_lit(sql)})")
            zoned = {n for n, t in info if t == "TIMESTAMP"}
            f["pairs"] = [(s_, st, d, "TIMESTAMP WITH TIME ZONE" if d in zoned and dt == "TIMESTAMP" else dt) for s_, st, d, dt in f["pairs"]]
        delivered = {d: t for _, _, d, t in f.get("pairs", [])}
        collapse = frozenset(c for c in defects if defects[c] and delivered.get(c) == "BOOLEAN")
    # The ClickHouse type the ledger names is graded below; the not-narrower rule covers the rest.
    ledgered = frozenset(c for c, r in ch_of.items() if r.get("clickhouse"))
    null_class = frozenset(c for c, xs in defects.items() if None in xs)
    bad = grade_findings(f, {}, forms, native, engine != "mongo", dict.fromkeys([*(spec.get("overrides") or {}), *ledgered]),
                         collapse=collapse, null_class=null_class)
    for col, r in ch_of.items():
        want, got = r.get("clickhouse"), wh_types.get(col)
        if target != "clickhouse" or not want or got is None:
            continue
        # A marked row (known_defect, or a ClickHouse defect) excuses only the type it declares today.
        marked = r.get("clickhouse_defect") or r.get("known_defect") or row_of.get(col, {}).get("known_defect")
        allowed = {want, r.get("today_clickhouse")} if marked else {want}
        # A key column cannot be Nullable in ClickHouse.
        if not any(got == w or (col in key and w == f"Nullable({got})") for w in allowed if w):
            bad.append(f"TYPE: `{col}` ({native[col]}): the ledger loads ClickHouse {sorted(w for w in allowed if w)}, the table has {got}")
    bad += [f"WAREHOUSE: {n}" for n in notes if "still exist" in n]
    out = {"failures": [f"WAREHOUSE {target} `{fq}`: {b}" for b in bad], "notes": notes,
           "facts": {k: v for k, v in f.items() if k not in ("pairs", "dst_cols", "src_stats", "dst_stats")}}
    if partial:
        out["partial"] = "; ".join(partial)
    return out


def storage_schema_lags(e: BaseException) -> bool:
    """True when a Storage API read session names a column the table gained after the session's schema snapshot."""
    return "read session" in str(e) and "do not exist in the table schema" in str(e)


def transient(e: BaseException) -> bool:
    """True for a failure that says nothing about the data: a deadlock victim or a dropped transport."""
    from .duck import transient as transport_down

    return "deadlock" in str(e) or transport_down(e)


def _self_test() -> None:
    bq = {"target": "bigquery", "project": "p", "dataset": "d"}
    assert load_run_filter({"export": "users", "table": "public.users_pg", "load": bq}) == \
        "export_name = 'public_users_pg' AND ends_with(target_table, '.d.public_users_pg')"
    assert "'q'" in load_run_filter({"export": "q", "table": None, "load": bq}), "a query export is keyed by its name"
    assert "'db.t'" in load_run_filter({"export": "x", "table": "t", "load": {"target": "clickhouse", "database": "db"}})
    assert transient(RuntimeError("PerformWork() - CURL error [35]=SSL connect error"))
    assert transient(RuntimeError("PerformWork() - CURL error [28]=Timeout was reached"))
    assert transient(RuntimeError("Transaction (Process ID 61) was deadlocked ... chosen as the deadlock victim"))
    assert transient(RuntimeError("BigQuery Authentication Failed\n\nUnderlying authentication error:\n  PerformWork() - CURL error [28]=Timeout was reached"))
    assert not transient(RuntimeError("BigQuery Authentication Failed\n\nNo usable authentication credentials were found."))
    assert storage_schema_lags(RuntimeError("Binder Error: Error while creating read session: Permanent error, with a last "
                                            "message of request failed: The following selected fields do not exist in the table schema: w"))
    assert not storage_schema_lags(RuntimeError("Binder Error: Referenced column \"w\" not found"))
    assert not transient(RuntimeError("CURL error [22]=HTTP response code said error"))
    cancelled = RuntimeError("Binder Error: Error while creating read session: Permanent error, with a last message of CANCELLED")
    assert transient(cancelled) and not storage_schema_lags(cancelled), "a dropped BigQuery read session is transport"
    assert not transient(RuntimeError("Binder Error: Error while creating read session: Permanent error, with a last "
                                      "message of request failed: The following selected fields do not exist in the table schema: w"))
    assert not transient(RuntimeError('Binder Error: Referenced column "CANCELLED" not found'))
    import duckdb

    from . import duck

    cancelled = duckdb.BinderException("Error while creating read session: Permanent error, with a last message of CANCELLED")
    calls, slept = [], []

    def dropped():
        calls.append(1)
        raise cancelled

    try:
        duck.retry(dropped, sleep=slept.append)
    except duck.OracleUnavailable as e:
        assert "CANCELLED" in str(e), e
    else:
        raise AssertionError("a read session dropped on every try must end as OracleUnavailable")
    assert len(calls) == duck.READ_TRIES and len(slept) == duck.READ_TRIES - 1, (calls, slept)
    calls.clear()

    def dropped_once():
        calls.append(1)
        if len(calls) == 1:
            raise cancelled
        return "rows"

    assert duck.retry(dropped_once, sleep=slept.append) == "rows" and len(calls) == 2, calls
    now = iter([0.0, 176.0])
    calls.clear()
    try:
        duck.retry(dropped, clock=lambda: next(now), sleep=slept.append)
    except duck.OracleUnavailable:
        assert len(calls) == 1, "past the budget the oracle must give up, not wait for another try"
    wrong = duckdb.BinderException('Referenced column "w" not found')
    try:
        duck.retry(lambda: (_ for _ in ()).throw(wrong), sleep=slept.append)
    except duckdb.BinderException:
        pass
    else:
        raise AssertionError("a wrong-data error must surface as itself, never as an infrastructure verdict")

    assert not transient(RuntimeError("CURL error [356]=x")), "a code must match whole"
    assert not transient(RuntimeError("Binder Error: Referenced column \"id\" not found"))
    assert type_loss("INTEGER", "BIGINT") is None
    assert type_loss("BIGINT", "INTEGER")
    assert type_loss("UBIGINT", "BIGINT")
    assert type_loss("UINTEGER", "BIGINT") is None
    assert type_loss("DECIMAL(38,10)", "DOUBLE")
    assert type_loss("DECIMAL(10,2)", "DECIMAL(12,2)") is None
    assert type_loss("DECIMAL(10,4)", "DECIMAL(10,2)")
    assert type_loss("TIMESTAMP", "DATE")
    assert type_loss("TIMESTAMP_NS", "TIMESTAMP")
    assert type_loss("TIMESTAMP WITH TIME ZONE", "TIMESTAMP"), "a zoned timestamp delivered naive is a loss"
    assert type_loss("VARCHAR", "BIGINT"), "text delivered as a number is a loss ('02134' is not 2134)"
    assert type_loss("VARCHAR", "JSON") is None
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
    rows = {"NUMERIC": {"delivery": "decimal_plain"}, "BIGINT": {"delivery": "Int64", "known_defect": "x", "today_delivery": "Int32"}}
    assert any("type ledger delivers decimal_plain" in b for b in grade_findings(led, rows, {"decimal_plain"}, {"n": "NUMERIC"}, True))
    xfail = {**clean, "pairs": [["b", "BIGINT", "b", "INTEGER"]]}
    assert not grade_findings(xfail, rows, set(), {"b": "BIGINT"}, True), "a known_defect excuses its today_delivery"
    assert grade_findings({**xfail, "pairs": [["b", "BIGINT", "b", "SMALLINT"]]}, rows, set(), {"b": "BIGINT"}, True), \
        "a known_defect excuses only its declared type, never a third one"
    one = lambda st, dt, nat, **kw: grade_findings({**clean, "pairs": [["c", st, "c", dt]]}, {}, set(), {"c": nat}, True, **kw)  # noqa: E731
    assert one("DECIMAL(20,2)", "DOUBLE", "NUMERIC(20,2)", overrides={"c": "decimal(20,2)"}), "a `columns:` override is graded against its type"
    assert not one("DECIMAL(20,2)", "DECIMAL(20,2)", "NUMERIC(20,2)", overrides={"c": "decimal(20, 2)"})
    assert not one("DECIMAL(20,2)", "DOUBLE", "NUMERIC(20,2)", overrides={"c": None}), "a load names its overrides, not their parquet types"
    assert one("VARCHAR", "DOUBLE", "NUMERIC(50,2)"), "a numeric read as text is graded from its catalog type"
    assert one("VARCHAR", "DOUBLE", "NUMBER"), "an unbounded NUMBER delivered as a double is a loss"
    assert one("VARCHAR", "BIGINT", "VARCHAR(10)"), "a text column delivered as a number is a loss"
    assert not one("VARCHAR", "TIMESTAMP", "DATETIME2"), "a temporal the oracle renders as text is the ledger's to grade"
    assert one("TIMESTAMP WITH TIME ZONE", "TIMESTAMP", "TIMESTAMPTZ(3)")
    phantom = ["COUNT(*): source 1, delivered 2", "COUNT(`id`) (non-null): source 1, delivered 2",
               "VALUES: 0 source row(s) not delivered, 1 delivered row(s) not in the source; differing column(s) []"]
    assert known_defect_covers("delivered-only rows", phantom)
    assert not known_defect_covers("delivered-only rows", [*phantom, "COUNT(*): source 2, delivered 1"]), "a lost row is another class"
    assert not known_defect_covers("delivered-only rows", [*phantom, "TYPE: `v` source BIGINT delivered as INTEGER: x"])
    assert not known_defect_covers("delivered-only rows", ["VALUES: 1 source row(s) not delivered, 1 delivered row(s) not in the source"])
    assert not known_defect_covers("delivered-only rows", []), "no disagreement is not the defect"
    lost = ["COUNT(*): source 4, delivered 2", "COUNT(DISTINCT `id`): source 4, delivered 2",
            "VALUES: 2 source row(s) not delivered, 0 delivered row(s) not in the source; differing column(s) []"]
    assert known_defect_covers("undelivered rows", lost)
    assert not known_defect_covers("undelivered rows", phantom), "a phantom row is another class"
    assert not known_defect_covers("undelivered rows", ["VALUES: 2 source row(s) not delivered, 1 delivered row(s) not in the source"])
    assert not known_defect_covers("undelivered rows", []), "no disagreement is not the defect"
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
    _anchor_self_test()
    _compare_self_test()
    _layout_self_test()
    _ndjson_self_test()
    _stdout_self_test()
    print("rig_oracle self-test ok")


class _Mem:
    """An in-memory DuckDB session with the `duck.Oracle` calls `compare` uses."""

    def __init__(self) -> None:
        import duckdb

        self.db = duckdb.connect()

    def rows(self, sql: str) -> list:
        """Every row of `sql`."""
        return self.db.sql(sql).fetchall()

    def scalar(self, sql: str) -> object:
        """The first cell of `sql`."""
        return self.rows(sql)[0][0]


def _stdout_self_test() -> None:
    """A stdout run's ledger is the success row it recorded since it began: an older row, a failed one, a wrong count or a second row is a finding."""
    ora = _Mem()
    ora.db.sql("ATTACH ':memory:' AS st")
    ora.db.sql("CREATE TABLE st.export_metrics (export_name VARCHAR, run_at VARCHAR, total_rows BIGINT, status VARCHAR)")
    ora.db.sql("INSERT INTO st.export_metrics VALUES ('e', '2026-10-07T01:00:00.5+00:00', 50, 'success'), "
               "('e', '2026-10-07T02:00:00.123456+00:00', 7, 'failed'), ('other', '2026-10-07T02:00:00+00:00', 50, 'success')")
    spec = {"state": "state.db", "export": "e", "since": "2026-10-07T01:00:00Z"}
    assert stdout_ledger(ora, spec, 50) == [], stdout_ledger(ora, spec, 50)
    assert stdout_ledger(ora, {**spec, "state": None}, 3) == [], "no state DB is the caller's PARTIAL, not a finding here"
    assert "total_rows [50]" in stdout_ledger(ora, spec, 49)[0] and "holds 49 row(s)" in stdout_ledger(ora, spec, 49)[0]
    assert "total_rows []" in stdout_ledger(ora, {**spec, "since": "2026-10-07T01:00:01Z"}, 50)[0], "a row from before this run is not its ledger"
    ora.db.sql("INSERT INTO st.export_metrics VALUES ('e', '2026-10-07T03:00:00+00:00', 50, 'success')")
    assert "total_rows [50, 50]" in stdout_ledger(ora, spec, 50)[0], "one run records one success row"


def _ndjson_self_test() -> None:
    """A `rivet cdc` NDJSON line reads back by the source's column names: a delete's key from its before image, its position as text."""
    import tempfile

    ora = _Mem()
    ora.db.sql("CREATE TABLE source_rows (id BIGINT, v VARCHAR)")
    path = os.path.join(tempfile.mkdtemp(prefix="rig-ndjson-"), "e.jsonl")
    with open(path, "w") as f:
        f.write('{"op":"insert","table":"t","before":null,"after":[1,"a"],"pos":{"lsn":"0/10"},"seq":0}\n'
                '{"op":"delete","table":"t","before":[2,null],"after":null,"pos":{"lsn":"0/20"},"seq":1}\n')
    got = ora.rows(f"SELECT id, v, __op, json_extract_string(__pos, '$.lsn'), __seq FROM {_ndjson(ora, [path], 'postgres')} ORDER BY __seq")
    assert got == [("1", "a", "insert", "0/10", 0), ("2", None, "delete", "0/20", 1)], got
    assert [c for c, _ in _columns(ora, _ndjson(ora, [path], "mongo"))][:2] == ["_id", "document"]


def _compare_self_test() -> None:
    """`compare` and the anchor image, end to end over in-memory relations."""
    import tempfile

    def diff(src: str, dst: str, **kw) -> tuple[int, int]:
        ora = _Mem()
        ora.db.sql(f"CREATE TABLE a AS {src}")
        ora.db.sql(f"CREATE TABLE b AS {dst}")
        f = compare(ora, "a", "b", **kw)
        return f["only_src"], f["only_dst"]

    j = "SELECT 1 AS id, {!r}::VARCHAR AS j"
    assert diff(j.format('{"x": 3.141592653589793238462}'), j.format('{"x":3.141592653589793}')) == (1, 1), \
        "a JSON number rounded through a double is a difference"
    assert diff(j.format('{"a":1,"a":2}'), j.format('{"a":2}')) == (1, 1), "a collapsed duplicate JSON key is a difference"
    assert diff(j.format('{"b": 1, "a": [1, 2.50]}'), j.format('{"a":[1,2.5],"b":1}')) == (0, 0), "key order and number spelling are not"
    t = "SELECT {!r}::VARCHAR AS t"
    for a, b in ((" 2035-08-07 09:08:07 ", "2035-08-07T09:08:07Z"), ("1:02:03", "001:02:03"), ('  {"a":1}', '{"a":1}'), ("PT", "PT0S")):
        assert diff(t.format(a), t.format(b), verbatim=frozenset({"t"})) == (1, 1), f"text {a!r} delivered as {b!r} is a difference"
    assert diff(t.format("1:2:3.4.5"), t.format("PT1..S")) == (1, 1), "malformed interval text compares as text, never raises"
    assert diff("SELECT '02134'::VARCHAR AS z", "SELECT 2134::BIGINT AS z") == (1, 1), "text '02134' is not the number 2134"
    assert diff("SELECT 2134::BIGINT AS z", "SELECT '2134'::VARCHAR AS z") == (0, 0), "a number delivered as its text is the same value"
    assert diff("SELECT -0.0::DOUBLE AS f", "SELECT 0.0::DOUBLE AS f") == (1, 1), "a lost float sign is a difference"
    assert diff("SELECT 1234567890123456.78::DECIMAL(20,2) AS m", "SELECT 1234567890123456.78::DOUBLE AS m") == (1, 1), \
        "a decimal delivered through a double loses its cents"
    assert diff("SELECT 0.10::DECIMAL(10,2) AS m", "SELECT 0.1::DOUBLE AS m") == (0, 0)
    flags = "SELECT * FROM (VALUES (1, {}), (2, {}), (3, {})) v(id, b)"
    assert diff(flags.format(1, 0, 5), flags.format(0, 1, 1), defects={"b": ["5"]}, key=["id"]) == (2, 0), \
        "a swap between rows is not excused by a sample on a third row"
    assert diff(flags.format(1, 0, 5), flags.format(1, 0, 1), defects={"b": ["5"]}, key=["id"]) == (0, 0), "a declared sample may differ"
    assert diff(flags.format(1, 0, 5), flags.format(1, 7, 1), defects={"b": ["5"]}) != (0, 0), \
        "without a key, a delivered value no sample explains is still a difference"

    ora = _Mem()
    ora.db.sql("CREATE TABLE s AS SELECT '1' AS \"ID\", '7' AS k")
    ora.db.sql("CREATE TABLE w AS SELECT 1::DECIMAL(38,9) AS \"ID\", '7' AS k UNION ALL SELECT 2, '7'")
    held = f"SELECT count(*) FROM w WHERE EXISTS (SELECT 1 FROM s WHERE {key_match(ora, 's', 's', 'w', 'w', ['ID', 'k'])})"
    assert ora.scalar(held) == 1, "a NUMBER key read from BigQuery as DECIMAL(38,9) is the source's key, by value"
    ora = _Mem()
    ora.db.sql("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) v(id, v)")
    with tempfile.TemporaryDirectory() as d:
        img = os.path.join(d, "anchor.parquet")
        write_image(ora, "t", img)
        assert image_state(img) == "image" and image_state(os.path.join(d, "none.parquet")) is None
        assert changes_between(ora, None, img, ["id"]) is None, "a first run with no earlier image owes nothing knowable"
        rec = os.path.join(d, "cursor.json")
        a, b = os.path.join(d, "a"), os.path.join(d, "b")
        for out, high in ((a, "40"), (b, "50")):
            os.makedirs(out)
            with open(os.path.join(out, "m.json"), "w") as fh:
                json.dump({"source": {"extraction": {"cursor_column": "id", "cursor_low": "999", "cursor_high": high}}}, fh)
        spec = {"cursor_record": rec, "out_dir": a}
        record, low = delta_window(spec, a, ["m.json"])
        assert low is None, "a stream's first destination owes every row, whatever cursor_low rivet wrote"
        save_delta_window(spec, record, ["m.json"])
        assert delta_window({**spec, "out_dir": b}, b, ["m.json"])[1] == "40", "a later destination starts where the stream's last graded run ended"
        assert delta_window(spec, a, ["m.json"])[1] is None, "a destination keeps the bound it was first graded with"
        owed = changes_between(ora, None, img, ["id"], first_owes_all=True)
        assert sorted(r[0] for r in ora.rows(f"SELECT id FROM {owed}")) == ["1", "2", "3"], "an engine anchored server-side owes all"
        ora.db.sql("UPDATE t SET v = 'B' WHERE id = 2; INSERT INTO t VALUES (4, 'd'); DELETE FROM t WHERE id = 3")
        got = sorted(r[0] for r in ora.rows(f"SELECT id FROM {changed_keys(ora, img, 't', ['id'])}"))
        assert got == ["2", "4"], f"rows changed since the anchor are the inserted and updated ones, got {got}"
        ora.db.sql("ALTER TABLE t ADD COLUMN w INTEGER")
        got = sorted(r[0] for r in ora.rows(f"SELECT id FROM {changed_keys(ora, img, 't', ['id'])}"))
        assert got == ["2", "4"], f"an added column alone changes no row, got {got}"
        assert sorted(r[0] for r in ora.rows(f"SELECT id FROM {changed_keys(ora, None, 't', ['id'])}")) == ["1", "2", "4"], \
            "with no table at the anchor every row is new"


def _anchor_self_test() -> None:
    """Where a stream anchors, and the keys a PostgreSQL slot's pending changes name."""
    import tempfile

    for engine in ("mysql", "oracle", "mongo", "postgres"):
        assert anchor_action(engine, False, True) == "begin", f"{engine}: no slot/checkpoint yet, so this run anchors at its open"
    assert anchor_action("mssql", False, False) == "owe_all", "SQL Server with no checkpoint reads its whole capture instance"
    assert anchor_action("postgres", True, False) == "slot", "a slot found existing anchored before this run"
    assert anchor_action("postgres", True, True) is None and anchor_action("mysql", True, True) is None, "a recorded anchor is kept"
    assert anchor_action("mysql", True, False) is None, "a checkpoint no seen run wrote: the anchor is unknowable"
    assert anchor_action("mssql", None, False) is None, "no slot or checkpoint configured: nothing to say"
    lines = [
        "BEGIN 1",
        "table public.t: INSERT: id[bigint]:1 \"Order\"[text]:'a'' id[bigint]:9' v[integer]:1",
        "table public.t: UPDATE: old-key: id[bigint]:2 new-tuple: id[bigint]:3 \"Order\"[text]:null v[integer[]]:'{1}'",
        "table public.t: DELETE: id[bigint]:4",
        "table public.t: DELETE: (no-tuple-data)",
        "table public.other: INSERT: id[bigint]:5",
        "table \"S\".\"T x\": INSERT: \"K\"[text]:'it''s' n[int]:6",
        "COMMIT 1",
    ]
    assert test_decoding_keys(lines, ("public", "t"), ["id"]) == [("1",), ("2",), ("3",), ("4",)], \
        "every key a change names, an UPDATE's old and new; never a token inside a quoted value or another table's"
    assert test_decoding_keys(lines, ("S", "T x"), ["K", "n"]) == [("it's", "6")], "quoted identifiers and literals are unquoted"
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "k.parquet")
        write_keys([("1",), ("2",)], ["id"], p)
        assert _Mem().rows(f"SELECT id FROM read_parquet({_lit(p)}) ORDER BY id") == [("1",), ("2",)]


def _layout_self_test() -> None:
    """The delivered layouts the oracle reads: CSV text, sub-prefix manifests, hive buckets, absence, numeric text."""
    import tempfile

    import pyarrow as pa
    import pyarrow.parquet as pq

    assert absent(Exception('relation "t" does not exist')) and absent(Exception("cross-database references are not implemented"))
    assert not absent(Exception('Conversion Error: Could not convert string "NaN" to DECIMAL(18,2)')), "a read error is an oracle error, never absence"
    assert "format(''%s'', \"n\")" in _pg_projection("t", {"n": "NUMERIC(18,2)"}, {}), "a bounded NUMERIC is read as text: NaN has no DECIMAL"
    with tempfile.TemporaryDirectory() as d:
        ora = _Mem()
        ora.db.sql("CREATE TABLE src AS SELECT * FROM (VALUES (1, true, '\\x0A\\xFF'::BLOB, '', NULL::VARCHAR, 1.50::DECIMAL(10,2), 'a,\"b'), "
                   "(2, false, ''::BLOB, 'x', 'y', 2.00, 'z')) v(id, b, bin, e, n, m, q)")
        csv = os.path.join(d, "p.csv")
        with open(csv, "w") as fh:
            fh.write('id,b,bin,e,n,m,q\n1,true,0aff,"",,1.50,"a,""b"\n2,false,"",x,y,2.00,z\n')
        ora.db.sql(f"CREATE TABLE got AS SELECT * FROM {_parts(ora, [csv], 'csv')}")
        src = f"(SELECT {csv_text(_columns(ora, 'src'))} FROM src)"
        f = compare(ora, src, "got", numbers=frozenset({"m"}), verbatim=frozenset({"e", "n", "q"}))
        assert (f["only_src"], f["only_dst"], f["missing"]) == (0, 0, []), f"rivet's documented CSV text is the source: {f}"
        with open(csv, "w") as fh:
            fh.write('id,b,bin,e,n,m\n1,true,0aff,,,1.50\n2,false,"",x,y,2.00\n')
        ora.db.sql(f"CREATE OR REPLACE TABLE got AS SELECT * FROM {_parts(ora, [csv], 'csv')}")
        f = compare(ora, src, "got", numbers=frozenset({"m"}), verbatim=frozenset({"e", "n", "q"}))
        assert f["missing"] == ["q"] and f["only_src"] == 1, f"a dropped column and an empty string written as NULL are differences: {f}"

        for day, v in (("2024-01-01", "2024-01-01 10:00:00"), ("2024-01-02", "2024-01-03 00:00:00"), (HIVE_NULL, None)):
            os.makedirs(os.path.join(d, f"c={day}", "exp"))
            pq.write_table(pa.table({"c": pa.array([v], pa.string())}), os.path.join(d, f"c={day}", "exp", "part.parquet"))
            with open(os.path.join(d, f"c={day}", "exp", "manifest-r.json"), "w") as fh:
                json.dump({"status": "success", "run_id": "r", "parts": [{"path": "part.parquet"}]}, fh)
        names = [f"c={day}/exp/manifest-r.json" for day in ("2024-01-01", "2024-01-02", HIVE_NULL)]
        parts = declared_parts(d, names)
        assert len(parts) == 3, f"a manifest in a sub-prefix declares parts beside itself: {parts}"
        assert misfiled(ora, parts, "c") == ["PARTITION: 1 row(s) sit under a `c=` directory whose label does not match their value"], \
            "a row under the wrong day's directory is a finding; the NULL bucket holding NULL is not"
        assert stream_parts({"dirs": [os.path.join(d, "c=2024-01-01", "exp")]}, ["r"]) and not stream_parts({"dirs": [d]}, ["r"])


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
    if sys.argv[1:] in (["grade"], ["grade-load"], ["grade-stdout"], ["image"]):
        spec = json.load(sys.stdin)
        run = {"grade": grade, "grade-load": grade_load, "grade-stdout": grade_stdout, "image": take_image}[sys.argv[1]]
        for attempt in range(3):
            try:
                verdict = run(spec)
                break
            except Unreachable as e:
                verdict = {"skip": str(e)}
                break
            except Exception as e:  # noqa: BLE001 — every read is idempotent, so a transient one is re-run
                if attempt == 2 or not transient(e):
                    raise
                time.sleep(2 * (attempt + 1))
        if verdict.get("failures") and spec.get("known_defect"):
            verdict["known_defect"] = known_defect_covers(spec["known_defect"], verdict["failures"])
        sys.stdout.write(json.dumps(verdict, default=str))
    else:
        _self_test()
