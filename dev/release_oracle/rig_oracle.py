"""The Rust test rig's default oracle: one DuckDB session grades one run's declared output.

The rig (tests/common/rig/verify.rs) only gathers facts — engine, source URL, table or
query, the export's own filter, the manifests the run wrote, the state DB — and hands
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

import json
import os
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
NATIVE_FITS = {("year", "SMALLINT"), ("binary_float", "FLOAT")}
#: Oracle catalog types whose python-oracledb value is a number (read as exact text, compared by value).
ORACLE_NUMERIC = {"number", "float", "binary_float", "binary_double"}
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


def ledger_text_forms(engine: str, mode: str) -> set[str]:
    """Native types the type ledger (docs/type-capability-matrix.yaml) delivers as TEXT for `engine` x `mode`."""
    import yaml

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    with open(os.path.join(root, "docs", "type-capability-matrix.yaml")) as f:
        doc = yaml.safe_load(f)
    return set(((doc.get("engines") or {}).get(engine) or {}).get(mode) or {})


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
    """The window a delta export has covered in THIS destination: up to the latest manifest's `cursor_high`, from the first manifest's `cursor_low` (exclusive, unless that row was delivered)."""
    ordered = in_run_order(root, manifests)
    windows = [(_load(root, n).get("source") or {}).get("extraction") or {} for n in ordered]
    windows = [w for w in windows if w.get("cursor_column") and w.get("cursor_high") is not None]
    if not windows:
        return None
    col = windows[-1]["cursor_column"]
    c = coalesced if col == "_rivet_coalesced_cursor" and coalesced else _qi(col)
    preds = [f"{c} IS NOT NULL", f"{c} <= {_lit(str(windows[-1]['cursor_high']))}"]
    low = windows[0].get("cursor_low")
    if low is not None:
        preds.append(f"({c} > {_lit(str(low))} OR CAST({c} AS VARCHAR) IN (SELECT CAST({c} AS VARCHAR) FROM got))")
    return " AND ".join(preds)


def manifest_facts(root: str, manifests: list[str]) -> tuple[list[str], int]:
    """(run ids, summed `row_count`) of the named Success manifests under `root`."""
    ids, rows = [], 0
    for name in manifests:
        doc = _load(root, name)
        if doc.get("run_id"):
            ids.append(doc["run_id"])
        rows += int(doc.get("row_count") or 0)
    return ids, rows


def _pg_projection(table: str, native: dict, text_forms: set[str]) -> str:
    """A server-side SELECT rendering ledger TEXT deliveries and unbounded numerics with PostgreSQL's own text (the scanner reads a bare numeric as DOUBLE)."""
    cols = []
    for name, nat in native.items():
        q = _qi(name)
        cols.append(f"{q}::text AS {q}" if nat in text_forms or nat in ("numeric", "numeric[]") else q)
    return f"postgres_query('pg', {_lit('SELECT ' + ', '.join(cols) + ' FROM ' + table)})"


def _oracle_register(ora, url: str, sql: str) -> dict:
    """Rows of an Oracle SELECT (read by python-oracledb) registered as `ora_src`; NUMBER as exact text, INTERVAL YEAR TO MONTH as ISO text, VECTOR as a list, an object as JSON. Returns `{column: "number"}` for the NUMBER columns."""
    import array
    import decimal

    import pyarrow as pa

    from .value_diff import oracle_result

    numeric: dict = {}

    def cell(name: str, v: object) -> object:
        if isinstance(v, decimal.Decimal):
            numeric[name] = "number"
            return str(v)
        if type(v).__name__ == "IntervalYM":
            return f"P{v.years}Y{v.months}M"
        if isinstance(v, array.array):
            return list(v)
        whole = lambda d: int(d) if d == d.to_integral_value() else float(d)  # noqa: E731 — JSON numbers as JSON reads them
        return json.dumps(v, default=lambda d: whole(d) if isinstance(d, decimal.Decimal) else str(d)) if isinstance(v, dict) else v

    names, rows = oracle_result(url, sql)
    cols = list(zip(*[[cell(n, v) for n, v in zip(names, r)] for r in rows])) or [[] for _ in names]

    def column(c: list) -> pa.Array:
        a = pa.array(c)
        return pa.array(c, pa.string()) if pa.types.is_null(a.type) else a

    ora.db.register("ora_src", pa.table({n: column(list(c)) for n, c in zip(names, cols)}))
    return numeric


def _source(ora, spec: dict, text_forms: set[str]) -> tuple[str, list[str], dict]:
    """(source relation, primary key columns, native type per column) for the spec's engine."""
    from .value_diff import oracle_rows

    engine, table, query = spec["engine"], spec.get("table") or "", spec.get("query")
    schema, leaf = table.split(".", 1) if "." in table and engine != "mongo" else (None, table)
    native: dict = {}
    if engine == "oracle":
        from .value_diff import oracle_table_select

        native = _oracle_register(ora, spec["url"], query.rstrip().rstrip(";") if query else oracle_table_select(spec["url"], table))
        rel = "ora_src"
        owner = f"owner = {_lit(schema.upper())}" if schema else "owner = USER"
        if not query:
            native.update({r["COLUMN_NAME"]: r["DATA_TYPE"].lower() for r in oracle_rows(
                spec["url"],
                f"SELECT column_name, data_type FROM all_tab_columns WHERE {owner} AND table_name = {_lit(leaf.upper())}",
            )})
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
        native = dict(ora.rows(
            "SELECT * FROM postgres_query('pg', " + _lit(
                "SELECT a.attname::text, CASE WHEN t.typname LIKE '\\_%' THEN substr(t.typname, 2) || '[]' "
                "WHEN t.typname = 'numeric' AND a.atttypmod <> -1 THEN format_type(a.atttypid, a.atttypmod) "
                "ELSE t.typname::text END FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid "
                f"WHERE a.attrelid = {reg}::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
            ) + ")"
        ))
        return _pg_projection(table, native, text_forms), key, native
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
        native = dict(ora.rows(
            "SELECT * FROM mysql_query('my', " + _lit(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {_lit(schema or db)} "
                f"AND TABLE_NAME = {_lit(leaf)}"
            ) + ")"
        ))
        return f"my.{schema or db}.{leaf}", key, native
    if engine == "mssql":
        sch = schema or "dbo"
        if query:
            return f"mssql_scan('ms', {_lit(query)})", [], native
        key = [r[0] for r in ora.rows(
            "SELECT * FROM mssql_scan('ms', " + _lit(
                "SELECT c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id "
                "AND ic.index_id = i.index_id JOIN sys.columns c ON c.object_id = ic.object_id AND "
                f"c.column_id = ic.column_id WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID({_lit(sch + '.' + leaf)}) "
                "ORDER BY ic.key_ordinal"
            ) + ")"
        )]
        return f"ms.{sch}.{leaf}", key, native
    if engine == "mongo":
        # The scanner infers a schema (flattening nested keys); only `_id` is shared with the document blob.
        return f'(SELECT "_id" FROM mg.{spec["database"]}.{_qi(leaf)})', ["_id"], native
    raise ValueError(f"no source reader for engine {engine!r}")


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
    ora, src: str, dst: str | None, bits: frozenset = frozenset(), numbers: frozenset = frozenset()
) -> dict:
    """Column pairing, per-column counts and the canon multiset difference between `src` and `dst`; `bits` names MySQL BIT(n) columns (bytes read as an unsigned integer), `numbers` source columns read as numeric text."""
    import decimal

    from .value_diff import canon

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
    sp = ", ".join(f"{_proj(s, st, dt, st)} AS c{i}" for i, (s, st, _, dt) in enumerate(pairs)) or "1 AS c0"
    dp = ", ".join(f"{_proj(d, st, dt, dt)} AS c{i}" for i, (_, st, d, dt) in enumerate(pairs)) or "1 AS c0"
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
    try:
        rs = ora.rows("SELECT * FROM s EXCEPT ALL SELECT * FROM d")
        rd = ora.rows("SELECT * FROM d EXCEPT ALL SELECT * FROM s")
    except Exception:  # noqa: BLE001 — incomparable native types: every row goes through canon
        rs, rd = ora.rows("SELECT * FROM s"), ora.rows("SELECT * FROM d")
    numeric = {i for i, (s, st, _, dt) in enumerate(pairs) if _is_num(st) or _is_num(dt) or s in numbers}
    bit = {i for i, p in enumerate(pairs) if p[0] in bits}
    nanos = {i for i, (_, st, _, dt) in enumerate(pairs) if "TIMESTAMP_NS" in (st, dt)}

    def cell(i: int, v: object) -> object:
        from .value_diff import _secs

        if i in bit and isinstance(v, (bytes, bytearray)):
            return int.from_bytes(v, "big")
        if i in nanos and (isinstance(v, int) or (isinstance(v, str) and v.lstrip("-").isdigit())):
            return ("ts", _secs(decimal.Decimal(v).scaleb(-9)))
        return _numtext(v) if i in numeric else v

    def key(r: tuple) -> str:
        return json.dumps(canon([cell(i, v) for i, v in enumerate(r)]), default=str, sort_keys=True)

    cs, cd = Counter(map(key, rs)), Counter(map(key, rd))
    only_s, only_d = cs - cd, cd - cs
    if only_s or only_d:
        col = lambda rows, i: Counter(json.dumps(canon(cell(i, r[i])), default=str) for r in rows)  # noqa: E731
        out["diff_columns"] = [p[0] for i, p in enumerate(pairs) if col(rs, i) != col(rd, i)]
    out["only_src"], out["only_dst"] = sum(only_s.values()), sum(only_d.values())
    out["only_src_sample"] = [s[:600] for s in sorted(only_s)[:5]]
    out["only_dst_sample"] = [s[:600] for s in sorted(only_d)[:5]]
    return out


def grade_findings(
    f: dict, text_forms: set[str], native: dict, check_types: bool, overrides: frozenset = frozenset()
) -> list[str]:
    """Every disagreement in the findings `f`, one line each; empty means the run is sound (a `columns:` override is the export's own declared type, not graded)."""
    bad = []
    if f.get("dst_cols"):
        bad += [f"TYPE: source column `{m}` is absent from the delivered parquet" for m in f["missing"]]
    for col, st, _, dt in f.get("pairs", []) if check_types else []:
        nat = native.get(col)
        if col in overrides or (nat, dt) in NATIVE_FITS:
            continue
        if nat in text_forms and dt != "VARCHAR":
            bad.append(f"TYPE: `{col}` ({nat}) has a TEXT delivery in the type ledger but was delivered as {dt}")
        elif (why := type_loss(st, dt)) is not None:
            bad.append(f"TYPE: `{col}` source {st} delivered as {dt}: {why}")
    if "src_count" in f:
        if f["src_count"] != f["dst_count"]:
            bad.append(f"COUNT(*): source {f['src_count']}, delivered {f['dst_count']}")
        for (col, *_), a, b in zip(f["pairs"], f["src_stats"], f["dst_stats"]):
            if a[0] != b[0]:
                bad.append(f"COUNT(`{col}`) (non-null): source {a[0]}, delivered {b[0]}")
            if a[1] != b[1]:
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


def _state_table(ora, name: str) -> str:
    """`st.<schema>.<name>` wherever the state DB keeps it (a least-privilege role keeps its own schema)."""
    rows = ora.rows(
        f"SELECT schema_name FROM duckdb_tables() WHERE database_name = 'st' AND table_name = {_lit(name)}"
    )
    return f"st.{rows[0][0]}.{name}" if rows else f"st.{name}"


def _counters(ora, spec: dict, new_parts: list[str], run_ids: list[str]) -> dict:
    """rivet's own ledger for this run: export_metrics success rows and the file_log rows of the declared parts."""
    if not (spec.get("state") and run_ids):
        return {}
    ids = ", ".join(_lit(r) for r in run_ids)
    names = "[" + ", ".join(_lit(os.path.basename(p)) for p in new_parts) + "]::VARCHAR[]"
    runs, rows = ora.rows(
        f"SELECT count(*), coalesce(sum(total_rows), 0) FROM {_state_table(ora, 'export_metrics')} "
        f"WHERE run_id IN ({ids}) AND status = 'success'"
    )[0]
    fl_parts, fl_rows = ora.rows(
        f"SELECT count(DISTINCT regexp_extract(file_name, '[^/]+$')), coalesce(sum(row_count), 0) "
        f"FROM {_state_table(ora, 'file_log')} WHERE run_id IN ({ids}) "
        f"AND list_contains({names}, regexp_extract(file_name, '[^/]+$'))"
    )[0]
    return {"metrics_runs": runs, "metrics_rows": rows, "file_log_parts": fl_parts,
            "file_log_rows": fl_rows, "declared_parts": len(new_parts)}


def _meta_leg(ora, files: list[str], engine: str, snapshot: bool) -> str:
    """One SELECT over `files` carrying `__op`, `__pos`, `__seq` and the change order `__ord` (a snapshot leg sorts first)."""
    rel = f"read_parquet({_plist(files)}, union_by_name = true)"
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
    forms = ledger_text_forms(engine, "cdc" if cdc else "batch")
    kw = {"state": spec["state"]} if spec.get("state") else {}
    # Two threads: every live test runs this, and a scanner opens a connection per thread.
    with Oracle(config={"threads": 2}, **kw, **_attach(spec)) as ora:
        ora.db.sql("SET TimeZone = 'UTC'")
        src, key, native = _source(ora, spec, forms)
        key = spec.get("key") or key
        filt = watermark(out_dir, graded, spec.get("cursor_expr")) if cumulative and not cdc else None
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
                f"SELECT *, {i} AS __mseq FROM read_parquet({_plist(ps)}, union_by_name = true)"
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
        bits = frozenset(c for c, n in native.items() if n == "bit") if engine == "mysql" else frozenset()
        numbers = frozenset(c for c, n in native.items() if n in ORACLE_NUMERIC) if engine == "oracle" else frozenset()
        f = compare(ora, src, dst, bits, numbers) if dst or not cdc else {}
        part_rows = ora.scalar(f"SELECT count(*) FROM read_parquet({_plist(new_parts)}, union_by_name = true)") if new_parts else 0
        f.update(part_rows=part_rows, manifest_rows=manifest_rows, **_counters(ora, spec, new_parts, run_ids))
    overrides = frozenset(spec.get("overrides") or [])
    failures = grade_findings(f, forms, native, engine != "mongo", overrides)
    facts = {k: v for k, v in f.items() if k not in ("pairs", "dst_cols", "src_stats", "dst_stats")}
    return {"failures": failures, "notes": notes, "facts": facts, "key": key}


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
    bad = "\n".join(grade_findings(f, set(), {}, True))
    for cls in ("column `gone`", "TYPE: `id`", "COUNT(*)", "COUNT(`id`)", "COUNT(DISTINCT `id`)", "VALUES",
                "export_metrics.total_rows 5"):
        assert cls in bad, f"{cls} missing from {bad}"
    clean = {**f, "missing": [], "pairs": [["id", "BIGINT", "id", "BIGINT"]], "src_count": 2,
             "src_stats": [[2, 2]], "dst_stats": [[2, 2]], "only_src": 0, "metrics_rows": 2}
    assert not grade_findings(clean, set(), {}, True), grade_findings(clean, set(), {}, True)
    assert grade_findings({**clean, "file_log_rows": 3}, set(), {}, True), "a wrong file_log is a finding"
    assert grade_findings({**clean, "file_log_parts": 0}, set(), {}, True), "a declared part file_log lacks is a finding"
    assert _numtext("1.50") == _numtext("1.5000"), "a numeric text compares by value"
    assert grade_findings({**clean, "manifest_rows": 3}, set(), {}, True), "a wrong manifest row_count is a finding"
    led = {**clean, "pairs": [["n", "DOUBLE", "n", "DOUBLE"]]}
    assert any("TEXT delivery" in b for b in grade_findings(led, {"numeric"}, {"n": "numeric"}, True))
    print("rig_oracle self-test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["grade"]:
        spec = json.load(sys.stdin)
        try:
            verdict = grade(spec)
        except Exception as e:  # noqa: BLE001 — a SQL Server catalog deadlock victim is retried once
            if "deadlock" not in str(e):
                raise
            verdict = grade(spec)
        sys.stdout.write(json.dumps(verdict, default=str))
    else:
        _self_test()
