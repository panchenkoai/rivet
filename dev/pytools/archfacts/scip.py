"""Run `rust-analyzer scip` and read the SCIP index with a dependency-free protobuf wire reader."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

DEFINITION = 1

# Field numbers from scip.proto (sourcegraph/scip): the only part of the schema this reader depends on.
INDEX_METADATA, INDEX_DOCUMENTS, INDEX_EXTERNAL = 1, 2, 3
DOC_PATH, DOC_OCCURRENCES, DOC_SYMBOLS = 1, 2, 3
OCC_RANGE, OCC_SYMBOL, OCC_ROLES = 1, 2, 3
SYM_SYMBOL, SYM_KIND, SYM_SIGNATURE, SIGNATURE_TEXT = 1, 5, 7, 5
META_TOOL, TOOL_VERSION = 2, 2


@dataclass
class Document:
    """One indexed file: occurrences as (line, col, end_col, symbol, roles), 0-based."""

    path: str
    occurrences: list[tuple[int, int, int, str, int]] = field(default_factory=list)


@dataclass
class Index:
    """The parts of a SCIP index archfacts reads."""

    tool_version: str = ""
    documents: list[Document] = field(default_factory=list)
    kinds: dict[str, int] = field(default_factory=dict)
    public: set[str] = field(default_factory=set)


def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Decode one varint at `pos`; (value, next position)."""
    result = shift = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if b < 0x80:
            return result, pos
        shift += 7


def fields(buf: bytes, start: int = 0, end: int | None = None):
    """Yield (field number, wire type, value) for one message; length-delimited values are (start, end) spans."""
    pos = start
    end = len(buf) if end is None else end
    while pos < end:
        key, pos = _varint(buf, pos)
        num, wt = key >> 3, key & 7
        if wt == 0:
            val, pos = _varint(buf, pos)
            yield num, wt, val
        elif wt == 2:
            n, pos = _varint(buf, pos)
            yield num, wt, (pos, pos + n)
            pos += n
        elif wt == 1:
            yield num, wt, buf[pos : pos + 8]
            pos += 8
        elif wt == 5:
            yield num, wt, buf[pos : pos + 4]
            pos += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wt} at byte {pos}")


def _packed(buf: bytes, start: int, end: int) -> list[int]:
    """A packed repeated int32 field."""
    out = []
    pos = start
    while pos < end:
        v, pos = _varint(buf, pos)
        out.append(v)
    return out


def _occurrence(buf: bytes, start: int, end: int) -> tuple[int, int, int, str, int] | None:
    """One Occurrence as (line, col, end_col, symbol, roles); None when it has no symbol or spans lines."""
    rng: list[int] = []
    symbol = ""
    roles = 0
    for num, wt, val in fields(buf, start, end):
        if num == OCC_RANGE and wt == 2:
            rng = _packed(buf, *val)
        elif num == OCC_RANGE:
            rng.append(val)
        elif num == OCC_SYMBOL:
            symbol = buf[val[0] : val[1]].decode()
        elif num == OCC_ROLES:
            roles = val
    if not symbol or len(rng) < 3:
        return None
    if len(rng) == 4 and rng[2] != rng[0]:
        return None
    return rng[0], rng[1], rng[-1], symbol, roles


def _symbol_info(buf: bytes, start: int, end: int, index: Index) -> None:
    """Record one SymbolInformation's kind and whether its signature starts with `pub`."""
    symbol = ""
    kind = 0
    public = False
    for num, _, val in fields(buf, start, end):
        if num == SYM_SYMBOL:
            symbol = buf[val[0] : val[1]].decode()
        elif num == SYM_KIND:
            kind = val
        elif num == SYM_SIGNATURE:
            for snum, _, sval in fields(buf, *val):
                if snum == SIGNATURE_TEXT:
                    public = buf[sval[0] : sval[0] + 4] == b"pub "
    if symbol:
        if kind:
            index.kinds[symbol] = kind
        if public:
            index.public.add(symbol)


def parse(buf: bytes) -> Index:
    """Decode a serialized scip.Index."""
    index = Index()
    for num, _, val in fields(buf):
        if num == INDEX_METADATA:
            for mnum, _, mval in fields(buf, *val):
                if mnum == META_TOOL:
                    for tnum, _, tval in fields(buf, *mval):
                        if tnum == TOOL_VERSION:
                            index.tool_version = buf[tval[0] : tval[1]].decode()
        elif num == INDEX_DOCUMENTS:
            doc = Document(path="")
            for dnum, _, dval in fields(buf, *val):
                if dnum == DOC_PATH:
                    doc.path = buf[dval[0] : dval[1]].decode()
                elif dnum == DOC_OCCURRENCES:
                    occ = _occurrence(buf, *dval)
                    if occ:
                        doc.occurrences.append(occ)
                elif dnum == DOC_SYMBOLS:
                    _symbol_info(buf, *dval, index)
            index.documents.append(doc)
        elif num == INDEX_EXTERNAL:
            _symbol_info(buf, *val, index)
    return index


def encode_varint(v: int) -> bytes:
    """Encode one varint (used by the reader's own round-trip test)."""
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        out.append(b | (0x80 if v else 0))
        if not v:
            return bytes(out)


def encode_field(num: int, value: int | bytes) -> bytes:
    """Encode one varint or length-delimited field."""
    if isinstance(value, int):
        return encode_varint(num << 3) + encode_varint(value)
    return encode_varint(num << 3 | 2) + encode_varint(len(value)) + value


SYMBOL = re.compile(r"^rust-analyzer cargo (\S+) (\S+) (.*)$")
IMPL = re.compile(r"impl#\[(`[^`]*`|[^\]]*)\](?:\[(`[^`]*`|[^\]]*)\])?")


def split_symbol(symbol: str) -> tuple[str, str] | None:
    """(crate, descriptor string) of a global rust-analyzer symbol; None for locals."""
    m = SYMBOL.match(symbol)
    if not m:
        return None
    return m.group(1), m.group(3)


def _impl(m: re.Match) -> str:
    """`impl#[T][Trait]` as `<T as Trait>#`, `impl#[T]` as `T#`."""
    ty, tr = m.group(1).strip("`"), (m.group(2) or "").strip("`")
    return (f"<{ty} as {tr}>" if tr else ty) + "#"


def qualname(symbol: str) -> str:
    """A readable `crate::a::Type::method` path for a symbol."""
    parts = split_symbol(symbol)
    if not parts:
        return symbol
    desc = IMPL.sub(_impl, parts[1]).replace("().", "#")
    names = [n.strip("`!:") for n in re.split(r"[/#]|\.(?![^<]*>)", desc)]
    names = [n for n in names if n and n != "crate"]
    return "::".join([parts[0].replace("-", "_"), *names])


def version() -> str | None:
    """`rust-analyzer --version`, or None when the component is not installed."""
    try:
        p = subprocess.run(["rust-analyzer", "--version"], capture_output=True, text=True)
    except OSError:
        return None
    return p.stdout.strip() if p.returncode == 0 and p.stdout.strip() else None


def feature_config(features: str) -> dict:
    """The rust-analyzer cargo config for a named feature set."""
    if features == "default":
        return {"cargo": {"features": []}}
    if features == "no-default-jemalloc":
        return {"cargo": {"noDefaultFeatures": True, "features": ["jemalloc"]}}
    raise ValueError(f"unknown feature set {features!r}")


def run(crate_dir: Path, features: str, out: Path, cargo_target: Path | None = None) -> dict:
    """Index `crate_dir` into `out`, building into `cargo_target` when given; returns wall seconds and the indexer's peak RSS in MB."""
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as cfg:
        json.dump(feature_config(features), cfg)
    started = time.monotonic()
    with tempfile.TemporaryFile("w+") as err:
        try:
            proc = subprocess.Popen(["rust-analyzer", "scip", ".", "--output", str(out), "--config-path", cfg.name],
                                    cwd=crate_dir, stdout=subprocess.DEVNULL, stderr=err,
                                    env={**os.environ, **({"CARGO_TARGET_DIR": str(cargo_target)} if cargo_target else {})})
            _, status, usage = os.wait4(proc.pid, 0)
            proc.returncode = os.waitstatus_to_exitcode(status)
        finally:
            os.unlink(cfg.name)
        err.seek(0)
        stderr = err.read()
    wall = time.monotonic() - started
    if proc.returncode != 0 or not out.exists():
        raise RuntimeError(f"rust-analyzer scip failed ({proc.returncode}): {stderr[-800:]}")
    scale = 1 << 20 if os.uname().sysname == "Darwin" else 1 << 10
    return {"wall_s": round(wall, 1), "peak_rss_mb": round(usage.ru_maxrss / scale), "duplicate_symbols": stderr.count("Duplicate symbol:")}
