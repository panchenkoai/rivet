"""Derive the architecture facts of one crate from a resolution backend (scip: exact, treesitter: by-name, approximate)."""

from __future__ import annotations

import hashlib
import json
import re
import time
import tomllib
from collections import Counter, defaultdict
from pathlib import Path

from . import config, gitfacts, scip
from .rustsyn import KEYWORDS, File, Item, digest

Target = tuple[str, int, int]
ITEM_KINDS = ("fn", "struct", "enum", "trait", "union", "const", "static", "type", "macro")
TYPE_KINDS = ("struct", "enum", "trait", "union", "type")
SCIP_ENUM, SCIP_VARIANT = 11, 12
SCIP_GENERATED = {17: "fn", 26: "fn", 49: "struct", 11: "enum", 53: "trait", 8: "const", 45: "static"}
BUILTIN = frozenset("_ u8 u16 u32 u64 u128 usize i8 i16 i32 i64 i128 isize f32 f64 bool char str".split())
FEATURE = re.compile(r'(not\()?feature="([^"]+)"')


def module_of(path: str) -> str:
    """The module id of a source path: `src/a/b/mod.rs` -> `a::b`, `src/lib.rs` -> `crate`."""
    parts = path.removesuffix(".rs").split("/")
    if parts[0] == "src":
        parts = parts[1:]
    if parts and parts[-1] == "mod":
        parts = parts[:-1]
    if parts == ["lib"] or not parts:
        return "crate"
    return "::".join(parts)


def load_sources(root: Path) -> dict[str, File]:
    """Scan every Rust file of the crate, carrying `#[cfg]` and test-ness from `mod x;` declarations to the file."""
    texts: dict[str, str] = {}
    for top in config.SOURCE_DIRS:
        for path in sorted((root / top).rglob("*.rs")) if (root / top).is_dir() else []:
            texts[str(path.relative_to(root))] = path.read_text(errors="replace")
    flags: dict[str, tuple[bool, tuple[str, ...]]] = {p: (p.split("/")[0] in config.TEST_DIRS, ()) for p in texts}
    files = {p: File(p, t, *flags[p]) for p, t in texts.items()}
    pending = list(files)
    while pending:
        parent = pending.pop()
        f = files[parent]
        here = parent.rsplit("/", 1)[0]
        stem = parent.rsplit("/", 1)[-1].removesuffix(".rs")
        for name, test, cfg, attr_path in f.mod_decls:
            bases = [here, f"{here}/{stem}"] if stem not in ("mod", "lib", "main") else [here]
            cands = [f"{here}/{attr_path}"] if attr_path else [c for b in bases for c in (f"{b}/{name}.rs", f"{b}/{name}/mod.rs")]
            child = next((c for c in cands if c in files), None)
            if not child:
                continue
            want = (flags[child][0] or test or f.file_test, tuple(dict.fromkeys(f.file_cfg + cfg + flags[child][1])))
            if want != flags[child]:
                flags[child] = want
                files[child] = File(child, texts[child], *want)
                pending.append(child)
    return files


def qual(path: str, it: Item) -> str:
    """A readable qualified name for a syntactic item."""
    parts = [module_of(path), *it.scope]
    if it.owner:
        parts.append(f"<{it.owner} as {it.trait}>" if it.trait and it.owner_kind == "impl" else it.owner)
    return "::".join([*parts, it.name])


def cfg_active(cfg: tuple[str, ...], enabled: tuple[str, ...]) -> bool:
    """Whether a chain of cfg predicates holds for a feature set, reading only `feature = "x"` and its negation."""
    for entry in cfg:
        for neg, name in FEATURE.findall(entry):
            if bool(neg) == (name in enabled) and "any(" not in entry:
                return False
    return True


class ScipBackend:
    """Type-resolved references from a rust-analyzer SCIP index."""

    name = "scip"
    approx = False

    def __init__(self, root: Path, files: dict[str, File], index: scip.Index):
        self.files = files
        self.index = index
        self.defs: dict[str, list[Target]] = defaultdict(list)
        self.sym_at: dict[str, dict[int, str]] = {}
        self.def_at: dict[str, set[int]] = {}
        self.def_sym: dict[Target, str] = {}
        for doc in index.documents:
            f = files.get(doc.path)
            if f is None and doc.path.endswith(".rs") and (root / doc.path).is_file():
                f = files[doc.path] = File(doc.path, (root / doc.path).read_text(errors="replace"), doc.path.split("/")[0] != "src")
            at = self.sym_at.setdefault(doc.path, {})
            def_at = self.def_at.setdefault(doc.path, set())
            for line0, col, _end, sym, roles in doc.occurrences:
                if f is not None and not f.ascii:
                    col = len(f.src[line0].encode()[:col].decode(errors="ignore")) if line0 < len(f.src) else col
                key = line0 + 1 << 16 | col
                at[key] = sym
                if roles & scip.DEFINITION:
                    def_at.add(key)
                    if not sym.startswith("local "):
                        self.defs[sym].append((doc.path, line0 + 1, col))
                        self.def_sym[(doc.path, line0 + 1, col)] = sym
        self.own = {parts[0] for sym in self.defs if (parts := scip.split_symbol(sym))}

    def _resolve(self, sym: str | None, path: str) -> Target | None:
        """The definition of `sym` as seen from `path`: rust-analyzer reuses one symbol across the crate's targets."""
        cands = self.defs.get(sym) if sym else None
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]
        top = path.split("/")[:2]
        return (next((c for c in cands if c[0] == path), None) or next((c for c in cands if c[0].split("/")[:2] == top), None)
                or next((c for c in cands if c[0].startswith("src/")), None) or cands[0])

    def active(self, f: File, it: Item) -> bool:
        """An item exists in this feature set when the index has a symbol at its name."""
        at = self.sym_at.get(f.path)
        if at is None:
            return False
        if it.kind == "impl":
            return any(p is not None and (p[0] << 16 | p[1]) in at for p in (it.self_pos, it.trait_pos))
        return (it.line << 16 | it.col) in at

    def refs(self):
        """(path, line, col, target) for every non-definition occurrence of a crate-defined, non-module symbol."""
        for doc in self.index.documents:
            path = doc.path
            def_at = self.def_at[path]
            for key, sym in self.sym_at[path].items():
                if key in def_at or sym.endswith("/"):
                    continue
                tgt = self._resolve(sym, path)
                if tgt is not None:
                    yield path, key >> 16, key & 0xFFFF, tgt

    def ext_refs(self):
        """(path, line, col, crate, name) for every occurrence of a symbol another crate defines."""
        defs = self.defs
        for doc in self.index.documents:
            for key, sym in self.sym_at[doc.path].items():
                if sym in defs or sym.startswith("local "):
                    continue
                parts = scip.split_symbol(sym)
                if parts and parts[0] not in self.own and not sym.endswith("/"):
                    yield doc.path, key >> 16, key & 0xFFFF, parts[0].replace("-", "_"), sym

    def at(self, path: str, line: int, col: int) -> list[Target]:
        """The crate definition the token at this position resolves to."""
        tgt = self._resolve(self.sym_at.get(path, {}).get(line << 16 | col), path)
        return [tgt] if tgt else []

    def describe(self, path: str, line: int, col: int) -> str:
        """A readable name for whatever the token resolves to, crate-local or external."""
        sym = self.sym_at.get(path, {}).get(line << 16 | col)
        return self._name(sym) if sym else ""

    def _name(self, sym: str) -> str:
        """A readable name; the crate's own symbols drop the crate prefix."""
        name = scip.qualname(sym)
        parts = scip.split_symbol(sym)
        return name.split("::", 1)[-1] if parts and parts[0] in self.own else name

    def file_active(self, path: str) -> bool:
        """Whether the index covers this file at all."""
        return path in self.sym_at

    def variant_enum(self, path: str, line: int, col: int) -> list[Target]:
        """The enum whose variant the token names."""
        sym = self.sym_at.get(path, {}).get(line << 16 | col)
        if not sym or self.index.kinds.get(sym) != SCIP_VARIANT:
            return []
        parent = sym[: sym.rstrip("#").rfind("#") + 1]
        tgt = self._resolve(parent, path)
        return [tgt] if tgt and self.index.kinds.get(parent) == SCIP_ENUM else []

    def generated_items(self):
        """Definitions the index has that no syntactic item explains: items produced by a macro call."""
        for (path, line, col), sym in self.def_sym.items():
            f = self.files.get(path)
            kind = SCIP_GENERATED.get(self.index.kinds.get(sym, 0))
            if f is None or kind is None or (line, col) in f.item_at:
                continue
            via = next((i for i in f.items if i.kind == "invoke" and i.line <= line <= i.end_line), None)
            if via is not None:
                yield path, line, col, kind, self._name(sym), sym in self.index.public, via

    def unresolved(self, inactive: dict[str, list[tuple[int, int]]]) -> dict:
        """Identifiers of indexed production code that carry no symbol, apart from code that is compiled out."""
        total = missing = dead = 0
        names: Counter[str] = Counter()
        unindexed = []
        for path, f in sorted(self.files.items()):
            if not path.startswith("src/"):
                continue
            at = self.sym_at.get(path)
            if at is None:
                unindexed.append(path)
                continue
            spans = inactive.get(path, [])
            for i, kind in enumerate(f.kinds):
                if kind != "i" or f.in_macro_def[i] or f.in_attr[i] or f.texts[i] in KEYWORDS or f.texts[i] in BUILTIN or f.texts[i - 1] == "mod":
                    continue
                line = f.lines[i]
                if (line << 16 | f.cols[i]) in at:
                    total += 1
                elif any(s <= line <= e for s, e in spans):
                    dead += 1
                else:
                    total += 1
                    missing += 1
                    names[f.texts[i]] += 1
        ratio = missing / total if total else 0.0
        return {
            "identifiers": total, "unresolved": missing, "ratio": round(ratio, 4), "compiled_out_identifiers": dead,
            "unindexed_files": unindexed, "top_unresolved": [{"name": n, "count": c} for n, c in names.most_common(15)],
            "threshold": config.DEGRADED_UNRESOLVED_RATIO, "degraded": ratio > config.DEGRADED_UNRESOLVED_RATIO,
        }


class NameBackend:
    """By-name resolution over tokens: the deliberately approximate comparison backend."""

    name = "treesitter"
    approx = True

    def __init__(self, root: Path, files: dict[str, File], features: str):
        self.files = files
        self.enabled = config.FEATURE_SETS[features]
        self.fns: dict[str, list[tuple[Target, Item]]] = defaultdict(list)
        self.types: dict[str, list[tuple[Target, Item]]] = defaultdict(list)
        for path, f in files.items():
            for it in f.items:
                if it.kind == "fn" and self.active(f, it):
                    self.fns[it.name].append(((path, it.line, it.col), it))
                elif it.kind in TYPE_KINDS + ("const", "static", "macro") and self.active(f, it):
                    self.types[it.name].append(((path, it.line, it.col), it))
        manifest = tomllib.loads((root / "Cargo.toml").read_text())
        deps = set(manifest.get("dependencies", {})) | set(manifest.get("dev-dependencies", {}))
        self.deps = {d.replace("-", "_") for d in deps}

    def active(self, f: File, it: Item) -> bool:
        """Read `#[cfg(feature = ...)]` chains against the feature set; any other predicate counts as on."""
        return cfg_active(it.cfg, self.enabled)

    def file_active(self, path: str) -> bool:
        """Whether the file's own `mod` declaration is compiled in for this feature set."""
        return cfg_active(self.files[path].file_cfg, self.enabled)

    def _pick(self, path: str, cands: list[tuple[Target, Item]]) -> list[Target]:
        """Candidates defined in the same file win; otherwise every same-named candidate is credited."""
        local = [t for t, _ in cands if t[0] == path]
        return local or [t for t, _ in cands]

    def _candidates(self, f: File, i: int) -> list[Target]:
        """Every definition the identifier at token `i` could name."""
        T, K = f.texts, f.kinds
        name = T[i]
        nxt = T[i + 1] if i + 1 < len(T) else ""
        prev = T[i - 1] if i else ""
        if f.in_use[i]:
            return self._pick(f.path, self.fns.get(name, []) + self.types.get(name, []))
        if nxt == "(" or (nxt == "::" and i + 2 < len(T) and T[i + 2] == "<"):
            cands = self.fns.get(name, [])
            if prev == ".":
                cands = [c for c in cands if c[1].has_self]
            elif prev == "::" and i >= 2 and K[i - 2] == "i":
                q = T[i - 2]
                if q in ("super", "self", "crate"):
                    cands = [c for c in cands if not c[1].owner]
                elif q == "Self":
                    cands = [c for c in cands if c[0][0] == f.path and c[1].owner]
                else:
                    cands = [c for c in cands if c[1].owner == q or (not c[1].owner and module_of(c[0][0]).split("::")[-1] == q)]
            else:
                cands = [c for c in cands if not c[1].owner]
            if cands or name[0].islower():
                return self._pick(f.path, cands)
        if nxt == "!":
            return self._pick(f.path, [c for c in self.types.get(name, []) if c[1].kind == "macro"])
        return self._pick(f.path, [c for c in self.types.get(name, []) if c[1].kind != "macro"])

    def refs(self):
        """(path, line, col, target) for every identifier that names a crate definition by spelling."""
        for path, f in self.files.items():
            for i, kind in enumerate(f.kinds):
                if kind != "i" or f.in_macro_def[i] or f.in_attr[i] or f.texts[i] in KEYWORDS:
                    continue
                line, col = f.lines[i], f.cols[i]
                if (line, col) in f.item_at:
                    continue
                for tgt in self._candidates(f, i):
                    yield path, line, col, tgt

    def ext_refs(self):
        """(path, line, col, crate, name) for every path that starts at a dependency's crate name."""
        for path, f in self.files.items():
            T = f.texts
            for i, kind in enumerate(f.kinds):
                if kind == "i" and T[i] in self.deps and i + 1 < len(T) and T[i + 1] == "::" and (i == 0 or T[i - 1] not in ("::", ".")):
                    yield path, f.lines[i], f.cols[i], T[i], T[i]

    def at(self, path: str, line: int, col: int) -> list[Target]:
        """Every definition the token at this position could name."""
        f = self.files[path]
        i = f.pos.get((line, col))
        return self._candidates(f, i) if i is not None else []

    def describe(self, path: str, line: int, col: int) -> str:
        """The token's own spelling: nothing more is known."""
        f = self.files[path]
        i = f.pos.get((line, col))
        return f.texts[i] if i is not None else ""

    def variant_enum(self, path: str, line: int, col: int) -> list[Target]:
        """The enum named by an `Enum::Variant` path; glob-imported and `Self::` variants are not seen."""
        f = self.files[path]
        i = f.pos.get((line, col))
        if i is None or i + 2 >= len(f.texts) or f.texts[i + 1] != "::" or (i and f.texts[i - 1] == "::"):
            return []
        return self._pick(path, [c for c in self.types.get(f.texts[i], []) if c[1].kind == "enum"])

    def generated_items(self):
        """Macro-generated items are invisible to a token scan."""
        return iter(())

    def unresolved(self, inactive: dict) -> dict:
        """A token scan cannot tell a resolved reference from an unresolved one."""
        return {"identifiers": None, "unresolved": None, "ratio": None, "degraded": False, "approx": True}


def _sccs(graph: dict[str, set[str]]) -> list[list[str]]:
    """Strongly connected components with more than one member (iterative Tarjan)."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on: set[str] = set()
    stack: list[str] = []
    out: list[list[str]] = []
    for root in sorted(graph):
        if root in index:
            continue
        work = [(root, iter(sorted(graph.get(root, ()))))]
        index[root] = low[root] = len(index)
        stack.append(root)
        on.add(root)
        while work:
            node, it = work[-1]
            advanced = False
            for nxt in it:
                if nxt not in index:
                    index[nxt] = low[nxt] = len(index)
                    stack.append(nxt)
                    on.add(nxt)
                    work.append((nxt, iter(sorted(graph.get(nxt, ())))))
                    advanced = True
                    break
                if nxt in on:
                    low[node] = min(low[node], index[nxt])
            if advanced:
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[node])
            if low[node] == index[node]:
                comp = []
                while True:
                    w = stack.pop()
                    on.discard(w)
                    comp.append(w)
                    if w == node:
                        break
                if len(comp) > 1:
                    out.append(sorted(comp))
    return sorted(out, key=lambda c: (-len(c), c))


def dup_groups(files: dict[str, File], active) -> list[dict]:
    """Exact, renamed and near-duplicate groups of production function bodies."""
    fns = []
    for path, f in sorted(files.items()):
        if not path.startswith("src/"):
            continue
        for it in f.items:
            if it.kind == "fn" and it.body and not it.test and it.body[1] - it.body[0] - 1 >= config.DUP_MIN_TOKENS and active(f, it):
                fns.append((f"{path}:{it.line}", qual(path, it), f.body_signature(it, False), f.body_signature(it, True)))
    groups: list[dict] = []
    seen: set[frozenset[int]] = set()

    def emit(kind: str, members: list[int], similarity: float) -> None:
        key = frozenset(members)
        if len(members) < 2 or key in seen:
            return
        seen.add(key)
        groups.append({
            "kind": kind, "tokens": min(len(fns[m][2]) for m in members), "similarity": round(similarity, 2),
            "members": [{"id": fns[m][0], "name": fns[m][1]} for m in sorted(members, key=lambda m: fns[m][0])],
        })

    for kind, col in (("exact", 2), ("renamed", 3)):
        by: dict[str, list[int]] = defaultdict(list)
        for n, fn in enumerate(fns):
            by[digest(fn[col])].append(n)
        for members in by.values():
            emit(kind, members, 1.0)
    k = config.DUP_SHINGLE
    shingles = [{hash(tuple(fn[3][j : j + k])) for j in range(len(fn[3]) - k + 1)} for fn in fns]
    owners: dict[int, list[int]] = defaultdict(list)
    for n, sh in enumerate(shingles):
        for s in sh:
            owners[s].append(n)
    shared: Counter[tuple[int, int]] = Counter()
    for members in owners.values():
        if 1 < len(members) <= config.DUP_COMMON_SHINGLE:
            for a in range(len(members)):
                for b in range(a + 1, len(members)):
                    shared[(members[a], members[b])] += 1
    parent = list(range(len(fns)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    lowest: dict[int, float] = {}
    for (a, b), n in shared.items():
        j = n / (len(shingles[a]) + len(shingles[b]) - n)
        if j >= config.DUP_NEAR_JACCARD:
            ra, rb = find(a), find(b)
            parent[ra] = rb
            lowest[rb] = min(j, lowest.get(ra, 1.0), lowest.get(rb, 1.0))
    comps: dict[int, list[int]] = defaultdict(list)
    for n in range(len(fns)):
        comps[find(n)].append(n)
    for r, members in comps.items():
        emit("near", members, lowest.get(r, 1.0))
    groups.sort(key=lambda g: (-g["tokens"] * len(g["members"]), g["members"][0]["id"]))
    return groups


def derive(root: Path, files: dict[str, File], backend, engines: dict) -> dict:
    """Every index-derived fact, from one backend's references."""
    approx = backend.approx
    for f in files.values():
        f.item_at = {(it.line, it.col): it for it in f.items if it.kind != "impl"}
    inactive: dict[str, list[tuple[int, int]]] = defaultdict(list)
    live: dict[Target, tuple[File, Item]] = {}
    for path, f in files.items():
        for it in f.items:
            if it.kind == "invoke":
                continue
            if backend.active(f, it):
                if it.kind != "impl":
                    live[(path, it.line, it.col)] = (f, it)
            elif not f.in_macro_def[f.pos.get((it.line, it.col), 0)]:
                inactive[path].append((it.line, it.end_line))

    def engine_of(path: str) -> str:
        return next((e for e, spec in engines.items() if any(path == p or path.startswith(p.rstrip("/") + "/") for p in spec["paths"])), "")

    sites: dict[Target, list[tuple[str, int, bool]]] = defaultdict(list)
    edges: dict[str, Counter[str]] = defaultdict(Counter)
    callees: dict[str, Counter[Target]] = defaultdict(Counter)
    leaks: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for path, line, col, tgt in backend.refs():
        f = files.get(path)
        if f is None:
            continue
        i = f.pos.get((line, col))
        in_use = bool(i is not None and f.in_use[i])
        pub_use = bool(i is not None and f.in_pub_use[i])
        test = f.is_test_line(line)
        if not in_use:
            sites[tgt].append((path, line, test))
        if test or (in_use and not pub_use) or not path.startswith("src/") or not tgt[0].startswith("src/"):
            continue
        src, dst = module_of(path), module_of(tgt[0])
        if src != dst:
            edges[src][dst] += 1
            callees[src][tgt] += 1
        home = engine_of(tgt[0])
        hit = live.get(tgt)
        if home and engine_of(path) != home and hit and hit[1].kind in TYPE_KINDS:
            leaks[(home, src)][qual(tgt[0], hit[1])] += 1
    crate_engine = {c: e for e, spec in engines.items() for c in spec["crates"]}
    ext: dict[str, Counter[str]] = defaultdict(Counter)
    for path, line, _col, crate, name in backend.ext_refs():
        f = files.get(path)
        if f is None or not path.startswith("src/") or crate in config.STD_CRATES or f.is_test_line(line):
            continue
        ext[module_of(path)][crate] += 1
        home = crate_engine.get(crate)
        if home and engine_of(path) != home:
            leaks[(home, module_of(path))][scip.qualname(name) if " " in name else name] += 1

    def label(tgt: Target) -> str:
        hit = live.get(tgt)
        if hit:
            return qual(tgt[0], hit[1])
        return backend.describe(*tgt) or f"{tgt[0]}:{tgt[1]}"

    items: dict[str, dict] = {}

    def add_item(path: str, tgt: Target, kind: str, name: str, vis: str, cfg: tuple, **extra) -> dict:
        ss = sites.get(tgt, [])
        prod = [s for s in ss if not s[2]]
        f = files[path]
        callers = []
        for p, ln, test in ss[: config.CALLERS_KEPT]:
            fn = files[p].enclosing_fn(ln)
            callers.append({"at": f"{p}:{ln}", "in": qual(p, fn) if fn else module_of(p), "test": test})
        rec = {
            "id": f"{path}:{tgt[1]}", "name": name, "kind": kind, "vis": vis, "module": module_of(path), "cfg": list(cfg),
            "call_sites": len(prod), "callers_modules": sorted({module_of(p) for p, _, _ in prod}), "test_refs": len(ss) - len(prod),
            "callers": callers, **extra,
        }
        if approx:
            rec["approx"] = ["call_sites", "callers_modules", "test_refs", "callers"] + (["passthrough_to"] if "passthrough_to" in extra else [])
            if kind == "fn":
                rec["ambiguous"] = len(backend.fns.get(name.rsplit("::", 1)[-1], [])) > 1
        items[f"{path}:{tgt[1]}:{tgt[2]}"] = rec
        return rec

    stats: dict[str, dict] = {}
    for path, f in sorted(files.items()):
        if not path.startswith("src/"):
            continue
        pub_items = pub_params = 0
        for it in f.items:
            tgt = (path, it.line, it.col)
            if it.kind not in ITEM_KINDS or it.test or tgt not in live:
                continue
            extra = {}
            if it.kind == "fn":
                extra["params"] = len(it.params)
                extra["loc"] = it.end_line - it.line + 1
                c = f.passthrough_callee(it)
                if c is not None:
                    to = backend.at(path, f.lines[c], f.cols[c])
                    name = " | ".join(sorted({label(t) for t in to})) or backend.describe(path, f.lines[c], f.cols[c])
                    extra["passthrough_to"] = name or f.texts[c]
                if it.trait and it.owner_kind == "impl":
                    extra["implements"] = it.trait
            if it.kind == "enum":
                extra["variants"] = it.variants
            public = it.public and not (it.owner_kind == "impl" and it.trait)
            add_item(path, tgt, it.kind, qual(path, it), it.vis, it.cfg, **extra)
            if public:
                pub_items += 1
                pub_params += len(it.params)
        stats[path] = {"pub_items": pub_items, "pub_params_total": pub_params}
    for path, line, col, kind, name, public, via in backend.generated_items():
        if not path.startswith("src/") or files[path].is_test_line(line):
            continue
        add_item(path, (path, line, col), kind, name, "pub" if public else "priv", via.cfg, macro_generated=via.name)
        if public:
            stats[path]["pub_items"] += 1

    graph = {m: set(d) for m, d in edges.items()}
    incoming: dict[str, Counter[str]] = defaultdict(Counter)
    for src, dsts in edges.items():
        for dst, n in dsts.items():
            incoming[dst][src] = n
    sccs = _sccs(graph)
    modules: dict[str, dict] = {}
    for path, f in sorted(files.items()):
        if not path.startswith("src/"):
            continue
        m = module_of(path)
        if not backend.file_active(path):
            continue
        st = stats[path]
        surface = st["pub_items"] + st["pub_params_total"]
        rec = {
            "path": path, **st, "impl_loc": f.loc, "test_loc": f.test_loc,
            "depth_proxy": round(f.loc / surface, 1) if surface else None,
            "fan_in": len(incoming.get(m, ())), "fan_out": len(edges.get(m, ())),
            "fan_in_modules": dict(incoming[m].most_common()) if m in incoming else {},
            "fan_out_modules": dict(edges[m].most_common()) if m in edges else {},
            "cycles": sorted(d for d in edges.get(m, ()) if m in edges.get(d, ())),
            "ext_deps": dict(ext[m].most_common()) if m in ext else {},
            "callees": [{"item": label(t), "id": f"{t[0]}:{t[1]}", "refs": n} for t, n in callees[m].most_common(40)] if m in callees else [],
        }
        if approx:
            rec["approx"] = ["fan_in", "fan_out", "fan_in_modules", "fan_out_modules", "cycles", "ext_deps", "callees"]
        modules[m] = rec

    traits = []
    impls_of: dict[Target, list[dict]] = defaultdict(list)
    for path, f in files.items():
        for it in f.items:
            if it.kind == "impl" and it.trait_pos and backend.active(f, it):
                who = backend.at(path, *it.self_pos) if it.self_pos else []
                for tgt in backend.at(path, *it.trait_pos):
                    impls_of[tgt].append({"type": label(who[0]) if len(who) == 1 else (it.self_pos and backend.describe(path, *it.self_pos)) or it.name, "at": f"{path}:{it.line}", "test": it.test or f.file_test})
    by_name: dict[str, list[Target]] = defaultdict(list)
    for tgt, (f, it) in live.items():
        if it.kind == "trait":
            by_name[it.name].append(tgt)
    macro_approx: set[Target] = set()
    for path, f in files.items():
        for it in f.items:
            if it.kind != "macro" or not it.generated_impl_of:
                continue
            calls = sites.get((path, it.line, it.col), []) if not approx else [
                (p, i.line, i.test or g.file_test) for p, g in files.items() for i in g.items if i.kind == "invoke" and i.name == it.name]
            for tname in it.generated_impl_of:
                cands = [t for t in by_name.get(tname, []) if t[0] == path] or by_name.get(tname, [])
                for tgt in cands:
                    if len(cands) > 1:
                        macro_approx.add(tgt)
                    for p, ln, test in calls:
                        impls_of[tgt].append({"type": f"via {it.name}!", "at": f"{p}:{ln}", "test": test})
    for tgt, (f, it) in sorted(live.items()):
        if it.kind != "trait" or it.test or not tgt[0].startswith("src/"):
            continue
        impls = sorted(impls_of.get(tgt, []), key=lambda i: i["at"])
        prod = [i for i in impls if not i["test"]]
        rec = {
            "id": f"{tgt[0]}:{tgt[1]}", "name": qual(tgt[0], it), "impls_prod": len(prod), "impls_test": len(impls) - len(prod),
            "hypothetical_seam": len(prod) == 1, "implementors": impls,
        }
        if approx or tgt in macro_approx:
            rec["approx"] = ["impls_prod", "impls_test", "hypothetical_seam", "implementors"]
        if approx:
            rec["ambiguous"] = len(by_name[it.name]) > 1
        traits.append(rec)

    matches: dict[Target, list[tuple[str, int, bool]]] = defaultdict(list)
    for path, f in files.items():
        for site in f.match_sites:
            enums: set[Target] = set()
            for a, b in site.patterns:
                for j in range(a, b):
                    if f.kinds[j] == "i" and f.texts[j] not in KEYWORDS:
                        enums.update(backend.variant_enum(path, f.lines[j], f.cols[j]))
            for e in enums:
                matches[e].append((path, site.line, f.is_test_line(site.line)))
    enum_matches = []
    for tgt, ms in matches.items():
        hit = live.get(tgt)
        if not hit or not tgt[0].startswith("src/"):
            continue
        prod = sorted((p, ln) for p, ln, test in ms if not test)
        rec = {
            "enum": qual(tgt[0], hit[1]), "id": f"{tgt[0]}:{tgt[1]}", "variants": hit[1].variants, "match_sites": len(prod),
            "test_sites": len(ms) - len(prod), "modules": sorted({module_of(p) for p, _ in prod}),
            "sites": [f"{p}:{ln}" for p, ln in prod[: config.SITES_KEPT]],
        }
        if approx:
            rec["approx"] = ["match_sites", "test_sites", "modules", "sites"]
        enum_matches.append(rec)
    enum_matches.sort(key=lambda r: (-r["match_sites"], r["enum"]))

    engine_leaks = [
        {"engine": e, "from": m, "refs": sum(c.values()), "names": dict(c.most_common(8)), **({"approx": ["refs", "names"]} if approx else {})}
        for (e, m), c in leaks.items()
    ]
    engine_leaks.sort(key=lambda r: (r["engine"], -r["refs"], r["from"]))
    return {
        "modules": modules, "items": items, "traits": traits, "cycles": sccs, "enum_matches": enum_matches, "engine_leaks": engine_leaks,
        "dup_groups": dup_groups(files, backend.active), "unresolved": backend.unresolved(inactive),
        "compiled_out": sorted(f"{p}:{s}" for p, spans in inactive.items() if p.startswith("src/") for s, _ in spans),
    }


def cache_key(root: Path, features: str, backend: str, ra_version: str) -> tuple[str, str, str]:
    """(head sha, key hash, dirty hash): the identity of one index-derived result."""
    sha = gitfacts.head(root)
    dirty = gitfacts.dirty_hash(root, config.INDEX_INPUTS)
    raw = "|".join([features, backend, ra_version if backend == "scip" else "", str(config.SCHEMA), dirty])
    return sha, hashlib.sha1(raw.encode()).hexdigest()[:10], dirty


def facts_path(out_dir: Path, sha: str, key: str) -> Path:
    """Where the facts of one cache key live."""
    return out_dir / f"{sha[:12]}-{key}.json"


def index_half(root: Path, features: str, backend: str, engines: dict, scip_path: Path | None, cargo_target: Path | None = None) -> tuple[dict, dict]:
    """(facts, timing) of the index-derived half for one backend."""
    timing: dict = {}
    t0 = time.monotonic()
    files = load_sources(root)
    timing["scan_s"] = round(time.monotonic() - t0, 1)
    if backend == "scip":
        cost = scip_path.with_suffix(".cost.json")
        if not scip_path.exists():
            cost.write_text(json.dumps(scip.run(root, features, scip_path, cargo_target)))
        timing["index"] = json.loads(cost.read_text()) if cost.exists() else None
        t1 = time.monotonic()
        index = scip.parse(scip_path.read_bytes())
        timing["parse_s"] = round(time.monotonic() - t1, 1)
        be = ScipBackend(root, files, index)
    else:
        be = NameBackend(root, files, features)
    t2 = time.monotonic()
    out = derive(root, files, be, engines)
    timing["derive_s"] = round(time.monotonic() - t2, 1)
    return out, timing


def feature_diff(mine: dict, other: dict, other_features: str) -> dict:
    """Items and modules that exist in one feature set and not in the other."""

    def only(a: dict, b: dict) -> list[dict]:
        rows = [a[k] for k in a.keys() - b.keys()]
        rows.sort(key=lambda r: (r["id"].rsplit(":", 1)[0], int(r["id"].rsplit(":", 1)[1])))
        return [{"id": r["id"], "name": r["name"], "kind": r["kind"]} for r in rows]

    return {
        "other": other_features,
        "only_here": only(mine["items"], other["items"]),
        "only_there": only(other["items"], mine["items"]),
        "modules_only_here": sorted(mine["modules"].keys() - other["modules"].keys()),
        "modules_only_there": sorted(other["modules"].keys() - mine["modules"].keys()),
    }


def collect(root: Path, features: str, backend: str, out_dir: Path, engines: dict | None = None, force: bool = False,
            git_root: Path | None = None) -> tuple[Path, dict]:
    """Collect (or reuse) the facts for one feature set and backend, write them, and return (path, facts)."""
    engines = config.ENGINES if engines is None else engines
    ra = scip.version() if backend == "scip" else ""
    if backend == "scip" and not ra:
        raise RuntimeError("rust-analyzer is not installed for this toolchain: run `rustup component add rust-analyzer rust-src`")
    sha, key, dirty = cache_key(root, features, backend, ra or "")
    path = facts_path(out_dir, sha, key)
    out_dir.mkdir(parents=True, exist_ok=True)
    cached = json.loads(path.read_text()) if path.exists() and not force else None
    if cached:
        facts = cached
        facts["meta"]["cache"] = "hit"
    else:
        raw = hashlib.sha1("|".join([features, ra or "", dirty]).encode()).hexdigest()[:10]
        scip_path = out_dir / f"{sha[:12]}-{raw}.scip"
        if force and scip_path.exists():
            scip_path.unlink()
        derived, timing = index_half(root, features, backend, engines, scip_path)
        facts = {
            "meta": {
                "schema": config.SCHEMA, "sha": sha, "dirty": bool(dirty), "features": features, "backend": backend,
                "backend_impl": "rust-analyzer scip" if backend == "scip" else "token-level scan, by-name resolution (no tree-sitter binding)",
                "approx": backend != "scip", "rust_analyzer": ra or None, "key": key, "cache": "miss", "timing": timing,
                "degraded": bool(derived["unresolved"]["degraded"]),
            },
            **derived,
        }
    groot = git_root or root
    facts["git"] = gitfacts.collect_git(groot, ".", config.GIT_WINDOW, config.COCHANGE_MAX_FILES, config.COCHANGE_MIN_TOGETHER)
    facts["adrs"] = gitfacts.adr_index(groot, groot / "docs" / "adr")
    facts["context_terms"] = gitfacts.context_terms(groot / "CONTEXT.md")
    for other_features in config.FEATURE_SETS:
        if other_features == features:
            continue
        _, okey, _ = cache_key(root, other_features, backend, ra or "")
        opath = facts_path(out_dir, sha, okey)
        if opath.exists():
            other = json.loads(opath.read_text())
            facts["feature_diff"] = feature_diff(facts, other, other_features)
            other["feature_diff"] = feature_diff(other, facts, features)
            opath.write_text(json.dumps(other, indent=1, sort_keys=True))
    path.write_text(json.dumps(facts, indent=1, sort_keys=True))
    return path, facts
