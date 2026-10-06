"""Markdown views over one facts file; nothing here recomputes a fact."""

from __future__ import annotations

import re

MAX_LINES = 300
CODE = re.compile(r"\.(rs|py)$")

# A lens orders the sections and may narrow every section to paths matching a pattern.
LENSES: dict[str, tuple[tuple[str, ...], str]] = {
    "hotspots": (("churn", "cochange", "cycles", "depth", "seams", "passthrough", "dups", "enums", "leaks", "adrs", "terms"), ""),
    "seams": (("seams", "leaks", "passthrough", "enums", "depth", "cycles", "dups", "churn", "cochange", "adrs", "terms"), ""),
    "cdc": (("churn", "seams", "enums", "dups", "cochange", "passthrough", "leaks", "depth", "cycles", "adrs", "terms"), r"cdc"),
    "load": (("churn", "seams", "enums", "dups", "cochange", "passthrough", "depth", "cycles", "leaks", "adrs", "terms"), r"(^|/)(load|types)(/|\.rs)"),
    "runners": (("churn", "dups", "enums", "cochange", "seams", "passthrough", "depth", "cycles", "leaks", "adrs", "terms"), r"(^|/)pipeline(/|\.rs)"),
    "state-config-cli": (("churn", "depth", "passthrough", "enums", "cochange", "dups", "seams", "cycles", "leaks", "adrs", "terms"), r"(^|/)(state|config|cli)(/|\.rs)"),
    "harness": (("churn", "cochange", "adrs", "terms"), r"^(tests|dev|\.github)/"),
}


def _flag(rec: dict) -> str:
    """The marker a row carries when its numbers are approximate."""
    return " ~approx" if rec.get("approx") else ""


def _area(module: str) -> str:
    """The top-level area of a module id."""
    return module.split("::")[0]


def _module_path(facts: dict) -> dict[str, str]:
    """Module id -> source path."""
    return {m: r["path"] for m, r in facts["modules"].items()}


def _sections(facts: dict, keep) -> dict[str, tuple[str, list[str]]]:
    """Every section as (heading, rows), rows already ordered by relevance."""
    mods = facts["modules"]
    by_path = {r["path"]: (m, r) for m, r in mods.items()}
    out: dict[str, tuple[str, list[str]]] = {}

    rows = []
    for c in facts["git"]["churn"]:
        if not CODE.search(c["path"]) or not keep(c["path"]):
            continue
        m = by_path.get(c["path"])
        extra = f" | impl_loc {m[1]['impl_loc']}, fan_in {m[1]['fan_in']}, fan_out {m[1]['fan_out']}" if m else ""
        rows.append(f"- `{c['path']}` — {c['commits']} commits, {c['lines']} lines changed{extra}")
    out["churn"] = (f"Churn: files by commits touching them, last {facts['git']['commits']} commits (`facts:git.churn`)", rows)

    rows = []
    for p in facts["git"]["cochange"]:
        a, b = p["a"], p["b"]
        if not (CODE.search(a) and CODE.search(b)) or a.rsplit("/", 1)[0] == b.rsplit("/", 1)[0] or not (keep(a) or keep(b)):
            continue
        rows.append(f"- `{a}` + `{b}` — together {p['together']} of {p['a_commits']}/{p['b_commits']} commits, confidence {p['confidence']}")
    out["cochange"] = (f"Co-change: file pairs in different directories (commits wider than {facts['git']['max_files']} files skipped; `facts:git.cochange`)", rows)

    rows = []
    paths = _module_path(facts)
    pairs = []
    for m, r in mods.items():
        for d in r["cycles"]:
            if m < d and d in mods:
                pairs.append((min(r["fan_out_modules"].get(d, 0), mods[d]["fan_out_modules"].get(m, 0)), m, d))
    for comp in facts["cycles"]:
        rows.append(f"- strongly connected component of {len(comp)} modules: " + ", ".join(comp[:8]) + (" ..." if len(comp) > 8 else ""))
    for w, a, b in sorted(pairs, key=lambda p: (_area(p[1]) == _area(p[2]), -p[0], p[1])):
        if keep(paths[a]) or keep(paths[b]):
            rows.append(f"- `{a}` <-> `{b}` — {mods[a]['fan_out_modules'].get(b, 0)} refs one way, {mods[b]['fan_out_modules'].get(a, 0)} back{_flag(mods[a])}")
    out["cycles"] = ("Cycles: mutually dependent modules, cross-area pairs first (`facts:cycles`, `facts:modules.<m>.cycles`)", rows)

    rows = []
    ranked = sorted((r["depth_proxy"], m) for m, r in mods.items() if r["depth_proxy"] is not None and r["impl_loc"] >= 40 and keep(r["path"]))
    for dp, m in ranked:
        r = mods[m]
        rows.append(f"- `{r['path']}` — depth_proxy {dp} ({r['impl_loc']} impl lines / {r['pub_items']} pub items + {r['pub_params_total']} params), "
                    f"fan_in {r['fan_in']}, fan_out {r['fan_out']}{_flag(r)}")
    out["depth"] = ("Modules by depth_proxy, lowest first — a PROXY (implementation lines per unit of interface), not depth (`facts:modules.<m>.depth_proxy`)", rows)

    rows = []
    for t in sorted(facts["traits"], key=lambda t: (not t["hypothetical_seam"], t["impls_prod"], t["name"])):
        if not keep(t["id"]) and not any(keep(i["at"]) for i in t["implementors"]):
            continue
        who = ", ".join(f"{i['type']} ({i['at']})" for i in t["implementors"] if not i["test"])
        mark = "ONE production impl (hypothetical seam)" if t["hypothetical_seam"] else f"{t['impls_prod']} production impls"
        rows.append(f"- `{t['name']}` ({t['id']}) — {mark}, {t['impls_test']} test impls: {who}{_flag(t)}")
    out["seams"] = ("Traits and their implementors, single-implementor traits first (`facts:traits`)", rows)

    rows = []
    passes = [i for i in facts["items"].values() if i.get("passthrough_to") and i["vis"] != "priv" and keep(i["id"])]
    for i in sorted(passes, key=lambda i: (-i["call_sites"], i["id"])):
        rows.append(f"- `{i['name']}` ({i['id']}) -> `{i['passthrough_to']}` — {i['call_sites']} call sites from {len(i['callers_modules'])} modules{_flag(i)}")
    out["passthrough"] = ("Pass-throughs: public functions whose body is one call forwarding their own parameters (`facts:items.<id>.passthrough_to`)", rows)

    rows = []
    for g in facts["dup_groups"]:
        if any(keep(m["id"]) for m in g["members"]):
            rows.append(f"- {g['kind']} x{len(g['members'])}, {g['tokens']} tokens, similarity {g['similarity']}: " + "; ".join(f"`{m['name']}` ({m['id']})" for m in g["members"][:6]))
    out["dups"] = ("Duplicate function bodies by normalised tokens (`facts:dup_groups`)", rows)

    rows = []
    for e in facts["enum_matches"]:
        if e["match_sites"] >= 2 and (keep(e["id"]) or any(keep(s) for s in e["sites"])):
            rows.append(f"- `{e['enum']}` ({e['id']}, {e['variants']} variants) — {e['match_sites']} match sites in {len(e['modules'])} modules{_flag(e)}")
    out["enums"] = ("Enums by number of production `match` sites (`facts:enum_matches`)", rows)

    rows = []
    for leak in facts["engine_leaks"]:
        names = ", ".join(f"{n} x{c}" for n, c in list(leak["names"].items())[:4])
        if keep(paths.get(leak["from"], leak["from"])):
            rows.append(f"- {leak['engine']} named from `{leak['from']}` — {leak['refs']} refs: {names}{_flag(leak)}")
    out["leaks"] = ("Engine-specific types and driver crates referenced outside the engine's own modules (`facts:engine_leaks`)", rows)

    rows = [f"- {a['number']} {a['title']} [{a['status'] or 'no status line'}] — {len(a['paths'])} paths: " + ", ".join(a["paths"][:4]) for a in facts["adrs"]
            if not a["paths"] or any(keep(p) for p in a["paths"]) or keep("")]
    out["adrs"] = ("ADR index (`facts:adrs`)", rows)

    by_section: dict[str, list[str]] = {}
    for t in facts["context_terms"]:
        by_section.setdefault(t["section"], []).append(t["term"])
    out["terms"] = ("CONTEXT.md glossary terms (`facts:context_terms`)", [f"- {s}: " + ", ".join(ts) for s, ts in by_section.items()])
    return out


def architect(facts: dict, lens: str, top: int = 15) -> str:
    """The digest an architecture review starts from: the lens orders the sections and sizes them."""
    if lens not in LENSES:
        raise SystemExit(f"unknown lens {lens!r}; choose one of: {', '.join(LENSES)}")
    order, pattern = LENSES[lens]
    rx = re.compile(pattern) if pattern else None
    sections = _sections(facts, (lambda p: bool(rx.search(p))) if rx else (lambda p: True))
    meta, un = facts["meta"], facts["unresolved"]
    lines = [
        f"# archfacts digest — lens `{lens}`",
        "",
        f"- commit `{meta['sha'][:12]}`{' (DIRTY tree)' if meta['dirty'] else ''}, features `{meta['features']}`, backend `{meta['backend']}` ({meta['backend_impl']})",
    ]
    if meta["approx"]:
        lines.append("- APPROXIMATE backend: every row marked `~approx` is resolved by spelling, not by type; do not cite it as a fact")
    else:
        lines.append(f"- unresolved references: {un['unresolved']} of {un['identifiers']} identifiers ({un['ratio']:.2%}, threshold {un['threshold']:.0%})"
                     + (" — DEGRADED: treat caller and implementor counts as lower bounds" if meta["degraded"] else ""))
    diff = facts.get("feature_diff")
    if diff:
        lines.append(f"- feature diff vs `{diff['other']}`: {len(diff['only_here'])} items and {len(diff['modules_only_here'])} modules exist only here, "
                     f"{len(diff['only_there'])} only there (`facts:feature_diff`)")
    else:
        lines.append("- feature diff: not available (collect the other feature set too)")
    lines += [
        f"- {len(facts['modules'])} modules, {len(facts['items'])} items, {len(facts['traits'])} traits" + (f"; narrowed to paths matching `{pattern}`" if pattern else ""),
        "- These are facts, not findings. Cite a row as `facts:<field>` and still open every file:line yourself.",
        "",
    ]
    for n, name in enumerate(order):
        heading, rows = sections[name]
        limit = top if n < 3 else max(4, top // 2)
        lines.append(f"## {heading}")
        lines += rows[:limit] if rows else ["- none"]
        if len(rows) > limit:
            lines.append(f"- ... {len(rows) - limit} more (raise `--top`)")
        lines.append("")
    if len(lines) > MAX_LINES:
        lines = lines[: MAX_LINES - 1] + [f"(truncated at {MAX_LINES} lines; lower `--top` or read the JSON)"]
    return "\n".join(lines)


def zoom(facts: dict, path: str) -> str:
    """Public items, caller modules and callees of one file or directory."""
    path = path.rstrip("/")
    inside = {m: r for m, r in facts["modules"].items() if r["path"] == path or r["path"].startswith(path + "/")}
    if not inside:
        return f"no module under `{path}` in these facts (features `{facts['meta']['features']}`)"
    lines = [f"# archfacts zoom — `{path}` ({facts['meta']['backend']}, `{facts['meta']['features']}`, {facts['meta']['sha'][:12]})", ""]
    callers: dict[str, int] = {}
    callees: dict[str, tuple[str, int]] = {}
    for m, r in inside.items():
        for src, n in r["fan_in_modules"].items():
            if src not in inside:
                callers[src] = callers.get(src, 0) + n
        for c in r["callees"]:
            if facts["modules"].get(_owner(facts, c["id"])) is None or _owner(facts, c["id"]) not in inside:
                name, n = callees.get(c["id"], (c["item"], 0))
                callees[c["id"]] = (name, n + c["refs"])
    lines.append(f"## Modules ({len(inside)})")
    for m, r in sorted(inside.items()):
        lines.append(f"- `{r['path']}` — {r['pub_items']} pub items, {r['impl_loc']} impl lines, depth_proxy {r['depth_proxy']}, fan_in {r['fan_in']}, fan_out {r['fan_out']}{_flag(r)}")
    lines += ["", "## Public items (call sites / caller modules / test refs)"]
    items = [i for i in facts["items"].values() if i["module"] in inside and i["vis"] != "priv"]
    for i in sorted(items, key=lambda i: (-i["call_sites"], i["id"]))[:120]:
        outside = [c for c in i["callers_modules"] if c not in inside]
        tail = f" -> passes through to `{i['passthrough_to']}`" if i.get("passthrough_to") else ""
        lines.append(f"- {i['kind']} `{i['name']}` ({i['id']}) — {i['call_sites']} / {len(i['callers_modules'])} ({len(outside)} outside) / {i['test_refs']}{tail}{_flag(i)}")
    lines += ["", "## Direct caller modules (references into this path)"]
    lines += [f"- `{m}` — {n}" for m, n in sorted(callers.items(), key=lambda kv: -kv[1])[:40]] or ["- none"]
    lines += ["", "## Callees outside this path (references out)"]
    lines += [f"- `{name}` ({cid}) — {n}" for cid, (name, n) in sorted(callees.items(), key=lambda kv: -kv[1][1])[:40]] or ["- none"]
    return "\n".join(lines)


def _owner(facts: dict, item_id: str) -> str:
    """The module id that owns an item id (`path:line`)."""
    path = item_id.rsplit(":", 1)[0]
    return next((m for m, r in facts["modules"].items() if r["path"] == path), "")


def diagnose(facts: dict, name: str) -> str:
    """Callers and test references of every function the name matches."""
    hits = [i for i in facts["items"].values() if i["kind"] == "fn" and (i["name"] == name or i["name"].endswith("::" + name) or i["id"] == name)]
    if not hits:
        return f"no function named `{name}` in these facts (features `{facts['meta']['features']}`)"
    lines = [f"# archfacts diagnose — `{name}` ({facts['meta']['backend']}, `{facts['meta']['features']}`, {facts['meta']['sha'][:12]})", ""]
    for i in sorted(hits, key=lambda i: i["id"]):
        lines.append(f"## `{i['name']}` ({i['id']}){_flag(i)}")
        lines.append(f"- {i['vis']} fn, {i.get('params', 0)} params, {i.get('loc', 0)} lines; {i['call_sites']} production call sites from {len(i['callers_modules'])} modules; {i['test_refs']} test references")
        if i.get("passthrough_to"):
            lines.append(f"- passes through to `{i['passthrough_to']}`")
        if i.get("implements"):
            lines.append(f"- implements trait `{i['implements']}`: calls through the trait are counted on the trait method, not here")
        for label, test in (("Production callers", False), ("Test references", True)):
            rows = [c for c in i["callers"] if c["test"] == test]
            lines.append(f"### {label} ({len(rows)})")
            lines += [f"- {c['at']} in `{c['in']}`" for c in rows[:60]] or ["- none"]
        lines.append("")
    return "\n".join(lines)


def diff(facts: dict) -> str:
    """Items and modules that exist in only one of the two feature sets."""
    d = facts.get("feature_diff")
    if not d:
        return "no feature diff in these facts: collect both feature sets on the same commit"
    here = facts["meta"]["features"]
    lines = [f"# archfacts feature diff — `{here}` vs `{d['other']}`", ""]
    for title, mods, items in ((f"Only with `{here}`", d["modules_only_here"], d["only_here"]), (f"Only with `{d['other']}`", d["modules_only_there"], d["only_there"])):
        lines.append(f"## {title}: {len(mods)} modules, {len(items)} items")
        lines += [f"- module `{m}`" for m in mods]
        lines += [f"- {i['kind']} `{i['name']}` ({i['id']})" for i in items[:80]]
        if len(items) > 80:
            lines.append(f"- ... {len(items) - 80} more")
        lines.append("")
    return "\n".join(lines)
