"""archfacts graded against a fixture crate with known ground truth, and against hand-verified facts about rivet."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

from . import collect as collector
from . import config, gitfacts, rustsyn, scip, views

FIXTURE = Path(__file__).resolve().parent / "fixture"
EXPECTED = Path(__file__).resolve().parent / "expected.yaml"
FIXTURE_ENGINES = {"oracle": {"paths": ["src/extra.rs"], "crates": []}}
SEGMENT = re.compile(r"([\w-]+)(?:\[([^\]]+)\])?")


class Checks:
    """Counts passed, failed and skipped assertions; a skip always names its reason."""

    def __init__(self) -> None:
        self.ok = 0
        self.failed: list[str] = []
        self.skipped: list[str] = []

    def eq(self, label: str, got, want) -> None:
        """One exact-value assertion."""
        if got == want:
            self.ok += 1
        else:
            self.failed.append(f"{label}: expected {want!r}, got {got!r}")

    def skip(self, label: str, reason: str) -> None:
        """Record a check that could not run."""
        self.skipped.append(f"{label} ({reason})")


def lookup(facts: dict, expr: str):
    """Resolve `traits[name=x::Y].impls_prod`-style paths; a `[k=v,k2=v2]` selector must match exactly one record."""
    cur = facts
    for seg in re.findall(r"[\w-]+(?:\[[^\]]+\])?", expr):
        key, sel = SEGMENT.fullmatch(seg).groups()
        cur = cur[key]
        if sel:
            want = dict(pair.split("=", 1) for pair in sel.split(","))
            rows = cur.values() if isinstance(cur, dict) else cur
            hits = [r for r in rows if all(str(r.get(k)) == v for k, v in want.items())]
            if len(hits) != 1:
                raise KeyError(f"{seg}: {len(hits)} records match")
            cur = hits[0]
    return cur


def unit(c: Checks) -> None:
    """The pieces that need neither an index nor the fixture."""
    occ = scip.encode_field(1, b"".join(scip.encode_varint(v) for v in (4, 7, 12))) + scip.encode_field(2, b"rust-analyzer cargo k 0.1.0 m/f().") + scip.encode_field(3, 1)
    sig = scip.encode_field(5, b"pub fn f()")
    sym = scip.encode_field(1, b"rust-analyzer cargo k 0.1.0 m/f().") + scip.encode_field(5, 17) + scip.encode_field(7, sig)
    doc = scip.encode_field(1, b"src/m.rs") + scip.encode_field(2, occ) + scip.encode_field(3, sym)
    meta = scip.encode_field(2, scip.encode_field(2, b"9.9"))
    idx = scip.parse(scip.encode_field(1, meta) + scip.encode_field(2, doc))
    c.eq("scip reader: tool version", idx.tool_version, "9.9")
    c.eq("scip reader: occurrence", idx.documents[0].occurrences, [(4, 7, 12, "rust-analyzer cargo k 0.1.0 m/f().", 1)])
    c.eq("scip reader: kind", idx.kinds, {"rust-analyzer cargo k 0.1.0 m/f().": 17})
    c.eq("scip reader: pub signature", idx.public, {"rust-analyzer cargo k 0.1.0 m/f()."})
    c.eq("scip reader: varint over one byte", scip._varint(scip.encode_varint(300), 0), (300, 2))
    c.eq("symbol name: method", scip.qualname("rust-analyzer cargo k 0.1.0 a/b/impl#[T]go()."), "k::a::b::T::go")
    c.eq("symbol name: trait impl", scip.qualname("rust-analyzer cargo k 0.1.0 a/impl#[T][`From<u8>`]from()."), "k::a::<T as From<u8>>::from")
    c.eq("symbol name: variant", scip.qualname("rust-analyzer cargo my-k 0.1.0 a/E#V#"), "my_k::a::E::V")

    f = rustsyn.File("src/x.rs", 'fn a<\'t>(s: &\'t str) -> char { /* /* nested */ } */ let _r = r#"} fn no()"#; \'}\' }\npub(crate) fn b() {}\n')
    c.eq("scanner: strings, chars and nested comments hide braces", [(i.name, i.vis, i.line, i.end_line) for i in f.items], [("a", "priv", 1, 1), ("b", "crate", 2, 2)])
    f = rustsyn.File("src/x.rs", "impl<T: Into<u8>> Tr<T> for Wrap<T> where T: Copy { fn m(&self, a: u8, (b, c): (u8, u8)) {} }")
    imp, m = f.items
    c.eq("scanner: impl header", (imp.trait, imp.name), ("Tr", "Wrap"))
    c.eq("scanner: params exclude self", (m.params, m.has_self, m.owner, m.trait), (["a", "c"], True, "Wrap", "Tr"))
    f = rustsyn.File("src/x.rs", "#[cfg(test)]\nmod t {\n fn h() {}\n}\n#[cfg(all(unix, feature = \"x\"))]\nfn g() {}\n#[tokio::test]\nasync fn it() {}\n")
    c.eq("scanner: test and cfg flags", [(i.name, i.test, i.cfg) for i in f.items],
         [("t", True, ()), ("h", True, ()), ("g", False, ('all(unix,feature="x")',)), ("it", True, ())])
    c.eq("scanner: test line ranges", (f.is_test_line(3), f.is_test_line(6), f.is_test_line(8)), (True, False, True))
    f = rustsyn.File("src/x.rs", "fn p(&self, a: u8) -> u8 { self.inner.go(a).await? }\nfn q(a: u8) -> u8 { go(a + 1) }\nfn r(a: u8) -> W { W(a) }\n")
    c.eq("scanner: pass-through shape", [f.texts[t] if (t := f.passthrough_callee(i)) is not None else None for i in f.items], ["go", None, None])
    f = rustsyn.File("src/x.rs", "fn m(e: E) -> u8 { match e { E::A | E::B if x => 1, E::C(v) => { v } _ => 0 } }")
    c.eq("scanner: match arm patterns stop at the guard", [" ".join(f.texts[a:b]) for a, b in f.match_sites[0].patterns], ["E :: A | E :: B", "E :: C ( v )", "_"])

    c.eq("module id", [collector.module_of(p) for p in ("src/lib.rs", "src/a/mod.rs", "src/a/b.rs", "tests/t.rs")], ["crate", "a", "a::b", "tests::t"])
    c.eq("cfg: feature off", collector.cfg_active(('feature="oracle"',), ("jemalloc",)), False)
    c.eq("cfg: feature on", collector.cfg_active(('feature="oracle"',), ("jemalloc", "oracle")), True)
    c.eq("cfg: negated feature", collector.cfg_active(('not(feature="oracle")',), ("oracle",)), False)
    c.eq("cfg: unknown predicate counts as on", collector.cfg_active(("unix",), ()), True)
    rows = {"t": [{"a": "x", "b": "y", "n": 1}, {"a": "x", "b": "z", "n": 2}]}
    c.eq("lookup: two-key selector", lookup(rows, "t[a=x,b=z].n"), 2)
    try:
        lookup(rows, "t[a=x].n")
        c.eq("lookup: an ambiguous selector is an error", "no error", "KeyError")
    except KeyError:
        c.ok += 1
    c.eq("sccs", collector._sccs({"a": {"b"}, "b": {"a", "c"}, "c": set(), "d": {"d"}}), [["a", "b"]])

    wide = "\n".join(f"1\t1\tw{n}.rs" for n in range(31))
    log = f"@c1\n3\t1\ta.rs\n2\t0\tb.rs\n@c2\n1\t1\ta.rs\n1\t0\tb.rs\n@c3\n5\t5\ta.rs\n@c4\n1\t0\ta.rs\n1\t0\tb.rs\n{wide}\n"
    g = gitfacts.churn_and_cochange(gitfacts.parse_log(log), 30, 2)
    c.eq("git: churn counts every commit", g["churn"][0], {"path": "a.rs", "commits": 4, "lines": 17})
    c.eq("git: a commit wider than the cap adds no pair", (g["wide_commits_skipped"], g["cochange"]),
         (1, [{"a": "a.rs", "b": "b.rs", "together": 2, "a_commits": 4, "b_commits": 3, "confidence": 0.67}]))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "docs" / "adr").mkdir(parents=True)
        (root / "src").mkdir()
        (root / "src" / "a.rs").write_text("")
        (root / "docs" / "adr" / "0007-x.md").write_text("# ADR-0007: A title\n\n**Status**: Accepted  \n\nSee `src/a.rs`, `a.rs` and `src/missing.rs`.\n")
        (root / "CONTEXT.md").write_text("## Sec\n\n**Term**:\nText.\n_Avoid_: other\n")
        c.eq("adr index", gitfacts.adr_index(root, root / "docs" / "adr"),
             [{"file": "docs/adr/0007-x.md", "number": "0007", "title": "A title", "status": "Accepted", "paths": ["src/a.rs"]}])
        c.eq("glossary", gitfacts.context_terms(root / "CONTEXT.md"), [{"term": "Term", "section": "Sec", "avoid": "other"}])


def item(facts: dict, name: str) -> dict:
    """The one item with this qualified name."""
    return lookup(facts, f"items[name={name}]")


def fixture_common(c: Checks, tag: str, d: dict, nd: dict) -> None:
    """Facts both backends must get right on the fixture: they need no name resolution."""
    c.eq(f"{tag}: test-only function has no production call site", (item(d, "alpha::only_tested")["call_sites"], item(d, "alpha::only_tested")["test_refs"]), (0, 2))
    c.eq(f"{tag}: unambiguous free function", (item(d, "alpha::alpha_leaf")["call_sites"], item(d, "alpha::alpha_leaf")["callers_modules"]), (3, ["beta", "facade"]))
    c.eq(f"{tag}: pass-through to a free function", item(d, "facade::forward")["passthrough_to"], "alpha::alpha_leaf")
    c.eq(f"{tag}: a call with a computed argument is not a pass-through", "passthrough_to" in item(d, "beta::use_gadget"), False)
    c.eq(f"{tag}: duplicate bodies", [(g["kind"], [m["name"] for m in g["members"]]) for g in d["dup_groups"]],
         [("near", ["shapes::dup_one", "shapes::dup_two", "shapes::dup_renamed", "shapes::dup_near"]),
          ("renamed", ["shapes::dup_one", "shapes::dup_two", "shapes::dup_renamed"]), ("renamed", ["beta::clamp_low", "beta::clamp_high"]),
          ("exact", ["shapes::dup_one", "shapes::dup_two"])])
    c.eq(f"{tag}: two-module cycle", (d["modules"]["beta"]["cycles"], d["cycles"]), (["alpha"], [["alpha", "beta", "crate", "shapes"]]))
    c.eq(f"{tag}: trait with two implementors", (lookup(d, "traits[name=beta::Solo].impls_prod"), lookup(d, "traits[name=beta::Solo].hypothetical_seam")), (2, False))
    c.eq(f"{tag}: impl_loc excludes test lines", (d["modules"]["alpha"]["impl_loc"], d["modules"]["alpha"]["test_loc"]), (37, 8))
    c.eq(f"{tag}: pub_params_total", (d["modules"]["facade"]["pub_items"], d["modules"]["facade"]["pub_params_total"]), (7, 6))
    c.eq(f"{tag}: depth_proxy is impl lines per unit of interface", d["modules"]["facade"]["depth_proxy"], round(38 / 13, 1))
    c.eq(f"{tag}: engine leak", [(e["engine"], e["from"], e["refs"], e["names"]) for e in d["engine_leaks"]], [("oracle", "facade", 1, {"extra::OraThing": 1})])
    c.eq(f"{tag}: no engine leak without the feature", nd["engine_leaks"], [])
    c.eq(f"{tag}: cfg(feature) module is absent without the feature", ("extra" in d["modules"], "extra" in nd["modules"]), (True, False))
    c.eq(f"{tag}: cfg(feature) items are compiled out without the feature", nd["compiled_out"], ["src/extra.rs:1", "src/extra.rs:3", "src/facade.rs:44"])
    fd = collector.feature_diff(d, nd, "no-default-jemalloc")
    c.eq(f"{tag}: feature diff", ([i["name"] for i in fd["only_here"]], fd["only_there"], fd["modules_only_here"]),
         (["extra::OraThing", "extra::extra_only", "facade::leak"], [], ["extra"]))


def fixture_scip(c: Checks, d: dict) -> None:
    """Exact ground truth where spelling is not enough."""
    c.eq("scip: same-named methods are told apart", (item(d, "alpha::Widget::render")["call_sites"], item(d, "alpha::Gadget::render")["call_sites"]), (4, 1))
    c.eq("scip: method callers", (item(d, "alpha::Widget::render")["callers_modules"], item(d, "alpha::Gadget::render")["callers_modules"]), (["beta", "facade"], ["beta"]))
    c.eq("scip: pass-through to a method names its type", item(d, "facade::forward_method")["passthrough_to"], "alpha::Widget::render")
    c.eq("scip: trait with one production impl", [(t["name"], t["impls_prod"], t["impls_test"], t["hypothetical_seam"]) for t in d["traits"]],
         [("beta::Solo", 2, 0, False), ("shapes::Shape", 3, 0, False), ("shapes::Solo", 1, 1, True)])
    c.eq("scip: macro-generated impl is counted", [i["type"] for i in lookup(d, "traits[name=shapes::Shape].implementors")], ["shapes::Square", "shapes::Circle", "via impl_shape!"])
    c.eq("scip: macro-generated function is an item", {k: item(d, "alpha::generated")[k] for k in ("vis", "macro_generated", "call_sites", "callers_modules")},
         {"vis": "pub", "macro_generated": "make_fn", "call_sites": 1, "callers_modules": ["beta"]})
    c.eq("scip: pub_items counts the macro-generated function", d["modules"]["alpha"]["pub_items"], 9)
    c.eq("scip: a `pub use` re-export resolves to the defining module", (item(d, "alpha::Widget")["call_sites"], item(d, "alpha::Widget")["callers_modules"]), (5, ["alpha", "beta", "facade", "shapes"]))
    c.eq("scip: cycle through the re-export", (d["modules"]["alpha"]["cycles"], d["modules"]["shapes"]["fan_out_modules"]), (["beta", "shapes"], {"alpha": 2}))
    c.eq("scip: fan in and out", (d["modules"]["alpha"]["fan_in"], d["modules"]["alpha"]["fan_out"], d["modules"]["facade"]["fan_out_modules"]), (4, 2, {"alpha": 4, "beta": 1, "extra": 1}))
    c.eq("scip: enum match sites see glob-imported variants", [(e["enum"], e["match_sites"], e["sites"]) for e in d["enum_matches"]], [("facade::Mode", 2, ["src/facade.rs:18", "src/facade.rs:27"])])
    c.eq("scip: a same-named function of a test crate is another function", (item(d, "crate::version")["callers_modules"], item(d, "crate::version")["test_refs"]), (["beta", "bin::one"], 0))
    c.eq("scip: same-named functions of two binaries are told apart", (item(d, "bin::one::helper")["call_sites"], item(d, "bin::two::helper")["call_sites"]), (1, 1))
    c.eq("scip: nothing is marked approx", [k for k, i in d["items"].items() if "approx" in i] + [t["name"] for t in d["traits"] if "approx" in t], [])
    c.eq("scip: every reference resolves", (d["unresolved"]["unresolved"], d["unresolved"]["degraded"]), (0, False))


def fixture_name(c: Checks, d: dict) -> None:
    """The approximate backend: ambiguous answers must be marked, and the known-wrong ones are pinned as wrong."""
    w, g = item(d, "alpha::Widget::render"), item(d, "alpha::Gadget::render")
    c.eq("treesitter: same-named methods collapse (wrong: truth is 4 and 1)", (w["call_sites"], g["call_sites"]), (5, 5))
    c.eq("treesitter: ambiguous method is marked", (w["ambiguous"], "call_sites" in w["approx"], "callers_modules" in w["approx"], "test_refs" in w["approx"]), (True, True, True, True))
    fm = item(d, "facade::forward_method")
    c.eq("treesitter: pass-through target is a guess", (fm["passthrough_to"], "passthrough_to" in fm["approx"]), ("alpha::Gadget::render | alpha::Widget::render", True))
    solo = lookup(d, "traits[name=shapes::Solo]")
    c.eq("treesitter: same-named traits collapse (wrong: truth is 1 impl, a hypothetical seam)", (solo["impls_prod"], solo["hypothetical_seam"]), (2, False))
    c.eq("treesitter: trait counts are marked", (solo["ambiguous"], "impls_prod" in solo["approx"], "hypothetical_seam" in solo["approx"]), (True, True, True))
    c.eq("treesitter: every trait is marked approx", [t["name"] for t in d["traits"] if "impls_prod" not in t.get("approx", [])], [])
    c.eq("treesitter: macro-generated function is invisible (wrong: it exists)", [i["name"] for i in d["items"].values() if i["name"].endswith("generated")], [])
    e = d["enum_matches"][0]
    c.eq("treesitter: glob-imported variants are missed (wrong: truth is 2)", (e["match_sites"], "match_sites" in e["approx"]), (1, True))
    c.eq("treesitter: module graph is marked", sorted(d["modules"]["alpha"]["approx"]), ["callees", "cycles", "ext_deps", "fan_in", "fan_in_modules", "fan_out", "fan_out_modules"])
    c.eq("treesitter: every item is marked approx", [i["id"] for i in d["items"].values() if "call_sites" not in i.get("approx", [])], [])
    c.eq("treesitter: unresolved count is unknown, not zero", (d["unresolved"]["unresolved"], d["unresolved"]["approx"]), (None, True))


def fixture_views(c: Checks, tag: str, d: dict) -> None:
    """The views render from a facts dict alone and stay inside the line budget."""
    facts = {
        "meta": {"sha": "0" * 40, "dirty": False, "features": "default", "backend": tag, "backend_impl": "x", "approx": tag != "scip", "degraded": False},
        "git": gitfacts.churn_and_cochange(gitfacts.parse_log("@c\n1\t1\tsrc/alpha.rs\n1\t1\ttests/it.rs\n" * 3), 30, 3), "adrs": [], "context_terms": [], **d,
    }
    text = views.architect(facts, "hotspots", 500)
    c.eq(f"{tag}: digest stays within the line budget", len(text.splitlines()) <= views.MAX_LINES, True)
    c.eq(f"{tag}: digest never calls the proxy depth", ("depth_proxy" in text, re.search(r"\bdepth [0-9]", text)), (True, None))
    c.eq(f"{tag}: digest lists the co-change pair", "`src/alpha.rs` + `tests/it.rs` — together 3 of 3/3 commits, confidence 1.0" in text, True)
    c.eq(f"{tag}: a lens narrows the sections to its paths", "src/alpha.rs" in views.architect({**facts, "git": facts["git"]}, "harness", 5), True)
    c.eq(f"{tag}: zoom lists public items and caller modules", ("fn `alpha::alpha_leaf` (src/alpha.rs:27) — 3 / 2 (2 outside) / 0" in views.zoom(facts, "src/alpha.rs"), "- `beta` —" in views.zoom(facts, "src/alpha.rs")), (True, True))
    diag = views.diagnose(facts, "only_tested")
    c.eq(f"{tag}: diagnose lists test references", ("0 production call sites" in diag, "tests/it.rs:3" in diag, "src/alpha.rs:55 in `alpha::tests::only_tested_adds_seven`" in diag), (True, True, True))
    wide = {**facts, "git": gitfacts.churn_and_cochange(gitfacts.parse_log("".join(f"@c{n}\n1\t1\tsrc/f{n}.rs\n" for n in range(400))), 30, 3)}
    long = views.architect(wide, "hotspots", 500).splitlines()
    c.eq(f"{tag}: an oversized digest is cut at the budget and says so", (len(long), long[-1].startswith("(truncated at 300 lines")), (views.MAX_LINES, True))
    if tag == "scip":
        c.eq("scip: the single-implementor trait is flagged in the digest", "`shapes::Solo` (src/shapes.rs:5) — ONE production impl (hypothetical seam), 1 test impls" in text, True)
    else:
        c.eq("treesitter: digest rows carry the approx marker", ("~approx" in text, "APPROXIMATE backend" in text), (True, True))


def degraded(c: Checks) -> None:
    """An index that resolves nothing must say `degraded`, not report zero callers as a fact."""
    files = collector.load_sources(FIXTURE)
    empty = scip.Index(documents=[scip.Document(p) for p in files])
    d = collector.derive(FIXTURE, files, collector.ScipBackend(FIXTURE, files, empty), FIXTURE_ENGINES)
    c.eq("an index without occurrences is degraded", (d["unresolved"]["degraded"], d["unresolved"]["ratio"]), (True, 1.0))


def rivet_expected(c: Checks, root: Path, out: Path) -> None:
    """expected.yaml: hand-verified facts about rivet; any drift fails until a human re-verifies and edits the file."""
    import yaml

    for features in config.FEATURE_SETS:
        collector.collect(root, features, "scip", out)
    cache = {features: collector.collect(root, features, "scip", out)[1] for features in config.FEATURE_SETS}
    for row in yaml.safe_load(EXPECTED.read_text()):
        features = row.get("features", "default")
        if row.get("rev"):
            log = gitfacts.git(root, "log", row["rev"], f"-{config.GIT_WINDOW}", "--numstat", "--format=@%H", "--no-renames", "--", ".")
            facts = {"git": gitfacts.churn_and_cochange(gitfacts.parse_log(log), config.COCHANGE_MAX_FILES, config.COCHANGE_MIN_TOGETHER)}
        else:
            facts = cache[features]
        try:
            got = lookup(facts, row["fact"])
        except (KeyError, TypeError) as e:
            got = f"<missing: {e}>"
        c.eq(f"expected.yaml: {row['fact']} [{features}]", got, row["value"])
    for features, facts in cache.items():
        c.eq(f"rivet index is not degraded [{features}]", facts["meta"]["degraded"], False)


def run(root: Path, out: Path, rivet: bool = False, require_index: bool = False, no_index: bool = False) -> int:
    """Run every check that can run here; exit 1 on any failure, and on a skip when the index is required."""
    c = Checks()
    unit(c)
    degraded(c)
    name = {f: collector.index_half(FIXTURE, f, "treesitter", FIXTURE_ENGINES, None)[0] for f in config.FEATURE_SETS}
    fixture_common(c, "treesitter", name["default"], name["no-default-jemalloc"])
    fixture_name(c, name["default"])
    fixture_views(c, "treesitter", name["default"])
    indexer = None if no_index else scip.version()
    why = "--no-index" if no_index else "rust-analyzer is not installed: rustup component add rust-analyzer rust-src"
    if indexer:
        with tempfile.TemporaryDirectory() as tmp:
            exact = {f: collector.index_half(FIXTURE, f, "scip", FIXTURE_ENGINES, Path(tmp) / f"{f}.scip", Path(tmp) / "target")[0] for f in config.FEATURE_SETS}
        fixture_common(c, "scip", exact["default"], exact["no-default-jemalloc"])
        fixture_scip(c, exact["default"])
        fixture_views(c, "scip", exact["default"])
        if rivet:
            rivet_expected(c, root, out)
    else:
        c.skip("fixture on the scip backend, both feature sets", why)
        if rivet:
            c.skip("expected.yaml against rivet", why)
    if not rivet:
        c.skip("expected.yaml against rivet", "pass --rivet; it indexes the whole crate")
    for line in c.failed:
        print(f"FAIL {line}")
    for line in c.skipped:
        print(f"SKIP {line}")
    verdict = "FAILED" if c.failed or (require_index and not indexer) else "ok"
    print(f"archfacts selftest: {verdict} — {c.ok} passed, {len(c.failed)} failed, {len(c.skipped)} skipped")
    return 0 if verdict == "ok" else 1
