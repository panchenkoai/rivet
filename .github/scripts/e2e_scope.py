#!/usr/bin/env python3
"""Which E2E a pull request needs: `skip` when every changed path is one the E2E lane never reads, `select` (only the live modules that exercise the changed engine, target or destination) when every changed source path has an owner, else `full`.

    e2e_scope.py --base SHA [--head SHA]   print `scope=full|skip` and `reason=…` (for $GITHUB_OUTPUT)
    e2e_scope.py --self-test

Conservative by construction: a path is skippable only when it is documentation (`*.md`,
`changelog.d/`, `docs/gifs/`) that no test or source file reads, or harness code under a
prefix the E2E lane never imports or calls (`UNUSED_BY_E2E`). Anything else, an unclassified
path included, and an empty diff, selects the full E2E. Which docs files are read is derived
from the tree on every run (string literals and `include_str!` under `src/` and `tests/`), never
from a hand list.
"""

from __future__ import annotations

import functools
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: Harness prefixes the E2E lane neither imports nor calls (the self-test greps the lane and the tests for them).
UNUSED_BY_E2E = ("dev/seeded/", ".github/scripts/", "dev/spikes/")
#: Single files only the mutation and the offline lanes read.
UNUSED_FILES = (".cargo/mutants.toml",)
#: Files under those prefixes that still decide E2E: this script itself.
ALWAYS_FULL = (".github/scripts/e2e_scope.py", ".github/scripts/e2e_legs.py")
#: A string literal or `include_str!` that names a file under docs/ (or a top-level *.md).
READ_DOC = re.compile(r'"((?:\.\./)*(?:docs/[A-Za-z0-9_./-]+|[A-Za-z0-9_-]+\.md))"')


def e2e_rust_texts(root: Path = ROOT) -> list[str]:
    """The Rust the E2E lane runs: all of src/, tests/common/, and every test file that holds an ignored test."""
    out = [f.read_text(errors="ignore") for f in (root / "src").rglob("*.rs")]
    for f in (root / "tests").rglob("*.rs"):
        t = f.read_text(errors="ignore")
        if "#[ignore" in t or "tests/common/" in str(f.relative_to(root)) + "/":
            out.append(t)
    return out


def lane_text(root: Path = ROOT) -> str:
    """The E2E jobs of ci.yml, from `e2e-build` on."""
    ci = (root / ".github/workflows/ci.yml").read_text()
    return ci[ci.index("  e2e-build:"):]


def e2e_python(root: Path = ROOT) -> set[str]:
    """Every dev/ Python file the E2E lane runs: the modules its steps and its tests name, and what they import."""
    import ast
    named = re.compile(r"\b(dev[./][A-Za-z0-9_./]*[A-Za-z0-9_])")
    todo: list[Path] = []
    for text in [lane_text(root)] + e2e_rust_texts(root):
        for m in named.finditer(text):
            mod = m.group(1).removesuffix(".py").replace("/", ".")
            for cand in (root / (mod.replace(".", "/") + ".py"), root / mod.replace(".", "/") / "__init__.py"):
                if cand.is_file():
                    todo.append(cand)
    seen: set[Path] = set()
    while todo:
        f = todo.pop()
        if f in seen:
            continue
        seen.add(f)
        pkg = f.parent
        for init in (pkg / "__init__.py", pkg / "__main__.py"):
            if init.is_file() and init.name == "__init__.py":
                todo.append(init)
        try:
            tree = ast.parse(f.read_text(errors="ignore"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.ImportFrom):
                base = pkg
                for _ in range(max(node.level - 1, 0)):
                    base = base.parent
                if node.level:
                    head = base / (node.module or "").replace(".", "/")
                else:
                    head = root / (node.module or "").replace(".", "/")
                names = [str(head)] + [str(head / a.name) for a in node.names]
            elif isinstance(node, ast.Import):
                names = [str(root / a.name.replace(".", "/")) for a in node.names]
            for n in names:
                for cand in (Path(n + ".py"), Path(n) / "__init__.py"):
                    if cand.is_file() and str(cand).startswith(str(root / "dev")):
                        todo.append(cand)
    return {str(f.relative_to(root)) for f in seen}


def read_docs(root: Path = ROOT) -> set[str]:
    """Every docs path the Rust the E2E lane runs names: such a file is read by some E2E build or test, so it is never skippable."""
    found: set[str] = set()
    if True:
        for text in e2e_rust_texts(root):
            for m in READ_DOC.finditer(text):
                name = m.group(1)
                while name.startswith("../"):
                    name = name[3:]
                found.add(name)
    return found


def offline_only(path: str, root: Path = ROOT) -> bool:
    """A Rust test file under tests/ (not tests/common/, not a live module) that holds no ignored test: E2E runs only ignored tests."""
    if not (path.startswith("tests/") and path.endswith(".rs")) or path.startswith(("tests/common/", "tests/live/")):
        return False
    f = root / path
    return f.is_file() and "#[ignore" not in f.read_text(errors="ignore") and "mod " not in f.read_text(errors="ignore")


#: dev/ Python the E2E lane runs (`e2e_python`), computed once per process.
_E2E_PY: set[str] | None = None


def classify(path: str, read: set[str]) -> str:
    """`docs`, `unused` (harness the E2E lane never touches) or `full`."""
    global _E2E_PY
    if path in ALWAYS_FULL or path in read:
        return "full"
    if path.startswith(UNUSED_BY_E2E) or path in UNUSED_FILES or offline_only(path):
        return "unused"
    if path.startswith(("dev/release_oracle/", "dev/pytools/")) and path.endswith(".py"):
        if _E2E_PY is None:
            _E2E_PY = e2e_python()
        return "full" if path in _E2E_PY else "unused"
    if path.startswith("docs/gifs/") or path.startswith("changelog.d/") or path == "CHANGELOG.md":
        return "docs"
    if path.endswith(".md") or (path.startswith("docs/") and path.endswith((".yaml", ".yml"))):
        return "docs"
    return "full"


def scope(paths: list[str], read: set[str]) -> tuple[str, str]:
    """`(scope, reason)` for a diff of `paths`."""
    if not paths:
        return "full", "no changed path was read: the full E2E runs"
    kinds = {p: classify(p, read) for p in paths}
    full = [p for p, k in kinds.items() if k == "full"]
    if full:
        return "full", f"`{full[0]}` can change what E2E runs"
    if all(k == "docs" for k in kinds.values()):
        return "skip", "docs-only"
    return "skip", "docs and harness the E2E lane does not use"


# ── v2: selection by impact ─────────────────────────────────────────────────────

#: The owner a changed source path belongs to: an engine, a warehouse target, a destination, or `init`.
#: First match wins; a src/ path no rule names is a shared seam and selects the full E2E.
OWNED_PATHS = (
    (re.compile(r"^src/(source|preflight|init)/(postgres)(/|\.rs$)"), ("postgres",)),
    (re.compile(r"^src/connect/postgres\.rs$"), ("postgres",)),
    (re.compile(r"^src/(source|preflight|init)/(mysql)(/|\.rs$)"), ("mysql",)),
    (re.compile(r"^src/(source|preflight|init)/(mssql)(/|\.rs$)"), ("mssql",)),
    (re.compile(r"^src/(source|preflight|init)/(oracle)(/|\.rs$)"), ("oracle",)),
    (re.compile(r"^src/(source|preflight|init)/(mongo)(/|\.rs$)"), ("mongo",)),
    (re.compile(r"^src/load/(bigquery/|bq_rest\.rs$)"), ("bigquery",)),
    (re.compile(r"^src/load/snowflake\.rs$"), ("snowflake",)),
    (re.compile(r"^src/load/clickhouse\.rs$"), ("clickhouse",)),
    (re.compile(r"^src/destination/azure\.rs$"), ("azure",)),
    (re.compile(r"^src/destination/(gcs|gcs_auth)\.rs$"), ("gcs",)),
    (re.compile(r"^src/destination/s3\.rs$"), ("s3",)),
    (re.compile(r"^src/init/(yaml_scaffold|candidates|density|artifact|catalog_replay)\.rs$"), ("init",)),
)

#: What in a live module's text says it exercises an owner (case-insensitive). A module that names none is run for every change.
MODULE_MARKERS = {
    "postgres": r"\bpg_|Rig::pg|SqlEngine::Pg\b|POSTGRES_|postgres",
    "mysql": r"mysql",
    "mssql": r"mssql|sqlserver|sql server",
    "oracle": r"oracle",
    "mongo": r"mongo",
    "bigquery": r"bigquery|BqLive|\bbq_",
    "snowflake": r"snowflake|SfLive",
    "clickhouse": r"clickhouse",
    "azure": r"azure",
    "gcs": r"\bgcs|fake-gcs|fake_gcs",
    "s3": r"\bs3\b|minio",
    "init": r"\"init\"",
}
LIVE = ROOT / "tests" / "live"
#: Owners of one kind: a test whose NAME names only others of the changed owner's kind cannot reach the change.
KINDS = (
    {"postgres": "pg|postgres|postgresql", "mysql": "mysql", "mssql": "mssql|sqlserver|sql_server",
     "oracle": "oracle", "mongo": "mongo|mongodb"},
    {"bigquery": "bigquery|bq", "snowflake": "snowflake|sf", "clickhouse": "clickhouse|ch"},
    {"azure": "azure", "gcs": "gcs", "s3": "s3"},
)


def name_token(alts: str) -> str:
    """A regex for any of `alts` as a whole `_`-separated word of a test's full name."""
    return f"(?:^|_|::)(?:{alts})(?:_|$)"


def foreign_names(owners: set[str]) -> str | None:
    """The test-name regex of every owner that shares a kind with a changed owner and did not change; `None` when there is none."""
    others = [alts for kind in KINDS if owners & set(kind) for o, alts in kind.items() if o not in owners]
    return name_token("|".join(others)) if others else None


def own_names(owners: set[str]) -> str:
    """The test-name regex of the changed owners."""
    alts = [kind[o] for kind in KINDS for o in kind if o in owners]
    return name_token("|".join(alts)) if alts else "(?!)"


def owners_of_path(path: str) -> tuple[str, ...] | None:
    """The owners a changed path belongs to, `()` for a skippable path, `None` for a shared seam (full)."""
    for pat, owners in OWNED_PATHS:
        if pat.search(path):
            return owners
    return None


def module_owners(root: Path = ROOT) -> dict[str, set[str]]:
    """Every live module (`tests/live/<m>.rs`) and the owners its text names; an empty set means every change runs it."""
    out = {}
    for f in sorted((root / "tests" / "live").glob("*.rs")):
        # Code only: a comment that mentions an engine does not make a module exercise it.
        text = "\n".join(l for l in f.read_text(errors="ignore").splitlines() if not l.lstrip().startswith("//"))
        out[f.stem] = {o for o, pat in MODULE_MARKERS.items() if re.search(pat, text, re.I)}
    return out


def select(paths: list[str], read: set[str], modules: dict[str, set[str]]) -> tuple[str, str, list[str]]:
    v, why, chosen, _ = select_with_owners(paths, read, modules)
    return v, why, chosen


def select_with_owners(paths: list[str], read: set[str], modules: dict[str, set[str]]) -> tuple[str, str, list[str], set[str]]:
    """`(scope, reason, modules)`: `skip` (nothing E2E reads changed), `select` (only these live modules), or `full`."""
    verdict, reason = scope(paths, read)
    if verdict == "skip":
        return verdict, reason, [], set()
    code = [p for p in paths if classify(p, read) == "full"]
    owners: set[str] = set()
    own_files: set[str] = set()
    for p in code:
        m = re.match(r"^tests/live/([a-z0-9_]+)\.rs$", p)
        if m and m.group(1) in modules:
            own_files.add(m.group(1))
            continue
        o = owners_of_path(p)
        if o is None:
            return "full", f"`{p}` is a shared seam (or unmapped): the full E2E runs", [], set()
        owners.update(o)
    chosen = sorted(m for m, own in modules.items() if not own or own & owners or m in own_files)
    if len(chosen) == len(modules):
        return "full", "every live module exercises what changed", [], set()
    what = ", ".join(sorted(owners) + [f"tests/live/{m}.rs" for m in sorted(own_files)])
    return "select", f"{len(chosen)} of {len(modules)} live modules exercise {what}", chosen, owners


#: A test function and its body, up to the next item at column 0.
TEST_FN = re.compile(r"^#\[test\][^\n]*\n(?:#\[[^\n]*\n)*(?:pub )?fn ([a-z0-9_]+)\(\)[^\n]*\n(.*?)(?=^\S)", re.M | re.S)


def kept_by_body(chosen: list[str], owners: set[str], root: Path = ROOT) -> list[str]:
    """The tests of `chosen` the name rule would drop whose own body names a changed owner: they stay."""
    foreign = foreign_names(owners)
    if not foreign:
        return []
    f_re, o_re = re.compile(foreign), re.compile(own_names(owners))
    markers = [re.compile(MODULE_MARKERS[o], re.I) for o in owners if o in MODULE_MARKERS]
    keep = []
    for mod in chosen:
        text = (root / "tests" / "live" / f"{mod}.rs").read_text(errors="ignore") + "\n}\n"
        for name, body in TEST_FN.findall(text):
            full = f"{mod}::{name}"
            if f_re.search(full) and not o_re.search(full) and any(m.search(body) for m in markers):
                keep.append(full)
    return sorted(keep)


def nextest_filter(chosen: list[str], owners: set[str] = frozenset()) -> str:
    """The nextest expression: every other binary whole, and of `live_suite` the chosen modules, minus their tests
    whose name names only another owner of the changed owners' kind (a `_mssql` test for a MySQL change)."""
    picked = "test(/^(" + "|".join(chosen) + ")::/)"
    foreign = foreign_names(set(owners))
    if foreign:
        back = "".join(f" | test(={t})" for t in kept_by_body(chosen, set(owners)))
        picked = f"({picked} - (test(/{foreign}/) - test(/{own_names(set(owners))}/)){back})"
    return f"not binary(live_suite) | {picked}"


def changed(base: str, head: str) -> list[str]:
    """The paths that differ between the merge base of `base`..`head` and `head`; a failed diff raises."""
    out = subprocess.run(["git", "-C", str(ROOT), "diff", "--name-only", f"{base}...{head}"],
                         capture_output=True, text=True, check=True).stdout
    return [line for line in out.splitlines() if line.strip()]


def self_test() -> int:
    """The verdicts the workflow relies on, and the proof that the skippable harness is unused by E2E."""
    read = read_docs()
    assert scope(["src/load/mod.rs"], read)[0] == "full"
    assert scope(["tests/live/live_cdc.rs"], read)[0] == "full"
    assert scope(["docs/a.md", "src/main.rs"], read)[0] == "full", "one code path makes it full"
    assert scope(["some/new/place.txt"], read)[0] == "full", "an unclassified path is full"
    assert scope(["Cargo.toml"], read)[0] == "full" and scope(["docker-compose.yaml"], read)[0] == "full"
    assert scope([".github/workflows/ci.yml"], read)[0] == "full"
    assert scope([".github/scripts/e2e_scope.py"], read)[0] == "full", "this script decides E2E"
    assert scope([".github/scripts/e2e_legs.py"], read)[0] == "full", "the leg planner runs in the lane"
    assert scope([], read)[0] == "full", "an empty diff proves nothing"
    assert scope(["docs/concepts.md", "docs/how-to/new-page.md", "changelog.d/x.md", "CHANGELOG.md", "docs/gifs/a.gif"], read) \
        == ("skip", "docs-only")
    assert scope(["dev/seeded/seeds.yaml", "docs/a.md"], read)[0] == "skip"
    assert scope(["docs/type-capability-matrix.yaml"], read)[0] == "full", "a live test reads it"
    # Every docs file a test or source file names is full: derived, so a new reader is covered at once.
    assert scope(["README.md"], read)[0] == "full", "a test reads README.md"
    assert "docs/reference/errors.md" in read, "src/error.rs includes the generated errors reference"
    for path in read:
        assert classify(path, read) == "full", path
    # The skippable harness is not used by the E2E lane: no test, source file or E2E step names it.
    lane = lane_text()
    # What the lane runs: its own steps, the source, and every test file that holds an ignored test
    # (E2E runs `--run-ignored only`; the offline guards run in `Tests`).
    rs = [f.read_text(errors="ignore") for f in (ROOT / "tests").rglob("*.rs")]
    texts = [lane] + [t for t in rs if "#[ignore" in t] + [f.read_text(errors="ignore") for f in (ROOT / "src").rglob("*.rs")]
    for prefix in UNUSED_BY_E2E:
        module = prefix.rstrip("/").replace("/", ".")
        # A file the lane runs on purpose is in ALWAYS_FULL; any other use of the prefix is a finding.
        bare = [functools.reduce(lambda t, f: t.replace(f, ""), ALWAYS_FULL, t) for t in texts]
        hits = [t for t in bare if prefix in t or f"-m {module}" in t]
        assert not hits, f"{prefix} is used by the E2E lane or a test, so it cannot be skippable"
    py = e2e_python()
    assert "dev/release_oracle/rig_oracle.py" in py and "dev/release_oracle/state_lib.py" in py, sorted(py)[:20]
    assert classify("dev/release_oracle/rig_oracle.py", read) == "full", "the rig oracle grades every E2E cell"
    assert classify("dev/release_oracle/partner_shape.py", read) == "unused", "a gate-only stage"
    assert classify("tests/offline/ci_gate_steps_guard.rs", read) == "unused"
    assert classify("tests/offline/live_skip_policy_guard.rs", read) == "full", "it holds an ignored test"
    assert classify("tests/live_suite.rs", read) == "full" and classify("tests/offline_suite.rs", read) == "full"
    mods = module_owners()
    suite = set(re.findall(r"^\s*mod ([a-z0-9_]+);", (ROOT / "tests/live_suite.rs").read_text(), re.M)) - {"common"}
    assert suite == set(mods), ("every live module file is a live_suite module, and back", suite ^ set(mods))
    v, _, chosen = select(["src/source/mysql/mod.rs"], read, mods)
    assert v == "select" and "live_mysql_resume" in chosen and "live_mssql_variant" not in chosen, chosen
    assert all("mysql" in mods[m] for m in chosen), "a mysql change selects only modules that exercise mysql"
    v, _, chosen = select(["src/load/clickhouse.rs", "docs/a.md"], read, mods)
    assert v == "select" and "live_clickhouse_load" in chosen and "live_mysql_resume" not in chosen, chosen
    assert select(["tests/live/live_mysql_resume.rs"], read, mods)[2] == ["live_mysql_resume"]
    for seam in ("src/state/mod.rs", "src/pipeline/job.rs", "src/config/mod.rs", "src/destination/mod.rs",
                 "src/error.rs", "src/types/mod.rs", "Cargo.toml", "build.rs", "tests/common/mod.rs",
                 "docker-compose.yaml", "src/a_new_file.rs", "tests/live_suite.rs"):
        assert select(["src/source/mysql/mod.rs", seam], read, mods)[0] == "full", seam
    assert select(["docs/a.md"], read, mods)[0] == "skip"
    assert nextest_filter(["a", "b"]) == "not binary(live_suite) | test(/^(a|b)::/)"
    foreign = re.compile(foreign_names({"mysql"}))
    own = re.compile(own_names({"mysql"}))
    keep = lambda n: not foreign.search(n) or bool(own.search(n))
    assert keep("live_cdc::x_mysql") and keep("live_cdc::engine_agnostic") and keep("live_x::y_gcs")
    assert not keep("live_cdc::x_mssql") and not keep("live_cdc::pg_cdc_x") and not keep("live_x::y_mongo")
    assert keep("live_x::upgrade_continues"), "a word inside another word is not a token"
    assert keep("live_x::from_pg_to_mysql"), "a test naming the changed owner stays"
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "tests" / "live").mkdir(parents=True)
        (Path(d) / "tests" / "live" / "m.rs").write_text(
            "#[test]\nfn a_cell_mssql() {\n    run(SqlEngine::Mssql);\n}\n"
            "#[test]\nfn a_parity_mssql() {\n    compare(SqlEngine::Mssql, SqlEngine::Mysql);\n}\n")
        assert kept_by_body(["m"], {"mysql"}, Path(d)) == ["m::a_parity_mssql"], \
            "a foreign-named test whose body drives the changed engine is kept"
    print(f"self-test ok: e2e scope — {len(read)} docs files read by the build or tests stay full; "
          f"{len(mods)} live modules mapped to their owners")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) not in (2, 4) or argv[0] != "--base" or (len(argv) == 4 and argv[2] != "--head"):
        print(__doc__, file=sys.stderr)
        return 2
    head = argv[3] if len(argv) == 4 else "HEAD"
    verdict, reason, chosen, owners = select_with_owners(changed(argv[1], head), read_docs(), module_owners())
    print(f"scope={verdict}")
    print(f"reason={reason}")
    print(f"filter={nextest_filter(chosen, owners) if verdict == 'select' else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
