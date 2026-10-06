"""A token-level Rust scanner: items, spans, match sites and pass-through shapes. No name resolution happens here."""

from __future__ import annotations

import bisect
import hashlib
import re
from dataclasses import dataclass, field

TOKEN = re.compile(
    r"""
 (?P<w>\s+)
|(?P<c>//[^\n]*)
|(?P<b>/\*)
|(?P<r>(?:b|c)?r(?P<h>\#*)".*?"(?P=h))
|(?P<s>(?:b|c)?"(?:[^"\\]|\\.)*")
|(?P<q>b?'(?:\\(?:u\{[^}]*\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])')
|(?P<l>'[A-Za-z_]\w*)
|(?P<i>(?:r\#)?[^\W\d]\w*)
|(?P<n>\d\w*(?:\.\d\w*)?)
|(?P<p>::|->|=>|\.\.=|\.\.\.|\.\.|.)
""",
    re.X | re.S,
)

KEYWORDS = frozenset(
    "as async await break const continue crate dyn else enum extern false fn for if impl in let loop match mod move "
    "mut pub ref return self Self static struct super trait true type unsafe use where while union macro_rules".split()
)
OPEN = {"(": ")", "[": "]", "{": "}"}
CLOSE = {")", "]", "}"}
TEST_ATTR = re.compile(r"^(?:\w+::)*(?:test|rstest|test_case|bench|proptest)\b")
CFG_TEST = re.compile(r"^cfg\((?!.*not\(test)(?:.*\W)?test\b")
PATH_ATTR = re.compile(r'^path="([^"]+)"')


@dataclass
class Item:
    """One syntactic item; lines are 1-based, columns 0-based, token ranges half-open."""

    kind: str
    name: str
    line: int
    col: int
    end_line: int
    vis: str = "priv"
    test: bool = False
    cfg: tuple[str, ...] = ()
    scope: tuple[str, ...] = ()
    owner: str = ""
    owner_kind: str = ""
    trait: str = ""
    trait_pos: tuple[int, int] | None = None
    self_pos: tuple[int, int] | None = None
    params: list[str] = field(default_factory=list)
    has_self: bool = False
    body: tuple[int, int] | None = None
    variants: int = 0
    generated_impl_of: list[str] = field(default_factory=list)
    args: tuple[int, int] | None = None

    @property
    def public(self) -> bool:
        """Visible outside its module: any `pub` form, or a method declared by a trait."""
        return self.vis != "priv"


@dataclass
class MatchSite:
    """One `match` expression and the token ranges of its arm patterns."""

    line: int
    patterns: list[tuple[int, int]]


class File:
    """Tokens and items of one Rust source file."""

    def __init__(self, path: str, text: str, test: bool = False, cfg: tuple[str, ...] = ()):
        self.path = path
        self.kinds: list[str] = []
        self.texts: list[str] = []
        self.lines: list[int] = []
        self.cols: list[int] = []
        self.ascii = text.isascii()
        self.src = text.split("\n")
        self._tokenize(text)
        n = len(self.texts)
        self.mate = self._mates()
        self.in_use = bytearray(n)
        self.in_pub_use = bytearray(n)
        self.in_macro_def = bytearray(n)
        self.in_attr = bytearray(n)
        for i in range(n - 1):
            if self.texts[i] == "#" and self.kinds[i] == "p":
                o = i + 2 if self.texts[i + 1] == "!" else i + 1
                if o < n and self.texts[o] == "[" and o in self.mate:
                    for j in range(i, self.mate[o] + 1):
                        self.in_attr[j] = 1
        self.items: list[Item] = []
        self.mod_decls: list[tuple[str, bool, tuple[str, ...], str]] = []
        self.test_ranges: list[tuple[int, int]] = []
        self.file_test = test
        self.file_cfg = cfg
        self._items(0, n, test, cfg, (), "", "", "")
        self.pos = {(self.lines[i], self.cols[i]): i for i in range(n) if self.kinds[i] == "i"}
        fns = sorted((it.line, it.end_line, it) for it in self.items if it.kind == "fn" and it.body)
        self._fn_starts = [f[0] for f in fns]
        self._fns = fns
        self.match_sites = self._match_sites()
        self.loc, self.test_loc = self._loc()

    def _tokenize(self, text: str) -> None:
        """Fill the token arrays, dropping whitespace and comments."""
        kinds, texts, lines, cols = self.kinds, self.texts, self.lines, self.cols
        line, line_start, pos, size = 1, 0, 0, len(text)
        match = TOKEN.match
        while pos < size:
            m = match(text, pos)
            kind = m.lastgroup
            end = m.end()
            if kind == "b":
                depth, end = 1, pos + 2
                while depth and end < size:
                    a, b = text.find("/*", end), text.find("*/", end)
                    if b < 0:
                        end = size
                        break
                    if 0 <= a < b:
                        depth, end = depth + 1, a + 2
                    else:
                        depth, end = depth - 1, b + 2
            elif kind not in ("w", "c"):
                kinds.append(kind)
                texts.append(text[pos:end])
                lines.append(line)
                cols.append(pos - line_start)
            if kind in ("w", "b", "r", "s"):
                nl = text.count("\n", pos, end)
                if nl:
                    line += nl
                    line_start = text.rfind("\n", pos, end) + 1
            pos = end

    def _mates(self) -> dict[int, int]:
        """Matching bracket index for every bracket token, both directions."""
        mate: dict[int, int] = {}
        stack: list[int] = []
        for i, (k, t) in enumerate(zip(self.kinds, self.texts)):
            if k != "p":
                continue
            if t in OPEN:
                stack.append(i)
            elif t in CLOSE and stack:
                o = stack.pop()
                mate[o] = i
                mate[i] = o
        return mate

    def byte_col(self, line: int, col: int) -> int:
        """The UTF-8 byte column of a character column."""
        if self.ascii:
            return col
        return len(self.src[line - 1][:col].encode())

    def _skip_angles(self, j: int, end: int) -> int:
        """Index after the `<...>` group opening at `j`."""
        depth = 0
        T = self.texts
        while j < end:
            t = T[j]
            if t == "<":
                depth += 1
            elif t == ">":
                depth -= 1
                if depth == 0:
                    return j + 1
            elif t in OPEN and j in self.mate:
                j = self.mate[j]
            j += 1
        return end

    def _scan_to(self, j: int, end: int, stops: tuple[str, ...], skip: str = "([") -> int:
        """First index at or after `j` holding one of `stops`, skipping bracket groups named in `skip`."""
        T = self.texts
        K = self.kinds
        while j < end:
            t = T[j]
            if K[j] == "p":
                if t in stops:
                    return j
                if t in skip and j in self.mate:
                    j = self.mate[j]
            j += 1
        return end

    def _split_commas(self, start: int, end: int) -> list[tuple[int, int]]:
        """Top-level comma-separated token ranges of [start, end)."""
        out, depth, j, seg = [], 0, start, start
        T = self.texts
        while j < end:
            t = T[j]
            if self.kinds[j] == "p":
                if t in OPEN and j in self.mate:
                    j = self.mate[j]
                elif t == "<":
                    depth += 1
                elif t == ">" and depth:
                    depth -= 1
                elif t == "," and depth == 0:
                    out.append((seg, j))
                    seg = j + 1
            j += 1
        if seg < end:
            out.append((seg, end))
        return out

    def _params(self, start: int, end: int) -> tuple[list[str], bool]:
        """Non-self parameter names of a function and whether it takes self."""
        names, has_self = [], False
        T = self.texts
        for a, b in self._split_commas(start, end):
            while a < b and T[a] == "#" and T[a + 1] == "[":
                a = self.mate[a + 1] + 1
            head = [T[j] for j in range(a, min(b, a + 4)) if T[j] not in ("&", "mut") and self.kinds[j] != "l"]
            if head and head[0] == "self":
                has_self = True
                continue
            name = "_"
            for j in range(a, b):
                if T[j] == ":":
                    break
                if self.kinds[j] == "i" and T[j] not in KEYWORDS:
                    name = T[j]
            names.append(name)
        return names, has_self

    def _last_ident(self, start: int, end: int) -> int | None:
        """Index of the last identifier at angle depth 0 in [start, end): the head name of a type path."""
        depth, found, j = 0, None, start
        T = self.texts
        while j < end:
            t = T[j]
            if self.kinds[j] == "p":
                if t in OPEN and j in self.mate:
                    j = self.mate[j]
                elif t == "<":
                    depth += 1
                elif t == ">" and depth:
                    depth -= 1
            elif self.kinds[j] == "i" and depth == 0 and (t not in KEYWORDS or t == "Self"):
                found = j
            j += 1
        return found

    def _items(self, i: int, end: int, test: bool, cfg: tuple, scope: tuple, owner: str, owner_kind: str, trait: str) -> None:
        """Parse the items of one scope, recursing into mods, impls and traits."""
        T, K, L, C, mate = self.texts, self.kinds, self.lines, self.cols, self.mate
        while i < end:
            start = i
            attrs: list[str] = []
            while i + 1 < end and T[i] == "#" and (T[i + 1] == "[" or (T[i + 1] == "!" and i + 2 < end and T[i + 2] == "[")):
                inner = T[i + 1] == "!"
                o = i + 2 if inner else i + 1
                c = mate.get(o, o)
                text = "".join(T[o + 1 : c])
                if inner:
                    if CFG_TEST.match(text):
                        test = True
                else:
                    attrs.append(text)
                i = c + 1
            if i >= end:
                break
            item_test = test or any(TEST_ATTR.match(a) or CFG_TEST.match(a) for a in attrs)
            item_cfg = cfg + tuple(a[4:-1] for a in attrs if a.startswith("cfg(") and a != "cfg(test)")
            vis = "priv"
            if T[i] == "pub":
                vis = "pub"
                i += 1
                if i < end and T[i] == "(" and i in mate:
                    vis = "crate"
                    i = mate[i] + 1
            while i < end and (T[i] in ("async", "unsafe", "default") or (T[i] == "const" and T[i + 1] in ("fn", "unsafe", "async", "extern"))
                               or (T[i] == "extern" and i + 1 < end and (K[i + 1] == "s" or T[i + 1] == "fn"))):
                i += 2 if T[i] == "extern" and K[i + 1] == "s" else 1
            if i >= end:
                break
            t = T[i]
            if owner_kind == "trait" and vis == "priv":
                vis = "trait"

            def new(kind: str, name_idx: int, last: int, **kw) -> Item:
                it = Item(kind, T[name_idx], L[name_idx], C[name_idx], L[min(last, len(L) - 1)], vis=vis, test=item_test, cfg=item_cfg,
                          scope=scope, owner=owner, owner_kind=owner_kind, trait=trait, **kw)
                self.items.append(it)
                if item_test and not test:
                    self.test_ranges.append((L[start], it.end_line))
                return it

            if t == "fn" and i + 1 < end and K[i + 1] == "i":
                j = i + 2
                if j < end and T[j] == "<":
                    j = self._skip_angles(j, end)
                params, has_self = [], False
                if j < end and T[j] == "(" and j in mate:
                    params, has_self = self._params(j + 1, mate[j])
                    j = mate[j] + 1
                k = self._scan_to(j, end, ("{", ";"))
                body = (k, mate[k]) if k < end and T[k] == "{" and k in mate else None
                last = body[1] if body else min(k, end - 1)
                new("fn", i + 1, last, params=params, has_self=has_self, body=body)
                i = last + 1
            elif t in ("struct", "enum", "trait", "union") and i + 1 < end and K[i + 1] == "i" and T[i + 1] not in KEYWORDS:
                j = i + 2
                if j < end and T[j] == "<":
                    j = self._skip_angles(j, end)
                k = self._scan_to(j, end, ("{", ";"))
                has_body = k < end and T[k] == "{" and k in mate
                last = mate[k] if has_body else min(k, end - 1)
                it = new(t, i + 1, last)
                if has_body and t == "enum":
                    it.variants = len(self._split_commas(k + 1, last))
                if has_body and t == "trait":
                    self._items(k + 1, last, item_test, item_cfg, scope, T[i + 1], "trait", T[i + 1])
                i = last + 1
            elif t == "impl":
                k = self._scan_to(i + 1, end, ("{", ";"))
                j = i + 1
                if j < k and T[j] == "<":
                    j = self._skip_angles(j, k)
                w = j
                depth = 0
                split = None
                stop = k
                while w < k:
                    tw = T[w]
                    if K[w] == "p":
                        if tw in OPEN and w in mate:
                            w = mate[w]
                        elif tw == "<":
                            depth += 1
                        elif tw == ">" and depth:
                            depth -= 1
                    elif depth == 0 and tw == "where":
                        stop = w
                        break
                    elif depth == 0 and tw == "for" and split is None and T[w + 1] != "<":
                        split = w
                    w += 1
                tr = self._last_ident(j, split) if split is not None else None
                ty = self._last_ident(split + 1 if split is not None else j, stop)
                has_body = k < end and T[k] == "{" and k in mate
                last = mate[k] if has_body else min(k, end - 1)
                tname = T[tr] if tr is not None else ""
                it = Item("impl", T[ty] if ty is not None else "?", L[i], C[i], L[last], vis="priv", test=item_test, cfg=item_cfg, scope=scope,
                          trait=tname, trait_pos=(L[tr], C[tr]) if tr is not None else None, self_pos=(L[ty], C[ty]) if ty is not None else None)
                self.items.append(it)
                if item_test and not test:
                    self.test_ranges.append((L[start], it.end_line))
                if has_body:
                    self._items(k + 1, last, item_test, item_cfg, scope, it.name, "impl", tname)
                i = last + 1
            elif t == "mod" and i + 1 < end and K[i + 1] == "i":
                if i + 2 < end and T[i + 2] == "{" and i + 2 in mate:
                    last = mate[i + 2]
                    new("mod", i + 1, last)
                    self._items(i + 3, last, item_test, item_cfg, scope + (T[i + 1],), "", "", "")
                    i = last + 1
                else:
                    path = next((m.group(1) for a in attrs if (m := PATH_ATTR.match(a))), "")
                    self.mod_decls.append((T[i + 1], item_test, item_cfg, path))
                    i += 3
            elif t == "use":
                k = self._scan_to(i, end, (";",), skip="{")
                for j in range(i, min(k + 1, end)):
                    self.in_use[j] = 1
                    if vis != "priv":
                        self.in_pub_use[j] = 1
                i = k + 1
            elif t in ("const", "static", "type") and i + 1 < end:
                n = i + 2 if T[i + 1] == "mut" else i + 1
                k = self._scan_to(n, end, (";",), skip="([{")
                if K[n] == "i" and T[n] != "_":
                    new(t, n, min(k, end - 1))
                i = k + 1
            elif t == "macro_rules" and i + 3 < end and T[i + 1] == "!":
                o = i + 3
                last = mate.get(o, o)
                for j in range(i, last + 1):
                    self.in_macro_def[j] = 1
                it = new("macro", i + 2, last)
                for j in range(o, last - 3):
                    if T[j] == "impl" and K[j + 1] == "i" and T[j + 2] == "for" and T[j + 3] == "$":
                        it.generated_impl_of.append(T[j + 1])
                i = last + 1
                if i < end and T[i] == ";":
                    i += 1
            elif t == "extern" and i + 1 < end and T[i + 1] == "crate":
                i = self._scan_to(i, end, (";",)) + 1
            elif K[i] == "i" and t not in KEYWORDS:
                j = i
                while j + 2 < end and T[j + 1] == "::" and K[j + 2] == "i":
                    j += 2
                if j + 2 < end and T[j + 1] == "!" and T[j + 2] in OPEN and j + 2 in mate:
                    last = mate[j + 2]
                    new("invoke", j, last, args=(j + 3, last))
                    i = last + 1
                    if i < end and T[i] == ";":
                        i += 1
                else:
                    i += 1
            else:
                i += 1

    def enclosing_fn(self, line: int) -> Item | None:
        """The function whose span contains `line`."""
        k = bisect.bisect_right(self._fn_starts, line) - 1
        while k >= 0:
            s, e, it = self._fns[k]
            if s <= line <= e:
                return it
            if line - s > 5000:
                break
            k -= 1
        return None

    def is_test_line(self, line: int) -> bool:
        """Whether `line` is inside test-only code."""
        if self.file_test:
            return True
        return any(s <= line <= e for s, e in self.test_ranges)

    def _match_sites(self) -> list[MatchSite]:
        """Every `match` expression outside macro definitions, with its arm-pattern token ranges."""
        T, K, mate = self.texts, self.kinds, self.mate
        sites = []
        for i, t in enumerate(T):
            if t != "match" or K[i] != "i" or self.in_macro_def[i]:
                continue
            b = self._scan_to(i + 1, len(T), ("{", ";"))
            if b >= len(T) or T[b] != "{" or b not in mate:
                continue
            e = mate[b]
            patterns = []
            p = b + 1
            while p < e:
                while p < e and T[p] == "#" and T[p + 1] == "[" and p + 1 in mate:
                    p = mate[p + 1] + 1
                q = self._scan_to(p, e, ("=>",), skip="([{")
                if q >= e:
                    break
                g = p
                while g < q and not (T[g] == "if" and K[g] == "i"):
                    g = mate[g] + 1 if T[g] in OPEN and g in mate else g + 1
                patterns.append((p, min(g, q)))
                r = q + 1
                if r < e and T[r] == "{" and r in mate and (mate[r] + 1 >= e or T[mate[r] + 1] not in (".", "?")):
                    r = mate[r] + 1
                    p = r + 1 if r < e and T[r] == "," else r
                else:
                    p = self._scan_to(r, e, (",",), skip="([{") + 1
            sites.append(MatchSite(self.lines[i], patterns))
        return sites

    def _loc(self) -> tuple[int, int]:
        """(production, test) counts of lines that carry at least one token."""
        prod, test = set(), set()
        for line in set(self.lines):
            (test if self.is_test_line(line) else prod).add(line)
        return len(prod), len(test)

    def passthrough_callee(self, it: Item) -> int | None:
        """Token index of the callee when the body of `it` is one call that only forwards its own parameters."""
        if not it.body:
            return None
        T, K, mate = self.texts, self.kinds, self.mate
        b, e = it.body[0] + 1, it.body[1]
        while e > b:
            if T[e - 1] in ("?", ";"):
                e -= 1
            elif e - 2 >= b and T[e - 1] == "await" and T[e - 2] == ".":
                e -= 2
            else:
                break
        if e - b < 3 or T[e - 1] != ")" or e - 1 not in mate:
            return None
        o = mate[e - 1]
        callee = o - 1
        if callee < b or K[callee] != "i" or T[callee] in KEYWORDS or not (T[callee][0].islower() or T[callee][0] == "_"):
            return None
        chain = range(b, callee)
        for n, j in enumerate(chain):
            ok = (K[j] == "i") if n % 2 == 0 else (T[j] in (".", "::"))
            if not ok:
                return None
        if len(chain) % 2:
            return None
        allowed = set(it.params) | {"self"}
        for a, z in self._split_commas(o + 1, e - 1):
            while a < z and T[a] in ("&", "mut", "*"):
                a += 1
            if z - a != 1 or T[a] not in allowed:
                return None
        if not it.params and not it.has_self:
            return None
        return callee

    def body_signature(self, it: Item, rename: bool) -> list[str]:
        """The normalised token sequence of a function body; identifiers collapse to one symbol when `rename`."""
        out = []
        for j in range(it.body[0] + 1, it.body[1]):
            k, t = self.kinds[j], self.texts[j]
            if k == "i":
                out.append(t if (not rename or t in KEYWORDS) else "I")
            elif k == "n":
                out.append("0")
            elif k in ("s", "r"):
                out.append('""')
            elif k == "q":
                out.append("''")
            elif k == "l":
                out.append("'a")
            else:
                out.append(t)
        return out


def digest(tokens: list[str]) -> str:
    """A short stable hash of a token sequence."""
    return hashlib.sha1("\x1f".join(tokens).encode()).hexdigest()[:12]
