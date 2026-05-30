<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
| ------ | ---------- |
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.

## Verify before publishing agent-walk claims

When a subagent (`Agent(Explore, …)` walk, or any out-of-band
investigation) returns claims with **specific file paths or line
numbers**, do **not** copy those claims into reports, ADRs, or PR
descriptions without first opening the named files yourself.

Specific claims look authoritative; that does not make them correct.
A walk in this repo has produced false claims like *"struct X is
separate per engine"* (when the struct was already shared), *"function
Y exists twice"* (defined once as a method on a shared struct),
*"callers check Z() before each emission"* (when the dispatcher
already centralised the check). Each of these was a `grep` away from
falsification but landed in an HTML architecture report unverified.

Process rule: **for any specific structural claim** (file path, line
number, "exists twice", "duplicated across N callers") **the next
tool call after the walk's report is a `Read` / graph query on the
named site**. Only then write the claim into a deliverable.

The grep is cheap. The wrong claim in a report wastes a future
contributor's day chasing a smell that does not exist.
