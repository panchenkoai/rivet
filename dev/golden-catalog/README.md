# Golden-seed catalog

A versioned SQLite database (`golden_catalog.db`, built from `golden_catalog.sql`)
holding ONE authoritative metadata record per `(seed, engine)` for every fixture
the golden tests and the pre-release gate depend on — both the **normal** goldens
(exact pinned counts) and the **garbage** field-DB profiles (each triggers a
specific strategy / diagnostic / bug guard).

## Why

The keyset-parallel golden's expected numbers (10,000,000 rows / **21** part files
at `parallel=4, chunk_size=500000`) were scattered across a test, a `verify.sh`,
and a README — and one of them was wrong (the naive 20). One catalog makes the
expected outcome live next to the shape it describes, so a golden test / the
pre-release gate LOOKS IT UP instead of hard-coding a number per test:

```sql
SELECT expected_rows, expected_files
FROM golden_seed
WHERE name='keyset_sparse' AND engine='postgres';   -- 10000000 | 21
```

The garbage rows answer "what strategy should rivet pick for this shape, and what
did it guard?" for the dogfood sweep:

```sql
SELECT name, key_type, strategy, guards
FROM golden_seed WHERE category='garbage' AND engine='postgres';
```

## Columns

`name`, `category` (`normal`|`garbage`), `engine`, `schema_table`, `key_column`,
`key_type`, `row_count`, `key_min`/`key_max`/`span_per_row` (the sparsity signal),
`strategy` (`keyset`|`keyset-parallel`|`range`|`full`), `parallel`/`chunk_size`
(the pinned golden's config), `expected_rows`/`expected_files` (the golden — NULL
for a garbage profile with no pinned count), `guards` (the behaviour/bug it pins),
`sql_file`.

## Build

```bash
python3 -m dev.pytools.golden_catalog    # rm + rebuild golden_catalog.db from the .sql
```

`golden_catalog.sql` is the diffable source of truth; `golden_catalog.db` is the
committed ready-to-query artifact. Edit the `.sql`, then REBUILD — never hand-edit
the `.db`. Seeds themselves live in `dev/parallel_keyset/golden/*.sql` (normal) and
`dev/garbage/*.sql` (garbage, `make seed-garbage`); this catalog only DESCRIBES them.
