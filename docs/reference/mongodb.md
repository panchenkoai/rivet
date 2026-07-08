# MongoDB Reference

Rivet reads a MongoDB collection as a **JSON-blob** table: every document becomes
exactly two columns —

| column | type | contents |
| ------ | ---- | -------- |
| `_id` | `Utf8` | the document key, stringified (ObjectId → hex, int → decimal string, …) |
| `document` | `Utf8` + `arrow.json` extension | the **whole** BSON document as [extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/) |

Rivet does **not** flatten fields into typed columns. Documents in one collection
rarely share a schema, so rivet keeps each document intact as one JSON value and
lets the warehouse type it on the way in (`PARSE_JSON` → `VARIANT` on Snowflake,
`JSON` on BigQuery). This is lossless and schema-drift-proof: a new field in a
document never breaks a load.

Two run modes:

- **Batch** — a full snapshot of a collection to Parquet/CSV. Works against a
  standalone `mongod` or a replica set.
- **CDC** — change capture via a [change stream](https://www.mongodb.com/docs/manual/changeStreams/).
  **Requires a replica set** (a single-node replica set is fine); a standalone
  `mongod` cannot open a change stream.

---

## Prerequisites

- A connection URL: `mongodb://[user:pass@]host:port/database`.
- For a **port-mapped single-node replica set** (common in local/dev docker),
  append **`?directConnection=true`** — otherwise the driver tries to re-resolve
  the replica-set members by their in-container hostnames and fails with
  `ReplicaSetNoPrimary`.
- For **CDC**, the login needs a role that can run `changeStream` (e.g. `read` on
  the database). For **delete/update pre-images** (MongoDB 6.0+), the collection
  needs `changeStreamPreAndPostImages` enabled.

---

## Scenario A — Batch export (full snapshot)

Goal: copy a whole collection to Parquet, verify nothing was lost, and learn how
it will land in the warehouse.

### 1. Scaffold a config

```bash
rivet init --source "mongodb://127.0.0.1:27017/shop" -o mongo.yaml
```

Or write it by hand — the minimal batch config:

```yaml
# batch.yaml
source:
  type: mongo
  url: "mongodb://127.0.0.1:27017/shop"
  mongo:
    page_size: 5000          # keyset (seek) paging on _id — bounded query time
exports:
  - name: products
    table: products          # the collection name
    mode: full
    format: parquet
    parallel: 4              # _id-range fan-out (optional)
    destination:
      type: local
      path: "./out/products"
```

### 2. Preflight — `rivet check`

```console
$ rivet check -c batch.yaml

Export: products
  Strategy:     full-parallel(4)
  Mode:         full
  Row estimate: ~20K
  Verdict:      ACCEPTABLE
```

`check` is advisory — it never blocks the run. (A couple of lines in the report —
`max_connections`, "only chunked mode benefits from parallelism" — are worded for
SQL sources and don't apply to Mongo; ignore them.)

### 3. Warehouse portability — `rivet check --target snowflake`

```console
$ rivet check -c batch.yaml --target snowflake

  document  json → VARIANT   warn ~
     autoload: TEXT
     note: JSON autoloads as TEXT; recover native VARIANT with PARSE_JSON after load
     recover: PARSE_JSON("document")
```

This tells you the exact recovery: load `document` as `TEXT`/`VARCHAR`, then
`PARSE_JSON` it into a `VARIANT`. Swap `--target bigquery` for the BigQuery form.

### 4. Run + validate — `rivet run --validate`

```console
$ rivet run -c batch.yaml --validate

✓ products      keyset        20,000 rows    6 files   347.5 KB   0.8s
── products ──────────────────────────
  run_id:     products_20260708T120725.974
  rows:       20,000
  files:      6
  validated:  pass
```

`--validate` re-reads the output and checks the row counts. Add `--reconcile` to
compare the destination against a fresh `countDocuments` on the source.

### 5. What landed

```console
$ duckdb -c "SELECT COUNT(*), COUNT(DISTINCT _id) FROM read_parquet('out/products/*.parquet')"
20000, 20000                              # no loss, no duplicates across pages

$ duckdb -c "SELECT document FROM read_parquet('out/products/*.parquet') LIMIT 1"
{"_id":{"$oid":"6a4e…"},"sku":"P000001","name":"Item 1",
 "price":{"$numberDecimal":"1.99"},"qty":1,"tags":[],"meta":{…}}
```

Every document is verbatim relaxed extended JSON — note `$oid` and
`$numberDecimal` type tags, which a warehouse `PARSE_JSON` reconstructs.

---

## Scenario B — CDC (change capture)

Goal: capture inserts/updates/deletes from a collection, resumably, into Parquet.
**Needs a replica set.**

### 1. Config

```yaml
# cdc.yaml
source:
  type: mongo
  # directConnection=true is REQUIRED for a port-mapped single-node replica set
  url: "mongodb://127.0.0.1:27018/shop?directConnection=true"
exports:
  - name: orders_cdc
    table: orders
    mode: cdc
    format: parquet
    cdc:
      checkpoint: "./orders.ckpt"   # resume anchor (persisted resume token)
      initial: snapshot             # copy pre-existing docs first, then stream
      until_current: true           # bounded: drain the backlog, then exit
    destination:
      type: local
      path: "./cdc_out/orders"
```

### 2. Preflight — `rivet doctor`

```console
$ rivet doctor -c cdc.yaml

[OK]  CDC replica set — replica set (server 7.0.37)
[OK]  CDC capture tier — full-image-capable (6.0+) — delete/update pre-images
      ride when changeStreamPreAndPostImages is enabled on the collection
```

`doctor` proves the source is a replica set and reports the **capability tier**
(see [Capability tiers](#capability-tiers)).

### 3. First run — snapshot + drain

```console
$ rivet run -c cdc.yaml

✓ orders_cdc__snapshot_orders  full   3 rows   1 files       # the snapshot leg
── orders_cdc ──────────────────────────
  rows:   0                                                   # no changes yet
```

The **snapshot leg** copies the 3 pre-existing documents (they predate the change
stream, so the stream alone would miss them). The **CDC leg** then drains to
"now" — 0 changes — and, because `until_current: true`, exits. The checkpoint now
holds the resume token.

### 4. Some changes happen

```javascript
db.orders.insertOne({_id:4, total:75, status:"new"})
db.orders.updateOne({_id:1}, {$set:{status:"shipped"}})
db.orders.deleteOne({_id:3})
```

### 5. Resume — captures only what changed

```console
$ rivet run -c cdc.yaml

── orders_cdc ──────────────────────────
  rows:   3                                                   # only the 3 new changes
```

```console
$ duckdb -c "SELECT __op, _id, document FROM read_parquet('cdc_out/orders/*.parquet')
             WHERE __op IS NOT NULL ORDER BY __pos"
insert  4  {"_id":4,"total":75,"status":"new"}
update  1  {"_id":1,"total":100,"status":"shipped"}           # post-image
delete  3  (null)                                             # _id only, no pre-image
```

Each change row carries three metadata columns:

| column | meaning |
| ------ | ------- |
| `__op` | `insert` \| `update` \| `delete` |
| `__pos` | the resume token — a **distinct, order-preserving** position per event |
| `__seq` | always `0` for Mongo (see [Dedup ordering](#dedup-ordering)) |

### Scheduling

Run the same command on an interval (cron, systemd timer, Airflow). Each run
resumes from the checkpoint and drains to current. Because part files are named
from the millisecond `run_id`, consecutive runs into the same destination prefix
**never overwrite** each other.

---

## Config reference — `source.mongo.*`

| key | values | default | effect |
| --- | ------ | ------- | ------ |
| `json` | `relaxed` \| `canonical` | `relaxed` | how `document` renders (see [Type fidelity](#type-fidelity)) |
| `page_size` | `N` | — | keyset (seek) paging on `_id`; bounds query time on big collections |
| `resume` | `bool` | `false` | resume batch keyset paging across runs (reuses the export checkpoint) |
| `read_concern` | `server` \| `snapshot` | `server` | `snapshot` gives a point-in-time read (5.0+ replica set) |
| `no_cursor_timeout` | `bool` | `true` | keep a slow scan's cursor alive |

Everything else is the shared surface: `parallel` (an `_id`-range fan-out for
Mongo), `mode: cdc`, `cdc.{checkpoint, initial, until_current, max_events}`, and
`--target` / `--format` / `--validate` / `--reconcile` on the CLI.

---

## Type fidelity

Rivet stores `document` **verbatim** — there is no corruption at rest. Fidelity
downstream depends on the JSON mode and the reader:

- **`relaxed`** (default) renders numbers as bare JSON (`"qty": 1`), with type
  tags only where JSON can't express the type (`$oid`, `$numberDecimal`,
  `$date`). Compact and directly queryable.
- **`canonical`** type-tags **every** value (`{"$numberInt":"1"}`,
  `{"$numberLong":"…"}`). Verbose but unambiguous.

**The one trap — large 64-bit integers.** A relaxed `Int64` larger than 2⁵³
(9,007,199,254,740,992) is a bare JSON number. A reader that parses JSON numbers
as IEEE-754 **doubles** (most JavaScript-based tools) will round it. Two safe
paths:

- Target **Snowflake** or **BigQuery** — their `PARSE_JSON` parses JSON integers
  as exact `NUMBER` (up to 38 digits), **not** doubles. Verified round-trip:
  `9007199254740993` survives relaxed → Parquet → `PARSE_JSON` → `INTEGER`.
- Or set `json: canonical` — `$numberLong` is a string, lossless for any reader.

| value | relaxed + f64 JS reader | relaxed + Snowflake/BigQuery | canonical |
| ----- | ----------------------- | ---------------------------- | --------- |
| `Int64` ≤ 2⁵³ | exact | exact | exact |
| `Int64` > 2⁵³ | **rounded** ⚠ | exact | exact |
| `Decimal128` | exact (`$numberDecimal` string) | exact | exact |

**Guidance:** for a Snowflake/BigQuery target, `relaxed` is safe and the better
default. Choose `canonical` if a downstream f64 JSON parser will touch large
integers.

---

## Capability tiers

The change-stream feature set depends on the server version — `doctor` reports it:

| tier | versions | update/delete images |
| ---- | -------- | -------------------- |
| **current-state** | 4.4, 5.0 | update carries the current full document (`UpdateLookup`); delete carries `_id` only |
| **full-image-capable** | 6.0+ | pre-images available on delete/update when `changeStreamPreAndPostImages` is enabled |

---

## Caveats

- **`UpdateLookup` is current-state, not point-in-time.** An update's captured
  document is the document as it exists *when the stream reads the event*, not at
  the moment of the update. If a document is updated then deleted before the next
  capture, the update's `document` comes back `NULL` (the doc is gone) — this is
  at-least-once-correct (the delete is captured), not a loss. Frequent captures
  keep the post-image fresh.
- **A delete without a pre-image has `document = NULL`.** Enable
  `changeStreamPreAndPostImages` (6.0+) if you need the deleted document body.
- **Dedup ordering.** <a id="dedup-ordering"></a>To reconstruct current state from
  the change log, order by `__pos` alone and keep the last row per `_id` (deletes
  remove). Unlike SQL engines, Mongo gives **every** event a distinct `__pos`
  even inside one transaction, so `__seq` is always `0`.
- **`parallel` needs a comparable `_id`.** The `_id`-range fan-out works for
  ObjectId and other ordered `_id` types; a collection with mixed `_id` types
  falls back to single-worker keyset.
```
