- **Two `rivet init` runs over one MongoDB database could list its collections in different orders.**
  `listCollections` has no order, and the SQL engines' listings followed each server's collation (SQL
  Server's is case-insensitive). **Now** every engine's listing goes through one ordering in rivet (byte
  order of the name), so the scaffold is the same on every run; on SQL Server a schema with mixed-case
  names may list its exports in a different order than 0.32.1 did. **What to do:** nothing.
- Docs: `docs/reference/mongodb.md` said BigQuery loads `document` as `BYTES`; through `rivet load` it
  lands as `JSON` (only a schema-less autoload gives `BYTES`). `docs/reference/init.md` now names Oracle
  among the engines `rivet init` reads.
