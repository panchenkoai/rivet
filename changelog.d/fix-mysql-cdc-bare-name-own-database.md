- **A MySQL CDC config written by `rivet init --mode cdc` was refused when another database on the
  server held a table of the same name.** `init` writes the tables unqualified (`tables: [orders]`),
  and every stream run stopped with `RIVET_SOURCE_CDC_TABLE_UNRESOLVED` ("`orders` is unqualified …
  `<other_db>` also holds a table of that name"), at the catalog check or on the first foreign event
  in the binlog (0.32.1 and earlier). **Now** a bare name means the connection's own database when
  that database holds the table, and the same name's events from any other database are not
  captured. When the connection's database does not hold it and exactly one other database does,
  the stream captures that one (0.32.1 stopped there with MySQL error 1146 at the table probe).
  With none or several, it is refused as before. The catalog check, the table probe, the schema
  read, the baseline/backfill read and the binlog routing take that one rule, so the baseline and
  the stream read one table. The destination, the warehouse tables and the state of an existing
  stream are unchanged. **What to do:** nothing; a config that was refused for this runs as
  generated.
