- **Range chunking refused an Oracle `NUMBER(19)` key.** A `NUMBER(19)` primary key (the type a
  64-bit integer key usually has on Oracle) was read as not integer-family, so `mode: chunked` over it
  exited 1 (0.31.0 and earlier). **Now** `NUMBER(p, 0)` up to precision 19 is an integer range key for
  the planner and `rivet check`; a value past the 64-bit range is refused when the bounds are read,
  with a message that says so. `rivet init` still proposes keyset for such a key, which takes any
  value. **What to do:** nothing.
