- **A keyset run stopped before its first part blocked the remedy its refusal named.** A resumed
  MongoDB keyset run refused for an `_id` a `$gt` seek cannot page over (mixed BSON types, NaN) had
  already claimed the export's resume anchor, so the full scan the refusal recommends was refused
  `RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH` (exit 5) on every run, through 0.31.0. The same held
  for any keyset export whose run failed before it recorded a part. **Now** a run that ends with a
  resume anchor under which no part was recorded releases it, so the next run of any mode starts
  fresh; an anchor with recorded parts is kept for the resume as before. The refusal
  (`RIVET_SOURCE_KEYSET_KEY_UNORDERED`) now names every key to remove: `page_size`, `resume` and
  `parallel` (`resume: true` without `page_size` is itself refused). **What to do:** nothing for a
  new refusal. An export already stuck on an anchor a 0.31.0 run left: run the `rivet state reset`
  command the `RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH` refusal prints (that run recorded no part).
