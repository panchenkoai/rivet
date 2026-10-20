- **Two CDC configs `rivet init` wrote for one cluster named the same slot and `server_id`.**
  Every PostgreSQL scaffold named its slot `rivet_<table>` and every MySQL one `server_id: 4271`,
  so a second config (staging beside production) read the first one's stream (0.32.1 and
  earlier). **Now** `init --mode cdc -o <file>` names them after the file (`-o staging.yaml` ->
  slot `rivet_staging_public_cdc`, a `server_id` from a stable hash of the stem in 1000000 to
  1999999); a scaffold printed to stdout keeps the old names and marks them one per cluster.
  **What to do:** nothing for new configs; give an existing second config its own slot and
  `server_id` by hand.
