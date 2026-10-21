- **Two CDC configs written by `rivet init -o` into one directory shared one checkpoint and one
  destination.** Through 0.32.1 `init --mode cdc` named the checkpoint after the table or schema
  (`./cdc/<name>.ckpt`) and the CDC destination `cdc/` / `./output/cdc/` (per table `<t>_cdc/`)
  whatever `-o` said, so a second config of the same source, written beside the first, resumed the
  first one's position: each stream skipped what the other had read, exit 0. **Now** `init -o
  <stem>.yaml` names the checkpoint `./cdc/<stem>_<name>.ckpt` and the destination
  `<stem>/cdc/` / `./output/<stem>/cdc/` (per table `exports/<stem>/<t>_cdc/`), and refuses
  `RIVET_INIT_CDC_NAME_TAKEN`, exit 5, writing nothing, when the checkpoint already exists or another
  config in the directory names the same checkpoint or destination. A run records which config and
  export own its checkpoint (`<checkpoint>.owner`) and refuses a checkpoint another config still
  reads, `RIVET_SOURCE_CDC_CHECKPOINT_SHARED`, exit 5, before it reads a change. A checkpoint written
  before 0.32.2 has no owner record and is claimed by the first run that reads it; one whose owner no
  longer names it is claimed too. **What to do:** nothing for one config per checkpoint; for two
  configs on one checkpoint, give one of them a checkpoint and destination of its own and re-baseline it.
