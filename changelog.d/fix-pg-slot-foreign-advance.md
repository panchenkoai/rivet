- **A PostgreSQL CDC export whose slot another config consumed continued past the lost changes.**
  Two configs of one cluster with the same `slot:` read in turn: the second took the changes the
  first had not read, and the first one's next run continued from the slot with exit 0, the
  changes between gone from its output (0.32.1 and earlier). **Now** rivet records its checkpoint
  before every advance of the slot it makes (the empty-span release included), and a run whose
  slot was confirmed past its checkpoint is refused `RIVET_SOURCE_CDC_LOG_GAP`, exit 5, naming the
  other reader. **What to do:** give every config its own slot, checkpoint and state (see "Copying
  a CDC config" in `docs/reference/cdc.md`), then re-baseline the stream the refusal names.
