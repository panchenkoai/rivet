- **`rivet run` into a bucket that does not exist failed with the store's raw error.** The upload
  ended with `NotFound (persistent) at write, context: { uri: ... }` and no `RIVET_*` code (exit 1),
  while `rivet validate` and every read already named the missing bucket (0.31.0 and earlier).
  **Now** a write the store answers "not found" asks whether the bucket exists, and a missing one is
  refused `RIVET_DEST_CONTAINER_NOT_FOUND` naming it, on S3, GCS and Azure. **What to do:** nothing.
