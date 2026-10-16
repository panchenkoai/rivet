- **An Azure container removed while `rivet run` was writing into it was retried for minutes.** Each
  chunk's upload was retried as a transient error (Azurite answers its 404 with a `keep-alive:
  timeout=5` header, and the retry classifier read the word "timeout" in the store's answer as a
  timeout), so a run of ten chunks took over nine minutes to fail; a bucket removed on S3 or GCS
  failed in seconds (0.31.0 and earlier). **Now** a store's own final answer (not found, denied,
  invalid, unsupported) is permanent by its type, whatever words its headers carry, and a container
  that answered once but is gone by the time a write, listing or read fails is asked about again and
  refused `RIVET_DEST_CONTAINER_NOT_FOUND`. **What to do:** nothing.
