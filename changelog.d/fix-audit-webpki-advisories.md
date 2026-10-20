- **Two TLS advisories in the dependency tree.** `rustls-webpki` 0.103.15 could accept a distrusted
  intermediate (RUSTSEC-2026-0382) and `webpki-roots` 1.0.9 bundled roots that trusted the distrusted
  emSign intermediates (RUSTSEC-2026-0381); both reach rivet through `reqwest` (object stores, BigQuery,
  ClickHouse). **Now** they are 0.103.16 and 1.0.10; nothing else in the lock moved. **What to do:**
  nothing.
