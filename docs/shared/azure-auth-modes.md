| Mode | Fields | Notes |
|---|---|---|
| Account key | `account_name` + `account_key_env` | Long-lived storage-account key. |
| **SAS token** | `account_name` + `sas_token_env` | Short-lived, scope-limited credential issued out-of-band. |
| Anonymous | `allow_anonymous: true` | Azurite emulator and public read-only containers only. |

`account_key_env` and `sas_token_env` are **mutually exclusive** — picking
both is refused at config-load time with a message that names both
fields.  `account_name` is the prefix in
`<account>.blob.core.windows.net`; an explicit `endpoint:` takes
precedence over the derived URL, but only a **loopback** emulator
endpoint (Azurite) is accepted alongside credentials — a non-loopback
endpoint is rejected at config load unless `allow_anonymous: true`, which
itself cannot be combined with credentials, so sovereign clouds are not
reachable.

The Azure SAS-token body may be pasted with or without the leading `?`
— Rivet trims it transparently so `sv=…&sig=…` and `?sv=…&sig=…` are
both accepted.
