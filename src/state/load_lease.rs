use crate::error::Result;

use super::{StateRef, StateStore};

/// One holder at a time per key (a warehouse table, an export's checkpointed run). On a
/// Postgres state it is a `state_lease` row renewed by a heartbeat and expired by the
/// SERVER's clock — a single statement, so it holds behind a transaction-mode pooler,
/// where a session advisory lock does not. On a SQLite state it is an `flock` on a
/// sidecar, released by the OS when the holder dies.
pub struct LoadLease<'a> {
    store: &'a StateStore,
    key: String,
    holder: String,
    _file: Option<std::fs::File>,
    beat: Option<(
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        std::thread::JoinHandle<()>,
    )>,
}

impl Drop for LoadLease<'_> {
    fn drop(&mut self) {
        if let Some((stop, handle)) = self.beat.take() {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = handle.join();
        }
        if let StateRef::Postgres(_) = &self.store.state_ref {
            let _ = self.store.execute(
                "DELETE FROM state_lease WHERE lease_key = ?1 AND holder = ?2",
                &[self.key.as_str().into(), self.holder.as_str().into()],
            );
        }
    }
}

/// Seconds a Postgres lease outlives its last heartbeat (`RIVET_STATE_LEASE_TTL_S`, default 30).
fn lease_ttl_s() -> u64 {
    std::env::var("RIVET_STATE_LEASE_TTL_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&t| t > 0)
        .unwrap_or(30)
}

/// This host's name, the part of a holder id that says where its pid lives.
fn host_name() -> String {
    let mut buf = [0u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    if rc == 0 {
        String::from_utf8_lossy(&buf[..end]).into_owned()
    } else {
        String::new()
    }
}

/// Whether `holder` (`host:pid:nonce`) names a process on THIS host that no longer exists.
fn holder_is_dead_here(holder: &str, host: &str) -> bool {
    let mut parts = holder.splitn(3, ':');
    let (Some(h), Some(pid)) = (
        parts.next(),
        parts.next().and_then(|p| p.parse::<i32>().ok()),
    ) else {
        return false;
    };
    h == host
        && pid > 0
        && unsafe { libc::kill(pid, 0) } != 0
        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

/// Renew `key` for `holder` every third of the TTL on its own connection until `stop`.
fn heartbeat(
    url: String,
    key: String,
    holder: String,
    ttl: u64,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    // ponytail: a lost lease (renewal found no row of ours) is logged, not acted on — a run
    // stalled past the TTL can overlap its successor. Abort the run on loss if that bites.
    let mut client = match super::connect_pg(&url) {
        Ok(c) => c,
        Err(e) => {
            log::error!(
                "state lease '{key}': heartbeat cannot connect ({e:#}); it expires in {ttl} s"
            );
            return;
        }
    };
    let every = std::time::Duration::from_secs(ttl.div_ceil(3));
    let mut last = std::time::Instant::now();
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if last.elapsed() < every {
            continue;
        }
        last = std::time::Instant::now();
        let renewed = client.execute(
            "UPDATE state_lease SET expires_at = now() + CAST($3::text AS INTERVAL) \
             WHERE lease_key = $1 AND holder = $2",
            &[&key, &holder, &format!("{ttl} seconds")],
        );
        match renewed {
            Ok(1) => {}
            Ok(_) => {
                log::error!("state lease '{key}': lost — another process holds it now");
                return;
            }
            Err(e) => log::warn!("state lease '{key}': renewal failed ({e:#}); retrying"),
        }
    }
}

/// The sidecar an SQLite-state lease locks: beside the DB file, one per table.
fn lease_path(db: &std::path::Path, key: &str) -> std::path::PathBuf {
    let token = crate::manifest::file_token(key);
    if db.to_string_lossy() == ":memory:" {
        return std::env::temp_dir().join(format!("rivet-{}-{token}.lease", std::process::id()));
    }
    let mut name = db.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(format!(".lease-{token}"));
    db.with_file_name(name)
}

impl StateStore {
    /// Try to take the load lease for `key` (the target table); `None` when
    /// another process holds it.
    pub fn try_load_lease(&self, key: &str) -> Result<Option<LoadLease<'_>>> {
        match &self.state_ref {
            StateRef::Postgres(url) => {
                let ttl = lease_ttl_s();
                let host = host_name();
                let holder = format!(
                    "{host}:{}:{}",
                    std::process::id(),
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
                );
                let mut got = self.query_opt(
                    "INSERT INTO state_lease (lease_key, holder, expires_at) \
                     VALUES (?1, ?2, now() + CAST(CAST(?3 AS TEXT) AS INTERVAL)) \
                     ON CONFLICT (lease_key) DO UPDATE SET holder = EXCLUDED.holder, \
                     expires_at = EXCLUDED.expires_at WHERE state_lease.expires_at < now() \
                     RETURNING CAST(1 AS BIGINT)",
                    &[
                        key.into(),
                        holder.as_str().into(),
                        format!("{ttl} seconds").into(),
                    ],
                    |r| r.i64(0),
                )?;
                if got.is_none() {
                    // A holder that died on this host is taken over at once; the TTL only
                    // decides for a holder on another host, whose pid means nothing here.
                    let current = self.query_opt(
                        "SELECT holder FROM state_lease WHERE lease_key = ?1",
                        &[key.into()],
                        |r| r.text(0),
                    )?;
                    if let Some(dead) = current.filter(|h| holder_is_dead_here(h, &host)) {
                        got = self.query_opt(
                            "UPDATE state_lease SET holder = ?1, \
                             expires_at = now() + CAST(CAST(?2 AS TEXT) AS INTERVAL) \
                             WHERE lease_key = ?3 AND holder = ?4 RETURNING CAST(1 AS BIGINT)",
                            &[
                                holder.as_str().into(),
                                format!("{ttl} seconds").into(),
                                key.into(),
                                dead.as_str().into(),
                            ],
                            |r| r.i64(0),
                        )?;
                    }
                }
                if got.is_none() {
                    return Ok(None);
                }
                let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let handle = {
                    let (url, key, holder, stop) =
                        (url.clone(), key.to_string(), holder.clone(), stop.clone());
                    std::thread::spawn(move || heartbeat(url, key, holder, ttl, stop))
                };
                Ok(Some(LoadLease {
                    store: self,
                    key: key.to_string(),
                    holder,
                    _file: None,
                    beat: Some((stop, handle)),
                }))
            }
            StateRef::Sqlite(db) => {
                use std::os::unix::io::AsRawFd;
                let file = std::fs::File::create(lease_path(db, key))?;
                let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if rc == 0 {
                    return Ok(Some(LoadLease {
                        store: self,
                        key: key.to_string(),
                        holder: String::new(),
                        _file: Some(file),
                        beat: None,
                    }));
                }
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    return Ok(None);
                }
                Err(err.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only a holder on THIS host whose process is gone may be taken over without the TTL.
    #[test]
    fn a_dead_holder_on_this_host_is_recognised_and_nothing_else_is() {
        let host = host_name();
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead = child.id();
        child.wait().unwrap();
        assert!(holder_is_dead_here(&format!("{host}:{dead}:1"), &host));
        let me = std::process::id();
        assert!(
            !holder_is_dead_here(&format!("{host}:{me}:1"), &host),
            "a live holder"
        );
        assert!(
            !holder_is_dead_here(&format!("elsewhere:{dead}:1"), &host),
            "another host"
        );
        assert!(
            !holder_is_dead_here("garbage", &host),
            "an unparseable holder"
        );
    }

    /// Two loads of one table: the second is refused while the first holds the
    /// lease, and admitted once it is dropped — no timer, no cleanup step.
    #[test]
    fn a_second_load_of_the_same_table_waits_for_the_first_to_release() {
        let a = StateStore::open_in_memory().unwrap();
        let b = StateStore::open_in_memory().unwrap();
        let key = format!("p.d.orders_{}", std::process::id());
        let held = a.try_load_lease(&key).unwrap().expect("first holder");
        assert!(b.try_load_lease(&key).unwrap().is_none(), "busy while held");
        assert!(
            b.try_load_lease(&format!("{key}_other")).unwrap().is_some(),
            "another table is independent"
        );
        drop(held);
        assert!(
            b.try_load_lease(&key).unwrap().is_some(),
            "free once released"
        );
    }
}

#[cfg(test)]
mod lease_path_tests {
    use super::*;

    /// Where the SQLite lease sidecar lives: BESIDE the state DB for a real file,
    /// in the temp dir (per process) only for `:memory:`. Flipping the two gives
    /// every file-backed store a per-process path, so two rivets sharing one state
    /// DB would each "hold" the same table's lease.
    #[test]
    fn the_lease_sidecar_sits_beside_the_state_db() {
        let beside = lease_path(
            std::path::Path::new("/tmp/rivet-x/.rivet_state.db"),
            "p.d.orders",
        );
        assert_eq!(
            beside,
            std::path::Path::new("/tmp/rivet-x/.rivet_state.db.lease-p.d.orders")
        );
        let mem = lease_path(std::path::Path::new(":memory:"), "p.d.orders");
        assert!(mem.starts_with(std::env::temp_dir()), "{mem:?}");
        assert!(
            mem.to_string_lossy()
                .contains(&std::process::id().to_string()),
            "an in-memory store leases per process: {mem:?}"
        );
    }
}
