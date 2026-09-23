use crate::error::Result;

use super::{StateRef, StateStore};

/// One `rivet load` at a time per warehouse table. Held for the process: a
/// Postgres session advisory lock on a shared state, an `flock` on a sidecar next
/// to a SQLite state — both released by the OS when the holder dies, so a crash
/// never wedges the next load and no clock decides staleness.
pub struct LoadLease<'a> {
    store: &'a StateStore,
    key: String,
    _file: Option<std::fs::File>,
}

impl Drop for LoadLease<'_> {
    fn drop(&mut self) {
        if let StateRef::Postgres(_) = &self.store.state_ref {
            let _ = self.store.execute(
                "SELECT pg_advisory_unlock(hashtext(?1))",
                &[self.key.as_str().into()],
            );
        }
    }
}

/// The sidecar an SQLite-state lease locks: beside the DB file, one per table.
fn lease_path(db: &std::path::Path, key: &str) -> std::path::PathBuf {
    // The readable token is lossy (case, non-ASCII); the hash keeps distinct keys on distinct files.
    let token = format!(
        "{}-{:016x}",
        crate::manifest::file_token(key),
        xxhash_rust::xxh3::xxh3_64(key.as_bytes())
    );
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
            StateRef::Postgres(_) => {
                let got = self
                    .query_opt(
                        "SELECT CAST(CASE WHEN pg_try_advisory_lock(hashtext(?1)) THEN 1 ELSE 0 END AS BIGINT)",
                        &[key.into()],
                        |r| r.i64(0),
                    )?
                    .unwrap_or(0);
                Ok((got == 1).then(|| LoadLease {
                    store: self,
                    key: key.to_string(),
                    _file: None,
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
                        _file: Some(file),
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

    #[test]
    fn keys_a_lossy_token_would_merge_get_their_own_lease_file() {
        let db = std::path::Path::new("/tmp/state.db");
        let paths = [
            "p.d.Orders",
            "p.d.orders",
            "p.d.\u{437}\u{430}\u{43a}",
            "p.d.\u{442}\u{43e}\u{432}",
        ]
        .map(|k| lease_path(db, k).to_string_lossy().to_lowercase());
        for (i, a) in paths.iter().enumerate() {
            for b in &paths[i + 1..] {
                assert_ne!(a, b, "one flock would make one table refuse the other");
            }
        }
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
        assert_eq!(beside.parent(), Some(std::path::Path::new("/tmp/rivet-x")));
        assert!(
            beside
                .to_string_lossy()
                .starts_with("/tmp/rivet-x/.rivet_state.db.lease-p.d.orders-"),
            "{}",
            beside.display()
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
