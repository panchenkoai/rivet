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
