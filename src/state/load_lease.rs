use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak, mpsc};
use std::time::{Duration, Instant};

use crate::error::{CodedError, Result, codes};

use super::{StateRef, StateStore};

/// One holder at a time per key (a warehouse table, an export's checkpointed run). On a
/// Postgres state it is a `state_lease` row renewed by this process's lease keeper and
/// expired by the SERVER's clock — a single statement, so it holds behind a
/// transaction-mode pooler, where a session advisory lock does not. On a SQLite state it
/// is an `flock` on a sidecar, released by the OS when the holder dies.
pub struct LoadLease<'a> {
    store: &'a StateStore,
    key: String,
    holder: String,
    _file: Option<std::fs::File>,
    kept: Option<(Arc<Keeper>, u64)>,
}

impl LoadLease<'_> {
    /// Whether this process still holds the lease: always under an `flock`; on Postgres, while the keeper runs and renewed the row within the TTL.
    pub fn is_held(&self) -> bool {
        self.kept
            .as_ref()
            .is_none_or(|(keeper, id)| keeper.shared.holds(*id))
    }
}

impl Drop for LoadLease<'_> {
    fn drop(&mut self) {
        if let Some((keeper, id)) = self.kept.take() {
            keeper.shared.rows().remove(&id);
            let _ = self.store.execute(
                "DELETE FROM state_lease WHERE lease_key = ?1 AND holder = ?2",
                &[self.key.as_str().into(), self.holder.as_str().into()],
            );
        }
    }
}

#[cfg(test)]
impl<'a> LoadLease<'a> {
    /// A held lease on `key` as a Postgres state grants one, kept by a keeper with no thread and no connection.
    pub(crate) fn kept_for_test(store: &'a StateStore, key: &str) -> Self {
        let shared = Arc::new(Shared {
            ttl: 30,
            alive: AtomicBool::new(true),
            next_id: AtomicU64::new(0),
            rows: Mutex::new(HashMap::new()),
        });
        let id = shared.keep(key, "h:1:1", Instant::now());
        let keeper = Arc::new(Keeper {
            shared,
            stop: None,
            thread: None,
        });
        Self {
            store,
            key: key.to_string(),
            holder: "h:1:1".to_string(),
            _file: None,
            kept: Some((keeper, id)),
        }
    }

    /// Lose the lease the way a renewal that found another holder on its row does.
    pub(crate) fn lose_for_test(&self) {
        if let Some((keeper, _)) = &self.kept {
            let shared = &keeper.shared;
            shared.settle(&shared.asked(), &HashSet::new(), Instant::now());
        }
    }
}

/// One `state_lease` row the keeper renews.
struct Kept {
    key: String,
    holder: String,
    renewed: Instant,
    lost: bool,
}

impl Kept {
    /// Whether the row is still ours `age` after its last renewal: not lost, and less than a whole `ttl` seconds old.
    fn fresh(&self, age: Duration, ttl: u64) -> bool {
        !self.lost && age < Duration::from_secs(ttl)
    }
}

/// What the keeper's thread and the leases it keeps share.
struct Shared {
    ttl: u64,
    alive: AtomicBool,
    next_id: AtomicU64,
    rows: Mutex<HashMap<u64, Kept>>,
}

impl Shared {
    /// The kept rows; a poisoned lock still answers.
    fn rows(&self) -> MutexGuard<'_, HashMap<u64, Kept>> {
        self.rows.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Start keeping `key` for `holder`, whose row was written at `granted`.
    fn keep(&self, key: &str, holder: &str, granted: Instant) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let kept = Kept {
            key: key.to_string(),
            holder: holder.to_string(),
            renewed: granted,
            lost: false,
        };
        self.rows().insert(id, kept);
        id
    }

    /// Whether row `id` is still ours: the keeper runs, the row was ours at its last renewal, and that was within the TTL.
    fn holds(&self, id: u64) -> bool {
        self.alive.load(Ordering::Relaxed)
            && self
                .rows()
                .get(&id)
                .is_some_and(|k| k.fresh(k.renewed.elapsed(), self.ttl))
    }

    /// The `(id, key, holder)` of every row to renew.
    fn asked(&self) -> Vec<(u64, String, String)> {
        self.rows()
            .iter()
            .map(|(id, k)| (*id, k.key.clone(), k.holder.clone()))
            .collect()
    }

    /// Record one renewal sent at `sent`: an asked row among `ours` is fresh, any other asked row is lost.
    fn settle(
        &self,
        asked: &[(u64, String, String)],
        ours: &HashSet<(String, String)>,
        sent: Instant,
    ) {
        let mut rows = self.rows();
        for (id, key, holder) in asked {
            let Some(kept) = rows.get_mut(id) else {
                continue;
            };
            if ours.contains(&(key.clone(), holder.clone())) {
                kept.renewed = sent;
            } else if !kept.lost {
                kept.lost = true;
                log::error!("state lease '{key}': lost — another process holds it now");
            }
        }
    }

    /// Renew every kept row in one statement.
    fn renew(&self, client: &mut postgres::Client) -> std::result::Result<(), postgres::Error> {
        let asked = self.asked();
        if asked.is_empty() {
            return Ok(());
        }
        let keys: Vec<&str> = asked.iter().map(|(_, k, _)| k.as_str()).collect();
        let holders: Vec<&str> = asked.iter().map(|(_, _, h)| h.as_str()).collect();
        let sent = Instant::now();
        let rows = client.query(
            "UPDATE state_lease AS l SET expires_at = now() + CAST($3::text AS INTERVAL) \
             FROM unnest($1::text[], $2::text[]) AS mine(lease_key, holder) \
             WHERE l.lease_key = mine.lease_key AND l.holder = mine.holder \
             RETURNING l.lease_key, l.holder",
            &[&keys, &holders, &format!("{} seconds", self.ttl)],
        )?;
        let ours = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        self.settle(&asked, &ours, sent);
        Ok(())
    }
}

/// Marks the keeper dead when its thread ends, a panic included.
struct Dead<'a>(&'a AtomicBool);

impl Drop for Dead<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// The keeper's thread: renew every third of the TTL until the keeper is dropped or its connection is gone.
fn keep_alive(mut client: postgres::Client, shared: &Shared, stopped: &mpsc::Receiver<()>) {
    let _dead = Dead(&shared.alive);
    let every = Duration::from_secs(shared.ttl.div_ceil(3));
    while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(every) {
        match shared.renew(&mut client) {
            Ok(()) => {}
            Err(e) if client.is_closed() => {
                log::error!(
                    "state lease keeper: its connection is gone ({e:#}); every lease it kept is lost"
                );
                return;
            }
            Err(e) => log::warn!("state lease keeper: renewal failed ({e:#}); retrying"),
        }
    }
}

/// One connection and one thread that renew every Postgres lease this process holds on one state database.
struct Keeper {
    shared: Arc<Shared>,
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Keeper {
    /// Connect to `url` and start renewing; no lease is kept by a keeper that could not connect.
    fn start(url: &str, ttl: u64) -> Result<Self> {
        let client = super::connect_pg(url)?;
        let shared = Arc::new(Shared {
            ttl,
            alive: AtomicBool::new(true),
            next_id: AtomicU64::new(0),
            rows: Mutex::new(HashMap::new()),
        });
        let (stop, stopped) = mpsc::channel();
        let thread = {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("rivet-lease-keeper".into())
                .spawn(move || keep_alive(client, &shared, &stopped))?
        };
        Ok(Self {
            shared,
            stop: Some(stop),
            thread: Some(thread),
        })
    }
}

impl Drop for Keeper {
    fn drop(&mut self) {
        drop(self.stop.take());
        if self.thread.take().is_some_and(|t| t.join().is_err()) {
            log::error!("state lease keeper: its thread panicked; the leases it kept were lost");
        }
    }
}

/// The keepers of this process, one per state database URL.
static KEEPERS: Mutex<Vec<(String, Weak<Keeper>)>> = Mutex::new(Vec::new());

/// This process's running keeper for `url`, started first when there is none.
fn keeper_for(url: &str, ttl: u64) -> Result<Arc<Keeper>> {
    let mut keepers = KEEPERS.lock().unwrap_or_else(PoisonError::into_inner);
    let running = keepers
        .iter()
        .filter(|(u, _)| u == url)
        .filter_map(|(_, k)| k.upgrade())
        .find(|k| k.shared.alive.load(Ordering::Relaxed));
    if let Some(keeper) = running {
        return Ok(keeper);
    }
    let keeper = Arc::new(Keeper::start(url, ttl)?);
    keepers.retain(|(u, k)| u != url && k.strong_count() > 0);
    keepers.push((url.to_string(), Arc::downgrade(&keeper)));
    Ok(keeper)
}

/// The refusal of a lease whose keeper could not be started.
fn keeper_unavailable(key: &str, cause: &anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(CodedError::new(
        codes::STATE_LEASE_KEEPER_UNAVAILABLE,
        format!(
            "cannot take the lease on `{key}`: the connection that keeps this process's leases \
             alive could not be opened ({cause:#}). Nothing ran under the lease. rivet holds one \
             state connection per worker plus one for the lease keeper: free connections on the \
             state database (or raise its `max_connections`), raise the open-file limit \
             (`ulimit -n`), or lower `--pool`, then run again."
        ),
    ))
}

/// Seconds a Postgres lease outlives its last renewal (`RIVET_STATE_LEASE_TTL_S`, default 30).
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
                let keeper = keeper_for(url, ttl).map_err(|e| keeper_unavailable(key, &e))?;
                let granted = Instant::now();
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
                let id = keeper.shared.keep(key, &holder, granted);
                Ok(Some(LoadLease {
                    store: self,
                    key: key.to_string(),
                    holder,
                    _file: None,
                    kept: Some((keeper, id)),
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
                        kept: None,
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
        // A negative pid names a process GROUP to kill(2); it must never read as a dead holder.
        assert!(
            !holder_is_dead_here(&format!("{host}:-99999:1"), &host),
            "a negative pid"
        );
    }

    /// The host part of a holder id is the OS hostname, read independently of `host_name`.
    #[test]
    fn the_holder_host_is_the_os_hostname() {
        let os = std::process::Command::new("hostname").output().unwrap();
        assert_eq!(host_name(), String::from_utf8_lossy(&os.stdout).trim());
    }

    /// A keeper's shared state holding nothing, as a running keeper has it.
    fn shared(ttl: u64) -> Shared {
        Shared {
            ttl,
            alive: AtomicBool::new(true),
            next_id: AtomicU64::new(0),
            rows: Mutex::new(HashMap::new()),
        }
    }

    /// The `(key, holder)` pairs a renewal returned.
    fn ours(pairs: &[(&str, &str)]) -> HashSet<(String, String)> {
        pairs
            .iter()
            .map(|(k, h)| (k.to_string(), h.to_string()))
            .collect()
    }

    /// A kept row is held while the keeper runs, and not once the keeper's thread has ended.
    #[test]
    fn a_lease_is_held_only_while_its_keeper_runs() {
        let s = shared(30);
        let id = s.keep("p.d.orders", "h:1:1", Instant::now());
        assert!(s.holds(id));
        assert!(!s.holds(id + 1), "a row the keeper never kept");
        drop(Dead(&s.alive));
        assert!(!s.holds(id), "the keeper's thread ended");
    }

    /// A row last renewed a whole TTL ago is not held, whatever the keeper last heard.
    #[test]
    fn a_lease_not_renewed_within_its_ttl_is_not_held() {
        let s = shared(30);
        let now = Instant::now();
        let fresh = s.keep("p.d.a", "h:1:1", now - Duration::from_secs(29));
        let stale = s.keep("p.d.b", "h:1:2", now - Duration::from_secs(30));
        assert!(s.holds(fresh));
        assert!(!s.holds(stale));
    }

    /// The TTL boundary is exclusive: a row is ours up to, and not at, one whole TTL after its last renewal.
    #[test]
    fn a_lease_exactly_one_ttl_old_is_not_held() {
        let kept = |lost| Kept {
            key: "p.d.a".to_string(),
            holder: "h:1:1".to_string(),
            renewed: Instant::now(),
            lost,
        };
        let ttl = Duration::from_secs(30);
        assert!(kept(false).fresh(ttl - Duration::from_nanos(1), 30));
        assert!(!kept(false).fresh(ttl, 30), "exactly one TTL old");
        assert!(!kept(true).fresh(Duration::ZERO, 30), "a lost row");
    }

    /// A lease a keeper keeps answers for its own row: held while the row is ours, not once a renewal lost it.
    #[test]
    fn a_kept_lease_is_held_until_its_keeper_loses_its_row() {
        let store = StateStore::open_in_memory().unwrap();
        let lease = LoadLease::kept_for_test(&store, "p.d.orders");
        let other = LoadLease::kept_for_test(&store, "p.d.other");
        assert!(lease.is_held());
        lease.lose_for_test();
        assert!(!lease.is_held(), "a renewal found another holder");
        assert!(other.is_held(), "another keeper's row is untouched");
    }

    /// One renewal answers for every asked row: the rows that came back are fresh, the others are lost for good.
    #[test]
    fn one_renewal_settles_every_kept_row() {
        let s = shared(30);
        let granted = Instant::now() - Duration::from_secs(20);
        let a = s.keep("p.d.a", "h:1:1", granted);
        let b = s.keep("p.d.b", "h:1:2", granted);
        let c = s.keep("p.d.c", "h:1:3", granted);
        let mut asked = s.asked();
        asked.sort();
        assert_eq!(
            asked,
            vec![
                (a, "p.d.a".to_string(), "h:1:1".to_string()),
                (b, "p.d.b".to_string(), "h:1:2".to_string()),
                (c, "p.d.c".to_string(), "h:1:3".to_string()),
            ]
        );
        let late = s.keep("p.d.late", "h:1:4", granted);
        s.rows().remove(&c);
        let sent = Instant::now();
        s.settle(
            &asked,
            &ours(&[("p.d.a", "h:1:1"), ("p.d.c", "h:1:3")]),
            sent,
        );
        let rows = s.rows();
        assert_eq!((rows[&a].renewed, rows[&a].lost), (sent, false));
        assert_eq!((rows[&b].renewed, rows[&b].lost), (granted, true));
        assert_eq!((rows[&late].renewed, rows[&late].lost), (granted, false));
        assert!(!rows.contains_key(&c), "a released row is not kept again");
        drop(rows);
        assert!(s.holds(a) && !s.holds(b));
        s.settle(
            &asked,
            &ours(&[("p.d.a", "h:1:1"), ("p.d.b", "h:1:2")]),
            sent,
        );
        assert!(
            !s.holds(b),
            "a lost row stays lost when the row is ours again"
        );
    }

    /// A row of the same key under another holder is not ours.
    #[test]
    fn a_row_renewed_for_another_holder_is_lost() {
        let s = shared(30);
        let id = s.keep("p.d.a", "h:1:1", Instant::now());
        s.settle(&s.asked(), &ours(&[("p.d.a", "h:2:9")]), Instant::now());
        assert!(!s.holds(id));
    }

    /// The refusal of a lease with no keeper carries its code, the cause and what frees a connection.
    #[test]
    fn a_lease_without_a_keeper_is_refused_by_code() {
        let e = keeper_unavailable("p.d.orders", &anyhow::anyhow!("too many clients"));
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_STATE_LEASE_KEEPER_UNAVAILABLE")
        );
        assert_eq!(
            e.to_string(),
            "cannot take the lease on `p.d.orders`: the connection that keeps this process's \
             leases alive could not be opened (too many clients). Nothing ran under the lease. \
             rivet holds one state connection per worker plus one for the lease keeper: free \
             connections on the state database (or raise its `max_connections`), raise the \
             open-file limit (`ulimit -n`), or lower `--pool`, then run again."
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
        assert!(held.is_held(), "an flock is held until it is dropped");
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
