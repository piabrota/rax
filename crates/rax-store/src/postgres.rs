//! A PostgreSQL store and [`Locker`].
//!
//! Resources are stored as protobuf JSON documents in the same tables AX
//! uses, so RAX and AX can read each other's records. [`PostgresStore::open`]
//! takes a libpq-style connection URL; `PGHOST`, `PGPORT`, `PGUSER`,
//! `PGPASSWORD` and `PGDATABASE` fill in what it leaves out. It creates any
//! missing tables and indexes.

use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use rand::RngCore;
use rax_api::Manifest;
use rax_api::v1alpha1::{Model, Task, TaskStatus, Workspace};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Config, NoTls};

use crate::lock::{LockGuard, Locker, lock_key};
use crate::{
    DEFAULT_TASK_LIST_LIMIT, ResourceStore, Result, StoreError, TaskStore, all_atespaces,
    default_atespace, meta_key, prepare_model, prepare_task, prepare_workspace,
};

const SCHEMA: &str = include_str!("schema.sql");

/// The advisory lock key that serializes schema creation when several server
/// replicas start at once; concurrent `CREATE ... IF NOT EXISTS` statements
/// can otherwise fail.
const SCHEMA_LOCK_ID: i64 = 0x6178_7363_6865_6d61; // "axschema"

#[derive(Debug, Clone)]
pub struct PostgresOptions {
    /// The lease duration of a lock. Held locks are renewed every third of it,
    /// so this does not limit how long a lock may be held; it bounds how long
    /// a lock survives a crashed holder.
    pub lock_ttl: Duration,
    /// How often lock waiters retry acquisition.
    pub lock_retry_interval: Duration,
    pub max_connections: usize,
}

impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            lock_ttl: Duration::from_secs(30),
            lock_retry_interval: Duration::from_secs(1),
            max_connections: 16,
        }
    }
}

/// A PostgreSQL implementation of the store traits. It is also a [`Locker`]
/// whose locks are shared by every server replica using the same database.
#[derive(Clone)]
pub struct PostgresStore {
    pool: Pool,
    opts: PostgresOptions,
}

fn backend(context: &str) -> impl Fn(tokio_postgres::Error) -> StoreError + '_ {
    move |e| StoreError::Backend(format!("{context}: {}", describe(&e)))
}

/// Includes the database's own message, which `Display` on
/// `tokio_postgres::Error` leaves out.
fn describe(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{e}: {}", db.message()),
        None => e.to_string(),
    }
}

fn pool_error(e: deadpool_postgres::PoolError) -> StoreError {
    StoreError::Backend(format!("getting a database connection: {e}"))
}

/// Parses a connection URL, filling in what it leaves out from the libpq
/// environment variables.
pub fn parse_config(url: &str) -> Result<Config> {
    let mut cfg = Config::from_str(url)
        .map_err(|e| StoreError::Invalid(format!("parsing postgres URL: {e}")))?;
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    if cfg.get_hosts().is_empty()
        && let Some(h) = env("PGHOST")
    {
        cfg.host(&h);
    }
    if cfg.get_ports().is_empty()
        && let Some(p) = env("PGPORT").and_then(|p| p.parse().ok())
    {
        cfg.port(p);
    }
    if cfg.get_user().is_none()
        && let Some(u) = env("PGUSER")
    {
        cfg.user(&u);
    }
    if cfg.get_password().is_none()
        && let Some(p) = env("PGPASSWORD")
    {
        cfg.password(p);
    }
    if cfg.get_dbname().is_none()
        && let Some(d) = env("PGDATABASE")
    {
        cfg.dbname(&d);
    }
    Ok(cfg)
}

/// Describes where a connection URL points without its password, for logs.
pub fn describe_url(url: &str) -> String {
    let Ok(cfg) = Config::from_str(url) else {
        return "<unparseable>".into();
    };
    let host = match cfg.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
        #[cfg(unix)]
        Some(tokio_postgres::config::Host::Unix(p)) => p.display().to_string(),
        None => "localhost".into(),
    };
    let port = cfg.get_ports().first().copied().unwrap_or(5432);
    let user = cfg.get_user().unwrap_or("");
    let db = cfg.get_dbname().unwrap_or("");
    if host.contains(':') {
        format!("{user}@[{host}]:{port}/{db}")
    } else {
        format!("{user}@{host}:{port}/{db}")
    }
}

impl PostgresStore {
    /// Connects to the database at `url` and creates any missing tables.
    pub async fn open(url: &str, opts: PostgresOptions) -> Result<Self> {
        let cfg = parse_config(url)?;
        let mgr = Manager::from_config(
            cfg,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(opts.max_connections)
            .build()
            .map_err(|e| StoreError::Backend(format!("creating connection pool: {e}")))?;
        let store = Self { pool, opts };
        store.create_schema().await?;
        Ok(store)
    }

    /// Creates any missing tables and indexes. It only adds what is missing,
    /// so it is safe to run on every start.
    pub async fn create_schema(&self) -> Result<()> {
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let err = backend("creating postgres schema");
        let tx = client.transaction().await.map_err(&err)?;
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&SCHEMA_LOCK_ID])
            .await
            .map_err(&err)?;
        tx.batch_execute(SCHEMA).await.map_err(&err)?;
        tx.commit().await.map_err(&err)
    }

    async fn save<T: Manifest>(&self, table: &str, key: (String, String), m: &T) -> Result<()> {
        let data = rax_api::to_json(m)
            .map_err(|e| StoreError::Backend(format!("encoding {table}: {e}")))?;
        let client = self.pool.get().await.map_err(pool_error)?;
        client
            .execute(
                &format!(
                    "INSERT INTO {table} (atespace, name, data, saved_at)
                     VALUES ($1, $2, $3, clock_timestamp())
                     ON CONFLICT (atespace, name) DO UPDATE
                     SET data = EXCLUDED.data, saved_at = EXCLUDED.saved_at"
                ),
                &[&key.0, &key.1, &data],
            )
            .await
            .map_err(backend(&format!("saving {table}")))?;
        Ok(())
    }

    async fn get<T: Manifest>(&self, table: &str, atespace: &str, name: &str) -> Result<T> {
        let client = self.pool.get().await.map_err(pool_error)?;
        let row = client
            .query_opt(
                &format!("SELECT data FROM {table} WHERE atespace = $1 AND name = $2"),
                &[&default_atespace(atespace), &name],
            )
            .await
            .map_err(backend(&format!("getting {table}")))?
            .ok_or(StoreError::NotFound)?;
        decode(table, row.get(0))
    }

    /// Lists resources of one kind, most recently saved first. A `limit` of
    /// zero or less means no limit. Records that no longer parse are skipped
    /// and logged.
    async fn list<T: Manifest>(
        &self,
        table: &str,
        atespace: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<T>> {
        let mut query = format!("SELECT data FROM {table}");
        let mut args: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        if !all_atespaces(atespace) {
            args.push(Box::new(atespace.to_owned()));
            query += " WHERE atespace = $1";
        }
        query += " ORDER BY saved_at DESC, atespace, name";
        if limit > 0 {
            args.push(Box::new(limit));
            query += &format!(" LIMIT ${}", args.len());
        }
        if offset > 0 {
            args.push(Box::new(offset));
            query += &format!(" OFFSET ${}", args.len());
        }
        let params: Vec<&(dyn ToSql + Sync)> = args
            .iter()
            .map(|a| a.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let client = self.pool.get().await.map_err(pool_error)?;
        let rows = client
            .query(&query, &params)
            .await
            .map_err(backend(&format!("listing {table}")))?;
        Ok(rows
            .into_iter()
            .filter_map(|row| match decode(table, row.get(0)) {
                Ok(m) => Some(m),
                Err(e) => {
                    tracing::warn!(table, error = %e, "skipping unreadable record");
                    None
                }
            })
            .collect())
    }

    async fn delete(&self, table: &str, atespace: &str, name: &str) -> Result<()> {
        let client = self.pool.get().await.map_err(pool_error)?;
        client
            .execute(
                &format!("DELETE FROM {table} WHERE atespace = $1 AND name = $2"),
                &[&default_atespace(atespace), &name],
            )
            .await
            .map_err(backend(&format!("deleting {table}")))?;
        Ok(())
    }

    fn ttl_millis(&self) -> i64 {
        (self.opts.lock_ttl.as_millis() as i64).max(1)
    }

    /// Takes the lease for `key`, succeeding if no lease exists or the
    /// existing one has expired.
    async fn try_lock(&self, key: &str, token: &str) -> Result<bool> {
        let client = self.pool.get().await.map_err(pool_error)?;
        let n = client
            .execute(
                "INSERT INTO locks (key, token, expires_at)
                 VALUES ($1, $2, clock_timestamp() + $3::bigint * interval '1 millisecond')
                 ON CONFLICT (key) DO UPDATE
                 SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
                 WHERE locks.expires_at <= clock_timestamp()",
                &[&key, &token, &self.ttl_millis()],
            )
            .await
            .map_err(backend(&format!("acquiring lock {key}")))?;
        Ok(n == 1)
    }

    /// Extends the lease every third of the TTL until `stop` fires or the
    /// lease is found to belong to someone else, then releases it.
    async fn hold(self, key: String, token: String, mut stop: oneshot::Receiver<()>) {
        let interval = (self.opts.lock_ttl / 3).max(Duration::from_millis(1));
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            tokio::select! {
                _ = &mut stop => break,
                _ = ticker.tick() => {}
            }
            let renewed = tokio::time::timeout(interval, async {
                let client = self.pool.get().await.map_err(pool_error)?;
                client
                    .execute(
                        "UPDATE locks SET expires_at = clock_timestamp() + $3::bigint * interval '1 millisecond'
                         WHERE key = $1 AND token = $2",
                        &[&key, &token, &self.ttl_millis()],
                    )
                    .await
                    .map_err(backend("renewing lock lease"))
            })
            .await;
            match renewed {
                Ok(Ok(0)) => {
                    // The lease expired and was taken over; there is nothing
                    // left to renew or release.
                    tracing::error!(key, "lock lease lost before release");
                    return;
                }
                Ok(Ok(_)) => {}
                // Transient: keep trying while the remaining lease covers us.
                Ok(Err(e)) => tracing::warn!(key, error = %e, "renewing lock lease"),
                Err(_) => tracing::warn!(key, "renewing lock lease timed out"),
            }
        }
        let released = tokio::time::timeout(Duration::from_secs(5), async {
            let client = self.pool.get().await.map_err(pool_error)?;
            client
                .execute(
                    "DELETE FROM locks WHERE key = $1 AND token = $2",
                    &[&key, &token],
                )
                .await
                .map_err(backend("releasing lock"))
        })
        .await;
        if !matches!(released, Ok(Ok(_))) {
            // The lease will expire on its own after the TTL.
            tracing::warn!(key, "could not release lock");
        }
    }

    #[cfg(test)]
    async fn exec(&self, sql: &str) {
        self.pool
            .get()
            .await
            .unwrap()
            .batch_execute(sql)
            .await
            .unwrap();
    }
}

fn decode<T: Manifest>(table: &str, data: Value) -> Result<T> {
    rax_api::from_json_lenient(data)
        .map_err(|e| StoreError::Backend(format!("decoding {table}: {e}")))
}

#[async_trait]
impl TaskStore for PostgresStore {
    async fn save_task(&self, task: &mut Task) -> Result<()> {
        prepare_task(task)?;
        self.save("tasks", meta_key(&task.metadata), task).await
    }

    async fn get_task(&self, atespace: &str, name: &str) -> Result<Task> {
        self.get("tasks", atespace, name).await
    }

    async fn list_tasks(&self, atespace: &str, limit: i64, offset: i64) -> Result<Vec<Task>> {
        let limit = if limit <= 0 {
            DEFAULT_TASK_LIST_LIMIT
        } else {
            limit
        };
        self.list("tasks", atespace, limit, offset).await
    }

    async fn update_task_status(
        &self,
        atespace: &str,
        name: &str,
        status: TaskStatus,
    ) -> Result<()> {
        let atespace = default_atespace(atespace);
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let err = backend("updating task status");
        let tx = client.transaction().await.map_err(&err)?;
        let row = tx
            .query_opt(
                "SELECT data FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE",
                &[&atespace, &name],
            )
            .await
            .map_err(&err)?
            .ok_or(StoreError::NotFound)?;
        let mut task: Task = decode("tasks", row.get(0))?;
        task.status = Some(status);
        let data = rax_api::to_json(&task)
            .map_err(|e| StoreError::Backend(format!("encoding task: {e}")))?;
        tx.execute(
            "UPDATE tasks SET data = $3 WHERE atespace = $1 AND name = $2",
            &[&atespace, &name, &data],
        )
        .await
        .map_err(&err)?;
        tx.commit().await.map_err(&err)
    }

    async fn delete_task(&self, atespace: &str, name: &str) -> Result<()> {
        self.delete("tasks", atespace, name).await
    }
}

#[async_trait]
impl ResourceStore for PostgresStore {
    async fn save_workspace(&self, ws: &mut Workspace) -> Result<()> {
        prepare_workspace(ws)?;
        self.save("workspaces", meta_key(&ws.metadata), ws).await
    }

    async fn get_workspace(&self, atespace: &str, name: &str) -> Result<Workspace> {
        self.get("workspaces", atespace, name).await
    }

    async fn list_workspaces(&self, atespace: &str) -> Result<Vec<Workspace>> {
        self.list("workspaces", atespace, 0, 0).await
    }

    async fn delete_workspace(&self, atespace: &str, name: &str) -> Result<()> {
        self.delete("workspaces", atespace, name).await
    }

    async fn save_model(&self, model: &mut Model) -> Result<()> {
        prepare_model(model)?;
        self.save("models", meta_key(&model.metadata), model).await
    }

    async fn get_model(&self, atespace: &str, name: &str) -> Result<Model> {
        self.get("models", atespace, name).await
    }

    async fn list_models(&self, atespace: &str) -> Result<Vec<Model>> {
        self.list("models", atespace, 0, 0).await
    }

    async fn delete_model(&self, atespace: &str, name: &str) -> Result<()> {
        self.delete("models", atespace, name).await
    }
}

/// Locks are leases stored in the `locks` table. All lease times come from the
/// database clock, so replicas do not need synchronized clocks.
///
/// A lock is a row holding a random token and an expiry. Acquiring inserts the
/// row, or takes it over if its lease has expired; releasing deletes it, only
/// if it still holds the caller's token. Waiters retry every
/// `lock_retry_interval`.
///
/// Advisory locks are not used because they belong to a database session: a
/// lock held across a long Substrate call would pin a pooled connection, and
/// would be released silently if that connection dropped.
///
/// A held lock is kept alive by a background task that extends its lease
/// every third of the TTL until the guard is released, so the TTL bounds only
/// how long a lock can be orphaned after its holder crashes. Renewal is
/// best-effort and there is no fencing token: if renewals keep failing for
/// longer than the TTL, the lease can expire while the holder is still
/// working, and the loss is only logged.
#[async_trait]
impl Locker for PostgresStore {
    async fn lock(&self, kind: &str, atespace: &str, name: &str) -> Result<LockGuard> {
        let key = lock_key(kind, atespace, name);
        let mut token = [0u8; 16];
        rand::rng().fill_bytes(&mut token);
        let token = hex::encode(token);

        let mut ticker = tokio::time::interval(self.opts.lock_retry_interval);
        loop {
            // The first tick completes immediately: the lock is usually free.
            ticker.tick().await;
            if self.try_lock(&key, &token).await? {
                break;
            }
        }

        let (stop, stopped) = oneshot::channel();
        let holder = tokio::spawn(self.clone().hold(key, token, stopped));
        Ok(LockGuard::new(move || {
            let _ = stop.send(());
            Box::pin(async move {
                let _ = holder.await;
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    //! These tests need a database. They run when `RAX_TEST_POSTGRES_URL` is
    //! set, and drop and recreate every RAX table in it.

    use super::*;
    use crate::testing;

    async fn open(opts: PostgresOptions) -> Option<PostgresStore> {
        let Ok(url) = std::env::var("RAX_TEST_POSTGRES_URL") else {
            eprintln!("RAX_TEST_POSTGRES_URL not set; skipping Postgres tests");
            return None;
        };
        let s = PostgresStore::open(&url, opts).await.unwrap();
        s.exec("TRUNCATE tasks, workspaces, models, locks").await;
        Some(s)
    }

    /// The tests share one database, so they take turns.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn conformance() {
        let _serial = SERIAL.lock().await;
        let Some(s) = open(PostgresOptions::default()).await else {
            return;
        };
        testing::conformance(&s).await;
    }

    #[tokio::test]
    async fn create_schema_concurrently() {
        let _serial = SERIAL.lock().await;
        let Some(s) = open(PostgresOptions::default()).await else {
            return;
        };
        s.exec("DROP TABLE tasks, workspaces, models, locks").await;
        // Simulate several replicas starting at once.
        let runs: Vec<_> = (0..4)
            .map(|_| {
                tokio::spawn({
                    let s = s.clone();
                    async move { s.create_schema().await }
                })
            })
            .collect();
        for r in runs {
            r.await.unwrap().unwrap();
        }
        s.save_task(&mut testing::task("default", "after-create"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn records_with_unknown_fields_still_load() {
        let _serial = SERIAL.lock().await;
        let Some(s) = open(PostgresOptions::default()).await else {
            return;
        };
        s.exec(
            r#"INSERT INTO tasks (atespace, name, data, saved_at) VALUES
               ('default', 'newer', '{"metadata":{"name":"newer","atespace":"default"},"spec":{"image":"x","fromTheFuture":1}}', now()),
               ('default', 'broken', '{"metadata":"not an object"}', now())"#,
        )
        .await;
        let t = s.get_task("default", "newer").await.unwrap();
        assert_eq!(t.spec.unwrap().image, "x");
        // A record that no longer parses is skipped by lists.
        let list = s.list_tasks("default", 0, 0).await.unwrap();
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn lock_renews_lease_while_held() {
        let _serial = SERIAL.lock().await;
        let opts = PostgresOptions {
            lock_ttl: Duration::from_millis(300),
            lock_retry_interval: Duration::from_millis(20),
            ..Default::default()
        };
        let Some(s) = open(opts).await else { return };
        let held = s.lock("task", "default", "renewed").await.unwrap();
        // Hold well past the TTL; renewal must keep others out.
        let other =
            tokio::time::timeout(Duration::from_secs(1), s.lock("task", "default", "renewed"))
                .await;
        assert!(other.is_err(), "a renewed lock was taken over");
        held.release().await;
        let other = tokio::time::timeout(
            Duration::from_millis(500),
            s.lock("task", "default", "renewed"),
        )
        .await;
        assert!(other.is_ok(), "a released lock must be free right away");
    }

    #[tokio::test]
    async fn lock_takes_over_expired_lease() {
        let _serial = SERIAL.lock().await;
        let opts = PostgresOptions {
            lock_ttl: Duration::from_millis(200),
            lock_retry_interval: Duration::from_millis(20),
            ..Default::default()
        };
        let Some(s) = open(opts).await else { return };
        // A holder that crashed left its lease behind.
        s.exec(&format!(
            "INSERT INTO locks (key, token, expires_at) VALUES ('{}', 'dead', clock_timestamp() + interval '200 milliseconds')",
            lock_key("task", "default", "orphan")
        ))
        .await;
        let start = tokio::time::Instant::now();
        let guard =
            tokio::time::timeout(Duration::from_secs(2), s.lock("task", "default", "orphan"))
                .await
                .expect("an expired lease must be taken over")
                .unwrap();
        assert!(
            start.elapsed() >= Duration::from_millis(100),
            "the lease was taken before it expired"
        );
        guard.release().await;
    }

    /// Port of AX's TestRestartKeepsTasksAndReleasesLocks: a server crashes
    /// holding a lock, and a new one opens the same database. The task is
    /// still there, and the lock can be taken once its lease runs out.
    #[tokio::test]
    async fn restart_keeps_tasks_and_releases_locks() {
        let _serial = SERIAL.lock().await;
        let opts = PostgresOptions {
            lock_ttl: Duration::from_millis(300),
            lock_retry_interval: Duration::from_millis(20),
            ..Default::default()
        };
        let Some(s1) = open(opts.clone()).await else {
            return;
        };
        drop(s1);
        let url = std::env::var("RAX_TEST_POSTGRES_URL").unwrap();

        // The first server runs on its own runtime, which is torn down with
        // the lock still held: its lease is never released or renewed again.
        let (crash_url, crash_opts) = (url.clone(), opts.clone());
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let s = PostgresStore::open(&crash_url, crash_opts).await.unwrap();
                s.save_task(&mut testing::task("default", "survivor"))
                    .await
                    .unwrap();
                let guard = s.lock("task", "default", "survivor").await.unwrap();
                std::mem::forget(guard);
            });
            rt.shutdown_background();
        })
        .join()
        .unwrap();

        // Reopen without truncating, so the data from before the crash is kept.
        let s2 = PostgresStore::open(&url, opts).await.unwrap();
        let got = s2.get_task("default", "survivor").await.unwrap();
        assert_eq!(got.metadata.unwrap().name, "survivor");
        let guard = tokio::time::timeout(
            Duration::from_secs(5),
            s2.lock("task", "default", "survivor"),
        )
        .await
        .expect("the crashed server's lease must expire")
        .unwrap();
        guard.release().await;
    }

    #[test]
    fn describes_urls_without_passwords() {
        for (url, want) in [
            (
                "postgres://ax:secret@db.example:5433/ax?sslmode=disable",
                "ax@db.example:5433/ax",
            ),
            ("postgres://ax@localhost/ax", "ax@localhost:5432/ax"),
            ("postgres://u@[::1]:5432/d", "u@[::1]:5432/d"),
            ("not a url %%", "<unparseable>"),
        ] {
            let got = describe_url(url);
            assert_eq!(got, want, "{url}");
            assert!(!got.contains("secret"));
        }
    }
}
