// Dynamic client registry with optional database-backed sync.
//
// On startup, CLIENTS is seeded from config.json. If DIRECT_DATABASE_URL
// (or DATABASE_URL fallback) is set,
// a background task prefers LISTEN/NOTIFY for incremental updates and keeps
// a low-frequency reconcile as a safety net. If LISTEN/NOTIFY is unavailable
// (for example transaction pooling), it falls back to polling.

use std::{
    collections::{HashMap, HashSet},
    io,
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::future;
use tracing::{error, info, warn};

use crate::config::{ClientConfig, CONFIG};

/// Dynamic client map. Initialized from config.json on startup, then
/// replaced by database contents if DIRECT_DATABASE_URL / DATABASE_URL is set.
pub static CLIENTS: LazyLock<RwLock<HashMap<String, ClientConfig>>> =
    LazyLock::new(|| RwLock::new(CONFIG.clients.clone()));

const CLIENT_DATA_STALE_AFTER_SECS: u64 = 120;
const DIRTY_FLUSH_INTERVAL_MS: u64 = 100;
const FULL_RECONCILE_INTERVAL_SECS: u64 = 60;
const DB_HEALTH_CHECK_INTERVAL_SECS: u64 = 10;
const LISTEN_CHANNEL: &str = "gateway_clients_changed";
static LAST_SUCCESSFUL_SYNC_UNIX_SECS: AtomicU64 = AtomicU64::new(0);

fn signal_initial_sync_ready(initial_sync_ready_tx: &mut Option<tokio::sync::oneshot::Sender<()>>) {
    let Some(initial_sync_ready_tx) = initial_sync_ready_tx.take() else {
        return;
    };

    let _ = initial_sync_ready_tx.send(());
}

fn unix_now_secs() -> Option<u64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    Some(now.as_secs())
}

fn mark_sync_success() {
    if let Some(now_secs) = unix_now_secs() {
        LAST_SUCCESSFUL_SYNC_UNIX_SECS.store(now_secs, Ordering::Relaxed);
    }
}

fn should_reject_stale_client_data() -> bool {
    if std::env::var("DIRECT_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .is_err()
    {
        return false;
    }

    let last_success = LAST_SUCCESSFUL_SYNC_UNIX_SECS.load(Ordering::Relaxed);
    if last_success == 0 {
        // Database sync is configured but has not succeeded yet.
        // Keep startup compatibility with config-seeded clients.
        return false;
    }

    let Some(now_secs) = unix_now_secs() else {
        return false;
    };

    now_secs.saturating_sub(last_success) > CLIENT_DATA_STALE_AFTER_SECS
}

/// Result of a client authentication attempt.
pub enum ClientAuthResult {
    /// Authentication succeeded.
    Ok(String, HashSet<u64>),
    /// Auth backend (database) is stale/unavailable — caller should treat as
    /// transient failure (503), not invalid credentials (401).
    Stale,
    /// Credentials are missing, malformed, or wrong.
    Invalid,
}

/// Authenticate a WebSocket client by "client_id:secret" token.
pub fn authenticate_client_with_id(token: &str) -> ClientAuthResult {
    if should_reject_stale_client_data() {
        warn!(
            "Rejecting client authentication because database client data is stale (> {}s)",
            CLIENT_DATA_STALE_AFTER_SECS
        );
        return ClientAuthResult::Stale;
    }

    let Some((client_id, secret)) = token.split_once(':') else {
        return ClientAuthResult::Invalid;
    };
    let Ok(clients) = CLIENTS.read() else {
        return ClientAuthResult::Invalid;
    };
    let Some(client) = clients.get(client_id) else {
        return ClientAuthResult::Invalid;
    };

    if client.secret == secret {
        ClientAuthResult::Ok(client_id.to_string(), client.guilds.clone())
    } else {
        ClientAuthResult::Invalid
    }
}

const SELECT_CLIENTS_SQL: &str = "SELECT client_id, secret, guild_id, reachable_url FROM gateway_clients ORDER BY client_id ASC, updated_at DESC NULLS LAST, created_at DESC";
const SELECT_CLIENTS_BY_IDS_SQL: &str =
    "SELECT client_id, secret, guild_id, reachable_url FROM gateway_clients WHERE client_id = ANY($1::text[]) ORDER BY client_id ASC, updated_at DESC NULLS LAST, created_at DESC";
const CREATE_NOTIFY_FUNCTION_SQL: &str = "\
CREATE OR REPLACE FUNCTION notify_gateway_clients_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    changed_client_id TEXT;
    changed_guild_id TEXT;
BEGIN
    changed_client_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.client_id
        ELSE NEW.client_id
    END;
    changed_guild_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.guild_id
        ELSE NEW.guild_id
    END;

    PERFORM pg_notify(
        'gateway_clients_changed',
        format(
            '%s\t%s\t%s',
            TG_OP,
            COALESCE(changed_client_id, ''),
            COALESCE(changed_guild_id, '')
        )
    );
    RETURN NULL;
END;
$$";
const CREATE_NOTIFY_TRIGGER_SQL: &str = "\
DROP TRIGGER IF EXISTS gateway_clients_notify_trigger ON gateway_clients;
CREATE TRIGGER gateway_clients_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON gateway_clients
FOR EACH ROW
EXECUTE FUNCTION notify_gateway_clients_change();";
const LISTEN_SQL: &str = "LISTEN gateway_clients_changed";

#[derive(Clone, Copy)]
enum GatewayClientsChangeOperation {
    Insert,
    Update,
    Delete,
    Unknown,
}

impl GatewayClientsChangeOperation {
    fn from_tg_op(raw: &str) -> Self {
        match raw {
            "INSERT" => Self::Insert,
            "UPDATE" => Self::Update,
            "DELETE" => Self::Delete,
            _ => Self::Unknown,
        }
    }
}

struct GatewayClientsChangeEvent {
    operation: GatewayClientsChangeOperation,
    client_id: String,
    guild_id: Option<String>,
}

fn parse_gateway_clients_change_payload(payload: &str) -> GatewayClientsChangeEvent {
    let parts: Vec<&str> = payload.splitn(3, '\t').collect();

    if parts.len() == 3 {
        let operation = GatewayClientsChangeOperation::from_tg_op(parts[0]);
        let client_id = parts[1].to_string();
        let guild_id = if parts[2].is_empty() {
            None
        } else {
            Some(parts[2].to_string())
        };

        return GatewayClientsChangeEvent {
            operation,
            client_id,
            guild_id,
        };
    }

    // Backward compatibility for old trigger payloads that only sent client_id.
    GatewayClientsChangeEvent {
        operation: GatewayClientsChangeOperation::Unknown,
        client_id: payload.to_string(),
        guild_id: None,
    }
}

/// Normalize a database URL for tokio-postgres compatibility.
/// tokio-postgres only supports sslmode values: disable, prefer, require.
/// PlanetScale (and others) often emit sslmode=verify-full or verify-ca,
/// which tokio-postgres rejects as "invalid connection string".
/// We downgrade those to sslmode=require so the connection succeeds.
fn normalize_database_url(url: &str) -> String {
    url.replace("sslmode=verify-full", "sslmode=require")
        .replace("sslmode=verify-ca", "sslmode=require")
}

/// Start syncing the database for client config updates.
/// Prefers LISTEN/NOTIFY with incremental updates and falls back to polling.
pub async fn start_polling(
    database_url: String,
    initial_sync_ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
) {
    let database_url = normalize_database_url(&database_url);
    let mut initial_sync_ready_tx = initial_sync_ready_tx;
    info!("Starting database config sync");

    loop {
        match run_realtime_loop(&database_url, &mut initial_sync_ready_tx).await {
            Ok(()) => break,
            Err(e) => {
                warn!("LISTEN/NOTIFY sync failed: {e}. Falling back to polling mode for now");
            }
        }

        match run_poll_loop(&database_url, &mut initial_sync_ready_tx).await {
            Ok(()) => break,
            Err(e) => {
                error!("Database polling fallback failed: {e}, retrying LISTEN/NOTIFY in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn run_realtime_loop(
    database_url: &str,
    initial_sync_ready_tx: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Query connection: regular SELECT/DDL work.
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let rustls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);

    let (query_client, query_connection) = tokio_postgres::connect(database_url, tls).await?;
    tokio::spawn(async move {
        if let Err(e) = query_connection.await {
            error!("Database query connection lost: {e}");
        }
    });

    install_database_objects(&query_client).await?;

    // Listener connection: dedicated session for LISTEN/NOTIFY.
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let rustls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let listener_tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);

    let (listener_client, mut listener_connection) =
        tokio_postgres::connect(database_url, listener_tls).await?;

    let (listener_tx, mut listener_rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<String, io::Error>>();
    tokio::spawn(async move {
        loop {
            let maybe_message = future::poll_fn(|cx| listener_connection.poll_message(cx)).await;
            match maybe_message {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(notification))) => {
                    if notification.channel() != LISTEN_CHANNEL {
                        continue;
                    }

                    if listener_tx
                        .send(Ok(notification.payload().trim().to_string()))
                        .is_err()
                    {
                        break;
                    }
                }
                Some(Ok(tokio_postgres::AsyncMessage::Notice(notice))) => {
                    warn!("Database notice while listening for client changes: {notice}");
                }
                Some(Ok(_)) => {
                    continue;
                }
                Some(Err(error)) => {
                    let send_result = listener_tx.send(Err(io::Error::other(format!(
                        "Database listener poll error: {error}",
                    ))));
                    if send_result.is_err() {
                        return;
                    }
                    return;
                }
                None => {
                    let send_result = listener_tx.send(Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "Database listener connection closed",
                    )));
                    if send_result.is_err() {
                        return;
                    }
                    return;
                }
            }
        }
    });

    // Postgres LISTEN must be committed before the initial snapshot query.
    // Otherwise an insert that lands between snapshot load and LISTEN registration
    // is missed by both sources and tenant auth stays stale until full reconcile.
    listener_client.batch_execute(LISTEN_SQL).await?;

    let initial_clients = load_clients_snapshot(&query_client).await?;
    mark_sync_success();
    if let Ok(mut clients) = CLIENTS.write() {
        *clients = initial_clients;
    }
    signal_initial_sync_ready(initial_sync_ready_tx);

    info!(
        "Database connected, using LISTEN/NOTIFY on '{LISTEN_CHANNEL}' with {}s reconcile",
        FULL_RECONCILE_INTERVAL_SECS
    );

    let mut dirty_client_ids: HashSet<String> = HashSet::new();
    let mut should_full_reconcile = false;

    let mut dirty_flush_interval =
        tokio::time::interval(Duration::from_millis(DIRTY_FLUSH_INTERVAL_MS));
    dirty_flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    dirty_flush_interval.tick().await;

    let mut full_reconcile_interval =
        tokio::time::interval(Duration::from_secs(FULL_RECONCILE_INTERVAL_SECS));
    full_reconcile_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    full_reconcile_interval.tick().await;

    let mut db_health_check_interval =
        tokio::time::interval(Duration::from_secs(DB_HEALTH_CHECK_INTERVAL_SECS));
    db_health_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    db_health_check_interval.tick().await;

    loop {
        tokio::select! {
            maybe_listener_event = listener_rx.recv() => {
                let listener_event = match maybe_listener_event {
                    Some(event) => event,
                    None => {
                        return Err(Box::new(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "Database listener channel closed",
                        )));
                    }
                };

                let payload = listener_event?;
                if payload.is_empty() {
                    should_full_reconcile = true;
                } else {
                    let change_event = parse_gateway_clients_change_payload(&payload);
                    match change_event.operation {
                        GatewayClientsChangeOperation::Insert => {
                            info!(
                                "gateway_clients INSERT received: client_id='{}', guild_id='{}'",
                                change_event.client_id,
                                change_event.guild_id.as_deref().unwrap_or(""),
                            );
                        }
                        GatewayClientsChangeOperation::Delete => {
                            info!(
                                "gateway_clients DELETE received: client_id='{}', guild_id='{}'",
                                change_event.client_id,
                                change_event.guild_id.as_deref().unwrap_or(""),
                            );
                        }
                        GatewayClientsChangeOperation::Update => {
                            info!(
                                "gateway_clients UPDATE received: client_id='{}', guild_id='{}'",
                                change_event.client_id,
                                change_event.guild_id.as_deref().unwrap_or(""),
                            );
                        }
                        GatewayClientsChangeOperation::Unknown => {
                            info!(
                                "gateway_clients change received (legacy/unknown payload): '{}'",
                                payload,
                            );
                        }
                    }

                    if change_event.client_id.is_empty() {
                        should_full_reconcile = true;
                    } else {
                        dirty_client_ids.insert(change_event.client_id);
                    }
                }
            }
            _ = dirty_flush_interval.tick() => {
                if dirty_client_ids.is_empty() {
                    continue;
                }

                refresh_clients_by_ids(&query_client, &dirty_client_ids).await?;
                mark_sync_success();
                dirty_client_ids.clear();
            }
            _ = full_reconcile_interval.tick() => {
                should_full_reconcile = true;
            }
            _ = db_health_check_interval.tick() => {
                query_client.simple_query("SELECT 1").await?;
                mark_sync_success();
            }
        }

        if !should_full_reconcile {
            continue;
        }

        let clients = load_clients_snapshot(&query_client).await?;
        mark_sync_success();

        if let Ok(mut current_clients) = CLIENTS.write() {
            *current_clients = clients;
        }

        dirty_client_ids.clear();
        should_full_reconcile = false;
    }
}

async fn run_poll_loop(
    database_url: &str,
    initial_sync_ready_tx: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), tokio_postgres::Error> {
    // PlanetScale requires TLS. Build a rustls connector with Mozilla root CAs
    // so sslmode=require (or higher) works.
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let rustls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);

    let (client, connection) = tokio_postgres::connect(database_url, tls).await?;

    // The connection object runs the actual I/O; must be spawned.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection lost: {e}");
        }
    });

    info!("Database connected, polling for client config every 1s");

    loop {
        let new_clients = load_clients_snapshot(&client).await?;
        mark_sync_success();

        if let Ok(mut clients) = CLIENTS.write() {
            *clients = new_clients;
        }
        signal_initial_sync_ready(initial_sync_ready_tx);

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn install_database_objects(
    client: &tokio_postgres::Client,
) -> Result<(), tokio_postgres::Error> {
    client.batch_execute(CREATE_NOTIFY_FUNCTION_SQL).await?;
    client.batch_execute(CREATE_NOTIFY_TRIGGER_SQL).await?;

    Ok(())
}

async fn refresh_clients_by_ids(
    client: &tokio_postgres::Client,
    dirty_client_ids: &HashSet<String>,
) -> Result<(), tokio_postgres::Error> {
    if dirty_client_ids.is_empty() {
        return Ok(());
    }

    let client_ids: Vec<String> = dirty_client_ids.iter().cloned().collect();
    let rows = client
        .query(SELECT_CLIENTS_BY_IDS_SQL, &[&client_ids])
        .await?;
    let refreshed_clients =
        group_rows_into_clients(rows.into_iter().map(snapshot_client_row_from_row).collect());

    if let Ok(mut clients) = CLIENTS.write() {
        for client_id in dirty_client_ids {
            clients.remove(client_id);
        }

        clients.extend(refreshed_clients);
    }

    Ok(())
}

/// Query all client rows and group into the HashMap<client_id, ClientConfig>.
async fn load_clients_snapshot(
    client: &tokio_postgres::Client,
) -> Result<HashMap<String, ClientConfig>, tokio_postgres::Error> {
    let rows = client.query(SELECT_CLIENTS_SQL, &[]).await?;

    Ok(group_rows_into_clients(
        rows.into_iter().map(snapshot_client_row_from_row).collect(),
    ))
}

#[derive(Clone)]
struct SnapshotClientRow {
    client_id: String,
    secret: String,
    guild_id: String,
    reachable_url: Option<String>,
}

fn snapshot_client_row_from_row(row: tokio_postgres::Row) -> SnapshotClientRow {
    SnapshotClientRow {
        client_id: row.get(0),
        secret: row.get(1),
        guild_id: row.get(2),
        reachable_url: row.get(3),
    }
}

fn group_rows_into_clients(rows: Vec<SnapshotClientRow>) -> HashMap<String, ClientConfig> {
    let mut clients: HashMap<String, ClientConfig> = HashMap::new();

    for row in rows {
        let client_id = row.client_id;
        let secret = row.secret;
        let guild_id_str = row.guild_id;
        let reachable_url = row.reachable_url;

        let guild_id: u64 = match guild_id_str.parse() {
            Ok(id) => id,
            Err(_) => {
                warn!("Invalid guild_id '{guild_id_str}' for client '{client_id}', skipping");
                continue;
            }
        };

        clients
            .entry(client_id.clone())
            .and_modify(|c| {
                if c.secret != secret {
                    warn!(
                        "Conflicting secrets for client '{client_id}', keeping newest secret and skipping stale guild row"
                    );
                    return;
                }
                c.guilds.insert(guild_id);
                // Use reachable_url from any row (should be the same across all rows for a client)
                if c.reachable_url.is_none() && reachable_url.is_some() {
                    c.reachable_url = reachable_url.clone();
                }
            })
            .or_insert_with(|| {
                let mut guilds = HashSet::new();
                guilds.insert(guild_id);
                ClientConfig {
                    secret,
                    guilds,
                    reachable_url,
                }
            });
    }

    clients
}

#[cfg(test)]
mod tests {
    use super::{group_rows_into_clients, SnapshotClientRow};

    #[test]
    fn conflicting_secrets_keep_newest_row_and_skip_stale_guilds() {
        let clients = group_rows_into_clients(vec![
            SnapshotClientRow {
                client_id: String::from("client-1"),
                secret: String::from("new-secret"),
                guild_id: String::from("111"),
                reachable_url: Some(String::from("https://client.example")),
            },
            SnapshotClientRow {
                client_id: String::from("client-1"),
                secret: String::from("old-secret"),
                guild_id: String::from("222"),
                reachable_url: Some(String::from("https://stale.example")),
            },
            SnapshotClientRow {
                client_id: String::from("client-1"),
                secret: String::from("new-secret"),
                guild_id: String::from("333"),
                reachable_url: None,
            },
        ]);

        let client = clients.get("client-1").expect("client exists");
        assert_eq!(client.secret, "new-secret");
        assert_eq!(client.guilds.len(), 2);
        assert!(client.guilds.contains(&111));
        assert!(client.guilds.contains(&333));
        assert!(!client.guilds.contains(&222));
        assert_eq!(
            client.reachable_url.as_deref(),
            Some("https://client.example")
        );
    }
}
