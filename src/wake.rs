// Wake helpers for internet-reachable kimaki clients.
// Sends POST /kimaki/wake to the client's reachable URL and waits until
// kimaki reports discord.js is connected.

use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use tracing::warn;
use twilight_http::Client as TwilightClient;
use twilight_model::id::{marker::ChannelMarker, Id};

use crate::{config::CONFIG, db_config::CLIENTS, state::State};

const TYPING_PULSE: Duration = Duration::from_secs(7);
const TYPING_MAX: Duration = Duration::from_secs(35);
const TASK_WAKE_LEAD: Duration = Duration::from_secs(30);
const TASK_WAKE_POLL: Duration = Duration::from_secs(5);

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(35))
        .build()
        .expect("wake client")
});

pub async fn wake_client(client_id: &str, reachable_url: &str, token: &str) {
    let endpoint = format!("{}/kimaki/wake", reachable_url.trim_end_matches('/'));

    let response = HTTP_CLIENT
        .post(endpoint.clone())
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await;

    match response {
        Ok(res) => {
            if !res.status().is_success() {
                warn!(
                    "Wake request failed for client '{client_id}': endpoint={endpoint}, status={}",
                    res.status()
                );
            }
        }
        Err(error) => {
            warn!(
                "Wake request error for client '{client_id}': endpoint={endpoint}, error={error}"
            );
        }
    }
}

pub fn should_show_wake_typing(event_name: &str) -> bool {
    matches!(event_name, "MESSAGE_CREATE")
}

pub async fn pulse_typing(http: &TwilightClient, channel_id: u64) {
    let channel = Id::<ChannelMarker>::new(channel_id);
    if let Err(error) = http.create_typing_trigger(channel).await {
        warn!("Wake typing pulse failed for channel {channel_id}: {error}");
    }
}

pub fn spawn_wake_typing(
    http: Arc<TwilightClient>,
    state: State,
    client_id: String,
    channel_id: u64,
) {
    tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        loop {
            if state.is_client_connected(&client_id) {
                return;
            }
            if started.elapsed() >= TYPING_MAX {
                return;
            }
            pulse_typing(http.as_ref(), channel_id).await;
            tokio::time::sleep(TYPING_PULSE).await;
        }
    });
}

pub fn spawn_scheduled_wakes(state: State) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TASK_WAKE_POLL);
        interval.tick().await;
        loop {
            interval.tick().await;
            let due: Vec<(String, String, String)> = match CLIENTS.read() {
                Ok(clients) => clients
                    .iter()
                    .filter_map(|(client_id, config)| {
                        let next_wake_at = config.next_wake_at?;
                        let now = std::time::SystemTime::now();
                        let due_at = next_wake_at
                            .checked_sub(TASK_WAKE_LEAD)
                            .unwrap_or(next_wake_at);
                        if due_at > now {
                            return None;
                        }
                        if state.is_client_connected(client_id) {
                            return None;
                        }
                        if !state.should_wake_client(client_id, Duration::from_secs(10)) {
                            return None;
                        }
                        let url = config.reachable_url.as_ref()?;
                        Some((
                            client_id.clone(),
                            url.clone(),
                            format!("{client_id}:{}", config.secret),
                        ))
                    })
                    .collect(),
                Err(error) => {
                    warn!("Skipping scheduled wake poll: CLIENTS lock poisoned: {error}");
                    continue;
                }
            };
            for (client_id, url, token) in due {
                tokio::spawn(async move {
                    wake_client(&client_id, &url, &token).await;
                });
            }
        }
    });
}

pub fn discord_http_client() -> Arc<TwilightClient> {
    Arc::new(TwilightClient::new(CONFIG.token.clone()))
}
