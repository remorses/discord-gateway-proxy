use futures_util::StreamExt;
use itoa::Buffer;
#[cfg(feature = "simd-json")]
use simd_json::prelude::ValueAsMutArray;
use tokio::{sync::broadcast, time::Instant};
use tracing::{debug, trace};
use twilight_gateway::{
    parse, Event, EventTypeFlags, Message, Shard, ShardState as ConnectionState,
};
use twilight_model::gateway::event::GatewayEvent as TwilightGatewayEvent;

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use crate::{
    config::CONFIG,
    db_config::CLIENTS,
    deserializer::{EventTypeInfo, GatewayEvent, SequenceInfo},
    model::Ready,
    state::{BufferedClientEvent, Shard as ShardState, State},
    wake, SHUTDOWN,
};

/// (payload, sequence_info, guild_id)
/// guild_id is None for events without a guild context (USER_UPDATE, DMs, etc.)
pub type BroadcastMessage = (String, Option<SequenceInfo>, Option<u64>);

const TEN_SECONDS: Duration = Duration::from_secs(10);
const WAKE_COOLDOWN: Duration = Duration::from_secs(10);

fn should_buffer_event(event_name: &str) -> bool {
    matches!(
        event_name,
        "MESSAGE_CREATE"
            | "MESSAGE_UPDATE"
            | "MESSAGE_DELETE"
            | "THREAD_CREATE"
            | "THREAD_UPDATE"
            | "THREAD_DELETE"
    )
}

fn buffer_event_for_disconnected_clients(
    state: &State,
    payload: &str,
    sequence: Option<SequenceInfo>,
    guild_id: Option<u64>,
) {
    let Some(guild_id) = guild_id else {
        return;
    };

    let wake_targets: Vec<(String, String, String)> = CLIENTS
        .read()
        .map(|clients| {
            clients
                .iter()
                .filter_map(|(client_id, config)| {
                    if !config.guilds.contains(&guild_id) {
                        return None;
                    }
                    if state.is_client_connected(client_id) {
                        return None;
                    }

                    state.push_offline_event_for_client(
                        client_id,
                        BufferedClientEvent {
                            payload: payload.to_string(),
                            sequence: sequence.clone(),
                            guild_id: Some(guild_id),
                        },
                    );

                    let should_wake = state.should_wake_client(client_id, WAKE_COOLDOWN);
                    if !should_wake {
                        return None;
                    }

                    config.reachable_url.as_ref().map(|url| {
                        (
                            client_id.clone(),
                            url.clone(),
                            format!("{client_id}:{}", config.secret),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    for (client_id, url, token) in wake_targets {
        tokio::spawn(async move {
            wake::wake_client(&client_id, &url, &token).await;
        });
    }
}

pub async fn events(
    mut shard: Shard,
    shard_state: Arc<ShardState>,
    shard_id: u32,
    broadcast_tx: broadcast::Sender<BroadcastMessage>,
    state: State,
) {
    // This method only wants to relay events while the shard is in a READY state
    // Therefore, we only put events in the queue while we are connected and READY
    let mut is_ready = false;

    let mut buffer = Buffer::new();
    let shard_id_str = buffer.format(shard_id).to_owned();

    let mut last_metrics_update = Instant::now();

    let event_type_flags: EventTypeFlags = CONFIG.cache.clone().into();

    loop {
        // Update metrics if the last update was more than 10s ago
        let now = Instant::now();

        if now.duration_since(last_metrics_update) > TEN_SECONDS {
            let latencies = shard.latency().recent();
            let info = shard.state();
            update_shard_statistics(&shard_id_str, &shard_state, info, latencies);
            last_metrics_update = now;
        }

        let payload = match shard.next().await {
            Some(Ok(Message::Text(payload))) => payload,
            Some(Ok(Message::Close(_))) if SHUTDOWN.load(Ordering::Relaxed) => return,
            Some(Ok(Message::Close(_))) => {
                tracing::info!("Shard {shard_id} got a close message");

                continue;
            }
            Some(Err(e)) => {
                tracing::error!("Error receiving message: {e}");
                continue;
            }
            None => {
                tracing::warn!("Shard {shard_id} stream closed");
                return;
            }
        };

        // NOTE: payload cannot be modified because we have to do optional event parsing
        // later. Don't use simd_json::from_str on it because that will make the data useless.
        // Instead, clone it before mutating.
        let Some(event) = GatewayEvent::from_json(&payload) else {
            tracing::error!("Failed to deserialize gateway event");
            continue;
        };

        let (op, sequence, event_type, guild_id, channel_id) = event.into_parts();

        if let Some(EventTypeInfo(event_name, _)) = event_type {
            metrics::counter!("gateway_shard_events", "shard" => shard_id_str.clone(), "event_type" => event_name.to_owned()).increment(1);

            if event_name == "READY" {
                // Use the raw JSON from READY to create a new blank READY

                #[cfg(feature = "simd-json")]
                let mut ready: Ready =
                    unsafe { simd_json::from_str(&mut payload.clone()).unwrap() };
                #[cfg(not(feature = "simd-json"))]
                let mut ready: Ready = serde_json::from_str(&payload).unwrap();

                // Clear the guilds
                if let Some(guilds) = ready.d.get_mut("guilds") {
                    if let Some(arr) = guilds.as_array_mut() {
                        arr.clear();
                    }
                }

                // Override resume_gateway_url with the external URI of the proxy
                ready.d.insert(
                    String::from("resume_gateway_url"),
                    CONFIG.externally_accessible_url.clone().into(),
                );

                // We don't care if it was already set
                // since this data is timeless
                shard_state.ready.set_ready(ready.d);
                is_ready = true;
            } else if event_name == "RESUMED" {
                is_ready = true;
            } else if op.0 == 0 && is_ready {
                // We only want to relay dispatchable events, not RESUMEs and not READY
                // because we fake a READY event
                let payload_copy = payload.clone();
                trace!("[Shard {shard_id}] Sending payload to clients: {payload_copy:?}",);

                let _res = broadcast_tx.send((payload_copy.clone(), sequence.clone(), guild_id));

                if should_buffer_event(event_name) {
                    buffer_event_for_disconnected_clients(
                        &state,
                        &payload_copy,
                        sequence,
                        guild_id,
                    );
                }
                if wake::should_show_wake_typing(event_name) {
                    if let (Some(channel_id), Some(guild_id)) = (channel_id, guild_id) {
                        start_wake_typing_for_disconnected_clients(&state, guild_id, channel_id);
                    }
                }
            }
        }

        match parse(payload, event_type_flags) {
            Ok(Some(event)) => match event {
                TwilightGatewayEvent::Dispatch(_, event) => {
                    shard_state.guilds.update(Event::from(event));
                }
                TwilightGatewayEvent::InvalidateSession(can_resume) => {
                    debug!("[Shard {shard_id}] Session invalidated, resumable: {can_resume}");
                    if !can_resume {
                        shard_state.ready.set_not_ready();
                    }
                    is_ready = false;
                }
                _ => {}
            },
            Ok(None) => {
                // Event type not in event_type_flags, skipped
            }
            Err(e) => {
                tracing::warn!("[Shard {shard_id}] Failed to parse gateway event: {e:?}");
            }
        }
    }
}

fn start_wake_typing_for_disconnected_clients(state: &State, guild_id: u64, channel_id: u64) {
    let client_ids: Vec<String> = CLIENTS
        .read()
        .map(|clients| {
            clients
                .iter()
                .filter_map(|(client_id, config)| {
                    if !config.guilds.contains(&guild_id) {
                        return None;
                    }
                    if state.is_client_connected(client_id) {
                        return None;
                    }
                    if config.reachable_url.is_none() {
                        return None;
                    }
                    Some(client_id.clone())
                })
                .collect()
        })
        .unwrap_or_default();

    if client_ids.is_empty() {
        return;
    }

    let http = wake::discord_http_client();
    for client_id in client_ids {
        wake::spawn_wake_typing(http.clone(), state.clone(), client_id, channel_id);
    }
}

pub fn update_shard_statistics(
    shard_id: &str,
    shard_state: &Arc<ShardState>,
    connection_status: ConnectionState,
    latencies: &[Duration],
) {
    // There is no way around this, sadly
    let connection_status = match connection_status {
        ConnectionState::Active => 4.0,
        ConnectionState::Disconnected { .. } => 1.0,
        ConnectionState::Identifying => 2.0,
        ConnectionState::Resuming => 3.0,
        ConnectionState::FatallyClosed { .. } => 0.0,
    };

    let latency = latencies.first().map_or(f64::NAN, Duration::as_secs_f64);

    metrics::histogram!("gateway_shard_latency_histogram", "shard" => shard_id.to_string())
        .record(latency);
    metrics::gauge!(
        "gateway_shard_latency",
        "shard" => shard_id.to_string()
    )
    .set(latency);
    metrics::histogram!("gateway_shard_status", "shard" => shard_id.to_string())
        .record(connection_status);

    let stats = shard_state.guilds.stats();

    metrics::gauge!("gateway_cache_emojis", "shard" => shard_id.to_string())
        .set(stats.emojis() as f64);
    metrics::gauge!("gateway_cache_guilds", "shard" => shard_id.to_string())
        .set(stats.guilds() as f64);
    metrics::gauge!("gateway_cache_members", "shard" => shard_id.to_string())
        .set(stats.members() as f64);
    metrics::gauge!("gateway_cache_presences", "shard" => shard_id.to_string())
        .set(stats.presences() as f64);
    metrics::gauge!("gateway_cache_channels", "shard" => shard_id.to_string())
        .set(stats.channels() as f64);
    metrics::gauge!("gateway_cache_roles", "shard" => shard_id.to_string())
        .set(stats.roles() as f64);
    metrics::gauge!("gateway_cache_unavailable_guilds", "shard" => shard_id.to_string())
        .set(stats.unavailable_guilds() as f64);
    metrics::gauge!("gateway_cache_users", "shard" => shard_id.to_string())
        .set(stats.users() as f64);
    metrics::gauge!("gateway_cache_voice_states", "shard" => shard_id.to_string())
        .set(stats.voice_states() as f64);
}
