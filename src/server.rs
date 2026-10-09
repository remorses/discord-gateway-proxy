use bytes::Bytes;
use flate2::{Compress, Compression, FlushCompress, Status};
use futures_util::{Sink, SinkExt, StreamExt};
use http_body_util::Full;
use hyper::{body::Incoming, service::service_fn, Method, Request, Response, StatusCode};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use itoa::Buffer;
use metrics_exporter_prometheus::PrometheusHandle;
#[cfg(not(feature = "simd-json"))]
use serde_json::{to_string, Value as OwnedValue};
#[cfg(feature = "simd-json")]
use simd_json::{to_string, OwnedValue};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::{
        broadcast::error::RecvError,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use tokio_websockets::{CloseCode, Error, Limits, Message, ServerBuilder};
use tracing::{debug, error, info, trace, warn};

use std::{
    collections::HashSet,
    convert::Infallible,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    auth, db_config,
    deserializer::{GatewayEvent, SequenceInfo},
    dispatch::{Audience, BroadcastMessage},
    model::{parse_json, Identify, OutgoingCommand, RequestGuildMembers, Resume, VoiceStateUpdate},
    rest_proxy,
    state::{Session, SessionPrincipal, Shard, State, VoiceDecision},
    upgrade,
};

const HELLO: &str = r#"{"t":null,"s":null,"op":10,"d":{"heartbeat_interval":41250}}"#;
const HEARTBEAT_ACK: &str = r#"{"t":null,"s":null,"op":11,"d":null}"#;
const INVALID_SESSION: &str = r#"{"t":null,"s":null,"op":9,"d":false}"#;
const RESUMED: &str = r#"{"t":"RESUMED","s":null,"op":0,"d":{}}"#;

const TRAILER: [u8; 4] = [0x00, 0x00, 0xff, 0xff];

const CLIENT_COMMAND_LIMIT: u32 = 20;
const CLIENT_COMMAND_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Eq)]
enum ClientCommand {
    /// Send upstream on the shard of `guild_id`.
    Forward {
        guild_id: u64,
        command: String,
    },
    /// Voice state update: also needs the channel check and the voice owner lock.
    Voice {
        guild_id: u64,
        channel_id: Option<u64>,
        command: String,
    },
    Drop(&'static str),
}

/// Decide what a multi-tenant client may send on the shared bot session.
/// Only guild member requests (op 8) and voice state updates (op 4) for an
/// authorized guild go upstream, rebuilt from parsed fields. Presence (op 3)
/// would change the bot for every tenant.
fn filter_client_command(op: u8, payload: &str, authorized_guilds: &HashSet<u64>) -> ClientCommand {
    let (guild_id, channel_id, serialized) = match op {
        8 => {
            let Some(request) = parse_json::<RequestGuildMembers>(payload) else {
                return ClientCommand::Drop("invalid request guild members payload");
            };
            let serialized = to_string(&OutgoingCommand { op, d: &request.d });
            (request.d.guild_id, None, serialized)
        }
        4 => {
            let Some(update) = parse_json::<VoiceStateUpdate>(payload) else {
                return ClientCommand::Drop("invalid voice state update payload");
            };
            let serialized = to_string(&OutgoingCommand { op, d: &update.d });
            (update.d.guild_id, Some(update.d.channel_id), serialized)
        }
        _ => return ClientCommand::Drop("opcode not allowed for multi-tenant clients"),
    };

    let Ok(guild_id) = guild_id.parse::<u64>() else {
        return ClientCommand::Drop("invalid guild_id");
    };
    if !authorized_guilds.contains(&guild_id) {
        return ClientCommand::Drop("guild not authorized");
    }
    let command = match serialized {
        // Discord closes the shared shard with 4002 for payloads over 4096 bytes.
        Ok(command) if command.len() <= 4096 => command,
        Ok(_) => return ClientCommand::Drop("command exceeds gateway payload limit"),
        Err(_) => return ClientCommand::Drop("failed to serialize command"),
    };
    match channel_id {
        None => ClientCommand::Forward { guild_id, command },
        Some(None) => ClientCommand::Voice {
            guild_id,
            channel_id: None,
            command,
        },
        Some(Some(channel_id)) => match channel_id.parse::<u64>() {
            Ok(channel_id) => ClientCommand::Voice {
                guild_id,
                channel_id: Some(channel_id),
                command,
            },
            Err(_) => ClientCommand::Drop("invalid channel_id"),
        },
    }
}

fn compress_full(compressor: &mut Compress, output: &mut Vec<u8>, input: &[u8]) {
    let before_in = compressor.total_in() as usize;
    while (compressor.total_in() as usize) - before_in < input.len() {
        let offset = (compressor.total_in() as usize) - before_in;
        match compressor
            .compress_vec(&input[offset..], output, FlushCompress::None)
            .unwrap()
        {
            Status::Ok => {}
            Status::BufError => output.reserve(4096),
            Status::StreamEnd => break,
        }
    }

    while !output.ends_with(&TRAILER) {
        output.reserve(5);
        if compressor
            .compress_vec(&[], output, FlushCompress::Sync)
            .unwrap()
            == Status::StreamEnd
        {
            break;
        }
    }
}

/// Lazily initialized zlib compression state.
/// Avoids allocating ~200KB of zlib internal state + 32KB buffer per connection
/// when compression is not used (the common case for non-internet clients).
struct ZlibState {
    compress: Compress,
    buffer: Vec<u8>,
}

impl ZlibState {
    fn new() -> Self {
        Self {
            compress: Compress::new(Compression::fast(), true),
            buffer: Vec::with_capacity(32 * 1024),
        }
    }

    fn compress_and_send(&mut self, input: &[u8]) -> Bytes {
        self.buffer.clear();
        compress_full(&mut self.compress, &mut self.buffer, input);
        Bytes::from(self.buffer.clone())
    }
}

async fn sink_from_queue<S>(
    addr: SocketAddr,
    mut use_zlib: bool,
    compress_rx: oneshot::Receiver<Option<bool>>,
    mut message_stream: UnboundedReceiver<Message>,
    mut sink: S,
) -> Result<(), Error>
where
    S: Sink<Message, Error = Error> + Unpin + Send,
{
    // Zlib state is lazily initialized only when compression is actually needed.
    // This saves ~230KB per connection (flate2 Compress internal state + 32KB buffer)
    // for clients that don't request zlib-stream transport encoding.
    let mut zlib: Option<ZlibState> = None;

    // At first, we will have to send a HELLO
    if use_zlib {
        let state = zlib.get_or_insert_with(ZlibState::new);
        let compressed = state.compress_and_send(HELLO.as_bytes());

        sink.send(Message::binary(compressed)).await?;
    } else {
        sink.send(Message::text(HELLO.to_string())).await?;
    }

    // Process messages while waiting for the compression decision from
    // IDENTIFY/RESUME. Before this fix, we blocked on compress_rx here, which
    // prevented INVALID_SESSION and heartbeat ACKs from being delivered after a
    // failed RESUME (the compress oneshot is only resolved on successful
    // IDENTIFY/RESUME). This caused discord.js clients to enter an infinite
    // zombie reconnect loop after proxy restart because:
    //   1. HELLO was sent (before the block)
    //   2. Client sent RESUME → proxy queued INVALID_SESSION + heartbeat ACKs
    //   3. But they sat in the message_stream buffer, never reaching the client
    //   4. Client detected zombie (no heartbeat ACK) → reconnect → same loop
    let mut compress_rx = compress_rx;
    let mut compress_pending = true;

    while compress_pending {
        tokio::select! {
            result = &mut compress_rx => {
                if result == Ok(Some(true)) {
                    use_zlib = true;
                }
                compress_pending = false;
            }
            msg = message_stream.recv() => {
                match msg {
                    Some(msg) => {
                        trace!("[{addr}] Sending {msg:?}");
                        if use_zlib {
                            let state = zlib.get_or_insert_with(ZlibState::new);
                            let compressed = state.compress_and_send(&msg.into_payload());
                            sink.send(Message::binary(compressed)).await?;
                        } else {
                            sink.send(msg).await?;
                        }
                    }
                    None => return Ok(()),
                }
            }
        }
    }

    while let Some(msg) = message_stream.recv().await {
        trace!("[{addr}] Sending {msg:?}");

        if use_zlib {
            let state = zlib.get_or_insert_with(ZlibState::new);
            let compressed = state.compress_and_send(&msg.into_payload());

            sink.send(Message::binary(compressed)).await?;
        } else {
            sink.send(msg).await?;
        }
    }

    Ok(())
}

/// Forward events from a shard to a connected client.
///
/// If `authorized_guilds` is Some, only events for those guilds are forwarded.
/// If `authorized_guilds` is None, all events are forwarded (original behavior).
async fn forward_shard(
    session_id: String,
    shard_status: Arc<Shard>,
    stream_writer: UnboundedSender<Message>,
    send_guilds: bool,
    mut seq: usize,
    authorized_guilds: Option<Arc<HashSet<u64>>>,
    state: State,
    client_id: Option<String>,
) {
    let shard_id = shard_status.id;

    debug!("[Shard {shard_id}] Starting to send events to client",);

    // Wait until we have a valid READY payload for this shard
    let ready_payload = match shard_status.ready.wait_until_ready().await {
        Ok(payload) => payload,
        Err(_) => {
            error!("[Shard {shard_id}] Ready sender dropped; closing client connection");
            return;
        }
    };

    if send_guilds {
        // Get a fake ready payload to send to the client
        let mut ready_payload = shard_status.guilds.get_ready_payload(
            ready_payload,
            &mut seq,
            authorized_guilds.as_deref(),
        );

        // Overwrite the session ID in the READY
        ready_payload
            .d
            .insert(String::from("session_id"), OwnedValue::String(session_id));

        if let Ok(serialized) = to_string(&ready_payload) {
            debug!("[Shard {shard_id}] Sending newly created READY");
            let _res = stream_writer.send(Message::text(serialized));
        };

        // Send GUILD_CREATE/GUILD_DELETEs based on guild availability
        // Filter to only authorized guilds if specified
        for payload in shard_status
            .guilds
            .get_guild_payloads(&mut seq, authorized_guilds.as_deref())
        {
            trace!("[Shard {shard_id}] Sending newly created GUILD_CREATE/GUILD_DELETE payload");
            let _res = stream_writer.send(Message::text(payload));
        }
    } else {
        let _res = stream_writer.send(Message::text(RESUMED.to_string()));
    }

    // For formatting the sequence number as a string, reuse a buffer.
    let mut buffer = Buffer::new();

    let own_client_id = client_id.clone();
    // Replay buffered offline events before subscribing to live stream.
    if let Some(client_id) = client_id {
        let buffered = state.drain_offline_events_for_client(&client_id);
        for mut event in buffered {
            if let Some(ref guilds) = authorized_guilds {
                match event.guild_id {
                    Some(gid) => {
                        if !guilds.contains(&gid) {
                            continue;
                        }
                    }
                    None => {
                        continue;
                    }
                }
            }

            if let Some(SequenceInfo(_, sequence_range)) = event.sequence {
                seq += 1;
                event
                    .payload
                    .replace_range(sequence_range, buffer.format(seq));
            }

            let _res = stream_writer.send(Message::text(event.payload));
        }
    }

    // Subscribe to events for this shard
    let mut event_receiver = shard_status.events.subscribe();

    loop {
        let res = event_receiver.recv().await;

        if let Ok(BroadcastMessage {
            mut payload,
            sequence,
            guild_id,
            audience,
        }) = res
        {
            // Filter by authorized guilds if specified
            if let Some(ref guilds) = authorized_guilds {
                let allowed = match &audience {
                    Audience::Guild => true,
                    Audience::BotOnly => false,
                    Audience::Client(owner) => own_client_id.as_deref() == Some(&**owner),
                };
                if !allowed {
                    continue;
                }
                match guild_id {
                    Some(gid) => {
                        if !guilds.contains(&gid) {
                            // Event is for a guild this client isn't authorized for
                            continue;
                        }
                    }
                    None => {
                        // Event has no guild_id (USER_UPDATE, DMs, etc.)
                        // Skip these events for multi-tenant clients
                        continue;
                    }
                }
            }

            // Overwrite the sequence number
            if let Some(SequenceInfo(_, sequence_range)) = sequence {
                seq += 1;
                payload.replace_range(sequence_range, buffer.format(seq));
            }

            let _res = stream_writer.send(Message::text(payload));
        } else if let Err(RecvError::Lagged(amt)) = res {
            warn!("[Shard {shard_id}] Client is {amt} events behind!");
        }
    }
}

#[allow(clippy::too_many_lines)]
pub async fn handle_client<S: 'static + AsyncRead + AsyncWrite + Unpin + Send>(
    addr: SocketAddr,
    stream: S,
    state: State,
    use_zlib: bool,
) -> Result<(), Error> {
    // We use a oneshot channel to tell the forwarding task whether the IDENTIFY
    // contained a compression request
    let (compress_tx, compress_rx) = oneshot::channel();
    let mut compress_tx = Some(compress_tx);

    // We need to know which shard this client is connected to in order to send messages to it
    let mut shard_sender = None;

    let ws_conn = ServerBuilder::new()
        .limits(Limits::unlimited())
        .serve(stream);

    let (sink, mut stream) = ws_conn.split();

    // Write all messages from a queue to the sink
    let (stream_writer, stream_receiver) = unbounded_channel::<Message>();

    let mut sink_task = tokio::spawn(sink_from_queue(
        addr,
        use_zlib,
        compress_rx,
        stream_receiver,
        sink,
    ));

    let mut shard_forward_task = None;
    let mut active_client_id: Option<String> = None;
    // Set for multi-tenant clients: their commands are filtered before going upstream.
    let mut command_scope: Option<String> = None;
    // When true, teardown awaits sink_task flush instead of aborting immediately.
    // Set when we send INVALID_SESSION + close frame and need the client to
    // receive them before the connection drops.
    let mut graceful_close = false;

    while let Some(Ok(msg)) = stream.next().await {
        if !msg.is_text() && !msg.is_binary() {
            continue;
        }

        #[cfg(feature = "simd-json")]
        let mut payload = unsafe { msg.as_text().unwrap_unchecked().to_owned() };
        #[cfg(not(feature = "simd-json"))]
        let payload = unsafe { msg.as_text().unwrap_unchecked() };

        let Some(deserializer) = GatewayEvent::from_json(&payload) else {
            continue;
        };

        match deserializer.op() {
            1 => {
                trace!("[{addr}] Sending heartbeat ACK");
                let _res = stream_writer.send(Message::text(HEARTBEAT_ACK.to_string()));
            }
            2 => {
                debug!("[{addr}] Client is identifying");

                #[cfg(feature = "simd-json")]
                let maybe_identify = unsafe { simd_json::from_str(&mut payload) };
                #[cfg(not(feature = "simd-json"))]
                let maybe_identify = serde_json::from_str(&payload);

                let identify: Identify = match maybe_identify {
                    Ok(identify) => identify,
                    Err(e) => {
                        warn!("[{addr}] Invalid identify payload: {e:?}");
                        continue;
                    }
                };

                let (shard_id, shard_count) = (identify.d.shard[0], identify.d.shard[1]);

                if shard_count != state.shard_count {
                    warn!("[{addr}] Shard count from client identify mismatched, disconnecting",);
                    break;
                }

                if shard_id >= shard_count {
                    warn!("[{addr}] Shard ID from client is out of range, disconnecting",);
                    break;
                }

                let client_token = auth::normalize_gateway_token(&identify.d.token);
                let auth = match auth::authenticate_gateway_token(client_token) {
                    auth::GatewayAuthResult::Ok(ctx) => ctx,
                    auth::GatewayAuthResult::Stale => {
                        warn!("[{addr}] Auth backend stale, disconnecting");
                        break;
                    }
                    auth::GatewayAuthResult::Invalid => {
                        warn!("[{addr}] Token from client mismatched and not a valid client, disconnecting");
                        break;
                    }
                };

                if matches!(auth.principal, SessionPrincipal::Client(_)) {
                    debug!("[{addr}] Client authenticated as multi-tenant client");
                }

                let identified_client_id = match &auth.principal {
                    SessionPrincipal::Client(client_id) => Some(client_id.clone()),
                    _ => None,
                };
                if let Some(client_id) = &identified_client_id {
                    if active_client_id.is_none() {
                        state.mark_client_connected(client_id);
                        active_client_id = Some(client_id.clone());
                    }
                    // A fresh IDENTIFY means the client process has no voice
                    // connection, but the shared bot is still in its calls.
                    // Leave them, so its next join starts a new call. A join
                    // to the channel the bot is already in may get no events.
                    for guild_id in state.leave_voice_for_client(client_id, shard_id) {
                        info!("[{addr}] Client {client_id} identified again, leaving voice in guild {guild_id}");
                        state.send_voice_leave(guild_id);
                    }
                }

                command_scope.clone_from(&identified_client_id);

                trace!("[{addr}] Shard ID is {shard_id}");

                // Create a new session for this client
                let session = Session {
                    shard_id,
                    compress: identify.d.compress,
                    principal: auth.principal,
                    authorized_guilds: auth.authorized_guilds.clone(),
                    last_accessed: Instant::now(),
                };
                let session_id = state.create_session(session);

                // The client is connected to this shard, so prepare for sending commands to it
                let shard = state.shards[shard_id as usize].clone();
                shard_sender = Some(shard.sender.clone());

                if let Some(sender) = compress_tx.take() {
                    shard_forward_task = Some(tokio::spawn(forward_shard(
                        session_id,
                        shard,
                        stream_writer.clone(),
                        true,
                        0,
                        auth.authorized_guilds,
                        state.clone(),
                        identified_client_id,
                    )));

                    let _res = sender.send(identify.d.compress);
                }
            }
            6 => {
                debug!("[{addr}] Client is resuming");

                #[cfg(feature = "simd-json")]
                let maybe_resume = unsafe { simd_json::from_str(&mut payload) };
                #[cfg(not(feature = "simd-json"))]
                let maybe_resume = serde_json::from_str(&payload);

                let resume: Resume = match maybe_resume {
                    Ok(resume) => resume,
                    Err(e) => {
                        warn!("[{addr}] Invalid resume payload: {e:?}");
                        continue;
                    }
                };

                let client_token = auth::normalize_gateway_token(&resume.d.token);
                let resume_auth = match auth::authenticate_gateway_token(client_token) {
                    auth::GatewayAuthResult::Ok(ctx) => ctx,
                    auth::GatewayAuthResult::Stale => {
                        warn!("[{addr}] Auth backend stale during RESUME, disconnecting");
                        break;
                    }
                    auth::GatewayAuthResult::Invalid => {
                        warn!("[{addr}] Token from client mismatched, disconnecting");
                        break;
                    }
                };

                // Find the shard that has the matching session ID
                if let Some(session) = state.get_session(&resume.d.session_id) {
                    if session.principal != resume_auth.principal {
                        warn!(
                            "[{addr}] RESUME principal mismatch for session {}, rejecting",
                            resume.d.session_id
                        );
                        let _res = stream_writer.send(Message::text(INVALID_SESSION.to_string()));
                        // Close with 4007 (InvalidSeq) so discord.js does a fresh IDENTIFY
                        let _res = stream_writer.send(Message::close(
                            Some(CloseCode::try_from(4007u16).unwrap()),
                            "session rejected",
                        ));
                        graceful_close = true;
                        break;
                    }

                    let session_id = resume.d.session_id;
                    debug!("[{addr}] Successfully resuming session {session_id}",);

                    let shard = state.shards[session.shard_id as usize].clone();
                    shard_sender = Some(shard.sender.clone());
                    let authorized_guilds =
                        if matches!(session.principal, SessionPrincipal::Client(_)) {
                            resume_auth.authorized_guilds
                        } else {
                            session.authorized_guilds.clone()
                        };
                    let resumed_client_id = match &session.principal {
                        SessionPrincipal::Client(client_id) => Some(client_id.clone()),
                        _ => None,
                    };
                    if let Some(client_id) = &resumed_client_id {
                        if active_client_id.is_none() {
                            state.mark_client_connected(client_id);
                            active_client_id = Some(client_id.clone());
                        }
                    }

                    command_scope.clone_from(&resumed_client_id);

                    if let Some(sender) = compress_tx.take() {
                        shard_forward_task = Some(tokio::spawn(forward_shard(
                            session_id,
                            shard.clone(),
                            stream_writer.clone(),
                            false,
                            resume.d.seq,
                            authorized_guilds,
                            state.clone(),
                            resumed_client_id,
                        )));

                        let _res = sender.send(session.compress);
                    } else {
                        let _res = stream_writer.send(Message::text(INVALID_SESSION.to_string()));
                        let _res = stream_writer.send(Message::close(
                            Some(CloseCode::try_from(4007u16).unwrap()),
                            "session rejected",
                        ));
                        graceful_close = true;
                        break;
                    }
                } else {
                    let _res = stream_writer.send(Message::text(INVALID_SESSION.to_string()));
                    // Close with 4007 (InvalidSeq) so discord.js does a fresh IDENTIFY.
                    // This is the hot path after a proxy restart: all in-memory sessions
                    // are lost, so every RESUME attempt hits this branch.
                    let _res = stream_writer.send(Message::close(
                        Some(CloseCode::try_from(4007u16).unwrap()),
                        "session not found",
                    ));
                    graceful_close = true;
                    break;
                }
            }
            op => {
                let Some(sender) = &shard_sender else {
                    warn!("[{addr}] Client attempted to send payload before IDENTIFY",);
                    continue;
                };
                let Some(client_id) = &command_scope else {
                    trace!("[{addr}] Sending {payload:?} to Discord directly");
                    let _res = sender.send(payload.to_string());
                    continue;
                };
                let guilds = db_config::client_guilds(client_id).unwrap_or_default();
                let (guild_id, voice_channel, command) =
                    match filter_client_command(op, &payload, &guilds) {
                        ClientCommand::Forward { guild_id, command } => (guild_id, None, command),
                        ClientCommand::Voice {
                            guild_id,
                            channel_id,
                            command,
                        } => (guild_id, Some(channel_id), command),
                        ClientCommand::Drop(reason) => {
                            warn!("[{addr}] Dropped op {op} from client {client_id}: {reason}");
                            continue;
                        }
                    };
                if let Some(Some(channel_id)) = voice_channel {
                    if state.resolve_guild_id_for_channel(channel_id) != Some(guild_id) {
                        warn!("[{addr}] Dropped op 4 from client {client_id}: channel {channel_id} not in guild {guild_id}");
                        continue;
                    }
                }
                if !state.allow_client_command(
                    client_id,
                    CLIENT_COMMAND_LIMIT,
                    CLIENT_COMMAND_WINDOW,
                ) {
                    warn!(
                        "[{addr}] Client {client_id} hit the command rate limit, dropping op {op}"
                    );
                    continue;
                }
                if let Some(channel_id) = voice_channel {
                    if state.claim_voice(guild_id, client_id, channel_id.is_some())
                        == VoiceDecision::Deny
                    {
                        warn!("[{addr}] Dropped op 4 from client {client_id}: guild {guild_id} voice is owned by another client");
                        continue;
                    }
                }
                let Some(shard) = state.shard_for_guild(guild_id) else {
                    warn!(
                        "[{addr}] Dropped op {op}: shard of guild {guild_id} is not on this proxy"
                    );
                    continue;
                };
                trace!("[{addr}] Sending filtered op {op} to Discord");
                let _res = shard.sender.send(command);
            }
        }
    }

    debug!("[{addr}] Client disconnected");

    if graceful_close {
        // Stop the event producer first so it doesn't keep enqueuing payloads
        // through its stream_writer clone, which would delay the sink drain
        // or push the close frame further back in the queue.
        if let Some(task) = shard_forward_task.take() {
            task.abort();
        }
        // Drop the writer so the sink task sees the channel close after
        // draining queued messages (INVALID_SESSION + close frame).
        drop(stream_writer);
        // Give the sink task up to 2s to flush before force-aborting.
        if tokio::time::timeout(std::time::Duration::from_secs(2), &mut sink_task)
            .await
            .is_err()
        {
            warn!("[{addr}] Sink task did not flush within 2s, aborting");
            sink_task.abort();
        }
    } else {
        sink_task.abort();
    }

    if let Some(shard_forward_task) = shard_forward_task {
        shard_forward_task.abort();
    }

    if let Some(client_id) = active_client_id {
        state.mark_client_disconnected(&client_id);
    }

    Ok(())
}

const LANDING_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kimaki Gateway</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #0a0a0a;
    color: #e5e5e5;
  }
  .container {
    text-align: center;
    padding: 2rem;
  }
  h1 {
    font-size: 1.5rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
  }
  p {
    font-size: 0.95rem;
    color: #888;
  }
  a {
    color: #e5e5e5;
    text-decoration: underline;
    text-underline-offset: 3px;
  }
  a:hover { color: #fff; }
</style>
</head>
<body>
<div class="container">
  <h1>Discord Gateway Proxy</h1>
  <p><a href="https://kimaki.dev">kimaki.dev</a></p>
</div>
</body>
</html>"#;

async fn handler(
    addr: SocketAddr,
    request: Request<Incoming>,
    state: State,
    metrics: &PrometheusHandle,
) -> Response<Full<Bytes>> {
    if request.uri().path().starts_with("/api/v10/") || request.uri().path().starts_with("/v10/") {
        return rest_proxy::handle_rest_request(request, state).await;
    }

    match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => Response::builder()
            .status(StatusCode::OK)
            .body(Full::from(metrics.render()))
            .unwrap(),
        (&Method::GET, "/shard-count") => {
            let mut buffer = itoa::Buffer::new();
            let shard_count_str = buffer.format(state.shard_count);

            Response::builder()
                .status(StatusCode::OK)
                .body(Full::from(shard_count_str.to_string()))
                .unwrap()
        }
        (&Method::GET, "/") => {
            // Landing page — only served for plain HTTP requests (no websocket upgrade header).
            // Websocket clients hitting / still get the upgrade via the fallback arm.
            if request.headers().contains_key("upgrade") {
                upgrade::server(addr, request, state)
            } else {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/html; charset=utf-8")
                    .body(Full::from(LANDING_HTML))
                    .unwrap()
            }
        }
        // Provide websocket upgrade for any other path (backwards compatibility).
        _ => upgrade::server(addr, request, state),
    }
}

pub async fn run(port: u16, state: State, metrics_handle: PrometheusHandle) -> Result<(), Error> {
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to bind TCP listener: {e}");
            return Ok(());
        }
    };

    info!("Listening on {addr}");

    loop {
        let (conn, addr) = match listener.accept().await {
            Ok((stream, addr)) => (stream, addr),
            Err(e) => {
                error!("Failed to accept connection: {e}");
                return Ok(());
            }
        };

        trace!("[{addr:?}] New connection");

        let state = state.clone();
        let metrics_handle = metrics_handle.clone();

        tokio::spawn(async move {
            if let Err(e) = auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(
                    TokioIo::new(conn),
                    service_fn(move |incoming: Request<Incoming>| {
                        let state = state.clone();
                        let metrics_handle = metrics_handle.clone();
                        async move {
                            Ok::<_, Infallible>(
                                handler(addr, incoming, state, &metrics_handle).await,
                            )
                        }
                    }),
                )
                .await
            {
                error!("Error handling connection: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{filter_client_command, ClientCommand};
    use std::collections::HashSet;

    const GUILD: u64 = 1_422_625_037_164_351_591;

    fn guilds() -> HashSet<u64> {
        HashSet::from([GUILD])
    }

    #[test]
    fn forwards_rebuilt_commands_for_authorized_guild() {
        let members = r#"{"op":8,"d":{"guild_id":"1422625037164351591","user_ids":["1","2"],"limit":0,"nonce":"abc","extra":"dropped"}}"#;
        assert_eq!(
            filter_client_command(8, members, &guilds()),
            ClientCommand::Forward {
                guild_id: GUILD,
                command: r#"{"op":8,"d":{"guild_id":"1422625037164351591","limit":0,"user_ids":["1","2"],"nonce":"abc"}}"#
                    .to_string(),
            }
        );
        let join = r#"{"op":4,"d":{"guild_id":"1422625037164351591","channel_id":"7","self_mute":false,"self_deaf":true,"self_video":true}}"#;
        assert_eq!(
            filter_client_command(4, join, &guilds()),
            ClientCommand::Voice {
                guild_id: GUILD,
                channel_id: Some(7),
                command: r#"{"op":4,"d":{"guild_id":"1422625037164351591","channel_id":"7","self_mute":false,"self_deaf":true}}"#
                    .to_string(),
            }
        );
        let leave = r#"{"op":4,"d":{"guild_id":"1422625037164351591","channel_id":null,"self_mute":false,"self_deaf":false}}"#;
        assert_eq!(
            filter_client_command(4, leave, &guilds()),
            ClientCommand::Voice {
                guild_id: GUILD,
                channel_id: None,
                command: leave.to_string(),
            }
        );
    }

    #[test]
    fn drops_commands_outside_scope() {
        let oversized = format!(
            r#"{{"op":8,"d":{{"guild_id":"1422625037164351591","query":"{}","limit":1}}}}"#,
            "a".repeat(5000)
        );
        let cases = [
            (8, r#"{"op":8,"d":{"guild_id":"999","query":"","limit":0}}"#),
            (
                8,
                r#"{"op":8,"d":{"guild_id":["1422625037164351591"],"query":"","limit":0}}"#,
            ),
            (
                8,
                r#"{"op":8,"d":{"guild_id":"1422625037164351591","guild_id":"999","query":""}}"#,
            ),
            (8, r#"{"op":8,"d":{"guild_id":"not-a-snowflake"}}"#),
            (8, oversized.as_str()),
            (
                3,
                r#"{"op":3,"d":{"since":null,"activities":[],"status":"invisible","afk":false}}"#,
            ),
            (
                4,
                r#"{"op":4,"d":{"guild_id":"999","channel_id":"1","self_mute":false,"self_deaf":false}}"#,
            ),
            (
                4,
                r#"{"op":4,"d":{"guild_id":"1422625037164351591","channel_id":"x","self_mute":false,"self_deaf":false}}"#,
            ),
            (
                4,
                r#"{"op":4,"d":{"guild_id":"1422625037164351591","channel_id":"1"}}"#,
            ),
            (31, r#"{"op":31,"d":{"guild_ids":["1422625037164351591"]}}"#),
        ];
        for (op, payload) in cases {
            assert!(
                matches!(
                    filter_client_command(op, payload, &guilds()),
                    ClientCommand::Drop(_)
                ),
                "op {op} should be dropped: {payload}"
            );
        }
    }
}
