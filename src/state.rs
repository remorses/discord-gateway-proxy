use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::sync::{broadcast, watch};
use twilight_gateway::MessageSender;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use crate::{cache, config::CONFIG, dispatch::BroadcastMessage, model::JsonObject};

const SESSION_TTL: Duration = Duration::from_secs(30 * 60);
const OFFLINE_EVENT_BUFFER_LIMIT: usize = 200;
/// Per client. Heartbeats clear them every 41s, and only user messages count.
const UNACKED_EVENT_LIMIT: usize = 20;
/// How long a voice owner may stay disconnected before the bot leaves its call.
pub const VOICE_OWNER_GRACE: Duration = Duration::from_secs(60);

/// The multi-tenant client that owns the shared bot's voice connection in a
/// guild. Discord allows one voice connection per bot per guild, and the
/// `VOICE_SERVER_UPDATE` token must only reach the client that asked for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoiceOwner {
    pub client_id: String,
    /// Set while the owner has no live gateway connection.
    pub disconnected_at: Option<Instant>,
    /// False after the owner (or the proxy for it) sent a leave. The guild
    /// is free only once the bot is also out of voice, so a queued leave or
    /// a late event of the old call never reaches the next owner. No timer
    /// can free it: Discord gives no delivery bound for queued commands.
    pub wants_voice: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum VoiceDecision {
    Allow,
    Deny,
}

/// Whether `client_id` may send a voice state update in a guild with
/// `owner`. `bot_in_voice` is the bot's voice state in that guild from the
/// cache. Another client gets the guild only when the owner gave it up and
/// the bot is out of voice.
pub fn decide_voice_command(
    owner: Option<&VoiceOwner>,
    client_id: &str,
    bot_in_voice: bool,
) -> VoiceDecision {
    match owner {
        None => VoiceDecision::Allow,
        Some(owner) if owner.client_id == client_id => VoiceDecision::Allow,
        Some(owner) if !owner.wants_voice && !bot_in_voice => VoiceDecision::Allow,
        Some(_) => VoiceDecision::Deny,
    }
}

#[derive(Clone)]
pub struct BufferedClientEvent {
    pub payload: String,
    pub sequence: Option<crate::deserializer::SequenceInfo>,
    pub guild_id: Option<u64>,
    /// A `MESSAGE_CREATE` not sent by the shared bot.
    pub user_message: bool,
}

/// A user message sent on a client connection whose heartbeat has not
/// confirmed its sequence number yet.
pub struct UnackedEvent {
    pub connection: u64,
    pub seq: usize,
    pub event: BufferedClientEvent,
}

#[derive(Default)]
pub struct ClientUnacked {
    /// The newest connection that took the unacked events. Older dead
    /// sockets stop recording: that connection gets their events live.
    newest_connection: u64,
    events: VecDeque<UnackedEvent>,
}

/// Manager for the READY state of a shard.
/// Uses tokio::sync::watch to avoid the missed-notify race that exists
/// with Notify + RwLock (notification can fire between is_ready() check
/// and notified().await registration, causing false timeouts).
pub struct Ready {
    tx: watch::Sender<Option<JsonObject>>,
    rx: watch::Receiver<Option<JsonObject>>,
}

impl Ready {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self { tx, rx }
    }

    pub fn is_ready(&self) -> bool {
        self.rx.borrow().is_some()
    }

    pub fn set_ready(&self, payload: JsonObject) {
        let _ = self.tx.send(Some(payload));
    }

    pub fn set_not_ready(&self) {
        let _ = self.tx.send(None);
    }

    /// Wait until the shard has received a READY payload.
    /// Uses watch::changed() which is race-free: if the value changed
    /// between our last read and the await, changed() returns immediately.
    /// Returns Err if the sender is dropped before the shard becomes ready.
    pub async fn wait_until_ready(&self) -> Result<JsonObject, ReadySenderDropped> {
        let mut rx = self.rx.clone();
        loop {
            {
                let val = rx.borrow_and_update();
                if let Some(ref payload) = *val {
                    return Ok(payload.clone());
                }
            }
            // wait for next change — cannot miss notifications because
            // borrow_and_update() marks the current value as seen
            if rx.changed().await.is_err() {
                return Err(ReadySenderDropped);
            }
        }
    }
}

#[derive(Debug)]
pub struct ReadySenderDropped;

/// State of a single shard.
pub struct Shard {
    /// ID of this shard.
    pub id: u32,
    /// Sender for this shard.
    pub sender: MessageSender,
    /// Handle for broadcasting events for this shard.
    pub events: broadcast::Sender<BroadcastMessage>,
    /// READY state manager for this shard.
    pub ready: Ready,
    /// Cache for guilds on this shard.
    pub guilds: cache::Guilds,
}

/// A session initiated by a client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionPrincipal {
    BotToken,
    Client(String),
    Unvalidated(String),
}

/// A session initiated by a client.
#[derive(Clone)]
pub struct Session {
    /// Shard ID that this session is for.
    pub shard_id: u32,
    /// Compression as requested in IDENTIFY.
    pub compress: Option<bool>,
    /// Auth principal used to create this session.
    pub principal: SessionPrincipal,
    /// Guild IDs this client is authorized to receive events for.
    /// None means all events are forwarded (legacy behavior).
    /// Some(set) means only events for guilds in the set are forwarded.
    pub authorized_guilds: Option<Arc<HashSet<u64>>>,
    /// Last time this session was used by a client.
    pub last_accessed: Instant,
}

/// Global state for all shards managed by the proxy.
pub struct Inner {
    /// State of all shards managed by the proxy.
    pub shards: Vec<Arc<Shard>>,
    /// Total shard count.
    pub shard_count: u32,
    /// All sessions active in the proxy.
    pub sessions: RwLock<HashMap<String, Session>>,
    /// Active live gateway connections per multi-tenant client.
    pub active_client_connections: RwLock<HashMap<String, usize>>,
    /// Last buffered dispatch events for disconnected clients.
    pub offline_event_buffers: RwLock<HashMap<String, VecDeque<BufferedClientEvent>>>,
    /// User messages sent to a client that its heartbeat did not confirm yet.
    /// A socket that died without a close still counts as connected until the
    /// heartbeat timeout, so events sent meanwhile would be lost without this.
    pub unacked_events: RwLock<HashMap<String, ClientUnacked>>,
    pub next_connection_id: AtomicU64,
    /// Last wake attempt timestamp per client to avoid wake storms.
    pub last_wake_attempts: RwLock<HashMap<String, Instant>>,
    /// Fixed-window count of upstream commands per multi-tenant client.
    /// All tenants share the 120 commands per 60s budget of each shard.
    pub client_command_windows: RwLock<HashMap<String, (Instant, u32)>>,
    /// Guild ID -> client that owns the bot's voice connection there.
    pub voice_owners: RwLock<HashMap<u64, VoiceOwner>>,
    /// The bot user ID from READY, 0 until the first READY.
    pub bot_user_id: AtomicU64,
}

impl Inner {
    fn prune_expired_sessions(sessions: &mut HashMap<String, Session>) {
        let now = Instant::now();
        sessions.retain(|_, session| now.duration_since(session.last_accessed) <= SESSION_TTL);
    }

    /// Get a session by its ID.
    pub fn get_session(&self, session_id: &str) -> Option<Session> {
        let mut sessions = self.sessions.write().unwrap();
        Self::prune_expired_sessions(&mut sessions);

        let session = sessions.get_mut(session_id)?;
        session.last_accessed = Instant::now();

        Some(session.clone())
    }

    /// Create a new session.
    pub fn create_session(&self, session: Session) -> String {
        let mut sessions = self.sessions.write().unwrap();
        Self::prune_expired_sessions(&mut sessions);

        let mut rng = thread_rng();
        loop {
            // Session IDs are 32 bytes of ASCII.
            let session_id: String = std::iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(32)
                .collect();

            if sessions.contains_key(&session_id) {
                continue;
            }

            sessions.insert(session_id.clone(), session);
            return session_id;
        }
    }

    pub fn resolve_guild_id_for_channel(&self, channel_id: u64) -> Option<u64> {
        self.shards
            .iter()
            .find_map(|shard| shard.guilds.resolve_guild_id_for_channel(channel_id))
    }

    pub fn mark_client_connected(&self, client_id: &str) {
        let mut active = self.active_client_connections.write().unwrap();
        let current = active.get(client_id).copied().unwrap_or(0);
        active.insert(client_id.to_string(), current + 1);
        drop(active);
        self.set_voice_owner_disconnected(client_id, None);
    }

    pub fn mark_client_disconnected(&self, client_id: &str) {
        let mut active = self.active_client_connections.write().unwrap();
        let current = active.get(client_id).copied().unwrap_or(0);
        if current <= 1 {
            active.remove(client_id);
            drop(active);
            self.set_voice_owner_disconnected(client_id, Some(Instant::now()));
            return;
        }
        active.insert(client_id.to_string(), current - 1);
    }

    fn set_voice_owner_disconnected(&self, client_id: &str, at: Option<Instant>) {
        let mut owners = self.voice_owners.write().unwrap();
        for owner in owners.values_mut() {
            if owner.client_id == client_id {
                owner.disconnected_at = at;
            }
        }
    }

    pub fn voice_owner(&self, guild_id: u64) -> Option<String> {
        let owners = self.voice_owners.read().unwrap();
        owners.get(&guild_id).map(|owner| owner.client_id.clone())
    }

    /// The bot's voice state in a guild, from the shard cache. Unknown
    /// counts as in voice, so a guild is never freed on missing data.
    fn bot_in_voice(&self, guild_id: u64) -> bool {
        let (Some(bot_user_id), Some(shard)) = (self.bot_user_id(), self.shard_for_guild(guild_id))
        else {
            return true;
        };
        !CONFIG.cache.voice_states || shard.guilds.has_voice_state(guild_id, bot_user_id)
    }

    /// Records the voice command as the new state of the guild if allowed.
    /// A join takes ownership; a leave gives it up once the bot is out of voice.
    pub fn claim_voice(&self, guild_id: u64, client_id: &str, joining: bool) -> VoiceDecision {
        let bot_in_voice = self.bot_in_voice(guild_id);
        let mut owners = self.voice_owners.write().unwrap();
        let decision = decide_voice_command(owners.get(&guild_id), client_id, bot_in_voice);
        if decision == VoiceDecision::Allow {
            if joining {
                owners.insert(
                    guild_id,
                    VoiceOwner {
                        client_id: client_id.to_string(),
                        disconnected_at: None,
                        wants_voice: true,
                    },
                );
            } else if let Some(owner) = owners.get_mut(&guild_id) {
                owner.wants_voice = false;
            }
        }
        decision
    }

    /// Gives up every guild a client owns on a shard and returns the guilds
    /// the bot must leave.
    pub fn leave_voice_for_client(&self, client_id: &str, shard_id: u32) -> Vec<u64> {
        let mut owners = self.voice_owners.write().unwrap();
        let mut guilds = Vec::new();
        for (guild_id, owner) in owners.iter_mut() {
            if owner.client_id == client_id
                && owner.wants_voice
                && self.shard_id_for_guild(*guild_id) == shard_id
            {
                owner.wants_voice = false;
                guilds.push(*guild_id);
            }
        }
        guilds
    }

    /// The bot's own `VOICE_STATE_UPDATE` with a null channel. It frees the
    /// guild if the owner gave it up. If the owner still wants voice it is a
    /// kick, or the leave before the owner's rejoin: the owner keeps the guild.
    pub fn on_bot_voice_leave(&self, guild_id: u64) {
        let mut owners = self.voice_owners.write().unwrap();
        if owners
            .get(&guild_id)
            .is_some_and(|owner| !owner.wants_voice)
        {
            owners.remove(&guild_id);
        }
    }

    /// Gives up the guilds of owners that stayed disconnected past the grace
    /// period or lost access to the guild (`authorized` is client ID ->
    /// guilds), and drops given-up owners once the bot is out of voice.
    /// Returns the guilds the bot must leave.
    pub fn take_stale_voice_owners(&self, authorized: &HashMap<String, HashSet<u64>>) -> Vec<u64> {
        let now = Instant::now();
        let in_voice: HashMap<u64, bool> = self
            .voice_owners
            .read()
            .unwrap()
            .keys()
            .map(|guild_id| (*guild_id, self.bot_in_voice(*guild_id)))
            .collect();
        let mut owners = self.voice_owners.write().unwrap();
        owners
            .retain(|guild_id, owner| owner.wants_voice || in_voice.get(guild_id) != Some(&false));
        let mut guilds = Vec::new();
        for (guild_id, owner) in owners.iter_mut() {
            let expired = owner
                .disconnected_at
                .is_some_and(|at| now.duration_since(at) >= VOICE_OWNER_GRACE);
            let revoked = !authorized
                .get(&owner.client_id)
                .is_some_and(|guilds| guilds.contains(guild_id));
            if owner.wants_voice && (expired || revoked) {
                owner.wants_voice = false;
                guilds.push(*guild_id);
            }
        }
        guilds
    }

    pub fn shard_id_for_guild(&self, guild_id: u64) -> u32 {
        ((guild_id >> 22) % u64::from(self.shard_count)) as u32
    }

    pub fn shard_for_guild(&self, guild_id: u64) -> Option<&Arc<Shard>> {
        let shard_id = self.shard_id_for_guild(guild_id);
        self.shards.iter().find(|shard| shard.id == shard_id)
    }

    /// Makes the bot leave voice in a guild.
    pub fn send_voice_leave(&self, guild_id: u64) {
        let Some(shard) = self.shard_for_guild(guild_id) else {
            return;
        };
        let _res = shard.sender.send(format!(
            r#"{{"op":4,"d":{{"guild_id":"{guild_id}","channel_id":null,"self_mute":false,"self_deaf":false}}}}"#
        ));
    }

    pub fn bot_user_id(&self) -> Option<u64> {
        match self.bot_user_id.load(Ordering::Relaxed) {
            0 => None,
            id => Some(id),
        }
    }

    pub fn is_client_connected(&self, client_id: &str) -> bool {
        let active = self.active_client_connections.read().unwrap();
        active.get(client_id).copied().unwrap_or(0) > 0
    }

    pub fn push_offline_event_for_client(&self, client_id: &str, event: BufferedClientEvent) {
        let mut buffers = self.offline_event_buffers.write().unwrap();
        let entry = buffers
            .entry(client_id.to_string())
            .or_insert_with(VecDeque::new);
        if entry.len() >= OFFLINE_EVENT_BUFFER_LIMIT {
            let _ = entry.pop_front();
        }
        entry.push_back(event);
    }

    pub fn drain_offline_events_for_client(&self, client_id: &str) -> Vec<BufferedClientEvent> {
        let mut buffers = self.offline_event_buffers.write().unwrap();
        buffers
            .remove(client_id)
            .map(|deque| deque.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn record_unacked_event(&self, client_id: &str, unacked: UnackedEvent) {
        let mut clients = self.unacked_events.write().unwrap();
        let client = clients.entry(client_id.to_string()).or_default();
        if unacked.connection < client.newest_connection {
            return;
        }
        if client.events.len() >= UNACKED_EVENT_LIMIT {
            let _ = client.events.pop_front();
        }
        client.events.push_back(unacked);
    }

    /// A heartbeat carries the last sequence number the client received.
    pub fn ack_events(&self, client_id: &str, connection: u64, seq: usize) {
        if let Some(client) = self.unacked_events.write().unwrap().get_mut(client_id) {
            client
                .events
                .retain(|unacked| unacked.connection != connection || unacked.seq > seq);
        }
    }

    /// Unconfirmed events of the client's earlier connections, for replay on
    /// a new one. A client only connects again after it gave up the old socket.
    /// Call after subscribing to live events, so none falls in between.
    pub fn take_unacked_events(
        &self,
        client_id: &str,
        connection: u64,
    ) -> Vec<BufferedClientEvent> {
        let mut clients = self.unacked_events.write().unwrap();
        let client = clients.entry(client_id.to_string()).or_default();
        client.newest_connection = connection;
        std::mem::take(&mut client.events)
            .into_iter()
            .map(|unacked| unacked.event)
            .collect()
    }

    /// A client that closes with code 1000 shut down after reading everything.
    /// Any other end keeps the events for the next connection.
    pub fn drop_unacked_events(&self, client_id: &str, connection: u64) {
        if let Some(client) = self.unacked_events.write().unwrap().get_mut(client_id) {
            client
                .events
                .retain(|unacked| unacked.connection != connection);
        }
    }

    pub fn should_wake_client(&self, client_id: &str, cooldown: Duration) -> bool {
        let now = Instant::now();
        let mut wakes = self.last_wake_attempts.write().unwrap();
        let previous = wakes.get(client_id).copied();
        if let Some(last) = previous {
            if now.duration_since(last) < cooldown {
                return false;
            }
        }
        wakes.insert(client_id.to_string(), now);
        true
    }

    pub fn allow_client_command(&self, client_id: &str, limit: u32, window: Duration) -> bool {
        let now = Instant::now();
        let mut windows = self.client_command_windows.write().unwrap();
        let entry = windows.entry(client_id.to_string()).or_insert((now, 0));
        if now.duration_since(entry.0) >= window {
            *entry = (now, 0);
        }
        if entry.1 >= limit {
            return false;
        }
        entry.1 += 1;
        true
    }

    /// Remove offline event buffers and wake timestamps for client IDs that
    /// are no longer present in the CLIENTS registry. Without this, entries
    /// for deleted/uninstalled clients accumulate in memory forever.
    pub fn prune_stale_client_state(&self, valid_client_ids: &HashSet<String>) {
        {
            let mut buffers = self.offline_event_buffers.write().unwrap();
            buffers.retain(|client_id, _| valid_client_ids.contains(client_id));
        }
        {
            let mut events = self.unacked_events.write().unwrap();
            events.retain(|client_id, _| valid_client_ids.contains(client_id));
        }
        {
            let mut wakes = self.last_wake_attempts.write().unwrap();
            wakes.retain(|client_id, _| valid_client_ids.contains(client_id));
        }
        {
            let mut windows = self.client_command_windows.write().unwrap();
            windows.retain(|client_id, _| valid_client_ids.contains(client_id));
        }
    }
}

/// A reference to the [`StateInner`] of the proxy.
pub type State = Arc<Inner>;

#[cfg(test)]
mod tests {
    use super::{
        decide_voice_command, BufferedClientEvent, Inner, UnackedEvent, VoiceDecision, VoiceOwner,
    };
    use std::{
        collections::HashMap,
        sync::{atomic::AtomicU64, RwLock},
    };

    fn empty_state() -> Inner {
        Inner {
            shards: Vec::new(),
            shard_count: 1,
            sessions: RwLock::new(HashMap::new()),
            active_client_connections: RwLock::new(HashMap::new()),
            offline_event_buffers: RwLock::new(HashMap::new()),
            unacked_events: RwLock::new(HashMap::new()),
            next_connection_id: AtomicU64::new(0),
            last_wake_attempts: RwLock::new(HashMap::new()),
            client_command_windows: RwLock::new(HashMap::new()),
            voice_owners: RwLock::new(HashMap::new()),
            bot_user_id: AtomicU64::new(0),
        }
    }

    fn send(state: &Inner, connection: u64, seq: usize) {
        let event = BufferedClientEvent {
            payload: format!("m{connection}-{seq}"),
            sequence: None,
            guild_id: Some(1),
            user_message: true,
        };
        state.record_unacked_event(
            "c",
            UnackedEvent {
                connection,
                seq,
                event,
            },
        );
    }

    fn payloads(events: Vec<BufferedClientEvent>) -> Vec<String> {
        events.into_iter().map(|event| event.payload).collect()
    }

    #[test]
    fn unconfirmed_messages_of_a_dead_socket_reach_the_next_one() {
        let state = empty_state();
        send(&state, 0, 3);
        send(&state, 0, 4);
        state.ack_events("c", 0, 3);
        // The client reconnects while the dead socket is still open.
        assert_eq!(payloads(state.take_unacked_events("c", 1)), ["m0-4"]);
        // The dead socket keeps getting events; the new connection gets them live.
        send(&state, 0, 5);
        send(&state, 1, 1);
        // The new connection dies too, before confirming anything.
        assert_eq!(payloads(state.take_unacked_events("c", 2)), ["m1-1"]);

        // A shutdown with close code 1000 read everything.
        send(&state, 2, 1);
        state.drop_unacked_events("c", 2);
        assert!(state.take_unacked_events("c", 3).is_empty());
    }

    fn owner(wants_voice: bool) -> VoiceOwner {
        VoiceOwner {
            client_id: "a".to_string(),
            disconnected_at: None,
            wants_voice,
        }
    }

    #[test]
    fn voice_owner_keeps_the_guild_until_the_bot_is_out() {
        // (owner, client, bot in voice, expected)
        let cases = [
            (None, "b", true, VoiceDecision::Allow),
            (Some(owner(true)), "a", true, VoiceDecision::Allow),
            (Some(owner(true)), "b", false, VoiceDecision::Deny),
            (Some(owner(false)), "a", true, VoiceDecision::Allow),
            // Leave sent but not done yet: the old call is still up.
            (Some(owner(false)), "b", true, VoiceDecision::Deny),
            (Some(owner(false)), "b", false, VoiceDecision::Allow),
        ];
        for (owner, client_id, bot_in_voice, expected) in cases {
            assert_eq!(
                decide_voice_command(owner.as_ref(), client_id, bot_in_voice),
                expected
            );
        }
    }
}
