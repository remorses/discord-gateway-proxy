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

use crate::{cache, dispatch::BroadcastMessage, model::JsonObject};

const SESSION_TTL: Duration = Duration::from_secs(30 * 60);
const OFFLINE_EVENT_BUFFER_LIMIT: usize = 200;
/// How long a voice owner may stay disconnected before the bot leaves its call.
pub const VOICE_OWNER_GRACE: Duration = Duration::from_secs(60);
/// How long a guild stays locked after a leave that Discord never confirms
/// (for example a leave sent while the bot was not in voice).
pub const VOICE_LEAVE_TIMEOUT: Duration = Duration::from_secs(5);

/// The multi-tenant client that owns the shared bot's voice connection in a
/// guild. Discord allows one voice connection per bot per guild, and the
/// `VOICE_SERVER_UPDATE` token must only reach the client that asked for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoiceOwner {
    pub client_id: String,
    /// Set while the owner has no live gateway connection.
    pub disconnected_at: Option<Instant>,
    /// Set from a leave until Discord confirms it with the bot's null voice
    /// state. The guild stays locked meanwhile, so events of the old call
    /// (late `VOICE_SERVER_UPDATE`, the null state) cannot reach or free the
    /// next owner.
    pub leaving_since: Option<Instant>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum VoiceDecision {
    Allow,
    Deny,
}

/// Whether `client_id` may send a voice state update in a guild with `owner`.
/// The first client takes the guild; others wait until the owner's leave is
/// confirmed (the owner is removed) or times out.
pub fn decide_voice_command(
    owner: Option<&VoiceOwner>,
    client_id: &str,
    now: Instant,
) -> VoiceDecision {
    match owner {
        None => VoiceDecision::Allow,
        Some(owner) if owner.client_id == client_id => VoiceDecision::Allow,
        Some(VoiceOwner {
            leaving_since: Some(at),
            ..
        }) if now.duration_since(*at) >= VOICE_LEAVE_TIMEOUT => VoiceDecision::Allow,
        Some(_) => VoiceDecision::Deny,
    }
}

#[derive(Clone)]
pub struct BufferedClientEvent {
    pub payload: String,
    pub sequence: Option<crate::deserializer::SequenceInfo>,
    pub guild_id: Option<u64>,
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

    /// Records the voice command as the new state of the guild if allowed.
    /// A join takes ownership; a leave starts the leave transition.
    pub fn claim_voice(&self, guild_id: u64, client_id: &str, joining: bool) -> VoiceDecision {
        let now = Instant::now();
        let mut owners = self.voice_owners.write().unwrap();
        let decision = decide_voice_command(owners.get(&guild_id), client_id, now);
        if decision == VoiceDecision::Allow {
            if joining {
                owners.insert(
                    guild_id,
                    VoiceOwner {
                        client_id: client_id.to_string(),
                        disconnected_at: None,
                        leaving_since: None,
                    },
                );
            } else if let Some(owner) = owners.get_mut(&guild_id) {
                owner.leaving_since = Some(now);
            }
        }
        decision
    }

    /// Starts the leave transition for every guild a client owns on a shard
    /// and returns the guilds the bot must leave.
    pub fn leave_voice_for_client(&self, client_id: &str, shard_id: u32) -> Vec<u64> {
        let now = Instant::now();
        let mut owners = self.voice_owners.write().unwrap();
        let mut guilds = Vec::new();
        for (guild_id, owner) in owners.iter_mut() {
            if owner.client_id == client_id
                && owner.leaving_since.is_none()
                && self.shard_id_for_guild(*guild_id) == shard_id
            {
                owner.leaving_since = Some(now);
                guilds.push(*guild_id);
            }
        }
        guilds
    }

    /// The bot's own `VOICE_STATE_UPDATE`. A null channel confirms a leave
    /// (the owner is removed); without a pending leave it is a kick or the
    /// leave before an owner's rejoin, so it only starts the transition. A
    /// channel means the owner's join went through.
    pub fn on_bot_voice_state(&self, guild_id: u64, in_channel: bool) {
        let mut owners = self.voice_owners.write().unwrap();
        let Some(owner) = owners.get_mut(&guild_id) else {
            return;
        };
        if in_channel {
            owner.leaving_since = None;
        } else if owner.leaving_since.is_some() {
            owners.remove(&guild_id);
        } else {
            owner.leaving_since = Some(Instant::now());
        }
    }

    /// Starts the leave transition for owners that stayed disconnected past
    /// the grace period or lost access to the guild (`authorized` is client
    /// ID -> guilds), and drops leaves that timed out. Returns the guilds the
    /// bot must leave.
    pub fn take_stale_voice_owners(&self, authorized: &HashMap<String, HashSet<u64>>) -> Vec<u64> {
        let now = Instant::now();
        let mut owners = self.voice_owners.write().unwrap();
        owners.retain(|_, owner| {
            owner
                .leaving_since
                .is_none_or(|at| now.duration_since(at) < VOICE_LEAVE_TIMEOUT)
        });
        let mut guilds = Vec::new();
        for (guild_id, owner) in owners.iter_mut() {
            let expired = owner
                .disconnected_at
                .is_some_and(|at| now.duration_since(at) >= VOICE_OWNER_GRACE);
            let revoked = !authorized
                .get(&owner.client_id)
                .is_some_and(|guilds| guilds.contains(guild_id));
            if owner.leaving_since.is_none() && (expired || revoked) {
                owner.leaving_since = Some(now);
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
    use super::{decide_voice_command, VoiceDecision, VoiceOwner, VOICE_LEAVE_TIMEOUT};
    use std::time::{Duration, Instant};

    fn owner(leaving_since: Option<Instant>) -> VoiceOwner {
        VoiceOwner {
            client_id: "a".to_string(),
            disconnected_at: None,
            leaving_since,
        }
    }

    #[test]
    fn voice_owner_keeps_the_guild_until_its_leave_is_done() {
        let now = Instant::now();
        let recent = now - Duration::from_secs(1);
        let timed_out = now - VOICE_LEAVE_TIMEOUT;
        let cases = [
            (None, "b", VoiceDecision::Allow),
            (Some(owner(None)), "a", VoiceDecision::Allow),
            (Some(owner(None)), "b", VoiceDecision::Deny),
            (Some(owner(Some(recent))), "a", VoiceDecision::Allow),
            (Some(owner(Some(recent))), "b", VoiceDecision::Deny),
            (Some(owner(Some(timed_out))), "b", VoiceDecision::Allow),
        ];
        for (owner, client_id, expected) in cases {
            assert_eq!(
                decide_voice_command(owner.as_ref(), client_id, now),
                expected
            );
        }
    }
}
