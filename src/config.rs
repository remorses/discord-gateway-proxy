#[cfg(target_os = "linux")]
use futures_util::StreamExt;
#[cfg(target_os = "linux")]
use inotify::{Inotify, WatchMask};
use serde::Deserialize;
#[cfg(not(feature = "simd-json"))]
use serde_json::Error as JsonError;
#[cfg(feature = "simd-json")]
use simd_json::Error as JsonError;
use tracing_subscriber::{filter::LevelFilter, reload};
use twilight_cache_inmemory::ResourceType;
use twilight_gateway::{EventTypeFlags, Intents};
use twilight_model::gateway::presence::{Activity, Status};

#[cfg(target_os = "linux")]
use std::str::FromStr;
use std::{
    collections::{HashMap, HashSet},
    env::var,
    fmt::{Display, Formatter, Result as FmtResult},
    fs::read_to_string,
    process::exit,
    sync::LazyLock,
};

/// Configuration for a client that can connect to the proxy.
/// Each client has a secret token and a list of guild IDs they're authorized to receive events for.
#[derive(Deserialize, Clone)]
pub struct ClientConfig {
    /// Secret token the client uses to authenticate (sent in IDENTIFY payload)
    pub secret: String,
    /// List of guild IDs this client is authorized to receive events for.
    /// Guild IDs can be strings or numbers in the JSON config.
    #[serde(default, deserialize_with = "deserialize_guild_ids")]
    pub guilds: HashSet<u64>,
    /// When set, the gateway-proxy connects outbound to this URL's /gateway WS
    /// endpoint instead of waiting for the client to connect inbound.
    /// Used for cloud-deployed kimaki instances that are internet-reachable.
    #[serde(default)]
    pub reachable_url: Option<String>,
    /// Soonest local task/sleep on the cloud machine. Set from PlanetScale.
    /// JSON config does not carry this; the DB poller fills it in.
    #[serde(default, skip_deserializing)]
    pub next_wake_at: Option<std::time::SystemTime>,
}

/// Custom deserializer to handle guild IDs as either strings or numbers
fn deserialize_guild_ids<'de, D>(deserializer: D) -> Result<HashSet<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{SeqAccess, Visitor};

    struct GuildIdsVisitor;

    impl<'de> Visitor<'de> for GuildIdsVisitor {
        type Value = HashSet<u64>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a list of guild IDs (as strings or numbers)")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut set = HashSet::new();

            #[derive(Deserialize)]
            #[serde(untagged)]
            enum GuildId {
                String(String),
                Number(u64),
            }

            while let Some(id) = seq.next_element::<GuildId>()? {
                let parsed = match id {
                    GuildId::String(s) => s.parse::<u64>().map_err(serde::de::Error::custom)?,
                    GuildId::Number(n) => n,
                };
                set.insert(parsed);
            }

            Ok(set)
        }
    }

    deserializer.deserialize_seq(GuildIdsVisitor)
}

#[derive(Deserialize)]
pub struct Config {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "token_fallback")]
    pub token: String,
    pub intents: Intents,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub shards: Option<u32>,
    #[serde(default)]
    pub shard_start: Option<u32>,
    #[serde(default)]
    pub shard_end: Option<u32>,
    #[serde(default)]
    pub activity: Option<Activity>,
    #[serde(default = "default_status")]
    pub status: Status,
    #[serde(default = "default_backpressure")]
    pub backpressure: usize,
    #[serde(default = "default_validate_token")]
    pub validate_token: bool,
    #[serde(default)]
    pub twilight_http_proxy: Option<String>,
    /// Override the Discord gateway WebSocket URL that shards connect to.
    /// Useful for testing with a local fake Discord server.
    /// If not set, shards connect to wss://gateway.discord.gg (default).
    #[serde(default)]
    pub gateway_url: Option<String>,
    pub externally_accessible_url: String,
    #[serde(default)]
    pub cache: Cache,
    /// Map of client ID to client configuration.
    /// Clients authenticate using their ID and secret in the IDENTIFY token field.
    /// Format: "client_id:client_secret" or just use the bot token for legacy behavior.
    #[serde(default)]
    pub clients: HashMap<String, ClientConfig>,
}

#[derive(Deserialize, Clone)]
pub struct Cache {
    pub channels: bool,
    pub presences: bool,
    pub emojis: bool,
    pub current_member: bool,
    pub members: bool,
    pub roles: bool,
    pub scheduled_events: bool,
    pub stage_instances: bool,
    pub stickers: bool,
    pub users: bool,
    pub voice_states: bool,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            channels: true,
            presences: false,
            current_member: true,
            emojis: false,
            members: false,
            roles: true,
            scheduled_events: false,
            stage_instances: false,
            stickers: false,
            users: false,
            voice_states: false,
        }
    }
}

impl From<Cache> for EventTypeFlags {
    fn from(cache: Cache) -> Self {
        let mut flags = Self::GUILD_CREATE
            | Self::GUILD_DELETE
            | Self::GUILD_UPDATE
            | Self::READY
            | Self::GATEWAY_INVALIDATE_SESSION;

        if cache.members || cache.current_member {
            flags |= Self::MEMBER_ADD | Self::MEMBER_REMOVE | Self::MEMBER_UPDATE;
        }

        if cache.roles {
            flags |= Self::ROLE_CREATE | Self::ROLE_DELETE | Self::ROLE_UPDATE;
        }

        if cache.channels {
            flags |= Self::CHANNEL_CREATE
                | Self::CHANNEL_DELETE
                | Self::CHANNEL_UPDATE
                | Self::THREAD_CREATE
                | Self::THREAD_DELETE
                | Self::THREAD_LIST_SYNC
                | Self::THREAD_UPDATE;
        }

        if cache.presences {
            flags |= Self::PRESENCE_UPDATE;
        }

        if cache.emojis {
            flags |= Self::GUILD_EMOJIS_UPDATE;
        }

        if cache.scheduled_events {
            flags |= Self::GUILD_SCHEDULED_EVENTS;
        }

        if cache.stage_instances {
            flags |= Self::STAGE_INSTANCE_CREATE
                | Self::STAGE_INSTANCE_DELETE
                | Self::STAGE_INSTANCE_UPDATE;
        }

        if cache.voice_states {
            flags |= Self::VOICE_STATE_UPDATE | Self::VOICE_SERVER_UPDATE;
        }

        if cache.users {
            flags |= Self::USER_UPDATE;
        }

        flags
    }
}

impl From<Cache> for ResourceType {
    fn from(cache: Cache) -> Self {
        let mut resource_types = Self::GUILD | Self::USER_CURRENT;

        if cache.channels {
            resource_types |= Self::CHANNEL;
        }

        if cache.emojis {
            resource_types |= Self::EMOJI;
        }

        if cache.current_member {
            resource_types |= Self::MEMBER_CURRENT;
        }

        if cache.members {
            resource_types |= Self::MEMBER;
        }

        if cache.presences {
            resource_types |= Self::PRESENCE;
        }

        if cache.roles {
            resource_types |= Self::ROLE;
        }

        if cache.scheduled_events {
            resource_types |= Self::GUILD_SCHEDULED_EVENT;
        }

        if cache.stage_instances {
            resource_types |= Self::STAGE_INSTANCE;
        }

        if cache.stickers {
            resource_types |= Self::STICKER;
        }

        if cache.users {
            resource_types |= Self::USER;
        }

        if cache.voice_states {
            resource_types |= Self::VOICE_STATE;
        }

        resource_types
    }
}

fn default_log_level() -> String {
    String::from("info")
}

const fn default_port() -> u16 {
    7878
}

fn token_fallback() -> String {
    if let Ok(token) = var("TOKEN") {
        token
    } else {
        eprintln!("Config Error: token is not present and TOKEN environment variable is not set");
        exit(1);
    }
}

const fn default_status() -> Status {
    Status::Online
}

const fn default_backpressure() -> usize {
    100
}

const fn default_validate_token() -> bool {
    true
}

pub enum Error {
    InvalidConfig(JsonError),
    NotFound(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::InvalidConfig(s) => s.fmt(f),
            Self::NotFound(s) => f.write_fmt(format_args!("File {s} not found or access denied")),
        }
    }
}

#[cfg(feature = "simd-json")]
pub fn load(path: &str) -> Result<Config, Error> {
    let mut content = read_to_string(path).map_err(|_| Error::NotFound(path.to_string()))?;
    let config = unsafe { simd_json::from_str(&mut content) }.map_err(Error::InvalidConfig)?;

    Ok(config)
}

#[cfg(not(feature = "simd-json"))]
pub fn load(path: &str) -> Result<Config, Error> {
    let content = read_to_string(path).map_err(|_| Error::NotFound(path.to_string()))?;
    let config = serde_json::from_str(&content).map_err(Error::InvalidConfig)?;

    Ok(config)
}

/// Parse config from a JSON string.
/// Used when config is provided via the CONFIG env var instead of a file.
#[cfg(feature = "simd-json")]
pub fn load_from_str(json: &mut String) -> Result<Config, Error> {
    let config = unsafe { simd_json::from_str(json) }.map_err(Error::InvalidConfig)?;
    Ok(config)
}

#[cfg(not(feature = "simd-json"))]
pub fn load_from_str(json: &mut String) -> Result<Config, Error> {
    let config = serde_json::from_str(json).map_err(Error::InvalidConfig)?;
    Ok(config)
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    // If CONFIG env var is set, parse it as JSON directly instead of reading config.json.
    // This is useful for containerized deployments (fly.io, Docker) where mounting
    // a config file is inconvenient and secrets/env vars are the standard approach.
    if let Ok(mut config_json) = var("CONFIG") {
        match load_from_str(&mut config_json) {
            Ok(config) => return config,
            Err(err) => {
                eprintln!("Config Error (from CONFIG env var): {err}");
                exit(1);
            }
        }
    }

    match load("config.json") {
        Ok(config) => config,
        Err(err) => {
            eprintln!("Config Error: {err}");
            exit(1);
        }
    }
});

#[cfg(target_os = "linux")]
pub async fn watch_config_changes<S>(reload_handle: reload::Handle<LevelFilter, S>) {
    let Ok(inotify) = Inotify::init() else {
        tracing::error!("Failed to initialize inotify, log-levels cannot be reloaded on the fly");
        return;
    };

    if inotify
        .watches()
        .add("config.json", WatchMask::MODIFY)
        .is_err()
    {
        tracing::error!("Failed to add inotify watch, log-levels cannot be reloaded on the fly");
        return;
    };

    tracing::debug!("Inotify is initialized");

    let buffer = [0u8; 4096];
    // This method never returns Err
    let mut events = inotify.into_event_stream(buffer).unwrap();

    while let Some(Ok(_)) = events.next().await {
        // This currently only supports reloading log-levels
        if let Ok(config) = load("config.json") {
            let _ = reload_handle.modify(|filter| {
                *filter = LevelFilter::from_str(&config.log_level).unwrap_or(LevelFilter::INFO);
            });
            tracing::info!("Config was modified, reloaded log-level");
        } else {
            tracing::error!("Config was modified, but failed to reload");
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn watch_config_changes<S>(_reload_handle: reload::Handle<LevelFilter, S>) {
    tracing::warn!("Config hot-reload is only supported on Linux (requires inotify)");
}
