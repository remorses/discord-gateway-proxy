use serde::{de::DeserializeOwned, Deserialize, Serialize};
#[cfg(not(feature = "simd-json"))]
use serde_json::Value as OwnedValue;
#[cfg(feature = "simd-json")]
use simd_json::OwnedValue;

#[derive(Deserialize)]
pub struct Identify {
    pub d: IdentifyInfo,
}

#[derive(Deserialize)]
pub struct Resume {
    pub d: ResumeInfo,
}

#[derive(Deserialize)]
pub struct IdentifyInfo {
    #[serde(default)]
    pub compress: Option<bool>,
    pub shard: [u32; 2],
    pub token: String,
}

#[derive(Deserialize)]
pub struct ResumeInfo {
    pub session_id: String,
    pub seq: usize,
    pub token: String,
}

/// Opcode 8. Rebuilt field by field before it goes upstream, so a client
/// cannot smuggle fields that the guild check did not see.
#[derive(Deserialize)]
pub struct RequestGuildMembers {
    pub d: RequestGuildMembersInfo,
}

#[derive(Deserialize, Serialize)]
pub struct RequestGuildMembersInfo {
    pub guild_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presences: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_ids: Option<UserIds>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub enum UserIds {
    One(String),
    Many(Vec<String>),
}

/// Opcode 4. Rebuilt like opcode 8.
#[derive(Deserialize)]
pub struct VoiceStateUpdate {
    pub d: VoiceStateUpdateInfo,
}

#[derive(Deserialize, Serialize)]
pub struct VoiceStateUpdateInfo {
    pub guild_id: String,
    pub channel_id: Option<String>,
    pub self_mute: bool,
    pub self_deaf: bool,
}

/// The bot user in READY.
#[derive(Deserialize)]
pub struct ReadyUser {
    pub d: ReadyUserInfo,
}

#[derive(Deserialize)]
pub struct ReadyUserInfo {
    pub user: IdOnly,
}

#[derive(Deserialize)]
pub struct IdOnly {
    pub id: String,
}

/// The fields of a `VOICE_STATE_UPDATE` dispatch the proxy needs.
#[derive(Deserialize)]
pub struct VoiceStateEvent {
    pub d: VoiceStateEventInfo,
}

#[derive(Deserialize)]
pub struct VoiceStateEventInfo {
    pub user_id: String,
    #[serde(default)]
    pub channel_id: Option<String>,
}

pub fn parse_json<T: DeserializeOwned>(payload: &str) -> Option<T> {
    #[cfg(feature = "simd-json")]
    return unsafe { simd_json::from_str(&mut payload.to_owned()) }.ok();
    #[cfg(not(feature = "simd-json"))]
    return serde_json::from_str(payload).ok();
}

#[derive(Serialize)]
pub struct OutgoingCommand<'a, T> {
    pub op: u8,
    pub d: &'a T,
}

#[derive(Deserialize)]
pub struct Ready {
    pub d: JsonObject,
}

pub type JsonObject = halfbrown::HashMap<String, OwnedValue>;
