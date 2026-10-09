use serde::{Deserialize, Serialize};
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
