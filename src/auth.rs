// Shared authentication for gateway WebSocket and REST proxy paths.

use std::{collections::HashSet, sync::Arc};

use crate::{config::CONFIG, db_config, state::SessionPrincipal};

pub struct AuthContext {
    pub principal: SessionPrincipal,
    pub authorized_guilds: Option<Arc<HashSet<u64>>>,
}

pub fn normalize_gateway_token(token: &str) -> &str {
    token.split_whitespace().last().unwrap_or("")
}

/// Result of a gateway token authentication attempt.
pub enum GatewayAuthResult {
    Ok(AuthContext),
    /// Auth backend (database) is stale — transient failure, not invalid credentials.
    Stale,
    /// Credentials are invalid or missing.
    Invalid,
}

pub fn authenticate_gateway_token(token: &str) -> GatewayAuthResult {
    if token == CONFIG.token {
        return GatewayAuthResult::Ok(AuthContext {
            principal: SessionPrincipal::BotToken,
            authorized_guilds: None,
        });
    }

    match db_config::authenticate_client_with_id(token) {
        db_config::ClientAuthResult::Ok(client_id, guilds) => {
            return GatewayAuthResult::Ok(AuthContext {
                principal: SessionPrincipal::Client(client_id),
                authorized_guilds: Some(Arc::new(guilds)),
            });
        }
        db_config::ClientAuthResult::Stale => {
            return GatewayAuthResult::Stale;
        }
        db_config::ClientAuthResult::Invalid => {}
    }

    if CONFIG.validate_token {
        return GatewayAuthResult::Invalid;
    }

    GatewayAuthResult::Ok(AuthContext {
        principal: SessionPrincipal::Unvalidated(token.to_string()),
        authorized_guilds: None,
    })
}
