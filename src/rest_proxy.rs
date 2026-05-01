// HTTP REST proxy for Discord API with client token authorization.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, header::AUTHORIZATION, Request, Response, StatusCode};
#[cfg(not(feature = "simd-json"))]
use serde_json::Value as JsonValue;
#[cfg(feature = "simd-json")]
use simd_json::prelude::ValueAsScalar;
#[cfg(feature = "simd-json")]
use simd_json::OwnedValue as JsonValue;

use std::{collections::HashSet, sync::LazyLock};
use tracing::warn;

use crate::{
    auth,
    config::CONFIG,
    state::{SessionPrincipal, State},
};

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

#[derive(Debug)]
enum RouteScope {
    Guild(u64),
    Channel(u64),
    AllowedWithoutGuild,
    AllowedWithoutAuth,
    DeniedWithoutGuild,
}

fn json_error(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    let payload = format!(r#"{{"error":"{}"}}"#, message.replace('"', "\\\""));
    let mut response = Response::new(Full::from(Bytes::from(payload)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}

fn parse_snowflake(value: &str) -> Option<u64> {
    value.parse::<u64>().ok()
}

fn resolve_route_scope(path: &str) -> RouteScope {
    let segments: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    if segments.len() < 2 {
        return RouteScope::DeniedWithoutGuild;
    }

    let base_index = if segments[0] == "api" && segments[1] == "v10" {
        2
    } else if segments[0] == "v10" {
        1
    } else {
        return RouteScope::DeniedWithoutGuild;
    };

    if segments.len() <= base_index {
        return RouteScope::DeniedWithoutGuild;
    }

    let route = &segments[base_index..];

    if route.len() >= 2 && route[0] == "gateway" && route[1] == "bot" {
        return RouteScope::AllowedWithoutGuild;
    }

    if route.len() >= 2
        && route[0] == "users"
        && (route[1] == "@me" || route[1].eq_ignore_ascii_case("%40me"))
    {
        return RouteScope::AllowedWithoutGuild;
    }

    if route.len() >= 3 && route[0] == "interactions" {
        let Some(_interaction_id) = parse_snowflake(route[1]) else {
            return RouteScope::DeniedWithoutGuild;
        };
        if route[2].is_empty() {
            return RouteScope::DeniedWithoutGuild;
        }
        return RouteScope::AllowedWithoutAuth;
    }

    if route.len() >= 3 && route[0] == "webhooks" {
        let Some(_webhook_id) = parse_snowflake(route[1]) else {
            return RouteScope::DeniedWithoutGuild;
        };
        if route[2].is_empty() {
            return RouteScope::DeniedWithoutGuild;
        }
        return RouteScope::AllowedWithoutAuth;
    }

    if route.len() >= 2 && route[0] == "guilds" {
        return parse_snowflake(route[1]).map_or(RouteScope::DeniedWithoutGuild, RouteScope::Guild);
    }

    if route.len() >= 2 && route[0] == "channels" {
        let Some(channel_id) = parse_snowflake(route[1]) else {
            return RouteScope::DeniedWithoutGuild;
        };

        return RouteScope::Channel(channel_id);
    }

    if route.len() >= 4 && route[0] == "applications" && route[2] == "guilds" {
        return parse_snowflake(route[3]).map_or(RouteScope::DeniedWithoutGuild, RouteScope::Guild);
    }

    RouteScope::DeniedWithoutGuild
}

fn is_client_authorized_for_route(authorized_guilds: &HashSet<u64>, scope: &RouteScope) -> bool {
    match scope {
        RouteScope::Guild(guild_id) => authorized_guilds.contains(guild_id),
        RouteScope::Channel(_) => false,
        RouteScope::AllowedWithoutGuild => true,
        RouteScope::AllowedWithoutAuth => true,
        RouteScope::DeniedWithoutGuild => false,
    }
}

fn should_attach_bot_authorization(scope: &RouteScope, has_auth_context: bool) -> bool {
    has_auth_context && !matches!(scope, RouteScope::AllowedWithoutAuth)
}

fn should_skip_request_header(name: &str) -> bool {
    matches!(
        name,
        "authorization"
            | "host"
            | "connection"
            | "transfer-encoding"
            | "content-length"
            | "accept-encoding"
    )
}

fn should_skip_response_header(name: &str) -> bool {
    matches!(name, "transfer-encoding" | "content-length")
}

fn discord_rest_base_url() -> String {
    match &CONFIG.twilight_http_proxy {
        Some(proxy) => {
            if proxy.starts_with("http://") || proxy.starts_with("https://") {
                proxy.clone()
            } else {
                format!("http://{proxy}")
            }
        }
        None => String::from("https://discord.com"),
    }
}

#[cfg(feature = "simd-json")]
fn parse_guild_id_from_channel_payload(raw_payload: &[u8]) -> Option<u64> {
    let mut owned = raw_payload.to_vec();
    let payload = simd_json::to_owned_value(&mut owned).ok()?;
    let JsonValue::Object(object) = payload else {
        return None;
    };

    let guild_value = object.get("guild_id")?;
    let guild_id = guild_value.as_str()?;
    parse_snowflake(guild_id)
}

#[cfg(not(feature = "simd-json"))]
fn parse_guild_id_from_channel_payload(raw_payload: &[u8]) -> Option<u64> {
    let payload: JsonValue = serde_json::from_slice(raw_payload).ok()?;
    let JsonValue::Object(object) = payload else {
        return None;
    };

    let guild_value = object.get("guild_id")?;
    let guild_id = guild_value.as_str()?;
    parse_snowflake(guild_id)
}

async fn lookup_channel_guild_id(channel_id: u64) -> Option<u64> {
    let channel_url = format!(
        "{}/api/v10/channels/{}",
        discord_rest_base_url(),
        channel_id
    );

    let response = HTTP_CLIENT
        .get(channel_url)
        .header(AUTHORIZATION.as_str(), format!("Bot {}", CONFIG.token))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }

    let body = response.bytes().await.ok()?;
    parse_guild_id_from_channel_payload(&body)
}

async fn resolve_channel_guild_id(channel_id: u64, state: &State) -> Option<u64> {
    if let Some(guild_id) = state.resolve_guild_id_for_channel(channel_id) {
        return Some(guild_id);
    }

    lookup_channel_guild_id(channel_id).await
}

#[cfg(feature = "simd-json")]
fn rewrite_gateway_bot_payload(raw_payload: &[u8]) -> Result<Vec<u8>, ()> {
    let mut owned = raw_payload.to_vec();
    let mut payload = simd_json::to_owned_value(&mut owned).map_err(|_| ())?;

    let JsonValue::Object(ref mut object) = payload else {
        return Err(());
    };

    object.insert(
        String::from("url"),
        CONFIG.externally_accessible_url.clone().into(),
    );

    simd_json::to_string(&payload)
        .map(|serialized| serialized.into_bytes())
        .map_err(|_| ())
}

#[cfg(not(feature = "simd-json"))]
fn rewrite_gateway_bot_payload(raw_payload: &[u8]) -> Result<Vec<u8>, ()> {
    let mut payload: JsonValue = serde_json::from_slice(raw_payload).map_err(|_| ())?;

    let JsonValue::Object(ref mut object) = payload else {
        return Err(());
    };

    object.insert(
        String::from("url"),
        JsonValue::String(CONFIG.externally_accessible_url.clone()),
    );

    serde_json::to_vec(&payload).map_err(|_| ())
}

fn build_response(
    status: StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Bytes,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::from(body));
    *response.status_mut() = status;

    for (name, value) in &headers {
        if should_skip_response_header(name.as_str()) {
            continue;
        }

        let _res = response.headers_mut().insert(name, value.clone());
    }

    response
}

pub async fn handle_rest_request(
    request: Request<Incoming>,
    state: State,
) -> Response<Full<Bytes>> {
    let request_path = request.uri().path().to_string();
    let normalized_path = if request_path.starts_with("/v10/") {
        format!("/api{}", request_path)
    } else {
        request_path.clone()
    }
    .replace("/users/%40me", "/users/@me")
    .replace("/users/%40ME", "/users/@me");
    let normalized_uri = request.uri().query().map_or_else(
        || normalized_path.clone(),
        |query| format!("{}?{}", normalized_path, query),
    );
    let scope = resolve_route_scope(&normalized_path);

    let auth_header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .map(auth::normalize_gateway_token)
        .unwrap_or("");

    // Tokenized routes (interactions, webhooks) authenticate via URL token —
    // skip proxy auth entirely. discord.js sends Authorization on ALL requests
    // including these, so checking auth_header.is_empty() is wrong here.
    let auth_context = if matches!(scope, RouteScope::AllowedWithoutAuth) {
        None
    } else {
        let Some(auth_context) = auth::authenticate_gateway_token(auth_header) else {
            warn!(
                "REST auth rejected: missing or invalid credentials: path={}",
                normalized_path
            );
            return json_error(StatusCode::UNAUTHORIZED, "Invalid or missing credentials");
        };
        Some(auth_context)
    };

    if let Some(auth_context) = auth_context.as_ref() {
        if matches!(auth_context.principal, SessionPrincipal::Client(_)) {
            let Some(authorized_guilds) = auth_context.authorized_guilds.as_deref() else {
                warn!(
                    "REST auth rejected: missing guild authorization: path={}",
                    normalized_path
                );
                return json_error(StatusCode::FORBIDDEN, "Missing guild authorization");
            };

            if matches!(scope, RouteScope::Channel(_)) {
                let RouteScope::Channel(channel_id) = scope else {
                    return json_error(StatusCode::FORBIDDEN, "Channel route authorization failed");
                };

                let guild_id = resolve_channel_guild_id(channel_id, &state).await;
                let is_authorized = guild_id
                    .map(|resolved_guild_id| authorized_guilds.contains(&resolved_guild_id))
                    .unwrap_or(false);
                if !is_authorized {
                    warn!(
                        "REST auth rejected channel scope: channel_id={}, resolved_guild_id={:?}",
                        channel_id, guild_id
                    );
                    return json_error(
                        StatusCode::FORBIDDEN,
                        "REST route is outside the authorized guild scope",
                    );
                }
            } else if !is_client_authorized_for_route(authorized_guilds, &scope) {
                warn!(
                    "REST auth rejected route scope: path={}, scope={:?}",
                    normalized_path, scope
                );
                return json_error(
                    StatusCode::FORBIDDEN,
                    "REST route is outside the authorized guild scope",
                );
            }
        }
    }

    let method = request.method().clone();
    let headers = request.headers().clone();

    let collected_body = match request.into_body().collect().await {
        Ok(collected) => collected,
        Err(_) => {
            return json_error(StatusCode::BAD_REQUEST, "Failed to read request body");
        }
    };
    let body_bytes = collected_body.to_bytes();

    let upstream_url = format!("{}{}", discord_rest_base_url(), normalized_uri);
    let upstream_method = match reqwest::Method::from_bytes(method.as_str().as_bytes()) {
        Ok(parsed) => parsed,
        Err(_) => {
            return json_error(StatusCode::METHOD_NOT_ALLOWED, "Unsupported HTTP method");
        }
    };

    let mut upstream_request = HTTP_CLIENT.request(upstream_method, &upstream_url);

    for (name, value) in &headers {
        if should_skip_request_header(name.as_str()) {
            continue;
        }

        upstream_request = upstream_request.header(name, value);
    }

    if should_attach_bot_authorization(&scope, auth_context.is_some()) {
        upstream_request =
            upstream_request.header(AUTHORIZATION.as_str(), format!("Bot {}", CONFIG.token));
    }

    if !body_bytes.is_empty() {
        upstream_request = upstream_request.body(body_bytes.to_vec());
    }

    let upstream_response = match upstream_request.send().await {
        Ok(response) => response,
        Err(error) => {
            warn!(
                "REST proxy upstream request failed: path={}, error={}",
                normalized_path, error
            );
            return json_error(StatusCode::BAD_GATEWAY, "Failed to reach Discord API");
        }
    };

    let status = match StatusCode::from_u16(upstream_response.status().as_u16()) {
        Ok(code) => code,
        Err(_) => StatusCode::BAD_GATEWAY,
    };
    if !status.is_success() {
        warn!(
            "REST proxy upstream returned non-success status: path={}, status={}",
            normalized_path, status
        );
    }
    let response_headers = upstream_response.headers().clone();
    let response_body = match upstream_response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(
                "REST proxy failed to read upstream response body: path={}, error={}",
                normalized_path, error
            );
            return json_error(
                StatusCode::BAD_GATEWAY,
                "Failed to read Discord API response",
            );
        }
    };

    if normalized_path == "/api/v10/gateway/bot" && status.is_success() {
        let rewritten = match rewrite_gateway_bot_payload(&response_body) {
            Ok(payload) => payload,
            Err(_) => {
                return json_error(StatusCode::BAD_GATEWAY, "Invalid gateway response payload");
            }
        };

        return build_response(status, response_headers, Bytes::from(rewritten));
    }

    build_response(status, response_headers, response_body)
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_route_scope, should_attach_bot_authorization, should_skip_request_header,
        should_skip_response_header, RouteScope,
    };

    #[test]
    fn resolves_tokenized_routes_as_allowed_without_auth() {
        assert!(matches!(
            resolve_route_scope("/api/v10/interactions/123456789/token/callback"),
            RouteScope::AllowedWithoutAuth
        ));
        assert!(matches!(
            resolve_route_scope("/api/v10/webhooks/123456789/token/messages/@original"),
            RouteScope::AllowedWithoutAuth
        ));
    }

    #[test]
    fn denies_non_tokenized_webhook_routes_without_guild_scope() {
        assert!(matches!(
            resolve_route_scope("/api/v10/webhooks/123456789"),
            RouteScope::DeniedWithoutGuild
        ));
        assert!(matches!(
            resolve_route_scope("/api/v10/interactions/123456789"),
            RouteScope::DeniedWithoutGuild
        ));
    }

    #[test]
    fn never_attaches_bot_authorization_for_allowed_without_auth_routes() {
        assert!(!should_attach_bot_authorization(
            &RouteScope::AllowedWithoutAuth,
            true,
        ));
        assert!(!should_attach_bot_authorization(
            &RouteScope::AllowedWithoutAuth,
            false,
        ));
        assert!(should_attach_bot_authorization(
            &RouteScope::AllowedWithoutGuild,
            true
        ));
    }

    #[test]
    fn keeps_content_encoding_response_header() {
        assert!(!should_skip_response_header("content-encoding"));
        assert!(should_skip_response_header("content-length"));
        assert!(should_skip_response_header("transfer-encoding"));
    }

    #[test]
    fn skips_accept_encoding_request_header() {
        assert!(should_skip_request_header("accept-encoding"));
        assert!(should_skip_request_header("authorization"));
        assert!(!should_skip_request_header("user-agent"));
    }
}
