<!-- Agent guidance for gateway-proxy contributors and automation. -->

# gateway-proxy scope

gateway-proxy is a Discord proxy for both **Gateway WebSocket** and
**Discord REST** traffic.

- Handle websocket upgrade, IDENTIFY/RESUME auth, shard fanout, event filtering,
  and gateway session behavior.
- Handle REST `/api/v10/*` forwarding with the same client auth model used for
  guild-scoped WS filtering.
- Exposed operational endpoints: `/metrics`, `/shard-count`.

# multi-tenant REST invariants

gateway-proxy is multi-tenant. a client must never be able to read or mutate
resources outside its authorized guilds.

- guild resources must be explicitly guild-scoped and checked against
  `authorized_guilds`.
- only tokenized interaction/webhook routes can be treated as
  `AllowedWithoutAuth` (`/interactions/{id}/{token}/...`,
  `/webhooks/{id}/{token}/...`).
- never allow `/webhooks/{id}` as an unscoped allowlist route.
- for `AllowedWithoutAuth`, do not inject bot `Authorization` when forwarding.
- if route scope cannot be proven, fail closed.
- if Discord says the channel is unknown (`404` / code `10003`), return that
  Discord 404. Do not remap it to `403`. `kimaki project list --prune` treats
  404 as deleted and 403 as a permission error. Keep `403` only when the
  guild is known and not authorized. Use `502` for lookup transport failures.

# split with website

Onboarding flows are handled by the `website` package.

- gateway-proxy owns REST proxying (`/api/v10/*`) and `client_id:secret`
  authorization for REST requests.
- website handles OAuth callback and onboarding status endpoints only.

When changing one side of this split, document and validate compatibility with
the other side.

# Discord OpenAPI source of truth

Use Discord's official OpenAPI schema to validate REST route handling decisions:

- https://raw.githubusercontent.com/discord/discord-api-spec/main/specs/openapi.json

When editing `src/rest_proxy.rs`, check this schema first to confirm which
routes are bot-token routes vs tokenized unauth routes. Keep gateway-proxy
allowlists strict and fail-closed.

For this project, especially validate:

- `/interactions/{interaction_id}/{interaction_token}/...`
- `/webhooks/{webhook_id}/{webhook_token}/...`
- `/webhooks/{webhook_id}` (bot-token route; must not be allowlisted as unauth)

Useful `jq` commands for the large OpenAPI file:

```bash
# 1) list route-index, method-index, method, path (one row per operation)
jq -r '
  .paths
  | to_entries
  | to_entries[]
  | .key as $routeIndex
  | .value.key as $path
  | .value.value
  | to_entries
  | to_entries[]
  | .key as $methodIndex
  | .value.key as $method
  | select($method | test("^(get|post|put|patch|delete|head|options)$"))
  | "\($routeIndex)\t\($methodIndex)\t\($method|ascii_upcase)\t\($path)"
' ./tmp/discord-openapi.json

# 2) same listing, filtered by a path fragment (example: webhooks)
jq -r --arg q "/webhooks" '
  .paths
  | to_entries
  | to_entries[]
  | .key as $routeIndex
  | .value.key as $path
  | select($path | contains($q))
  | .value.value
  | to_entries
  | to_entries[]
  | .key as $methodIndex
  | .value.key as $method
  | select($method | test("^(get|post|put|patch|delete|head|options)$"))
  | "\($routeIndex)\t\($methodIndex)\t\($method|ascii_upcase)\t\($path)"
' ./tmp/discord-openapi.json

# 3) inspect one full route object by route index from command (1)
jq '.paths | to_entries[133]' ./tmp/discord-openapi.json

# 4) inspect one method object by route index + method index from command (1)
jq '.paths | to_entries[133].value | to_entries[2]' ./tmp/discord-openapi.json
```

Use these indices as stable anchors while reviewing route behavior in
`src/rest_proxy.rs`.

# deploying

ALWAYS use the deployment script to deploy gateway-proxy. NEVER use `fly deploy` directly.

```bash
cd gateway-proxy && pnpm run deployment
```

This cross-compiles the Rust binary locally on macOS (via `build:linux` using `x86_64-linux-musl-gcc`), then deploys a minimal scratch image with `Dockerfile.fly`. The `Dockerfile` (non-fly) is only for reference and is NOT used for deployment.

To skip the build step (e.g. re-deploy with same binary):

```bash
SKIP_BUILD=1 pnpm run deployment
```

The deployment script reads secrets from Doppler (project: `website`, stage: `production`) and sets them as Fly secrets automatically.

# database TLS

The gateway connects to PlanetScale Postgres which requires TLS. The code uses
`tokio-postgres-rustls` with Mozilla root CAs (`webpki-roots`). `tokio-postgres`
only supports `sslmode` values `disable`, `prefer`, `require` — it rejects
`verify-full` and `verify-ca` as "invalid connection string". The
`normalize_database_url()` function in `db_config.rs` rewrites those to
`sslmode=require` before connecting.
