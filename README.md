# gateway-proxy

> This is a very hacky project, so it might stop working if Discord changes their API core. This is unlikely, but keep that in mind while using the proxy.

This is a proxy for Discord gateway connections - clients can connect to this proxy instead of the Discord Gateway and interact with it just like they would with the Discord Gateway.
It also exposes a Discord REST proxy at `/api/v10/*` using the same auth model as gateway connections.

The proxy connects to Discord instead of the client - allowing for zero-downtime client restarts while the proxy keeps its connections to the gateway open. The proxy won't invalidate your sessions or disconnect you (exceptions below).

## How?

It connects all shards to Discord upfront and mimics to be the actual API gateway.

When a client sends an `IDENTIFY` payload, it takes the shard ID specified and relays all events for that shard to the client.

It also sends you self-crafted, but valid `READY` and `GUILD_CREATE`/`GUILD_DELETE` payloads at startup to keep your guild state up to date, just like Discord does, even though it doesn't reconnect when you do internally.

Because the `IDENTIFY` is not actually controlled by the client side, activity data must be specified in the config file and will have no effect when sent in the client's `IDENTIFY` payload.

It uses a minimal algorithm to replace the sequence numbers in incoming payloads with fake sequence numbers that are valid for the clients, but does not need to parse the JSON for that.

## Configuration

Create a file `config.json` and fill in these fields as you wish:

```json
{
  "log_level": "info",
  "token": "",
  "intents": 32511,
  "port": 7878,
  "activity": {
    "type": 0,
    "name": "on shard {{shard}} with kubernetes"
  },
  "status": "idle",
  "backpressure": 100,
  "validate_token": true,
  "externally_accessible_url": "ws://localhost:7878",
  "cache": {
    "channels": false,
    "presences": false,
    "emojis": false,
    "current_member": false,
    "members": false,
    "roles": false,
    "scheduled_events": false,
    "stage_instances": false,
    "stickers": false,
    "users": false,
    "voice_states": false
  }
}
```

You can omit the `token` key entirely and set the `TOKEN` environment variable when running to avoid putting credentials in the configuration file. Client tokens will be validated to match the one configured unless `validate_token` is set to `false`.

By default, the total shard count will be calculated using the `/api/gateway/bot` endpoint. If you want to change this, set `shards` to the amount of shards. It will also launch all shards by default, you can customize this to launch only a range of shards using `shard_start` and `shard_end` (start inclusive, end exclusive).

If you're using twilight's HTTP-proxy, set `twilight_http_proxy` to the `ip:port` of the HTTP proxy. This redirects all Discord REST API calls (like `/api/gateway/bot`) through that host.

To override the gateway WebSocket URL that shards connect to, set `gateway_url`. This is separate from the HTTP proxy -- it controls where the actual WebSocket connections go. Useful for testing with a local fake Discord server:

```json
{
  "gateway_url": "ws://127.0.0.1:54321/gateway",
  "twilight_http_proxy": "127.0.0.1:54321"
}
```

If `gateway_url` is not set, shards connect to `wss://gateway.discord.gg` (the default).

Take special care when setting cache flags, only enable what you actually need. The proxy will tend to send more than Discord would, so double check what your bot depends on.

## Multi-tenant mode

The proxy supports routing events to multiple independent clients, each receiving only events for their authorized guilds. This enables horizontal scaling where bot instances on different machines each handle a subset of guilds while sharing a single gateway connection.

```
                         Discord Gateway API
                               |
                      [gateway-proxy :7878]
                       single bot token
                       all shards, all guilds
                      /         |         \
               machine-1    machine-2    machine-3
               "us:secret1"  "eu:secret2" bot_token
               guilds:       guilds:      all guilds
               [111, 222]    [333, 444]   (legacy)
```

Add a `clients` map to `config.json`. Each key is a client ID and the value contains a secret and a list of guild IDs that client is authorized to receive events for:

```json
{
  "token": "YOUR_BOT_TOKEN",
  "intents": 32511,
  "port": 7878,
  "externally_accessible_url": "ws://proxy.internal:7878",
  "cache": {
    "channels": true,
    "roles": true,
    "members": true
  },
  "clients": {
    "us-east": {
      "secret": "random-secret-us-east",
      "guilds": ["1111111111111111", "2222222222222222"]
    },
    "eu-west": {
      "secret": "random-secret-eu-west",
      "guilds": ["3333333333333333", "4444444444444444"]
    }
  }
}
```

Guild IDs can be strings or numbers in the config.

**Connecting as a multi-tenant client:** Instead of the bot token, send `client_id:client_secret` as the token in your IDENTIFY payload. For example, `us-east:random-secret-us-east`. The proxy authenticates the client and only forwards:

- READY payloads containing only the client's authorized guilds
- GUILD_CREATE/GUILD_DELETE events for authorized guilds only
- Dispatch events that have a `guild_id` matching the authorized set

Events without a `guild_id` (DMs, USER_UPDATE, etc.) are **not forwarded** to multi-tenant clients since they can't be attributed to a specific guild. `VOICE_SERVER_UPDATE` only goes to the guild's voice owner (see [Voice](#voice)).

All tenants share one bot session, so the proxy also filters what multi-tenant clients send upstream:

| Opcode | Handling |
| --- | --- |
| 1 heartbeat, 2 identify, 6 resume | handled by the proxy |
| 8 request guild members | forwarded only for an authorized guild, rebuilt from parsed fields |
| 4 voice state | same, plus the channel must be in that guild and the client must own the guild's voice (see [Voice](#voice)) |
| 3 presence, all others | dropped |

Ops 4 and 8 share a limit of 20 commands per 60s per client, and go out on the shard of their guild.

**Backward compatibility:** Clients connecting with the real bot token (or `Bot YOUR_TOKEN`) get all events for all guilds, same as before. The `clients` config is optional -- omitting it preserves the original single-client behavior.

## REST proxy mode (`/api/v10/*`)

The proxy forwards Discord REST requests to `https://discord.com/api/v10/*`.

Authentication is shared with gateway auth:

- `Authorization: Bot <real_bot_token>` → full access (legacy behavior)
- `Authorization: Bot <client_id:client_secret>` → multi-tenant client access

For multi-tenant client credentials, REST requests are guild-scoped:

- Routes with `guild_id` are allowed only when that guild is in the client's authorized guild set.
- Channel routes are resolved to a guild via the proxy cache and filtered the same way.
- Routes without a resolvable guild context are denied unless they are explicitly required for client operation (for example `/api/v10/gateway/bot`).

`GET /api/v10/gateway/bot` rewrites the returned `url` field to the proxy's configured external URL so clients auto-discover the gateway proxy.

## Dynamic client config (database)

For deployments where clients/guilds change at runtime (e.g. when users install the bot in new servers), set `DIRECT_DATABASE_URL` to a Postgres connection string (preferred), with `DATABASE_URL` as fallback. The proxy loads the full table once, then listens for row-level changes with `LISTEN/NOTIFY` and applies incremental updates. It keeps a low-frequency full reconcile as a safety net, and falls back to polling mode if `LISTEN/NOTIFY` is unavailable.

```bash
DIRECT_DATABASE_URL=postgres://user:pass@host:5432/db ./gateway-proxy
```

The table is created automatically on first connection. One row per client+guild pair:

**SQL schema:**

```sql
CREATE TABLE IF NOT EXISTS gateway_clients (
    client_id  TEXT NOT NULL,
    secret     TEXT NOT NULL,
    guild_id   TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (client_id, guild_id)
);
```

**Prisma schema:**

```prisma
model GatewayClient {
  clientId  String   @map("client_id")
  secret    String
  guildId   String   @map("guild_id")
  updatedAt DateTime @default(now()) @map("updated_at") @db.Timestamptz

  @@id([clientId, guildId])
  @@map("gateway_clients")
}
```

**Example rows:**

| client_id | secret | guild_id |
|-----------|--------|----------|
| us-east | random-secret-us-east | 1111111111111111 |
| us-east | random-secret-us-east | 2222222222222222 |
| eu-west | random-secret-eu-west | 3333333333333333 |

When `DIRECT_DATABASE_URL` (or `DATABASE_URL` fallback) is set, the database becomes the sole source of truth for clients after the first successful sync. Config.json `clients` are used as the initial seed until then. If neither env var is set, config.json clients are used as before.

The realtime path depends on Postgres session features (`LISTEN/NOTIFY`). With PlanetScale, use a direct Postgres connection on port `5432`. PgBouncer transaction pooling (`6432`) does not support `LISTEN/NOTIFY` semantics.

## Running

Compiling this from source isn't the most fun, you'll need a nightly Rust compiler with the rust-src component installed. Then run `cargo build --release --target=MY_RUSTC_TARGET`, where `MY_RUSTC_TARGET` is probably `x86_64-unknown-linux-gnu`.

Instead, I recommend running the Docker images that are prebuilt by CI.

The Docker images are tagged based on the CPU microarchitecture that they are built and tuned for, currently either `znver3` (Zen 3), `znver2` (Zen 2), `haswell`, `sandybridge` or `x86-64` (the only target with SIMD disabled, therefore the most compatible).

To run the image, mount the config file at `/config.json`, for example:

```bash
docker run --rm -it -v /path/to/my/config.json:/config.json docker.io/gelbpunkt/gateway-proxy:haswell
```

## Connecting

Connecting is fairly simple, just hardcode the gateway URL in your client to `ws://localhost:7878`. Make sure not to ratelimit your connections on your end.

If you have not configured a shard count manually, you can check the amount of shards you need to create on your client by requesting `http://localhost:7878/shard-count`. The endpoint returns the number of shards running as plaintext.

**Important:** The proxy detects `zlib-stream` query parameters and `compress` fields in your `IDENTIFY` payloads and will encode packets if they are enabled, just like Discord. This comes with CPU overhead and is likely not desired in localhost networking. Make sure to disable this if so.

## Metrics

The proxy exposes Prometheus metrics at the `/metrics` endpoint. They contain event counters, cache size and shard latency histograms specific to each shard.

## Voice

The proxy carries only voice signaling. Audio goes from the client straight to Discord's voice server over UDP.

```
client ──op 4 join──▶ proxy (guild authorized? channel in guild? voice owner?) ──▶ Discord
Discord ──VOICE_STATE_UPDATE (bot)──▶ proxy ──▶ every client in the guild (shows the bot is busy)
Discord ──VOICE_SERVER_UPDATE (token)──▶ proxy ──▶ only the voice owner
owner ══ voice WebSocket + UDP ══▶ Discord voice server
```

Discord allows one voice connection per bot per guild, so each guild has one **voice owner**: the first client that joins. Joins from other clients are dropped until the owner is gone. A leave starts when:

- the owner sends a leave, or the bot is kicked from the channel
- the owner does a fresh IDENTIFY on that shard (its process restarted, so it has no voice connection; without the leave its next join to the same channel would get no events from Discord)
- the owner stays disconnected for 60s, or loses access to the guild

The guild stays locked until Discord confirms the leave with the bot's null `VOICE_STATE_UPDATE` (or 5s pass), so a late event of the old call never reaches or frees the next owner. The owner itself can join again at any time. Voice commands check the client's current guilds, not the ones from IDENTIFY.

The proxy never caches voice tokens, so every join is a real one. Enable the `voice_states` cache so `GUILD_CREATE` shows where the bot is.

`tests/voice.e2e.test.ts` runs the real proxy against [discord-digital-twin](../discord-digital-twin) with voice enabled: three clients, a TTS clip played with `@discordjs/voice`, and checks the owner lock, token routing and the received opus frames. It writes the received audio to `tmp/voice/received.wav`.

```bash
pnpm exec vitest run tests/voice.e2e.test.ts
```

## Caveats

A deploy restarts the upstream shard, which ends all voice calls.

## Performance

In theory, the proxy is very fast for the reasons mentioned above. In practice, this shows. There is almost zero overhead in latency.

Using 225 shards, with almost full caching (members, guilds, channels, roles, voice states) the proxy uses 11.7GB of memory and sits around 2% CPU usage over all 4c/8t of my machine. This again shows that the processing overhead is negligible, the only thing you can and should optimize on is the cache configuration.

## Kimaki onboarding flow

When used with [kimaki](https://kimaki.dev), the proxy enables a zero-config onboarding experience where users install a shared Discord bot without creating their own.

```
User's terminal                          Browser       Website (CF Worker)  Postgres    Gateway Proxy
───────────────                          ───────       ───────────────────  ────────    ─────────────
       │                                    │                   │               │             │
 1.    │ npx kimaki                         │                   │               │             │
       │                                    │                   │               │             │
 2.    │ generate clientId (UUID)           │                   │               │             │
       │   + clientSecret (hex)             │                   │               │             │
       │                                    │                   │               │             │
 3.    │ build Discord OAuth URL:           │                   │               │             │
       │   client_id = SHARED_APP_ID        │                   │               │             │
       │   state = {clientId, secret}       │                   │               │             │
       │   redirect_uri = /oauth/cb         │                   │               │             │
       │                                    │                   │               │             │
 4.    ├────────── open browser ───────────▶│                   │               │             │
       │                      discord.com/oauth2/authorize      │               │             │
       │                       user picks guild, clicks OK      │               │             │
       │                                    │                   │               │             │
 5.    │                                    ├ redirect + state ▶│               │             │
       │                                    │                   │               │             │
 6.    │                                    │                   ├─── upsert ───▶│             │
       │                                    │                   │ gateway_clients row         │
       │                                    │                   │ (client_id, secret, guild_id)
       │                                    │                   │               │             │
 7.    │                                    │◀ close this tab ──┤               │             │
       │                                    │                   │               │             │
 8.    │ poll /api/onboarding/status        │                   │               │             │
       │   every 2s (clientId+secret)       │                   │               │             │
       │                                    │                   │               │             │
 9.    │                                    │                   ├── findFirst ─▶│             │
       │◀─────────────────── { guild_id } ──────────────────────┤               │             │
       │                                    │                   │               │             │
10.    │ store creds in local SQLite        │                   │               │             │
       │   bot_mode = built-in              │                   │               │             │
       │                                    │                   │               │             │
11.    │ connect to gateway proxy           │                   │               │             │
       ├────────────────────────── IDENTIFY clientId:clientSecret ───────────────────────────▶│
       │                                    │                   │               LISTEN/NOTIFY▶│
       │                                    │                   │               │             │
12.    │◀──────────────────────────── READY (filtered to guild) ──────────────────────────────┤
       │                                    │                   │               │             │
13.    │ bot is live, events for guild only │                   │               │             │
       │                                    │                   │               │             │
```

**Step by step:**

1. User runs `npx kimaki` on their machine
2. CLI generates a unique `clientId` (UUID v4) and `clientSecret` (32-byte random hex)
3. CLI builds a Discord OAuth URL with the shared Kimaki bot's `client_id`, a `state` param containing the generated credentials as JSON, and `redirect_uri` pointing to the website
4. CLI opens the browser to the Discord authorize page
5. User picks a guild and authorizes — Discord redirects to `website/src/routes/oauth-callback.tsx` with `guild_id` and `state`
6. Website parses the state, upserts a `gateway_clients` row in Postgres with `(client_id, secret, guild_id)`
7. Browser shows a success page
8. CLI polls `GET /api/onboarding/status?client_id=...&secret=...` every 2 seconds
9. Website finds the row and returns `{ guild_id }`
10. CLI stores credentials in local SQLite (`bot_mode = "built-in"`)
11. Bot connects to the gateway proxy using `clientId:clientSecret` as the token
12. Proxy authenticates against its in-memory client map (kept in sync from DB notifications), sends a filtered READY containing only the authorized guild
13. Bot is live — all gateway events and REST requests are scoped to that guild

## Multiple users in the same guild

Multiple users can install the bot to the same guild independently. Each user gets their own `client_id` and `client_secret`, creating separate rows in `gateway_clients`:

| client_id | secret | guild_id |
|-----------|--------|----------|
| `aaa-111` | `secret_a` | `999888777` |
| `bbb-222` | `secret_b` | `999888777` |

The composite primary key `(client_id, guild_id)` ensures rows never collide across users. The proxy groups rows by `client_id` when building the client map, so each user authenticates independently and receives their own event stream — both filtered to the same guild.

Discord only has one bot installation per guild (the shared Kimaki bot), but the proxy multiplexes events to all clients authorized for that guild.

## Same user on multiple machines (same guild)

The same Discord user can onboard multiple machines to the same guild.

Important distinction:

- **Bot installation in Discord**: one shared Kimaki bot member per guild
- **Kimaki machine authorization**: one `client_id:secret` per machine (OAuth state)

That means machine A and machine B can both connect to the same guild without creating a second bot member.

How it works when the bot is already installed:

1. User runs onboarding on machine B, which generates a new `client_id` and `secret`
2. User opens the OAuth URL and authorizes the app for the same guild
3. Discord still redirects to the callback in authorization-code flow (returns `code`)
4. Website exchanges the code, verifies `guild_id`, and upserts `(client_id, guild_id)`
5. Machine B polls onboarding status and starts using `client_id:secret`

So there is still only one shared bot in the guild, but there can be many authorized client identities (one per machine) in `gateway_clients`.
