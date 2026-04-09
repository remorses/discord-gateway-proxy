#!/usr/bin/env tsx
/**
 * Test script to verify discord.js can connect through the gateway-proxy on fly.io.
 *
 * Connects to wss://discord-gateway.kimaki.dev instead of the real Discord
 * gateway. Uses `ws.buildStrategy` to patch the gateway URL that discord.js
 * discovers from GET /gateway/bot — REST calls still go to real Discord.
 *
 * The proxy's deploy config has all cache flags disabled, so the synthetic READY
 * contains no guild data (guilds: 0 is expected). The key verification is that
 * the WebSocket handshake + IDENTIFY + READY flow completes successfully.
 *
 * Usage:
 *   doppler run --project website --config production -- npx tsx gateway-proxy/scripts/test-gateway-client.ts
 */
import { Client, GatewayIntentBits } from 'discord.js'
import { SimpleShardingStrategy } from '@discordjs/ws'

const PROXY_URL = 'wss://discord-gateway.kimaki.dev'
const DISCONNECT_AFTER_MS = 10_000

const token = process.env['DISCORD_BOT_TOKEN']
if (!token) {
  console.error('DISCORD_BOT_TOKEN not set. Run with doppler or set the env var.')
  process.exit(1)
}

console.log(`Connecting to gateway proxy at ${PROXY_URL} ...`)

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  ws: {
    // Patch fetchGatewayInformation so discord.js connects to the proxy
    // instead of the real Discord gateway. REST calls (GET /gateway/bot)
    // still go to Discord to get shard count etc., but the returned
    // gateway URL is replaced with the proxy URL before any shard spawns.
    buildStrategy: (manager) => {
      const originalFetch = manager.fetchGatewayInformation.bind(manager)
      manager.fetchGatewayInformation = async (force?: boolean) => {
        const info = await originalFetch(force)
        console.log(`Original gateway URL: ${info.url}`)
        console.log(`Redirecting to proxy:  ${PROXY_URL}`)
        return { ...info, url: PROXY_URL }
      }
      return new SimpleShardingStrategy(manager)
    },
  },
})

client.once('ready', (readyClient) => {
  console.log(``)
  console.log(`READY received!`)
  console.log(`  Bot user: ${readyClient.user.tag}`)
  console.log(`  Guilds:   ${readyClient.guilds.cache.size}`)
  readyClient.guilds.cache.forEach((guild) => {
    console.log(`    - ${guild.name} (${guild.id})`)
  })

  console.log(``)
  console.log(`Connection verified. Disconnecting in ${DISCONNECT_AFTER_MS / 1000}s ...`)
  setTimeout(() => {
    client.destroy()
    console.log(`Disconnected.`)
    process.exit(0)
  }, DISCONNECT_AFTER_MS)
})

client.on('error', (err) => {
  console.error('Client error:', err)
})

client.on('warn', (msg) => {
  console.warn('Client warn:', msg)
})

client.login(token).catch((err) => {
  console.error('Login failed:', err)
  process.exit(1)
})
