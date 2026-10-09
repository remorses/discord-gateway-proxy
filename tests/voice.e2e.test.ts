// Voice through the real Rust proxy. The digital Discord twin plays Discord
// (gateway, REST, voice WebSocket and UDP); the proxy runs as a child process
// with three multi-tenant clients; discord.js + @discordjs/voice play a TTS
// clip. Checks the owner lock, that only the owner gets the voice token, and
// that the audio reaches the voice server byte for byte. The received audio
// is decoded to tmp/voice/received.wav (gitignored) so you can listen to it.

import { execFile, spawn, type ChildProcess } from 'node:child_process'
import fs from 'node:fs'
import net from 'node:net'
import path from 'node:path'
import { promisify } from 'node:util'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { ChannelType, Client, GatewayIntentBits, type Guild } from 'discord.js'
import {
  StreamType,
  VoiceConnectionStatus,
  createAudioPlayer,
  createAudioResource,
  entersState,
  joinVoiceChannel,
  type VoiceConnection,
} from '@discordjs/voice'
import OpusScript from 'opusscript'
import prism from 'prism-media'
import { DigitalDiscord } from 'discord-digital-twin/src'

const execFileAsync = promisify(execFile)

const PROXY_DIR = path.resolve(import.meta.dirname, '..')
const OUT_DIR = path.join(PROXY_DIR, 'tmp', 'voice')
const FIXTURE = path.resolve(PROXY_DIR, '../discord-digital-twin/tests/fixtures/voice-hello.ogg')
const SILENCE_FRAME = Buffer.from([0xf8, 0xff, 0xfe])

const GUILD = '500000000000000001'
const OTHER_GUILD = '500000000000000002'
const VOICE_1 = '500000000000000011'
const VOICE_2 = '500000000000000012'
const OTHER_VOICE = '500000000000000013'
const ALPHA = 'alpha:secret-a'
const BETA = 'beta:secret-b'
const GAMMA = 'gamma:secret-c'

let discord: DigitalDiscord
let proxy: ChildProcess
let proxyPort: number
const clients: Client[] = []
const connections: VoiceConnection[] = []

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      server.close(() => resolve(typeof address === 'object' && address ? address.port : 0))
    })
  })
}

// @discordjs/voice keeps one connection per (group, guild) in this process.
// Each client stands for a different kimaki machine, so each gets its own group.
const groups = new WeakMap<Client, string>()

async function connect(token: string): Promise<{ client: Client; rawEvents: string[] }> {
  const client = new Client({
    intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
    rest: { api: `http://127.0.0.1:${proxyPort}/api`, version: '10' },
  })
  const rawEvents: string[] = []
  client.on('raw', (packet: { t?: string | null }) => {
    if (packet.t) rawEvents.push(packet.t)
  })
  const ready = new Promise<void>((resolve) => client.once('clientReady', () => resolve()))
  await client.login(token)
  await ready
  clients.push(client)
  groups.set(client, `${token}#${clients.length}`)
  return { client, rawEvents }
}

function guildOf(client: Client, guildId: string): Guild {
  const guild = client.guilds.cache.get(guildId)
  if (!guild) throw new Error(`guild ${guildId} not in cache`)
  return guild
}

function join({ client, guildId, channelId }: { client: Client; guildId: string; channelId: string }): VoiceConnection {
  const connection = joinVoiceChannel({
    guildId,
    channelId,
    adapterCreator: guildOf(client, guildId).voiceAdapterCreator,
    selfDeaf: false,
    daveEncryption: false,
    group: groups.get(client),
  })
  connections.push(connection)
  return connection
}

// The bot's voice channel as the proxy's client sees it.
function botChannel(client: Client): string | null {
  return guildOf(client, GUILD).members.me?.voice.channelId ?? null
}

async function readOpusPackets(file: string): Promise<Buffer[]> {
  const packets: Buffer[] = []
  for await (const packet of fs.createReadStream(file).pipe(new prism.opus.OggDemuxer())) packets.push(packet)
  return packets
}

function writeWav(file: string, opusPackets: Buffer[]): void {
  const decoder = new OpusScript(48_000, 2, OpusScript.Application.AUDIO)
  const pcm = Buffer.concat(opusPackets.map((packet) => Buffer.from(decoder.decode(packet))))
  decoder.delete()
  const header = Buffer.alloc(44)
  header.write('RIFF', 0)
  header.writeUInt32LE(36 + pcm.length, 4)
  header.write('WAVEfmt ', 8)
  header.writeUInt32LE(16, 16)
  header.writeUInt16LE(1, 20)
  header.writeUInt16LE(2, 22)
  header.writeUInt32LE(48_000, 24)
  header.writeUInt32LE(48_000 * 4, 28)
  header.writeUInt16LE(4, 32)
  header.writeUInt16LE(16, 34)
  header.write('data', 36)
  header.writeUInt32LE(pcm.length, 40)
  fs.writeFileSync(file, Buffer.concat([header, pcm]))
}

// Asserts that a connection never becomes Ready (the proxy dropped its join).
async function expectNeverReady(connection: VoiceConnection): Promise<void> {
  for (let i = 0; i < 10; i++) {
    expect(connection.state.status).not.toBe(VoiceConnectionStatus.Ready)
    await new Promise((resolve) => setTimeout(resolve, 30))
  }
}

beforeAll(async () => {
  fs.mkdirSync(OUT_DIR, { recursive: true })
  const voiceChannel = (id: string, name: string) => ({ id, name, type: ChannelType.GuildVoice })
  discord = new DigitalDiscord({
    voice: true,
    guilds: [
      { id: GUILD, name: 'Shared', channels: [voiceChannel(VOICE_1, 'one'), voiceChannel(VOICE_2, 'two')] },
      { id: OTHER_GUILD, name: 'Other', channels: [voiceChannel(OTHER_VOICE, 'other')] },
    ],
  })
  await Promise.all([
    discord.start(),
    execFileAsync('cargo', ['build'], { cwd: PROXY_DIR, timeout: 600_000 }),
  ])
  discord.trustVoiceCertificate()

  proxyPort = await freePort()
  const config = {
    log_level: 'info',
    token: discord.botToken,
    intents:
      GatewayIntentBits.Guilds |
      GatewayIntentBits.GuildMessages |
      GatewayIntentBits.MessageContent |
      GatewayIntentBits.GuildVoiceStates,
    port: proxyPort,
    shards: 1,
    validate_token: true,
    externally_accessible_url: `ws://127.0.0.1:${proxyPort}`,
    twilight_http_proxy: `127.0.0.1:${discord.port}`,
    gateway_url: `ws://127.0.0.1:${discord.port}/gateway`,
    cache: {
      channels: true,
      roles: true,
      current_member: true,
      voice_states: true,
      presences: false,
      emojis: false,
      members: false,
      scheduled_events: false,
      stage_instances: false,
      stickers: false,
      users: false,
    },
    clients: {
      alpha: { secret: 'secret-a', guilds: [GUILD] },
      beta: { secret: 'secret-b', guilds: [GUILD] },
      gamma: { secret: 'secret-c', guilds: [OTHER_GUILD] },
    },
  }
  const { DATABASE_URL: _database, DIRECT_DATABASE_URL: _direct, ...env } = process.env
  const log = fs.openSync(path.join(OUT_DIR, 'gateway-proxy.log'), 'w')
  proxy = spawn(path.join(PROXY_DIR, 'target/debug/gateway-proxy'), {
    cwd: OUT_DIR,
    env: { ...env, CONFIG: JSON.stringify(config) },
    stdio: ['ignore', log, log],
  })
  for (let i = 0; ; i++) {
    const status = await fetch(`http://127.0.0.1:${proxyPort}/shard-count`)
      .then((response) => response.status)
      .catch(() => 0)
    if (status === 200) break
    if (i > 200) throw new Error('gateway-proxy did not start, see tmp/voice/gateway-proxy.log')
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
}, 600_000)

afterAll(async () => {
  // Destroyed first, so no voice connection tries to reconnect to the stopped twin.
  for (const connection of connections) {
    if (connection.state.status !== VoiceConnectionStatus.Destroyed) connection.destroy()
  }
  for (const client of clients) await client.destroy()
  proxy?.kill()
  await discord?.stop()
})

test('only the owner gets the voice token, and its audio reaches Discord', async () => {
  const alpha = await connect(ALPHA)
  const beta = await connect(BETA)

  const connection = join({ client: alpha.client, guildId: GUILD, channelId: VOICE_1 })
  await entersState(connection, VoiceConnectionStatus.Ready, 8_000)

  const player = createAudioPlayer()
  connection.subscribe(player)
  player.play(createAudioResource(fs.createReadStream(FIXTURE), { inputType: StreamType.OggOpus }))
  const stream = await discord.waitForVoiceStream({ channelId: VOICE_1, userId: discord.botUserId })
  writeWav(path.join(OUT_DIR, 'received.wav'), stream.opusPackets)

  const expected = await readOpusPackets(FIXTURE)
  expect(stream.opusPackets.slice(0, expected.length)).toEqual(expected)
  expect(stream.opusPackets.slice(expected.length).every((packet) => packet.equals(SILENCE_FRAME))).toBe(true)

  // beta sees that the bot is busy in voice, but never the token.
  await expect.poll(() => botChannel(beta.client), { timeout: 4_000, interval: 50 }).toBe(VOICE_1)
  expect(alpha.rawEvents.filter((event) => event.startsWith('VOICE_'))).toEqual([
    'VOICE_STATE_UPDATE',
    'VOICE_SERVER_UPDATE',
  ])
  expect(beta.rawEvents.filter((event) => event.startsWith('VOICE_'))).toEqual(['VOICE_STATE_UPDATE'])

  // beta cannot move the bot while alpha owns the call.
  const stolen = join({ client: beta.client, guildId: GUILD, channelId: VOICE_2 })
  await expectNeverReady(stolen)
  stolen.destroy()
  // Nor through REST (Modify Guild Member on the bot moves it with the bot token).
  const restMove = await fetch(`http://127.0.0.1:${proxyPort}/api/v10/guilds/${GUILD}/members/${discord.botUserId}`, {
    method: 'PATCH',
    headers: { authorization: `Bot ${BETA}`, 'content-type': 'application/json' },
    body: JSON.stringify({ channel_id: null }),
  })
  expect(restMove.status).toBe(403)
  // gamma is not authorized for this guild: its raw op 4 is dropped too.
  const gamma = await connect(GAMMA)
  await guildOf(gamma.client, OTHER_GUILD).shard.send({
    op: 4,
    d: { guild_id: GUILD, channel_id: VOICE_2, self_mute: false, self_deaf: false },
  })
  for (let i = 0; i < 10; i++) {
    expect(botChannel(alpha.client)).toBe(VOICE_1)
    expect(connection.state.status).toBe(VoiceConnectionStatus.Ready)
    await new Promise((resolve) => setTimeout(resolve, 30))
  }

  // After alpha leaves, the guild is free for beta.
  connection.destroy()
  await expect.poll(() => botChannel(beta.client), { timeout: 4_000, interval: 50 }).toBe(null)
  const handoff = join({ client: beta.client, guildId: GUILD, channelId: VOICE_2 })
  await entersState(handoff, VoiceConnectionStatus.Ready, 8_000)
  handoff.destroy()
  await expect.poll(() => botChannel(beta.client), { timeout: 4_000, interval: 50 }).toBe(null)
}, 30_000)

test('a fresh IDENTIFY ends the old call so the same join works again', async () => {
  const first = await connect(ALPHA)
  await entersState(join({ client: first.client, guildId: GUILD, channelId: VOICE_1 }), VoiceConnectionStatus.Ready, 8_000)

  // The kimaki process restarts: no leave is sent, the bot stays in VOICE_1.
  await first.client.destroy()
  const second = await connect(ALPHA)
  // Discord sends no events for a join to the channel the bot is already in.
  // The proxy made the bot leave on IDENTIFY, so this join is a real one.
  await entersState(join({ client: second.client, guildId: GUILD, channelId: VOICE_1 }), VoiceConnectionStatus.Ready, 8_000)
  expect(botChannel(second.client)).toBe(VOICE_1)
}, 30_000)
