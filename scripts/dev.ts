#!/usr/bin/env tsx
/**
 * Local dev runner for gateway-proxy.
 * Builds CONFIG from env vars (typically loaded via `doppler run`) and starts `cargo run`.
 */

import { spawn } from 'node:child_process'
import { GatewayIntentBits } from 'discord.js'
import process from 'node:process'

const defaultPort = 7878

function readPort() {
    const rawPort = process.env.GATEWAY_PORT
    if (!rawPort) {
        return defaultPort
    }

    const parsedPort = Number(rawPort)
    if (!Number.isInteger(parsedPort) || parsedPort <= 0 || parsedPort > 65535) {
        throw new Error(`Invalid GATEWAY_PORT value: ${rawPort}`)
    }

    return parsedPort
}

async function run() {
    const token = process.env.DISCORD_BOT_TOKEN || process.env.TOKEN
    if (!token) {
        throw new Error(
            'Expected DISCORD_BOT_TOKEN or TOKEN in environment (run with doppler)',
        )
    }

    const port = readPort()
    const externallyAccessibleUrl =
        process.env.GATEWAY_EXTERNAL_URL || `ws://localhost:${port}`

    // Must match the intents used by the CLI in discord-bot.ts createDiscordClient()
    const intents =
        GatewayIntentBits.Guilds |
        GatewayIntentBits.GuildMessages |
        GatewayIntentBits.MessageContent |
        GatewayIntentBits.GuildVoiceStates

    const config = {
        log_level: process.env.GATEWAY_LOG_LEVEL || 'info',
        intents,
        port,
        externally_accessible_url: externallyAccessibleUrl,
        cache: {
            channels: true,
            roles: true,
            current_member: true,
            presences: false,
            emojis: false,
            members: false,
            scheduled_events: false,
            stage_instances: false,
            stickers: false,
            users: false,
            voice_states: true,
        },
        token,
    }

    const child = spawn('cargo', ['run'], {
        cwd: process.cwd(),
        stdio: 'inherit',
        env: {
            ...process.env,
            TOKEN: token,
            CONFIG: JSON.stringify(config),
        },
    })

    await new Promise<void>((resolve, reject) => {
        child.on('error', (error) => {
            reject(new Error('Failed to launch cargo run', { cause: error }))
        })

        child.on('exit', (code, signal) => {
            if (signal) {
                reject(new Error(`cargo run exited from signal: ${signal}`))
                return
            }

            if (!code || code === 0) {
                resolve()
                return
            }

            reject(new Error(`cargo run exited with code ${code}`))
        })
    })
}

run().catch((error: unknown) => {
    if (error instanceof Error) {
        console.error(error.message)
        process.exit(1)
    }

    console.error('Unexpected non-Error failure in scripts/dev.ts')
    process.exit(1)
})
