# Degen Ops Discord Partner Bot

## Goal

Give partners a simple private Discord chatbot for Degen Ops buy decisions, routing, sell-through, and partner updates.

The bot is partner-scope only and read-only. It does not move money, change inventory, update listings, send customer messages, or expose owner-only raw cash/loan details.

## Discord Setup

Create a new Discord application and bot identity. Do not reuse the existing deal-ingest bot.

Recommended name:

```text
Degen Ops Agent
```

Required bot permissions:

- View Channel
- Send Messages
- Read Message History
- Use Slash Commands is optional for a later phase

Required privileged intent:

- Message Content Intent

Invite the bot only to the existing private channel:

```text
#degen-ops-bot
```

Copy the channel ID with Discord Developer Mode enabled.

## Green Environment

Add these values to the approved Green environment source used by the bot process:

```bash
DEGEN_OPS_DISCORD_BOT_TOKEN=replace_with_new_partner_bot_token
DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS=replace_with_degen_ops_bot_channel_id
DEGEN_OPS_DISCORD_ALLOWED_USER_IDS=replace_with_comma_separated_partner_owner_user_ids
DEGEN_OPS_DISCORD_MODEL=aws/anthropic/claude-haiku-4-5-v1
DEGEN_OPS_DISCORD_AUDIT_LOG=/opt/degen/degen-ops-hermes/home/logs/degen_ops_discord_bot.jsonl
```

The bot also needs the existing Green values for:

- `DEGEN_OPS_READONLY_DATABASE_URL`
- `NVIDIA_API_KEY` or `OPENAI_API_KEY`
- normal app import secrets such as `SESSION_SECRET` and `ADMIN_PASSWORD`

## Local Dry Run

```bash
python scripts/degen_ops_discord_bot.py --dry-run-config
```

## Green Service Shape

Run as a separate service. Do not use the existing Hermes gateway service.

Command:

```bash
cd /opt/degen/app
/opt/degen/app/.venv/bin/python scripts/degen_ops_discord_bot.py
```

Recommended service name:

```text
degen-ops-discord-bot.service
```

## Behavior

The bot replies only when all are true:

- message is in an allowlisted channel
- author is not a bot
- author is in `DEGEN_OPS_DISCORD_ALLOWED_USER_IDS`, unless the explicit `DEGEN_OPS_DISCORD_ALLOW_ANY_USER_IN_CHANNEL=true` override is set
- prompt is below `DEGEN_OPS_DISCORD_MAX_PROMPT_CHARS`

The bot always uses partner scope.

## Rollback

Stop and disable only:

```text
degen-ops-discord-bot.service
```

Then remove or blank:

```text
DEGEN_OPS_DISCORD_BOT_TOKEN
```

No app database rollback is required.
