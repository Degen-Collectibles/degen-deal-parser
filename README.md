# Degen Deal Parser

Internal Discord deal and ledger parser for Degen Collectibles.

This app:
- ingests watched Discord deal-log channels
- stores raw messages and attachments
- parses buys, sales, trades, and expenses
- provides review, reporting, and bookkeeping reconciliation pages
- supports a partner-facing `/deals` view and employee-friendly `/review` flow

## Local Run

From the project root:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Then open:
- `http://127.0.0.1:8000/login`

## Important Env Vars

Core:
- `DISCORD_BOT_TOKEN`
- `OPENAI_API_KEY`
- `DATABASE_URL`
- `SESSION_SECRET`

Auth:
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `REVIEWER_USERNAME`
- `REVIEWER_PASSWORD`

Session / deploy:
- `PUBLIC_BASE_URL`
- `SESSION_COOKIE_NAME`
- `SESSION_HTTPS_ONLY`
- `SESSION_SAME_SITE`
- `SESSION_DOMAIN`

Worker controls:
- `DISCORD_INGEST_ENABLED`
- `PARSER_WORKER_ENABLED`

## Render Deployment

Recommended public URL:
- `ops.degencollectibles.com`

Recommended first deployment:
- host the FastAPI app separately from Shopify
- keep `degencollectibles.com` on Shopify
- point a subdomain like `ops.degencollectibles.com` to Render
- this repo now includes [render.yaml](/C:/Users/jeffr/discord-deal-parser/live-deal-parser/render.yaml) as a starting blueprint

### Render service settings

Build command:

```bash
pip install -r requirements.txt
```

Start command:

```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 10000
```

If you use the included Render blueprint:
- it mounts a persistent disk at `/var/data`
- it sets `DATABASE_URL=sqlite:////var/data/degen_live.db`
- you still need to fill in the secret env vars in Render

### Recommended production env vars

```text
PUBLIC_BASE_URL=https://ops.degencollectibles.com
SESSION_SECRET=<strong random secret>
SESSION_COOKIE_NAME=degen_session
SESSION_HTTPS_ONLY=true
SESSION_SAME_SITE=lax
SESSION_DOMAIN=ops.degencollectibles.com
```

If you use SQLite initially:
- set `DATABASE_URL` to a writable persistent disk path on the host
- do not rely on ephemeral container storage if you care about preserving data

Better long-term option:
- move to Postgres before broader partner/employee use

### Shared Postgres setup

If you want:
- Discord ingestion running on your local machine
- the employee/partner web UI running on Render
- both sharing the same data

then use one shared Postgres database for both.

This app now supports either of these `DATABASE_URL` forms:

```text
postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME
postgres://USER:PASSWORD@HOST:5432/DBNAME
```

The app normalizes `postgres://...` to the SQLAlchemy `psycopg` driver automatically.

Recommended architecture:
- local machine:
  - `DISCORD_INGEST_ENABLED=true`
  - `PARSER_WORKER_ENABLED=true`
- Render web service:
  - `DISCORD_INGEST_ENABLED=false`
  - `PARSER_WORKER_ENABLED=false`

That keeps Discord/API rate-limit-sensitive work local while your hosted site stays available for employees and partners.

### Discord bot and parser worker

This app starts:
- the FastAPI web server
- the Discord ingestion bot
- the parser worker

So only run one deployed web instance unless you intentionally separate those responsibilities later.

## Shopify Domain Setup

Recommended approach:
- keep Shopify storefront on `degencollectibles.com`
- deploy this app on `ops.degencollectibles.com`

Do not use Shopify app proxy for this full internal tool unless you specifically want a storefront-coupled experience and are okay with proxy/auth constraints.

## Production Notes

- Rotate any existing Discord/OpenAI secrets before public deployment.
- Change default admin credentials before sharing the app.
- SQLite is okay for light internal use, but Postgres is the better next step.
- The app stores uploaded/imported bookkeeping state in the database, so persistent storage matters.
- If you split local ingestion from hosted UI, do not use two separate SQLite databases.
