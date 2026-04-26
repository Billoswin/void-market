# Void Market (self-hosted)

Void Market is an EVE Online market intelligence tool built with FastAPI + SQLite.

## Quick start (local dev on Windows)

From `void-market/`:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

copy .env.example .env
# edit .env with your values

uvicorn app.main:app --host 0.0.0.0 --port 8585
```

Open `http://localhost:8585/`.

## Deploy on Unraid (recommended: Docker)

### Option A: docker-compose (Unraid terminal)

1. Copy this project to your Unraid appdata, for example:
   - `/mnt/user/appdata/void-market/`
2. Create `.env` from the example:
   - `cp .env.example .env`
   - edit `.env`
3. Start:

```bash
docker compose up -d --build
```

App will be on `http://<unraid-ip>:8585/`.

### Persistent data

`docker-compose.yml` mounts `./data` into the container at `/app/data`.
That folder holds:
- `void_market.db`
- `killmail.db`
- uploaded SDE files (under `data/sde/`)

## EVE SSO setup (required for ESI features)

1. In the CCP Developer Portal, create an application (Authentication & API Access) and enable the scopes you need.
2. **Register the callback URL exactly** as your `VM_ESI_CALLBACK_URL`.

### LAN-only callback (what you selected)

Set:
- `VM_ESI_CALLBACK_URL=http://<unraid-ip>:8585/auth/callback`

Then register that same URL as the callback in the CCP app.

### If CCP rejects the LAN http callback

Some OAuth providers reject non-HTTPS callbacks (except localhost). If you can’t register or use an `http://<lan-ip>` callback, put Void Market behind a reverse proxy with TLS (Nginx Proxy Manager / SWAG) and use:
- `VM_ESI_CALLBACK_URL=https://market.yourdomain.com/auth/callback`

## First-run checklist

- Visit `/api/status` to confirm the app is running and DB initialized.
- Click login (or browse to `/auth/login`) to authenticate a character.
- Load SDE data:
  - Use the UI or POST to `/api/sde/upload` with a `.zip` containing the JSONL SDE exports this app expects.
  - Confirm via `/api/sde/status`.
- Set IDs in `.env`:
  - `VM_ALLIANCE_ID`
  - `VM_KEEPSTAR_STRUCTURE_ID`

## Useful endpoints

- `/` UI
- `/api/status` overall health/config/auth status
- `/auth/login` start SSO flow
- `/auth/callback` SSO callback (must match `VM_ESI_CALLBACK_URL`)
- `/api/sde/status` SDE load status
- `/api/sde/upload` upload SDE zip/jsonl

