"""
Void Market — Debug Routes

Temporary diagnostics for ESI auth troubleshooting.
"""
import logging
import base64
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.session import get_db

logger = logging.getLogger("void_market.debug")

debug_router = APIRouter(prefix="/debug", tags=["debug"])


@debug_router.get("/config")
async def debug_config():
    """Show current ESI configuration (redacted secret)."""
    secret = settings.esi_client_secret
    return {
        "client_id": settings.esi_client_id,
        "client_id_length": len(settings.esi_client_id),
        "secret_first_8": secret[:8] if secret else "EMPTY",
        "secret_last_4": secret[-4:] if secret else "EMPTY",
        "secret_length": len(secret),
        "callback_url": settings.esi_callback_url,
        "scopes": settings.esi_scopes,
        "scopes_count": len(settings.esi_scopes.split(" ")),
        "alliance_id": settings.alliance_id,
        "keepstar_id": settings.keepstar_structure_id,
    }


@debug_router.get("/auth-url")
async def debug_auth_url():
    """Show the exact authorize URL that would be generated."""
    from app.services.esi_auth import esi_auth
    url = esi_auth.get_auth_url()
    return {
        "url": url,
        "url_length": len(url),
    }


@debug_router.get("/test-token-exchange")
async def debug_test_exchange():
    """Test token exchange with a fake code to see what CCP returns.
    
    A fake code should return 400 with an error message.
    If it returns 401, credentials are wrong.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        # Method 1: POST body params (what we're using now)
        resp1 = await client.post(
            "https://login.eveonline.com/v2/oauth/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "login.eveonline.com",
            },
            data={
                "grant_type": "authorization_code",
                "code": "FAKE_TEST_CODE",
                "client_id": settings.esi_client_id,
                "client_secret": settings.esi_client_secret,
            },
        )

        # Method 2: Basic Auth header
        credentials = f"{settings.esi_client_id}:{settings.esi_client_secret}"
        basic_b64 = base64.b64encode(credentials.encode()).decode()
        
        resp2 = await client.post(
            "https://login.eveonline.com/v2/oauth/token",
            headers={
                "Authorization": f"Basic {basic_b64}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "login.eveonline.com",
            },
            data={
                "grant_type": "authorization_code",
                "code": "FAKE_TEST_CODE",
            },
        )

    return {
        "method_1_post_body": {
            "status": resp1.status_code,
            "response": resp1.text[:1000] if "html" not in resp1.headers.get("content-type", "") else f"HTML page (status {resp1.status_code})",
        },
        "method_2_basic_auth": {
            "status": resp2.status_code,
            "response": resp2.text[:1000] if "html" not in resp2.headers.get("content-type", "") else f"HTML page (status {resp2.status_code})",
        },
        "interpretation": {
            "400": "Credentials OK, code was bad (expected for fake code)",
            "401": "Credentials REJECTED by CCP",
            "403": "Forbidden - IP or account issue",
        }
    }


@debug_router.get("/callback-test", response_class=HTMLResponse)
async def debug_callback_page(request: Request):
    """Manual auth flow with full debug output."""
    from app.services.esi_auth import esi_auth
    auth_url = esi_auth.get_auth_url()
    
    return f"""
    <html><head><title>Void Market Debug</title>
    <style>
        body {{ background: #0a0e17; color: #b8c4d4; font-family: monospace; padding: 20px; }}
        a {{ color: #f0a830; }}
        pre {{ background: #111a25; padding: 15px; border: 1px solid #243045; overflow-x: auto; white-space: pre-wrap; }}
        h2 {{ color: #f0a830; }}
        .ok {{ color: #30d060; }}
        .err {{ color: #e04040; }}
    </style></head>
    <body>
    <h2>Void Market — Auth Debug</h2>
    
    <h3>Step 1: Config</h3>
    <pre>
Client ID: {settings.esi_client_id}
Secret: {settings.esi_client_secret[:8]}...{settings.esi_client_secret[-4:]} (len={len(settings.esi_client_secret)})
Callback: {settings.esi_callback_url}
Scopes: {len(settings.esi_scopes.split(' '))} scopes
    </pre>
    
    <h3>Step 2: Authorize URL</h3>
    <pre>{auth_url}</pre>
    <p><a href="{auth_url}">Click here to start auth flow</a></p>
    
    <h3>Step 3: After CCP redirects back</h3>
    <p>The callback will hit <code>/auth/callback</code>. Check the logs:</p>
    <pre>journalctl -u void-market --no-pager -n 50</pre>
    
    <h3>Quick Tests</h3>
    <ul>
        <li><a href="/debug/config">View full config</a></li>
        <li><a href="/debug/test-token-exchange">Test token exchange (fake code)</a></li>
    </ul>
    </body></html>
    """


@debug_router.get("/manual-exchange")
async def debug_manual_exchange(code: str):
    """Manually exchange an auth code with full debug output.
    
    Usage: /debug/manual-exchange?code=<paste_code_here>
    
    Copy the code from the callback URL after CCP redirects you.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        post_data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": settings.esi_client_id,
            "client_secret": settings.esi_client_secret,
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "login.eveonline.com",
        }

        logger.info(f"Manual exchange: code={code[:20]}...")
        logger.info(f"POST data keys: {list(post_data.keys())}")
        logger.info(f"client_id: {settings.esi_client_id}")
        logger.info(f"client_secret length: {len(settings.esi_client_secret)}")

        resp = await client.post(
            "https://login.eveonline.com/v2/oauth/token",
            headers=headers,
            data=post_data,
        )

        content_type = resp.headers.get("content-type", "")
        is_json = "json" in content_type
        
        result = {
            "status_code": resp.status_code,
            "content_type": content_type,
            "headers": dict(resp.headers),
        }

        if is_json:
            result["response_json"] = resp.json()
            if resp.status_code == 200:
                # Try to decode the JWT
                import jwt as pyjwt
                token = resp.json().get("access_token", "")
                try:
                    decoded = pyjwt.decode(token, options={"verify_signature": False}, algorithms=["RS256"])
                    result["jwt_decoded"] = {
                        "sub": decoded.get("sub"),
                        "name": decoded.get("name"),
                        "scp": decoded.get("scp"),
                        "exp": decoded.get("exp"),
                    }
                except Exception as e:
                    result["jwt_error"] = str(e)
        else:
            # HTML response — extract useful bits
            body = resp.text
            if "error" in body.lower():
                # Try to find error message
                import re
                error_match = re.search(r'Error code: <strong>(.*?)</strong>', body)
                if error_match:
                    result["error_code"] = error_match.group(1)
                result["response_snippet"] = body[:500]
            else:
                result["response_snippet"] = body[:500]

        result["interpretation"] = {
            200: "SUCCESS — token exchange worked",
            400: "Bad request — code may be expired/used, or redirect_uri mismatch",
            401: "Unauthorized — credentials rejected",
            403: "Forbidden",
        }.get(resp.status_code, f"Unexpected status {resp.status_code}")

        return result


# ─── Full Diagnostic Endpoints ──────────────────────────────

@debug_router.post("/sql")
async def debug_sql(request: Request):
    """Run arbitrary SQL against killmail.db or market DB.
    Body: {"db": "killmail"|"market", "sql": "SELECT ...", "params": []}
    """
    import sqlite3
    import json

    body = await request.json()
    db_name = body.get("db", "killmail")
    sql = body.get("sql", "")
    params = body.get("params", [])

    if not sql.strip():
        return {"error": "No SQL provided"}

    db_path = {
        "killmail": "/opt/void-market/data/killmail.db",
        "market": "/opt/void-market/data/void_market.db",
    }.get(db_name)

    if not db_path:
        return {"error": f"Unknown db: {db_name}. Use 'killmail' or 'market'"}

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(sql, params)

        if sql.strip().upper().startswith("SELECT") or sql.strip().upper().startswith("WITH") or sql.strip().upper().startswith("PRAGMA"):
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description] if cursor.description else []
            data = [dict(r) for r in rows]
            conn.close()
            return {"columns": columns, "rows": data, "count": len(data)}
        else:
            conn.commit()
            affected = cursor.rowcount
            conn.close()
            return {"affected": affected, "status": "ok"}
    except Exception as e:
        return {"error": str(e)}


@debug_router.post("/bash")
async def debug_bash(request: Request):
    """Run a bash command and return output.
    Body: {"cmd": "journalctl -u void-market --since '5 min ago'"}
    """
    import subprocess

    body = await request.json()
    cmd = body.get("cmd", "")

    if not cmd.strip():
        return {"error": "No command provided"}

    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=30
        )
        return {
            "stdout": result.stdout[-10000:] if result.stdout else "",
            "stderr": result.stderr[-5000:] if result.stderr else "",
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"error": "Command timed out (30s limit)"}
    except Exception as e:
        return {"error": str(e)}


@debug_router.get("/stats")
async def debug_stats():
    """Quick overview of both databases."""
    import sqlite3

    stats = {}

    # Killmail DB
    try:
        db = sqlite3.connect("/opt/void-market/data/killmail.db")
        stats["killmail"] = {
            "total": db.execute("SELECT COUNT(*) FROM killmails").fetchone()[0],
            "losses": db.execute("SELECT COUNT(*) FROM killmails WHERE is_loss=1").fetchone()[0],
            "kills": db.execute("SELECT COUNT(*) FROM killmails WHERE is_loss=0").fetchone()[0],
            "zero_isk": db.execute("SELECT COUNT(*) FROM killmails WHERE total_value=0 OR total_value IS NULL").fetchone()[0],
            "tagged_deploy": db.execute("SELECT COUNT(*) FROM killmails WHERE deployment_id IS NOT NULL").fetchone()[0],
            "tagged_fight": db.execute("SELECT COUNT(*) FROM killmails WHERE deployment_fight_id IS NOT NULL").fetchone()[0],
            "fights": db.execute("SELECT COUNT(*) FROM deployment_fights").fetchone()[0],
            "deployments": db.execute("SELECT COUNT(*) FROM deployments").fetchone()[0],
            "latest_kill": db.execute("SELECT killed_at, ship_name, solar_system_name FROM killmails ORDER BY killed_at DESC LIMIT 1").fetchone(),
            "date_range": db.execute("SELECT MIN(killed_at), MAX(killed_at) FROM killmails").fetchone(),
        }
        # Per-day counts for last 7 days
        stats["killmail"]["daily"] = [
            dict(zip(["date", "losses", "kills"], r))
            for r in db.execute("""
                SELECT DATE(killed_at),
                    SUM(CASE WHEN is_loss=1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN is_loss=0 THEN 1 ELSE 0 END)
                FROM killmails
                WHERE killed_at >= datetime('now', '-7 days')
                GROUP BY DATE(killed_at) ORDER BY DATE(killed_at) DESC
            """).fetchall()
        ]
        db.close()
    except Exception as e:
        stats["killmail"] = {"error": str(e)}

    # Market DB
    try:
        db = sqlite3.connect("/opt/void-market/data/void_market.db")
        stats["market"] = {
            "orders": db.execute("SELECT COUNT(*) FROM market_orders").fetchone()[0],
            "types": db.execute("SELECT COUNT(*) FROM sde_types").fetchone()[0],
            "snapshots": db.execute("SELECT COUNT(*) FROM market_snapshots").fetchone()[0],
        }
        db.close()
    except Exception as e:
        stats["market"] = {"error": str(e)}

    return stats


@debug_router.get("/logs")
async def debug_logs(lines: int = 50, grep: str = ""):
    """View recent void-market service logs."""
    import subprocess

    cmd = f"journalctl -u void-market --no-pager -n {min(lines, 500)}"
    if grep:
        cmd += f" | grep -i '{grep}'"

    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return {"lines": result.stdout.split("\n"), "count": len(result.stdout.split("\n"))}
    except Exception as e:
        return {"error": str(e)}
