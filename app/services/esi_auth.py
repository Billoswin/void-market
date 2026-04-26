"""
Void Market — ESI Authentication Service

PKCE flow (required by CCP's current developer portal).

Per CCP native SSO docs:
1. Generate code_verifier: random 32 bytes, base64url encoded (no padding)
2. Generate code_challenge: base64url(SHA256(code_verifier)) (no padding)
3. Authorize URL includes code_challenge + code_challenge_method=S256
4. Token exchange POST includes code_verifier + client_id (NO client_secret)
5. Headers: Content-Type: application/x-www-form-urlencoded, Host: login.eveonline.com
"""
import logging
import base64
import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import httpx
import jwt as pyjwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.database import EsiCharacter

logger = logging.getLogger("void_market.esi_auth")

EVE_SSO_AUTH_URL = "https://login.eveonline.com/v2/oauth/authorize/"
EVE_SSO_TOKEN_URL = "https://login.eveonline.com/v2/oauth/token"


class EsiAuthService:
    def __init__(self):
        self._http: httpx.AsyncClient | None = None
        self._pending_states: dict[str, datetime] = {}
        self._code_verifier: str | None = None

    async def start(self):
        self._http = httpx.AsyncClient(timeout=15.0)

    async def stop(self):
        if self._http:
            await self._http.aclose()

    def _generate_pkce(self) -> tuple[str, str]:
        """Generate PKCE code_verifier and code_challenge.
        
        Per CCP docs (native_sso_flow):
        - code_verifier: URL safe Base64 encoded random 32 byte string
        - code_challenge: URL safe Base64(SHA256(code_verifier)), no padding
        """
        raw = secrets.token_bytes(32)
        code_verifier = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
        
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        
        return code_verifier, code_challenge

    def get_auth_url(self) -> str:
        """Build authorize URL with PKCE challenge."""
        state = secrets.token_urlsafe(32)
        self._pending_states[state] = datetime.now(timezone.utc)

        # Clean old states
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
        self._pending_states = {
            s: t for s, t in self._pending_states.items() if t > cutoff
        }

        # Generate PKCE pair — store verifier for token exchange
        self._code_verifier, code_challenge = self._generate_pkce()

        logger.info(f"PKCE generated: verifier={self._code_verifier[:10]}... challenge={code_challenge[:10]}...")

        params = {
            "response_type": "code",
            "redirect_uri": settings.esi_callback_url,
            "client_id": settings.esi_client_id,
            "scope": settings.esi_scopes,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }

        return f"{EVE_SSO_AUTH_URL}?{urlencode(params)}"

    async def handle_callback(self, code: str, db: AsyncSession) -> EsiCharacter:
        """Exchange auth code for tokens using PKCE.
        
        POST body contains:
        - grant_type=authorization_code
        - code=<auth code>
        - client_id=<client id>
        - code_verifier=<the verifier from step 1>
        
        NO Authorization header. NO client_secret.
        """
        if not self._code_verifier:
            raise ValueError("No PKCE code_verifier — did you start the auth flow first?")

        logger.info(f"Token exchange: code={code[:20]}... verifier={self._code_verifier[:10]}...")

        resp = await self._http.post(
            EVE_SSO_TOKEN_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "login.eveonline.com",
            },
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": settings.esi_client_id,
                "code_verifier": self._code_verifier,
            },
        )

        if resp.status_code != 200:
            content_type = resp.headers.get("content-type", "")
            if "json" in content_type:
                error_body = resp.text[:500]
            else:
                error_body = f"HTML response (status {resp.status_code})"
            logger.error(f"Token exchange failed: HTTP {resp.status_code}")
            logger.error(f"Response: {error_body}")
            raise ValueError(f"Token exchange failed: HTTP {resp.status_code} — {error_body}")

        token_data = resp.json()
        access_token = token_data["access_token"]
        refresh_token = token_data.get("refresh_token", "")

        logger.info("Token exchange successful")

        # Decode JWT for character info
        decoded = pyjwt.decode(
            access_token,
            options={"verify_signature": False},
            algorithms=["RS256"],
        )

        sub = decoded.get("sub", "")
        parts = sub.split(":")
        if len(parts) < 3:
            raise ValueError(f"Unexpected JWT sub format: {sub}")

        character_id = int(parts[2])
        character_name = decoded.get("name", f"Character {character_id}")

        expires_in = token_data.get("expires_in", 1199)
        token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        # Upsert character — keep ALL existing characters active (multi-char support)
        existing = await db.get(EsiCharacter, character_id)
        if existing:
            existing.access_token = access_token
            existing.refresh_token = refresh_token
            existing.token_expiry = token_expiry
            existing.scopes = settings.esi_scopes
            existing.updated_at = datetime.now(timezone.utc)
            existing.is_active = True
            character = existing
        else:
            # Don't deactivate other characters — we want multi-char support
            character = EsiCharacter(
                character_id=character_id,
                character_name=character_name,
                access_token=access_token,
                refresh_token=refresh_token,
                token_expiry=token_expiry,
                scopes=settings.esi_scopes,
                is_active=True,
            )
            db.add(character)

        # Fetch corp/alliance
        try:
            pub_resp = await self._http.get(
                f"{settings.esi_base_url}/characters/{character_id}/",
                params={"datasource": "tranquility"},
            )
            if pub_resp.status_code == 200:
                pub_info = pub_resp.json()
                character.corporation_id = pub_info.get("corporation_id")
                character.alliance_id = pub_info.get("alliance_id")
        except Exception as e:
            logger.warning(f"Could not fetch character info: {e}")

        await db.commit()
        logger.info(f"Authenticated: {character_name} ({character_id})")
        return character

    async def get_active_character(self, db: AsyncSession) -> EsiCharacter | None:
        result = await db.execute(
            select(EsiCharacter).where(EsiCharacter.is_active == True)
        )
        return result.scalars().first()

    async def get_all_active_characters(self, db: AsyncSession) -> list[EsiCharacter]:
        result = await db.execute(
            select(EsiCharacter).where(EsiCharacter.is_active == True)
        )
        return list(result.scalars().all())

    async def get_all_character_ids(self, db: AsyncSession) -> list[int]:
        chars = await self.get_all_active_characters(db)
        return [c.character_id for c in chars]

    async def get_valid_token(self, db: AsyncSession, character_id: int = None) -> str | None:
        if character_id:
            char = await db.get(EsiCharacter, character_id)
            if not char or not char.is_active:
                return None
        else:
            char = await self.get_active_character(db)
        if not char:
            return None

        # SQLite returns naive datetimes — make comparison timezone-safe
        expiry = char.token_expiry
        if expiry and expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        if expiry <= now + timedelta(seconds=60):
            char = await self._refresh_token(char, db)

        return char.access_token

    async def _refresh_token(
        self, char: EsiCharacter, db: AsyncSession
    ) -> EsiCharacter:
        """Refresh token — also uses client_id, no secret."""
        resp = await self._http.post(
            EVE_SSO_TOKEN_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "login.eveonline.com",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": char.refresh_token,
                "client_id": settings.esi_client_id,
            },
        )

        if resp.status_code != 200:
            logger.error(f"Token refresh failed: {resp.status_code} {resp.text[:300]}")
            raise ValueError(f"Token refresh failed: {resp.status_code}")

        token_data = resp.json()

        char.access_token = token_data["access_token"]
        char.refresh_token = token_data.get("refresh_token", char.refresh_token)
        char.token_expiry = datetime.now(timezone.utc) + timedelta(
            seconds=token_data.get("expires_in", 1199)
        )
        char.updated_at = datetime.now(timezone.utc)
        await db.commit()

        logger.debug(f"Token refreshed for {char.character_name}")
        return char


esi_auth = EsiAuthService()
