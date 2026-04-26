"""
Void Market — ESI API Client

Handles authenticated ESI requests with:
- Automatic token refresh
- Pagination (X-Pages header)
- Rate limit awareness (X-Esi-Error-Limit-Remain)
- Retry with backoff
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.esi_client")


class EsiClient:
    """Wrapper around ESI with auth, pagination, and rate limiting."""

    def __init__(self):
        self._http: httpx.AsyncClient | None = None
        self._error_limit_remain: int = 100
        self._error_limit_reset: int = 0

    async def start(self):
        self._http = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": f"VoidMarket/{settings.app_version} (contact: in-game)"},
        )

    async def stop(self):
        if self._http:
            await self._http.aclose()

    async def get(
        self,
        path: str,
        db: AsyncSession,
        params: dict | None = None,
        authenticated: bool = True,
        character_id: int = None,
    ) -> dict | list:
        """Single-page GET request to ESI. character_id selects which char's token to use."""
        headers = {}
        if authenticated:
            token = await esi_auth.get_valid_token(db, character_id=character_id)
            if not token:
                raise ValueError("No authenticated character. Login via /auth/login first.")
            headers["Authorization"] = f"Bearer {token}"

        if params is None:
            params = {}
        params.setdefault("datasource", "tranquility")

        # Rate limit check
        if self._error_limit_remain < 10:
            wait = max(1, self._error_limit_reset)
            logger.warning(f"ESI error limit low ({self._error_limit_remain}), waiting {wait}s")
            await asyncio.sleep(wait)

        url = f"{settings.esi_base_url}{path}"
        resp = await self._http.get(url, headers=headers, params=params)

        # Track rate limits
        self._error_limit_remain = int(resp.headers.get("X-Esi-Error-Limit-Remain", 100))
        self._error_limit_reset = int(resp.headers.get("X-Esi-Error-Limit-Reset", 60))

        if resp.status_code == 502 or resp.status_code == 503:
            logger.warning(f"ESI {resp.status_code} on {path}, retrying in 5s...")
            await asyncio.sleep(5)
            resp = await self._http.get(url, headers=headers, params=params)

        resp.raise_for_status()
        return resp.json()

    async def get_paginated(
        self,
        path: str,
        db: AsyncSession,
        params: dict | None = None,
        authenticated: bool = True,
        character_id: int = None,
    ) -> list:
        """GET all pages from a paginated ESI endpoint."""
        headers = {}
        if authenticated:
            token = await esi_auth.get_valid_token(db, character_id=character_id)
            if not token:
                raise ValueError("No authenticated character.")
            headers["Authorization"] = f"Bearer {token}"

        if params is None:
            params = {}
        params.setdefault("datasource", "tranquility")

        all_results = []

        # First page
        params["page"] = 1
        url = f"{settings.esi_base_url}{path}"
        resp = await self._http.get(url, headers=headers, params=params)

        self._error_limit_remain = int(resp.headers.get("X-Esi-Error-Limit-Remain", 100))
        resp.raise_for_status()

        page_data = resp.json()
        if isinstance(page_data, list):
            all_results.extend(page_data)
        else:
            return [page_data]

        total_pages = int(resp.headers.get("X-Pages", 1))

        if total_pages > 1:
            logger.info(f"Fetching {total_pages} pages from {path}")

            for page in range(2, total_pages + 1):
                params["page"] = page
                await asyncio.sleep(0.1)  # Light throttle

                resp = await self._http.get(url, headers=headers, params=params)
                self._error_limit_remain = int(resp.headers.get("X-Esi-Error-Limit-Remain", 100))

                if self._error_limit_remain < 10:
                    wait = max(1, self._error_limit_reset)
                    logger.warning(f"Rate limit low, waiting {wait}s")
                    await asyncio.sleep(wait)

                resp.raise_for_status()
                page_data = resp.json()
                if isinstance(page_data, list):
                    all_results.extend(page_data)

        return all_results

    async def post(
        self,
        path: str,
        db: AsyncSession,
        json_data: Any = None,
        authenticated: bool = False,
        character_id: int = None,
    ) -> dict | list:
        """POST request to ESI. character_id selects which character's token to use."""
        headers = {}
        if authenticated or character_id:
            token = await esi_auth.get_valid_token(db, character_id=character_id)
            if not token:
                raise ValueError("No authenticated character.")
            headers["Authorization"] = f"Bearer {token}"

        params = {"datasource": "tranquility"}
        url = f"{settings.esi_base_url}{path}"

        resp = await self._http.post(url, headers=headers, params=params, json=json_data)
        resp.raise_for_status()
        return resp.json()


# Singleton
esi_client = EsiClient()
