"""
Authentication helpers for Rastro API.

Supports:
- API key auth (`RASTRO_API_KEY`)
- Bearer token auth (`RASTRO_ACCESS_TOKEN` / `RASTRO_BEARER_TOKEN`)
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class RastroAuth:
    """Holds authentication credentials for Rastro API."""

    token: str
    organization_id: Optional[str] = None
    base_url: str = "https://catalogapi.rastro.ai/api"

    @property
    def headers(self) -> dict:
        h = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        if self.organization_id:
            h["X-Organization-Id"] = self.organization_id
        return h


def load_auth_from_env() -> RastroAuth:
    """Load authentication from environment variables.

    Env vars:
        RASTRO_API_KEY: API key (rastro_pk_*)
        RASTRO_ACCESS_TOKEN: User bearer token from web auth session
        RASTRO_BEARER_TOKEN: Alias for RASTRO_ACCESS_TOKEN
        RASTRO_ORGANIZATION_ID: Organization UUID (optional, derived from key if absent)
        RASTRO_BASE_URL: API base URL (default: https://catalogapi.rastro.ai/api)
    """
    token = os.environ.get("RASTRO_API_KEY") or os.environ.get("RASTRO_ACCESS_TOKEN") or os.environ.get("RASTRO_BEARER_TOKEN")
    if not token:
        raise ValueError(
            "Authentication required: set one of RASTRO_API_KEY, RASTRO_ACCESS_TOKEN, or RASTRO_BEARER_TOKEN."
        )

    return RastroAuth(
        token=token,
        organization_id=os.environ.get("RASTRO_ORGANIZATION_ID"),
        base_url=os.environ.get("RASTRO_BASE_URL", "https://catalogapi.rastro.ai/api"),
    )
