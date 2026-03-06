"""
Authentication helpers for Rastro API.

Supports two authentication methods:
1. API key (RASTRO_API_KEY, rastro_pk_* prefix) - org-scoped
2. Access token (RASTRO_ACCESS_TOKEN) - user session from `rastro-mcp login`
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class RastroAuth:
    """Holds authentication credentials for Rastro API."""

    api_key: str
    organization_id: Optional[str] = None
    base_url: str = "https://api.rastro.ai/api"

    @property
    def headers(self) -> dict:
        h = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.organization_id:
            h["X-Organization-Id"] = self.organization_id
        return h


def load_auth_from_env() -> RastroAuth:
    """Load authentication from environment variables.

    Tries RASTRO_API_KEY first (org API key), then falls back to
    RASTRO_ACCESS_TOKEN (user session token from ``rastro-mcp login``).

    Env vars:
        RASTRO_API_KEY: API key (rastro_pk_*)
        RASTRO_ACCESS_TOKEN: User session token from browser login
        RASTRO_ORGANIZATION_ID: Organization UUID (optional)
        RASTRO_BASE_URL: API base URL (default: https://api.rastro.ai/api)
    """
    token = os.environ.get("RASTRO_API_KEY") or os.environ.get("RASTRO_ACCESS_TOKEN")
    if not token:
        raise ValueError(
            "Authentication required. Set RASTRO_API_KEY (org API key) "
            "or run `rastro-mcp login` to set RASTRO_ACCESS_TOKEN."
        )

    return RastroAuth(
        api_key=token,
        organization_id=os.environ.get("RASTRO_ORGANIZATION_ID"),
        base_url=os.environ.get("RASTRO_BASE_URL", "https://api.rastro.ai/api"),
    )
