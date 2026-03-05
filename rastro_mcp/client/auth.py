"""
Authentication helpers for Rastro API.

Supports API key authentication (rastro_pk_* prefix).
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class RastroAuth:
    """Holds authentication credentials for Rastro API."""

    api_key: str
    organization_id: Optional[str] = None
    base_url: str = "https://catalogapi.rastro.ai/api"

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

    Env vars:
        RASTRO_API_KEY: API key (rastro_pk_*)
        RASTRO_ORGANIZATION_ID: Organization UUID (optional, derived from key if absent)
        RASTRO_BASE_URL: API base URL (default: https://catalogapi.rastro.ai/api)
    """
    api_key = os.environ.get("RASTRO_API_KEY")
    if not api_key:
        raise ValueError("RASTRO_API_KEY environment variable is required")

    return RastroAuth(
        api_key=api_key,
        organization_id=os.environ.get("RASTRO_ORGANIZATION_ID"),
        base_url=os.environ.get("RASTRO_BASE_URL", "https://catalogapi.rastro.ai/api"),
    )
