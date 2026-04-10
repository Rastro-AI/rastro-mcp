"""
Authentication helpers for Rastro API.

Supports bearer authentication with either a user token or an API key.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values

@dataclass(init=False)
class RastroAuth:
    """Holds authentication credentials for Rastro API."""

    token: str
    organization_id: Optional[str] = None
    base_url: str = "https://catalogapi.rastro.ai/api"
    credential_source: str = "bearer"

    def __init__(
        self,
        token: Optional[str] = None,
        organization_id: Optional[str] = None,
        base_url: str = "https://catalogapi.rastro.ai/api",
        credential_source: str = "bearer",
        api_key: Optional[str] = None,
    ):
        resolved_token = token or api_key
        if not resolved_token:
            raise ValueError("A bearer token is required")

        self.token = resolved_token
        self.organization_id = organization_id
        self.base_url = base_url
        self.credential_source = credential_source

    @property
    def api_key(self) -> str:
        """Backward-compatible alias for existing callers."""
        return self.token

    @property
    def headers(self) -> dict:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        if self.organization_id:
            headers["X-Organization-Id"] = self.organization_id
        return headers


def load_auth_from_env() -> RastroAuth:
    """Load authentication from environment variables.

    Env vars:
        RASTRO_AUTH_TOKEN: Generic bearer token (preferred)
        RASTRO_USER_TOKEN: User bearer token / JWT
        RASTRO_API_KEY: API key (rastro_pk_*)
        RASTRO_ORGANIZATION_ID: Organization UUID override. Recommended for user tokens.
        RASTRO_BASE_URL: API base URL (default: https://catalogapi.rastro.ai/api)
    """
    def _load_auth_from_dotenv() -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Best-effort project dotenv lookup for local dev."""
        candidate_roots = []
        repo_root = Path(__file__).resolve().parents[2]
        for root in (Path.cwd(), repo_root):
            if root not in candidate_roots:
                candidate_roots.append(root)

        for root in candidate_roots:
            for filename in (".env.local", ".env"):
                env_path = root / filename
                if not env_path.is_file():
                    continue

                values = dotenv_values(env_path)
                for name in ("RASTRO_AUTH_TOKEN", "RASTRO_USER_TOKEN", "RASTRO_API_KEY"):
                    token = values.get(name)
                    if token:
                        return (
                            str(token),
                            values.get("RASTRO_ORGANIZATION_ID"),
                            values.get("RASTRO_BASE_URL"),
                            f"{env_path}:{name}",
                        )

        return None, None, None, None

    token = os.environ.get("RASTRO_AUTH_TOKEN")
    credential_source = "RASTRO_AUTH_TOKEN"
    organization_id = os.environ.get("RASTRO_ORGANIZATION_ID")
    base_url = os.environ.get("RASTRO_BASE_URL") or "https://catalogapi.rastro.ai/api"

    if not token:
        token = os.environ.get("RASTRO_USER_TOKEN")
        credential_source = "RASTRO_USER_TOKEN"

    if not token:
        token = os.environ.get("RASTRO_API_KEY")
        credential_source = "RASTRO_API_KEY"

    if not token:
        token, dotenv_org_id, dotenv_base_url, dotenv_source = _load_auth_from_dotenv()
        if token:
            credential_source = dotenv_source or ".env"
            organization_id = organization_id or dotenv_org_id
            if dotenv_base_url:
                base_url = dotenv_base_url

    if not token:
        from rastro_mcp.cli import load_token_from_file

        token = load_token_from_file()
        credential_source = "credentials_file"

    if not token:
        raise ValueError(
            "Authentication required. Set RASTRO_API_KEY or run `rastro-mcp login` to authenticate via browser."
        )

    return RastroAuth(
        token=token,
        organization_id=organization_id,
        base_url=base_url,
        credential_source=credential_source,
    )
