"""
HTTP client for Rastro public API.

Thin wrapper around httpx with auth, pagination, and error handling.
"""

import asyncio
import json
import os
from typing import Any, Dict, List, Optional

import httpx

from .auth import RastroAuth


class RastroAPIError(Exception):
    """Raised when the Rastro API returns an error."""

    def __init__(self, status_code: int, detail: str, response_body: Optional[dict] = None):
        self.status_code = status_code
        self.detail = detail
        self.response_body = response_body
        super().__init__(f"HTTP {status_code}: {detail}")


class RastroClient:
    """Async HTTP client for the Rastro public API."""

    def __init__(self, auth: RastroAuth, timeout: float = 60.0):
        self.auth = auth
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.auth.base_url,
                headers=self.auth.headers,
                timeout=self.timeout,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        await self._get_client()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        client = await self._get_client()
        resp = await client.request(method, path, **kwargs)
        if resp.status_code >= 400:
            try:
                body = resp.json()
                detail = body.get("detail", body.get("error", resp.text))
            except Exception:
                body = None
                detail = resp.text
            raise RastroAPIError(resp.status_code, str(detail), body)
        if resp.status_code == 204:
            return {}
        return resp.json()

    async def _request_with_retry(self, method: str, path: str, retries: int = 3, **kwargs) -> dict:
        """Retry transient network/server failures with short exponential backoff."""
        last_error: Optional[Exception] = None
        for attempt in range(max(1, retries)):
            try:
                return await self._request(method, path, **kwargs)
            except RastroAPIError as exc:
                last_error = exc
                is_retryable = exc.status_code >= 500 or exc.status_code in {408, 429}
                if not is_retryable or attempt == retries - 1:
                    raise
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    raise

            await asyncio.sleep(min(0.3 * (2**attempt), 2.0))

        if last_error:
            raise last_error
        raise RuntimeError("Request retries exhausted without a captured error")

    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    async def _paginate_items_sequential(
        self,
        path: str,
        page_size: int,
        params_extra: Optional[Dict[str, Any]] = None,
        expected_total: Optional[int] = None,
    ) -> List[dict]:
        """Reliable fallback pagination using offset += len(items)."""
        params_extra = params_extra or {}
        requested = max(1, int(page_size))
        offset = 0
        all_items: List[dict] = []
        seen_first_ids: set[str] = set()

        while True:
            params: Dict[str, Any] = {"limit": requested, "offset": offset, **params_extra}
            resp = await self._request_with_retry("GET", path, params=params)
            items = resp.get("items", []) or []
            if not items:
                break

            first_id = str((items[0] or {}).get("id") or "")
            if first_id and first_id in seen_first_ids:
                # Protect against backends that ignore offset and repeat page 1 forever.
                break
            if first_id:
                seen_first_ids.add(first_id)

            all_items.extend(items)

            total_from_page = self._parse_int(resp.get("total"))
            effective_total = expected_total if expected_total is not None else total_from_page
            if effective_total is not None and len(all_items) >= effective_total:
                return all_items[:effective_total]

            if len(items) < requested:
                break

            offset += len(items)

        return all_items

    async def _paginate_items_parallel(
        self,
        path: str,
        page_size: int,
        params_extra: Optional[Dict[str, Any]] = None,
        max_concurrency: Optional[int] = None,
    ) -> List[dict]:
        """Fast pagination when total is known and offset works correctly."""
        params_extra = params_extra or {}
        requested = max(1, int(page_size))
        concurrency = max(1, int(max_concurrency or os.environ.get("RASTRO_MCP_PULL_MAX_CONCURRENCY", "8")))
        page_timeout_seconds = max(5.0, float(os.environ.get("RASTRO_MCP_PULL_PAGE_TIMEOUT_SECONDS", "25")))

        first_resp = await self._request_with_retry("GET", path, params={"limit": requested, "offset": 0, **params_extra})
        first_items = first_resp.get("items", []) or []
        if not first_items:
            return []

        total = self._parse_int(first_resp.get("total"))
        if total is None:
            return await self._paginate_items_sequential(path=path, page_size=requested, params_extra=params_extra)

        if total <= len(first_items):
            return first_items[:total]

        effective_page_size = max(1, len(first_items))
        offsets = list(range(effective_page_size, total, effective_page_size))

        sem = asyncio.Semaphore(concurrency)

        async def _fetch_page(offset: int) -> tuple[int, List[dict]]:
            params = {"limit": requested, "offset": offset, **params_extra}
            async with sem:
                resp = await asyncio.wait_for(
                    self._request_with_retry("GET", path, params=params),
                    timeout=page_timeout_seconds,
                )
            return offset, (resp.get("items", []) or [])

        try:
            pages = await asyncio.gather(*(_fetch_page(offset) for offset in offsets))
        except Exception:
            fallback_page_size = max(50, min(requested, effective_page_size, 200))
            return await self._paginate_items_sequential(
                path=path,
                page_size=fallback_page_size,
                params_extra=params_extra,
                expected_total=total,
            )

        # Detect offset-ignored deployments; fallback to safe sequential pagination.
        first_id = str((first_items[0] or {}).get("id") or "")
        if first_id:
            non_empty = 0
            repeated_first_id = 0
            for _, page_items in pages:
                if not page_items:
                    continue
                non_empty += 1
                page_first_id = str((page_items[0] or {}).get("id") or "")
                if page_first_id == first_id:
                    repeated_first_id += 1
            if non_empty > 0 and repeated_first_id == non_empty:
                return await self._paginate_items_sequential(path=path, page_size=requested, params_extra=params_extra, expected_total=total)

        all_items = list(first_items)
        for _, page_items in sorted(pages, key=lambda p: p[0]):
            all_items.extend(page_items)

        if len(all_items) < total:
            return await self._paginate_items_sequential(path=path, page_size=requested, params_extra=params_extra, expected_total=total)

        return all_items[:total]

    # ── Catalog endpoints ──────────────────────────────────────────────

    async def list_catalogs(self, limit: int = 50, offset: int = 0) -> dict:
        return await self._request("GET", "/public/catalogs", params={"limit": limit, "offset": offset})

    async def get_catalog(self, catalog_id: str) -> dict:
        return await self._request("GET", f"/public/catalogs/{catalog_id}")

    async def get_catalog_schema(self, catalog_id: str, version: Optional[str] = None) -> dict:
        params = {}
        if version:
            params["version"] = version
        return await self._request("GET", f"/public/catalogs/{catalog_id}/schema", params=params)

    async def get_catalog_taxonomy(self, catalog_id: str) -> dict:
        return await self._request("GET", f"/public/catalogs/{catalog_id}/taxonomy")

    async def get_catalog_items(self, catalog_id: str, limit: int = 50, offset: int = 0, search: Optional[str] = None, sort_field: Optional[str] = None, sort_order: str = "asc") -> dict:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if search:
            params["search"] = search
        if sort_field:
            params["sort_field"] = sort_field
            params["sort_order"] = sort_order
        return await self._request("GET", f"/public/catalogs/{catalog_id}/items", params=params)

    async def get_catalog_raw_items(self, catalog_id: str, limit: int = 400, offset: int = 0, entity_type: Optional[str] = None) -> dict:
        """Get raw catalog_items rows (id/entity_type/parent_id/current_version/data)."""
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if entity_type:
            params["entity_type"] = entity_type
        return await self._request("GET", f"/public/catalogs/{catalog_id}/raw-items", params=params)

    async def get_catalog_items_all(self, catalog_id: str, page_size: int = 400, max_concurrency: Optional[int] = None) -> List[dict]:
        """Get all public catalog items using fast parallel pagination with safe fallback."""
        path = f"/public/catalogs/{catalog_id}/items"
        return await self._paginate_items_parallel(path=path, page_size=page_size, max_concurrency=max_concurrency)

    async def get_catalog_raw_items_all(
        self,
        catalog_id: str,
        page_size: int = 400,
        entity_type: Optional[str] = None,
        max_concurrency: Optional[int] = None,
    ) -> List[dict]:
        """Get all raw catalog_items rows using fast parallel pagination with safe fallback."""
        path = f"/public/catalogs/{catalog_id}/raw-items"
        params_extra: Dict[str, Any] = {}
        if entity_type:
            params_extra["entity_type"] = entity_type
        return await self._paginate_items_parallel(
            path=path,
            page_size=page_size,
            params_extra=params_extra,
            max_concurrency=max_concurrency,
        )

    async def get_catalog_item(self, catalog_id: str, item_id: str) -> dict:
        """Get a single catalog item by ID."""
        return await self._request("GET", f"/public/catalogs/{catalog_id}/items/{item_id}")

    async def update_catalog_item(self, catalog_id: str, item_id: str, data: dict) -> dict:
        """Update a single catalog item's data."""
        return await self._request("PUT", f"/public/catalogs/{catalog_id}/items/{item_id}", json=data)

    async def get_staged_changes(self, activity_id: str, limit: int = 50, offset: int = 0) -> dict:
        """Get staged changes for an activity.

        Backend contract uses page/page_size; MCP contract currently uses
        limit/offset, so we map deterministically.
        """
        if limit <= 0:
            limit = 50
        page = (offset // limit) + 1
        return await self._request(
            "GET",
            f"/activities/{activity_id}/staged-changes",
            params={"page": page, "page_size": limit},
        )

    async def bulk_review_activity_staged_changes(
        self,
        activity_id: str,
        action: str = "approve_all",
        change_ids: Optional[List[str]] = None,
        rejection_reason: Optional[str] = None,
    ) -> dict:
        """Bulk review staged changes for an activity."""
        if action.lower().startswith("approve"):
            raise PermissionError(
                "Programmatic staged-change approvals are disabled in MCP. "
                "Review and approve in the dashboard."
            )
        payload: Dict[str, Any] = {"action": action}
        if change_ids is not None:
            payload["change_ids"] = change_ids
        if rejection_reason is not None:
            payload["rejection_reason"] = rejection_reason
        return await self._request("POST", f"/activities/{activity_id}/staged-changes/bulk-review", json=payload)

    async def apply_activity(self, activity_id: str) -> dict:
        """Apply approved staged changes for an activity."""
        raise PermissionError(
            "Programmatic apply is disabled in MCP. "
            "Apply from dashboard review after manual approval."
        )

    # ── Activity endpoints ──────────────────────────────────────────────

    async def list_activities(self, catalog_id: str, status: Optional[str] = None, activity_type: Optional[str] = None, limit: int = 20, offset: int = 0) -> dict:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        if activity_type:
            params["type"] = activity_type
        return await self._request("GET", f"/public/catalogs/{catalog_id}/activities", params=params)

    async def get_activity(self, activity_id: str) -> dict:
        return await self._request("GET", f"/activities/{activity_id}")

    async def get_activity_staged_changes_summary(self, activity_id: str) -> dict:
        return await self._request("GET", f"/activities/{activity_id}/staged-changes/summary")

    async def cancel_activity(self, activity_id: str) -> dict:
        return await self._request("POST", f"/activities/{activity_id}/cancel")

    async def create_custom_transform_activity(self, catalog_id: str, payload: dict) -> dict:
        return await self._request("POST", f"/public/catalogs/{catalog_id}/activities/custom-transform", json=payload)

    async def create_activity(self, catalog_id: str, payload: dict) -> dict:
        return await self._request("POST", f"/public/catalogs/{catalog_id}/activities", json=payload)

    async def append_activity_staged_changes(self, activity_id: str, staged_changes: List[dict]) -> dict:
        """Append staged changes to an existing activity."""
        return await self._request("POST", f"/public/activities/{activity_id}/staged-changes/append", json={"staged_changes": staged_changes})

    async def set_activity_pending_review(self, activity_id: str, message: Optional[str] = None, output: Optional[dict] = None) -> dict:
        """Set an existing activity to pending_review and return review URL."""
        payload: Dict[str, Any] = {}
        if message is not None:
            payload["message"] = message
        if output is not None:
            payload["output"] = output
        return await self._request("POST", f"/public/activities/{activity_id}/pending-review", json=payload)

    async def list_catalog_snapshots(
        self,
        catalog_id: str,
        snapshot_type: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> dict:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if snapshot_type:
            params["snapshot_type"] = snapshot_type
        return await self._request("GET", f"/public/catalogs/{catalog_id}/snapshots", params=params)

    async def create_catalog_snapshot(self, catalog_id: str, reason: str) -> dict:
        return await self._request("POST", f"/public/catalogs/{catalog_id}/snapshots", json={"reason": reason})

    async def restore_catalog_snapshot(self, catalog_id: str, snapshot_id: str) -> dict:
        return await self._request("POST", f"/public/catalogs/{catalog_id}/snapshots/{snapshot_id}/restore")

    async def duplicate_catalog(self, catalog_id: str, payload: dict) -> dict:
        return await self._request("POST", f"/public/catalogs/{catalog_id}/duplicate", json=payload)

    async def delete_catalog(self, catalog_id: str) -> dict:
        """Delete a catalog by ID (irreversible)."""
        return await self._request("DELETE", f"/catalogs/{catalog_id}")

    async def save_activity_as_workflow(self, catalog_id: str, activity_id: str, payload: dict) -> dict:
        return await self._request("POST", f"/public/catalogs/{catalog_id}/activities/{activity_id}/save-workflow", json=payload)

    # ── Public workflow endpoints ─────────────────────────────────────

    async def list_workflows(self, limit: int = 50, offset: int = 0, search: Optional[str] = None) -> dict:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if search:
            params["search"] = search
        return await self._request("GET", "/public/workflows", params=params)

    async def execute_workflow(self, workflow_id: str, payload: dict) -> dict:
        return await self._request("POST", f"/public/workflows/{workflow_id}/execute", json=payload)

    async def get_workflow_run(self, workflow_run_id: str) -> dict:
        return await self._request("GET", f"/public/workflows/runs/{workflow_run_id}")

    # ── Image editor endpoints ──────────────────────────────────────────

    async def image_run(self, payload: dict) -> dict:
        return await self._request("POST", "/image-editor/runs", json=payload)

    async def image_status(self, run_id: str) -> dict:
        return await self._request("GET", f"/image-editor/runs/{run_id}")

    async def image_list(self, catalog_id: Optional[str] = None, item_id: Optional[str] = None, status: Optional[str] = None, tool: Optional[str] = None, limit: int = 20, offset: int = 0) -> dict:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if catalog_id:
            params["catalog_id"] = catalog_id
        if item_id:
            params["item_id"] = item_id
        if status:
            params["status"] = status
        if tool:
            params["tool"] = tool
        return await self._request("GET", "/image-editor/runs", params=params)

    # ── Enrichment endpoints ────────────────────────────────────────────

    async def enrich(self, payload: dict) -> dict:
        return await self._request("POST", "/public/enrich", json=payload)

    async def judge_catalog_rows(self, payload: dict) -> dict:
        return await self._request("POST", "/public/judge", json=payload)

    async def get_enrich_job(self, job_id: str) -> dict:
        return await self._request("GET", f"/public/enrich/{job_id}")
