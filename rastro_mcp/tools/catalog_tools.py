"""
catalog_* MCP tools - Rastro catalog read/write operations.

Tools:
- catalog_list
- catalog_get
- catalog_delete
- catalog_schema_get
- catalog_taxonomy_get
- catalog_items_query
- catalog_item_get
- catalog_item_update
- catalog_activity_list
- catalog_activity_get
- catalog_activity_get_staged_changes
- catalog_activity_create_transform
- catalog_snapshot_list
- catalog_snapshot_create
- catalog_snapshot_restore
- catalog_duplicate
- catalog_activity_save_workflow
- catalog_validate_content
"""

import asyncio
import json
import os
import re
import webbrowser
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.execution.bundle_validate import bundle_validate
from rastro_mcp.execution.path_safety import UnsafePathError, resolve_workspace_path
from rastro_mcp.models.contracts import (
    CATALOG_VALIDATE_PRESETS,
    BundleValidateInput,
    CatalogActivityCreateTransformInput,
    CatalogActivityGetInput,
    CatalogActivityGetStagedChangesInput,
    CatalogActivityListInput,
    CatalogActivitySaveWorkflowInput,
    CatalogDeleteInput,
    CatalogDuplicateInput,
    CatalogGetInput,
    CatalogGetMdInput,
    CatalogItemGetInput,
    CatalogItemsBulkUpdateInput,
    CatalogItemsQueryInput,
    CatalogItemUpdateInput,
    CatalogListInput,
    CatalogSchemaGetInput,
    CatalogSnapshotCreateInput,
    CatalogSnapshotListInput,
    CatalogSnapshotRestoreInput,
    CatalogTaxonomyGetInput,
    CatalogUpdateMdInput,
    CatalogUpdateQualityPromptInput,
    CatalogValidateContentFinding,
    CatalogValidateContentInput,
    CatalogValidateContentOutput,
    CatalogValidateContentRule,
    CreateTransformOutput,
    ValidationRules,
)


def _should_open_review_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = parsed.netloc.lower()
    scheme = parsed.scheme.lower()
    if host == "dashboard.rastro.ai":
        return scheme == "https"
    if host in {"localhost:3000", "127.0.0.1:3000"}:
        return scheme in {"http", "https"}
    return False


async def catalog_list(client: RastroClient, params: CatalogListInput) -> dict:
    """List all catalogs for the authenticated organization."""
    return await client.list_catalogs(limit=params.limit, offset=params.offset, organization_id=params.organization_id)


async def catalog_get(client: RastroClient, params: CatalogGetInput) -> dict:
    """Get a single catalog by ID."""
    return await client.get_catalog(params.catalog_id, organization_id=params.organization_id)


async def catalog_delete(client: RastroClient, params: CatalogDeleteInput) -> dict:
    """Delete a catalog after explicit confirmation."""
    catalog = await client.get_catalog(params.catalog_id)
    catalog_name = catalog.get("name")

    if params.expected_name and catalog_name != params.expected_name:
        raise ValueError(f"Catalog name mismatch. Expected '{params.expected_name}', got '{catalog_name}'. Refusing delete.")

    expected_phrase = f"DELETE {params.catalog_id}"
    if not params.confirm:
        return {
            "deleted": False,
            "requires_confirmation": True,
            "catalog_id": params.catalog_id,
            "catalog_name": catalog_name,
            "expected_confirmation": expected_phrase,
            "message": "Destructive operation blocked. Re-run with confirm=true and the exact confirmation phrase.",
        }
    if params.confirmation != expected_phrase:
        raise ValueError(f"Invalid confirmation phrase. Expected exactly: '{expected_phrase}'")

    await client.delete_catalog(params.catalog_id)
    return {
        "deleted": True,
        "catalog_id": params.catalog_id,
        "catalog_name": catalog_name,
        "message": "Catalog deleted successfully.",
    }


async def catalog_schema_get(client: RastroClient, params: CatalogSchemaGetInput) -> dict:
    """Get catalog schema definition with field metadata."""
    kwargs: Dict[str, Any] = {}
    if params.organization_id:
        kwargs["organization_id"] = params.organization_id
    return await client.get_catalog_schema(params.catalog_id, version=params.version, **kwargs)


async def catalog_taxonomy_get(client: RastroClient, params: CatalogTaxonomyGetInput) -> dict:
    """Get catalog taxonomy with computed inheritance."""
    kwargs: Dict[str, Any] = {}
    if params.organization_id:
        kwargs["organization_id"] = params.organization_id
    return await client.get_catalog_taxonomy(params.catalog_id, **kwargs)


async def catalog_update_quality_prompt(client: RastroClient, params: CatalogUpdateQualityPromptInput) -> dict:
    """Update the catalog's quality prompt used for judging and readiness checks."""
    return await client.update_catalog_quality_prompt(params.catalog_id, params.prompt)


async def catalog_update_md(client: RastroClient, params: CatalogUpdateMdInput) -> dict:
    """Update the catalog's markdown context (catalog_md) injected into enrichment and mapping prompts."""
    return await client.update_catalog_md(params.catalog_id, params.catalog_md)


async def catalog_get_md(client: RastroClient, params: CatalogGetMdInput) -> dict:
    """Get the catalog's markdown context (catalog_md)."""
    return await client.get_catalog_md(params.catalog_id)


async def catalog_items_query(client: RastroClient, params: CatalogItemsQueryInput) -> dict:
    """Query raw catalog items with pagination, search, sorting, and entity-type awareness."""
    kwargs: Dict[str, Any] = {}
    if params.organization_id:
        kwargs["organization_id"] = params.organization_id
    return await client.get_catalog_raw_items(
        catalog_id=params.catalog_id,
        limit=params.limit,
        offset=params.offset,
        entity_type=params.entity_type,
        search=params.search,
        sort_field=params.sort_field,
        sort_order=params.sort_order,
        **kwargs,
    )


async def catalog_item_get(client: RastroClient, params: CatalogItemGetInput) -> dict:
    """Get a single raw catalog item by ID, including entity_type and parent linkage."""
    kwargs: Dict[str, Any] = {}
    if params.organization_id:
        kwargs["organization_id"] = params.organization_id
    return await client.get_catalog_raw_item(params.catalog_id, params.item_id, **kwargs)


async def catalog_item_update(client: RastroClient, params: CatalogItemUpdateInput) -> dict:
    """Update a single catalog item's data directly.

    Disabled by default because backend PUT replaces full item data and can
    accidentally drop fields. Use staged custom-transform activities instead.
    """
    direct_update_enabled = os.environ.get("RASTRO_MCP_ENABLE_DIRECT_ITEM_UPDATE", "").lower() in {"1", "true", "yes", "on"}
    if not direct_update_enabled:
        raise ValueError("catalog_item_update is disabled by default for safety. " "Use catalog_activity_create_transform (activity-first) for edits. " "Set RASTRO_MCP_ENABLE_DIRECT_ITEM_UPDATE=true only for explicit break-glass runs.")
    return await client.update_catalog_item(params.catalog_id, params.item_id, params.data, organization_id=params.organization_id)


async def catalog_items_bulk_update(client: RastroClient, params: CatalogItemsBulkUpdateInput) -> dict:
    """Bulk upsert catalog items. Each item should include __catalog_item_id (database UUID) for updates, plus the fields to change."""
    return await client.bulk_upsert_catalog_items(params.catalog_id, params.items, organization_id=params.organization_id)


async def catalog_activity_list(client: RastroClient, params: CatalogActivityListInput) -> dict:
    """List activities for a catalog."""
    kwargs: Dict[str, Any] = {}
    if params.organization_id:
        kwargs["organization_id"] = params.organization_id
    return await client.list_activities(
        catalog_id=params.catalog_id,
        status=params.status,
        activity_type=params.activity_type,
        limit=params.limit,
        offset=params.offset,
        **kwargs,
    )


async def catalog_activity_get(client: RastroClient, params: CatalogActivityGetInput) -> dict:
    """Get a single activity by ID."""
    return await client.get_activity(params.activity_id)


async def catalog_activity_get_staged_changes(client: RastroClient, params: CatalogActivityGetStagedChangesInput) -> dict:
    """Get staged changes for a pending activity."""
    return await client.get_staged_changes(params.activity_id, limit=params.limit, offset=params.offset)


async def catalog_snapshot_list(client: RastroClient, params: CatalogSnapshotListInput) -> dict:
    """List catalog snapshots for rollback/history."""
    return await client.list_catalog_snapshots(
        catalog_id=params.catalog_id,
        snapshot_type=params.snapshot_type,
        limit=params.limit,
        offset=params.offset,
    )


async def catalog_snapshot_create(client: RastroClient, params: CatalogSnapshotCreateInput) -> dict:
    """Create a manual snapshot for rollback safety."""
    return await client.create_catalog_snapshot(params.catalog_id, params.reason)


async def catalog_snapshot_restore(client: RastroClient, params: CatalogSnapshotRestoreInput) -> dict:
    """Restore catalog to a previous snapshot."""
    return await client.restore_catalog_snapshot(params.catalog_id, params.snapshot_id)


async def catalog_duplicate(client: RastroClient, params: CatalogDuplicateInput) -> dict:
    """Duplicate a catalog schema and optionally copy items."""
    payload: Dict[str, Any] = {
        "include_items": params.include_items,
    }
    if params.name:
        payload["name"] = params.name
    if params.description is not None:
        payload["description"] = params.description
    return await client.duplicate_catalog(params.catalog_id, payload)


async def catalog_activity_save_workflow(client: RastroClient, params: CatalogActivitySaveWorkflowInput) -> dict:
    """Save an activity as a reusable workflow template."""
    payload: Dict[str, Any] = {
        "workflow_name": params.workflow_name,
        "timeout_seconds": params.timeout_seconds,
    }
    if params.workflow_description is not None:
        payload["workflow_description"] = params.workflow_description
    if params.python_code is not None:
        payload["python_code"] = params.python_code
    if params.attachments is not None:
        payload["attachments"] = params.attachments
    return await client.save_activity_as_workflow(params.catalog_id, params.activity_id, payload)


async def catalog_activity_create_transform(client: RastroClient, params: CatalogActivityCreateTransformInput) -> CreateTransformOutput:
    """Create a custom transform activity with staged changes and audit metadata.

    1. Loads staged changes from file or inline.
    2. Runs bundle validation.
    3. Creates the activity via the backend API.
    4. Optionally opens the review URL in a browser.
    """
    # Load staged changes
    staged_changes: List[Dict[str, Any]] = []
    if params.staged_changes_file_path:
        try:
            path = resolve_workspace_path(
                params.staged_changes_file_path,
                must_exist=True,
                expect_file=True,
                label="staged_changes_file_path",
            )
        except UnsafePathError as exc:
            raise ValueError(str(exc)) from exc
        if path.endswith(".jsonl"):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        staged_changes.append(json.loads(line))
        elif path.endswith(".json"):
            with open(path) as f:
                data = json.load(f)
            staged_changes = data if isinstance(data, list) else [data]
        elif path.endswith(".parquet"):
            import pandas as pd

            df = pd.read_parquet(path)
            staged_changes = df.to_dict("records")
    elif params.staged_changes_inline:
        staged_changes = params.staged_changes_inline

    # Run bundle validation
    validate_input = BundleValidateInput(
        catalog_id=params.catalog_id,
        staged_changes_path=params.staged_changes_file_path,
        diff_summary=params.diff_summary,
        schema_changes=params.schema_changes,
        taxonomy_changes=params.taxonomy_changes,
        rules=ValidationRules(),
    )
    validation = await bundle_validate(client, validate_input)

    if not validation.valid:
        error_msgs = "; ".join(e.message for e in validation.errors)
        raise ValueError(f"Bundle validation failed: {error_msgs}")

    # Build metadata/context payload used for the activity shell.
    activity_metadata: Dict[str, Any] = {
        "source": "mcp_custom_transform",
    }
    if params.script:
        activity_metadata["script"] = params.script.model_dump()
    if params.diff_summary:
        activity_metadata["diff_summary"] = params.diff_summary
    if validation.computed:
        activity_metadata["validation_report"] = {
            "valid": validation.valid,
            "errors": [e.model_dump() for e in validation.errors],
            "warnings": [w.model_dump() for w in validation.warnings],
            "computed": validation.computed,
        }
    if params.base_snapshot_id:
        activity_metadata["base_snapshot_id"] = params.base_snapshot_id

    # Store schema/taxonomy changes in input for apply-time hooks.
    input_data: Dict[str, Any] = {"description": params.activity_message}
    if params.schema_changes:
        input_data["schema_changes"] = params.schema_changes
    if params.taxonomy_changes:
        input_data["taxonomy_changes"] = params.taxonomy_changes

    activity_context: Dict[str, Any] = {}
    if params.activity_context:
        activity_context.update(params.activity_context)
    if params.attachments is not None:
        activity_context["attachments"] = params.attachments
    if params.session_context:
        activity_context["session_context"] = params.session_context
    if activity_context:
        input_data["activity_context"] = activity_context

    # Create a single activity shell first, then append staged changes in chunks.
    create_payload: Dict[str, Any] = {
        "type": "custom_transform",
        "description": params.activity_message,
        "metadata": activity_metadata,
        "status": "created",
        "staged_changes": [],
    }
    created = await client.create_activity(params.catalog_id, create_payload)
    activity_id = created["activity_id"]

    # Chunk append to avoid large request-body failures while keeping one activity.
    stage_batch_size = int(os.environ.get("RASTRO_MCP_STAGE_BATCH_SIZE", "500"))
    if stage_batch_size <= 0:
        stage_batch_size = 500
    stage_retries = int(os.environ.get("RASTRO_MCP_STAGE_RETRIES", "3"))
    if stage_retries <= 0:
        stage_retries = 3

    async def _append_chunk_with_retry(chunk: List[Dict[str, Any]]) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(stage_retries):
            try:
                return await client.append_activity_staged_changes(activity_id, chunk)
            except Exception as exc:
                last_error = exc
                if attempt == stage_retries - 1:
                    raise
                await asyncio.sleep(min(0.4 * (2**attempt), 2.0))
        if last_error:
            raise last_error
        raise RuntimeError("Failed to append staged changes")

    appended_total = 0
    if staged_changes:
        for i in range(0, len(staged_changes), stage_batch_size):
            chunk = staged_changes[i : i + stage_batch_size]
            append_result = await _append_chunk_with_retry(chunk)
            appended_total = max(appended_total, int(append_result.get("total_staged_changes", appended_total + len(chunk))))

    # Finalize to pending review and fetch canonical review URL.
    finalized: Optional[Dict[str, Any]] = None
    last_finalize_error: Optional[Exception] = None
    for attempt in range(stage_retries):
        try:
            finalized = await client.set_activity_pending_review(
                activity_id,
                message=params.activity_message,
                output=params.diff_summary,
            )
            break
        except Exception as exc:
            last_finalize_error = exc
            if attempt == stage_retries - 1:
                raise
            await asyncio.sleep(min(0.4 * (2**attempt), 2.0))

    if finalized is None:
        if last_finalize_error:
            raise last_finalize_error
        raise RuntimeError("Failed to finalize activity for review")

    output = CreateTransformOutput(
        activity_id=finalized["activity_id"],
        status=finalized["status"],
        staged_count=finalized.get("staged_count", appended_total),
        review_url=finalized["review_url"],
    )

    # Always attempt to open the review URL for user validation.
    try:
        if _should_open_review_url(output.review_url):
            webbrowser.open(output.review_url)
    except Exception:
        pass  # Non-critical

    return output


# ─── Content validation (read-only) ────────────────────────────────────────


def _resolve_value(data: Dict[str, Any], path: str) -> List[str]:
    """Resolve a dotted JSON path inside item `data`, returning all string values.

    Supports:
      - "title" -> data["title"]
      - "specs.finish" -> data["specs.finish"] (flat key with dot) OR nested
        data["specs"]["finish"] if the flat form is missing. Both conventions
        exist in rastro catalogs.
      - "product_variants[].title" -> [v["title"] for v in data["product_variants"]]
    """
    # Array wildcard: "product_variants[].foo.bar"
    if "[]" in path:
        head, _, tail = path.partition("[]")
        array_path = head.rstrip(".")
        sub_path = tail.lstrip(".")
        arr = _resolve_one(data, array_path)
        if not isinstance(arr, list):
            return []
        out: List[str] = []
        for el in arr:
            if isinstance(el, dict):
                out.extend(_resolve_value(el, sub_path) if sub_path else [str(el)])
            elif isinstance(el, str):
                out.append(el)
        return out

    val = _resolve_one(data, path)
    if val is None:
        return []
    if isinstance(val, str):
        return [val]
    if isinstance(val, list):
        return [str(x) for x in val if isinstance(x, (str, int, float))]
    return [str(val)]


def _resolve_one(data: Dict[str, Any], path: str) -> Any:
    """Try the flat-dot key first (legacy Rastro convention), then nested."""
    if path in data:
        return data[path]
    # Try nested only if no flat key exists
    node: Any = data
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return None
    return node


async def catalog_validate_content(
    client: RastroClient, params: CatalogValidateContentInput
) -> CatalogValidateContentOutput:
    """Run regex-based content validation against all items in a catalog.

    Useful for: catching forbidden tokens (`Corona`, `CL-...`, raw finish codes,
    `MR-16`, `lm`, lowercase `lumens`, `diecast`, `5in` as unit) before a push
    and right after any rewrite activity. Returns findings per-rule with a
    short match excerpt so agents can fix without re-fetching every row.

    Either `rules` OR `use_preset` must be supplied.
    """
    rule_dicts: List[Dict[str, Any]] = []
    if params.rules:
        rule_dicts.extend([r.model_dump() if hasattr(r, "model_dump") else r for r in params.rules])
    if params.use_preset:
        preset = CATALOG_VALIDATE_PRESETS.get(params.use_preset)
        if not preset:
            raise ValueError(
                f"Unknown preset '{params.use_preset}'. Available: "
                f"{sorted(CATALOG_VALIDATE_PRESETS.keys())}"
            )
        rule_dicts.extend(preset)
    if not rule_dicts:
        raise ValueError("Supply `rules=[...]` or `use_preset=...`.")

    # Compile all regexes upfront (fail fast on bad patterns)
    compiled: List[Dict[str, Any]] = []
    for r in rule_dicts:
        flags = re.IGNORECASE if r.get("case_insensitive") else 0
        try:
            rx = re.compile(r["pattern"], flags)
        except re.error as exc:
            raise ValueError(f"Rule '{r['name']}' has invalid regex: {exc}") from exc
        compiled.append({
            "name": r["name"],
            "rx": rx,
            "fields": r.get("fields", []),
            "mode": r.get("mode", "must_not_match"),
        })

    # Page through catalog items
    counts_by_rule: Dict[str, int] = {rd["name"]: 0 for rd in compiled}
    findings: List[CatalogValidateContentFinding] = []
    findings_cap: Dict[str, int] = {rd["name"]: 0 for rd in compiled}
    page_size = 500
    offset = 0
    scanned = 0
    while True:
        resp = await client.get_catalog_raw_items(
            catalog_id=params.catalog_id,
            limit=page_size,
            offset=offset,
            entity_type=params.entity_type,
            organization_id=params.organization_id,
        )
        items = resp.get("items") if isinstance(resp, dict) else None
        if items is None and isinstance(resp, list):
            items = resp
        items = items or []
        if not items:
            break

        for item in items:
            scanned += 1
            data = item.get("data") or {}
            item_id = item.get("id") or ""
            pid = data.get("product_id") if isinstance(data, dict) else None

            for rule in compiled:
                for field in rule["fields"]:
                    values = _resolve_value(data, field)
                    for v in values:
                        match = rule["rx"].search(v)
                        if rule["mode"] == "must_not_match" and match:
                            counts_by_rule[rule["name"]] += 1
                            if findings_cap[rule["name"]] < params.limit:
                                start = max(0, match.start() - 30)
                                end = min(len(v), match.end() + 30)
                                findings.append(CatalogValidateContentFinding(
                                    rule=rule["name"],
                                    field=field,
                                    product_id=pid,
                                    item_id=item_id,
                                    match_excerpt=v[start:end],
                                ))
                                findings_cap[rule["name"]] += 1
                        elif rule["mode"] == "must_match" and not match:
                            counts_by_rule[rule["name"]] += 1
                            if findings_cap[rule["name"]] < params.limit:
                                findings.append(CatalogValidateContentFinding(
                                    rule=rule["name"],
                                    field=field,
                                    product_id=pid,
                                    item_id=item_id,
                                    match_excerpt=(v[:60] + "…") if len(v) > 60 else v,
                                ))
                                findings_cap[rule["name"]] += 1

        if len(items) < page_size:
            break
        offset += page_size

    return CatalogValidateContentOutput(
        catalog_id=params.catalog_id,
        scanned=scanned,
        total_violations=sum(counts_by_rule.values()),
        counts_by_rule=counts_by_rule,
        findings=findings,
    )
