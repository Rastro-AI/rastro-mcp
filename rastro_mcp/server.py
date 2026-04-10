"""
Rastro MCP Server

Registers all tools in three categories:
- catalog_*: Rastro catalog operations
- service_*: Image editing and schema mapping
- execution_*: Local snapshot, diff, and validation

Usage:
    RASTRO_AUTH_TOKEN=<bearer-token> python -m rastro_mcp.server
    RASTRO_API_KEY=rastro_pk_... python -m rastro_mcp.server

Or via MCP stdio transport for integration with Claude/Codex.
"""

import asyncio
import json
import os
import sys
import traceback
from typing import Any, AsyncIterator, Dict

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.client.auth import RastroAuth, load_auth_from_env
from rastro_mcp.models.contracts import (
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
    CatalogVisualizeLocalInput,
    DiffComputeInput,
    ServiceImageHostInput,
    ServiceImageListInput,
    ServiceImageRunInput,
    ServiceImageStatusInput,
    ServiceJudgeCatalogRowsInput,
    ServiceMapToCatalogSchemaInput,
    SnapshotPullInput,
    StageDatasetInput,
)
from rastro_mcp.tools.catalog_tools import (
    catalog_activity_create_transform,
    catalog_activity_get,
    catalog_activity_get_staged_changes,
    catalog_activity_list,
    catalog_activity_save_workflow,
    catalog_delete,
    catalog_duplicate,
    catalog_get,
    catalog_get_md,
    catalog_item_get,
    catalog_item_update,
    catalog_items_bulk_update,
    catalog_items_query,
    catalog_list,
    catalog_schema_get,
    catalog_snapshot_create,
    catalog_snapshot_list,
    catalog_snapshot_restore,
    catalog_taxonomy_get,
    catalog_update_md,
    catalog_update_quality_prompt,
)
from rastro_mcp.tools.execution_tools import (
    execution_bundle_validate,
    execution_catalog_snapshot_pull,
    execution_catalog_stage_dataset,
    execution_local_diff_compute,
)
from rastro_mcp.tools.service_tools import (
    service_image_host,
    service_image_list,
    service_image_run,
    service_image_status,
    service_judge_catalog_rows,
    service_map_to_catalog_schema,
)
from rastro_mcp.tools.viewer_tools import catalog_visualize_local, image_review_local


def _is_truthy_env(name: str) -> bool:
    return os.environ.get(name, "").lower() in {"1", "true", "yes", "on"}


DIRECT_ITEM_UPDATE_ENABLED = _is_truthy_env("RASTRO_MCP_ENABLE_DIRECT_ITEM_UPDATE")

# ═══════════════════════════════════════════════════════════════════════════════
# Tool registry
# ═══════════════════════════════════════════════════════════════════════════════

TOOL_DEFINITIONS = [
    # ── Catalog tools ────────────────────────────────────────────────
    {
        "name": "catalog_list",
        "description": "List all catalogs for the authenticated organization.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 50, "description": "Max results to return"},
                "offset": {"type": "integer", "default": 0, "description": "Offset for pagination"},
                "organization_id": {"type": "string", "description": "Override organization UUID. The authenticated user must belong to this org."},
            },
        },
    },
    {
        "name": "catalog_get",
        "description": "Get a single catalog by ID, including metadata and item counts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "organization_id": {"type": "string", "description": "Override organization UUID. The authenticated user must belong to this org."},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_delete",
        "description": "Delete a catalog (irreversible). Requires explicit confirmation phrase.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "confirm": {"type": "boolean", "default": False, "description": "Must be true to execute deletion"},
                "confirmation": {"type": "string", "description": "Must exactly match: DELETE <catalog_id>"},
                "expected_name": {"type": "string", "description": "Optional safety check: expected catalog name must match"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_schema_get",
        "description": "Get catalog schema definition with field types, descriptions, scopes, and workflow metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "version": {"type": "string", "description": "Schema version (optional, defaults to current)"},
                "organization_id": {"type": "string", "description": "Override organization UUID"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_taxonomy_get",
        "description": "Get catalog taxonomy with hierarchy, attributes, and inheritance.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "organization_id": {"type": "string", "description": "Override organization UUID"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_update_quality_prompt",
        "description": "Set the catalog's quality prompt (used by the judge tool and readiness checks). Pass the full prompt text to replace the current one.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "prompt": {"type": "string", "description": "Quality prompt text — criteria for judging rows"},
            },
            "required": ["catalog_id", "prompt"],
        },
    },
    {
        "name": "catalog_update_md",
        "description": "Set the catalog's markdown context (catalog_md). This text is auto-injected into enrichment and mapping prompts. Pass the full markdown to replace.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "catalog_md": {"type": "string", "description": "Markdown context to inject into prompts"},
            },
            "required": ["catalog_id", "catalog_md"],
        },
    },
    {
        "name": "catalog_get_md",
        "description": "Get the catalog's markdown context (catalog_md).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_items_query",
        "description": "Query raw catalog rows with pagination, text search, sorting, and product/variant awareness.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "limit": {"type": "integer", "default": 50},
                "offset": {"type": "integer", "default": 0},
                "entity_type": {"type": "string", "enum": ["product", "variant"], "description": "Optional entity type filter"},
                "search": {"type": "string", "description": "Full-text search query"},
                "sort_field": {"type": "string", "description": "Field to sort by"},
                "sort_order": {"type": "string", "enum": ["asc", "desc"], "default": "asc"},
                "organization_id": {"type": "string", "description": "Override organization UUID"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_items_bulk_update",
        "description": "Bulk upsert catalog items. Each item dict should include __catalog_item_id (database UUID) for updates, plus the fields to set/update. Returns summary with items_processed/created/updated/failed counts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "items": {"type": "array", "items": {"type": "object"}, "description": "Array of item dicts with __catalog_item_id + fields to update"},
                "organization_id": {"type": "string", "description": "Override organization UUID"},
            },
            "required": ["catalog_id", "items"],
        },
    },
    {
        "name": "catalog_item_get",
        "description": "Get a single catalog item by ID with full data, entity type, and taxonomy attributes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "item_id": {"type": "string", "description": "Item UUID (database ID)"},
                "organization_id": {"type": "string", "description": "Override organization UUID"},
            },
            "required": ["catalog_id", "item_id"],
        },
    },
    {
        "name": "catalog_item_update",
        "description": "Update a single catalog item's data directly. Use for small, targeted edits (1-5 items). For bulk changes, use the snapshot-diff-stage pipeline instead.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "item_id": {"type": "string", "description": "Item UUID (database ID)"},
                "data": {"type": "object", "description": "Key-value pairs to update in item.data"},
                "organization_id": {"type": "string", "description": "Override organization UUID"},
            },
            "required": ["catalog_id", "item_id", "data"],
        },
    },
    {
        "name": "catalog_activity_list",
        "description": "List activities for a catalog with optional status/type filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID"},
                "status": {"type": "string", "description": "Filter by status (pending_review, completed, etc.)"},
                "activity_type": {"type": "string", "description": "Filter by type (custom_transform, etc.)"},
                "limit": {"type": "integer", "default": 20},
                "offset": {"type": "integer", "default": 0},
                "organization_id": {"type": "string", "description": "Override organization UUID"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_activity_get",
        "description": "Get a single activity by ID with full metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "activity_id": {"type": "string", "description": "Activity UUID"},
            },
            "required": ["activity_id"],
        },
    },
    {
        "name": "catalog_activity_get_staged_changes",
        "description": "Get staged changes for a pending activity. Returns paginated list of before/after data pairs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "activity_id": {"type": "string", "description": "Activity UUID"},
                "limit": {"type": "integer", "default": 50, "description": "Max results per page"},
                "offset": {"type": "integer", "default": 0, "description": "Offset for pagination"},
            },
            "required": ["activity_id"],
        },
    },
    {
        "name": "catalog_visualize_local",
        "description": "Build a local HTML artifact for visually inspecting a catalog, an activity's staged changes, or arbitrary rows, then optionally open it in the browser. Pass 'rows' for quick previews without a catalog.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID to visualize"},
                "activity_id": {"type": "string", "description": "Activity UUID to visualize"},
                "rows": {"type": "array", "items": {"type": "object"}, "description": "Arbitrary row dicts to visualize (no catalog/activity needed). Images are auto-detected from URLs in the data."},
                "mode": {"type": "string", "enum": ["auto", "catalog", "activity"], "default": "auto"},
                "title": {"type": "string", "description": "Optional custom viewer title"},
                "limit": {"type": "integer", "default": 500, "description": "Maximum matching records to load into the artifact"},
                "offset": {"type": "integer", "default": 0, "description": "Offset into the matching record set"},
                "search": {"type": "string", "description": "Optional text search to narrow what the viewer loads"},
                "output_dir": {"type": "string", "default": "./work/visualizations", "description": "Workspace-local directory where the artifact will be written"},
                "open_browser": {"type": "boolean", "default": True, "description": "Best-effort browser open after generating the artifact"},
            },
        },
    },
    {
        "name": "catalog_snapshot_list",
        "description": "List catalog snapshots for rollback/history.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string"},
                "snapshot_type": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "offset": {"type": "integer", "default": 0},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_snapshot_create",
        "description": "Create a manual snapshot for rollback safety.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string"},
                "reason": {"type": "string", "default": "Manual snapshot from MCP"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_snapshot_restore",
        "description": "Restore a catalog to a specific snapshot.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string"},
                "snapshot_id": {"type": "string"},
            },
            "required": ["catalog_id", "snapshot_id"],
        },
    },
    {
        "name": "catalog_duplicate",
        "description": "Duplicate a catalog schema and optionally copy source items.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string"},
                "name": {"type": "string"},
                "description": {"type": "string"},
                "include_items": {"type": "boolean", "default": False},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "catalog_activity_save_workflow",
        "description": "Save an activity as a reusable workflow template (csv_importer -> custom_code).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string"},
                "activity_id": {"type": "string"},
                "workflow_name": {"type": "string"},
                "workflow_description": {"type": "string"},
                "python_code": {"type": "string"},
                "attachments": {"type": "array"},
                "timeout_seconds": {"type": "integer", "default": 120},
            },
            "required": ["catalog_id", "activity_id", "workflow_name"],
        },
    },
    {
        "name": "catalog_activity_create_transform",
        "description": "Create a custom transform activity with staged changes, script provenance, and audit metadata. Validates the bundle, stages all changes into a single pending-review activity (chunked internally if needed), and opens the dashboard review URL.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Target catalog UUID"},
                "activity_message": {"type": "string", "description": "Human-readable description (e.g., 'Q2 price update')"},
                "script": {
                    "type": "object",
                    "description": "Transform script metadata",
                    "properties": {
                        "filename": {"type": "string"},
                        "content": {"type": "string"},
                        "sha256": {"type": "string"},
                    },
                },
                "diff_summary": {"type": "object", "description": "Diff summary from execution_local_diff_compute"},
                "validation_report": {"type": "object", "description": "Validation report from execution_bundle_validate"},
                "staged_changes_inline": {"type": "array", "description": "Inline staged changes array"},
                "staged_changes_file_path": {"type": "string", "description": "Path to staged changes JSONL/JSON/parquet file"},
                "schema_changes": {"type": "object", "description": "Schema changes to apply (Phase 1b)"},
                "taxonomy_changes": {"type": "object", "description": "Taxonomy changes to apply (Phase 1b)"},
                "attachments": {"type": "array", "description": "Attachment metadata. Stored under activity_context.attachments."},
                "activity_context": {"type": "object", "description": "Additional audit context persisted on activity.input.activity_context."},
                "session_context": {"type": "object", "description": "Deprecated alias for activity_context.session_context"},
                "base_snapshot_id": {"type": "string", "description": "Optional snapshot ID for audit trail"},
                "auto_open_review": {"type": "boolean", "default": True, "description": "Open dashboard review URL in browser"},
            },
            "required": ["catalog_id", "activity_message"],
        },
    },
    # ── Service tools ────────────────────────────────────────────────
    {
        "name": "service_map_to_catalog_schema",
        "description": "Map source items to a target catalog schema using AI enrichment. Forces web_search=false.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Target catalog UUID"},
                "items": {"type": "array", "description": "Source items to map"},
                "prompt": {"type": "string", "default": "Map source fields to target catalog schema"},
                "async_mode": {"type": "boolean", "default": False},
                "speed": {"type": "string", "enum": ["fast", "medium", "slow"], "default": "medium"},
            },
            "required": ["catalog_id", "items"],
        },
    },
    {
        "name": "service_judge_catalog_rows",
        "description": "Judge catalog rows for data quality. Supports multimodal: pass explicit image URLs via 'images' to evaluate whether images match row data. Uses the catalog's schema and quality_prompt automatically when catalog_id is provided.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "rows": {"type": "array", "description": "Rows to evaluate"},
                "catalog_id": {"type": "string", "description": "Catalog UUID (loads schema + quality_prompt automatically)"},
                "schema": {"type": "object", "description": "Inline schema (only if no catalog_id)"},
                "prompt": {"type": "string", "description": "Extra judging instructions (appended to catalog's quality_prompt)"},
                "model": {"type": "string", "default": "fast", "description": "Model preset: fast, medium, high"},
                "max_rows": {"type": "integer", "default": 200},
                "images": {"type": "object", "description": "Explicit image URLs per row index, e.g. {\"0\": [\"https://...\"]}. When provided, uses vision model to evaluate image-data consistency."},
            },
            "required": ["rows"],
        },
    },
    {
        "name": "service_image_host",
        "description": "Download an external image and host it on Supabase Storage. Returns the hosted URL.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source_url": {"type": "string", "description": "External image URL to download and host"},
            },
            "required": ["source_url"],
        },
    },
    {
        "name": "service_image_run",
        "description": "Submit an image editing job. Supports: generate, edit, inpaint, bg_remove, relight, upscale.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tool": {"type": "string", "enum": ["generate", "edit", "inpaint", "bg_remove", "relight", "upscale"], "description": "Image operation type"},
                "image_url": {"type": "string", "description": "Input image URL (required for edit/inpaint/bg_remove/relight/upscale)"},
                "mask_url": {"type": "string", "description": "Mask URL (required for inpaint)"},
                "prompt": {"type": "string", "description": "Text prompt (required for generate/edit/inpaint)"},
                "prompt_image_urls": {"type": "array", "items": {"type": "string"}, "description": "Additional reference images for multi-image prompts (e.g., a color swatch to match)"},
                "provider": {"type": "string", "description": "Model provider override"},
                "quality": {"type": "string", "enum": ["high", "low"]},
                "size": {"type": "string", "description": "Output size"},
                "num_options": {"type": "integer", "default": 1},
                "catalog_id": {"type": "string", "description": "Catalog ID for tracking"},
                "item_id": {"type": "string", "description": "Item ID for tracking"},
            },
            "required": ["tool"],
        },
    },
    {
        "name": "service_image_status",
        "description": "Get status/progress/result of an image editing run.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "Run UUID"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "service_image_list",
        "description": "List image editing runs with optional filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string"},
                "item_id": {"type": "string"},
                "status": {"type": "string"},
                "tool": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "offset": {"type": "integer", "default": 0},
            },
        },
    },
    {
        "name": "service_image_review",
        "description": "Open a local browser UI to review image generation results. Pass run IDs from service_image_run and optional context per run (product title, finish, etc.). Supports reviewing many runs in parallel.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_ids": {"type": "array", "items": {"type": "string"}, "description": "List of image editor run IDs to review"},
                "contexts": {"type": "object", "description": "Optional dict mapping run_id to context (e.g. {\"run-id\": {\"title\": \"CL-115 Rust\", \"finish\": \"Rust\"}})"},
                "title": {"type": "string", "default": "Image Generation Review"},
            },
            "required": ["run_ids"],
        },
    },
    # ── Execution tools ──────────────────────────────────────────────
    {
        "name": "execution_catalog_snapshot_pull",
        "description": "Export catalog rows + schema to local files (parquet/csv) for Python transforms. Uses fast parallel pagination with retries and safe fallback.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID to snapshot"},
                "output_dir": {"type": "string", "default": "./work/snapshots"},
                "format": {"type": "string", "enum": ["parquet", "csv"], "default": "parquet"},
                "sample_size": {"type": "integer", "description": "Limit rows for sampling (null = all)"},
                "page_size": {"type": "integer", "default": 400},
                "max_concurrency": {"type": "integer", "default": 8, "description": "Parallel page fetch concurrency"},
                "prefer_raw": {"type": "boolean", "default": True, "description": "Prefer /raw-items for full-fidelity row pulls"},
            },
            "required": ["catalog_id"],
        },
    },
    {
        "name": "execution_catalog_stage_dataset",
        "description": "One-command staging: compute diff from before/after datasets and create one pending-review activity. For product_grouped catalogs, validates that every variant's product_id has a matching product parent row.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Target catalog UUID"},
                "before_path": {"type": "string", "description": "Path to before dataset (parquet/csv)"},
                "after_path": {"type": "string", "description": "Path to after dataset (parquet/csv)"},
                "activity_message": {"type": "string", "description": "Human-readable review message"},
                "key_field": {"type": "string", "default": "__catalog_item_id", "description": "Column used to match rows. Null-key rows are treated as new inserts."},
                "script_path": {"type": "string", "description": "Optional Python script path for audit provenance"},
                "schema_changes": {"type": "object"},
                "taxonomy_changes": {"type": "object"},
                "attachments": {"type": "array"},
                "activity_context": {"type": "object"},
                "auto_open_review": {"type": "boolean", "default": True},
            },
            "required": ["catalog_id", "before_path", "after_path", "activity_message"],
        },
    },
    {
        "name": "execution_local_diff_compute",
        "description": "Compute row/field diff between before/after local datasets. Outputs staged_changes.jsonl for activity creation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "before_path": {"type": "string", "description": "Path to before dataset (parquet/csv)"},
                "after_path": {"type": "string", "description": "Path to after dataset (parquet/csv)"},
                "key_field": {
                    "type": "string",
                    "default": "__catalog_item_id",
                    "description": "Column used to match rows between datasets. Rows with null key are treated as new inserts. Use a business key (e.g. SKU column) to match by that field instead of database ID.",
                },
            },
            "required": ["before_path", "after_path"],
        },
    },
    {
        "name": "execution_bundle_validate",
        "description": "Validate a transform bundle before activity creation. Checks files, schema compatibility, row counts, and policy rules.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog_id": {"type": "string", "description": "Catalog UUID for schema validation"},
                "before_path": {"type": "string"},
                "after_path": {"type": "string"},
                "script_path": {"type": "string"},
                "staged_changes_path": {"type": "string"},
                "diff_summary": {"type": "object"},
                "schema_changes": {"type": "object"},
                "taxonomy_changes": {"type": "object"},
                "rules": {
                    "type": "object",
                    "properties": {
                        "allow_row_deletes": {"type": "boolean", "default": False},
                        "max_change_ratio_warning": {"type": "number", "default": 0.2},
                    },
                },
            },
            "required": ["catalog_id"],
        },
    },
]

if not DIRECT_ITEM_UPDATE_ENABLED:
    TOOL_DEFINITIONS = [t for t in TOOL_DEFINITIONS if t.get("name") != "catalog_item_update"]


# ═══════════════════════════════════════════════════════════════════════════════
# Tool dispatch
# ═══════════════════════════════════════════════════════════════════════════════


async def dispatch_tool(client: RastroClient, tool_name: str, arguments: Dict[str, Any]) -> Any:
    """Dispatch a tool call to the appropriate handler."""
    # Catalog tools
    if tool_name == "catalog_list":
        return await catalog_list(client, CatalogListInput(**arguments))
    elif tool_name == "catalog_get":
        return await catalog_get(client, CatalogGetInput(**arguments))
    elif tool_name == "catalog_delete":
        return await catalog_delete(client, CatalogDeleteInput(**arguments))
    elif tool_name == "catalog_schema_get":
        return await catalog_schema_get(client, CatalogSchemaGetInput(**arguments))
    elif tool_name == "catalog_taxonomy_get":
        return await catalog_taxonomy_get(client, CatalogTaxonomyGetInput(**arguments))
    elif tool_name == "catalog_update_quality_prompt":
        return await catalog_update_quality_prompt(client, CatalogUpdateQualityPromptInput(**arguments))
    elif tool_name == "catalog_update_md":
        return await catalog_update_md(client, CatalogUpdateMdInput(**arguments))
    elif tool_name == "catalog_get_md":
        return await catalog_get_md(client, CatalogGetMdInput(**arguments))
    elif tool_name == "catalog_items_query":
        return await catalog_items_query(client, CatalogItemsQueryInput(**arguments))
    elif tool_name == "catalog_item_get":
        return await catalog_item_get(client, CatalogItemGetInput(**arguments))
    elif tool_name == "catalog_item_update":
        if not DIRECT_ITEM_UPDATE_ENABLED:
            raise ValueError("catalog_item_update is disabled by default. " "Use catalog_activity_create_transform for safe staged edits.")
        return await catalog_item_update(client, CatalogItemUpdateInput(**arguments))
    elif tool_name == "catalog_items_bulk_update":
        return await catalog_items_bulk_update(client, CatalogItemsBulkUpdateInput(**arguments))
    elif tool_name == "catalog_activity_list":
        return await catalog_activity_list(client, CatalogActivityListInput(**arguments))
    elif tool_name == "catalog_activity_get":
        return await catalog_activity_get(client, CatalogActivityGetInput(**arguments))
    elif tool_name == "catalog_activity_get_staged_changes":
        return await catalog_activity_get_staged_changes(client, CatalogActivityGetStagedChangesInput(**arguments))
    elif tool_name == "catalog_visualize_local":
        result = await catalog_visualize_local(client, CatalogVisualizeLocalInput(**arguments))
        return result.model_dump()
    elif tool_name == "catalog_snapshot_list":
        return await catalog_snapshot_list(client, CatalogSnapshotListInput(**arguments))
    elif tool_name == "catalog_snapshot_create":
        return await catalog_snapshot_create(client, CatalogSnapshotCreateInput(**arguments))
    elif tool_name == "catalog_snapshot_restore":
        return await catalog_snapshot_restore(client, CatalogSnapshotRestoreInput(**arguments))
    elif tool_name == "catalog_duplicate":
        return await catalog_duplicate(client, CatalogDuplicateInput(**arguments))
    elif tool_name == "catalog_activity_save_workflow":
        return await catalog_activity_save_workflow(client, CatalogActivitySaveWorkflowInput(**arguments))
    elif tool_name == "catalog_activity_create_transform":
        result = await catalog_activity_create_transform(client, CatalogActivityCreateTransformInput(**arguments))
        return result.model_dump()

    # Service tools
    elif tool_name == "service_map_to_catalog_schema":
        return await service_map_to_catalog_schema(client, ServiceMapToCatalogSchemaInput(**arguments))
    elif tool_name == "service_judge_catalog_rows":
        return await service_judge_catalog_rows(client, ServiceJudgeCatalogRowsInput(**arguments))
    elif tool_name == "service_image_host":
        return await service_image_host(client, ServiceImageHostInput(**arguments))
    elif tool_name == "service_image_run":
        return await service_image_run(client, ServiceImageRunInput(**arguments))
    elif tool_name == "service_image_status":
        return await service_image_status(client, ServiceImageStatusInput(**arguments))
    elif tool_name == "service_image_list":
        return await service_image_list(client, ServiceImageListInput(**arguments))
    elif tool_name == "service_image_review":
        return await image_review_local(
            client,
            run_ids=arguments.get("run_ids", []),
            contexts=arguments.get("contexts"),
            title=arguments.get("title", "Image Generation Review"),
        )

    # Execution tools
    elif tool_name == "execution_catalog_snapshot_pull":
        result = await execution_catalog_snapshot_pull(client, SnapshotPullInput(**arguments))
        return result.model_dump()
    elif tool_name == "execution_catalog_stage_dataset":
        result = await execution_catalog_stage_dataset(client, StageDatasetInput(**arguments))
        return result.model_dump()
    elif tool_name == "execution_local_diff_compute":
        result = await execution_local_diff_compute(DiffComputeInput(**arguments))
        return result.model_dump()
    elif tool_name == "execution_bundle_validate":
        result = await execution_bundle_validate(client, BundleValidateInput(**arguments))
        return result.model_dump()

    else:
        raise ValueError(f"Unknown tool: {tool_name}")


# ═══════════════════════════════════════════════════════════════════════════════
# MCP stdio transport server
# ═══════════════════════════════════════════════════════════════════════════════


def _load_master_prompt() -> str:
    """Load master prompt from markdown file with a compact fallback."""
    prompt_path = os.path.join(os.path.dirname(__file__), "prompts", "master_prompt.md")
    try:
        with open(prompt_path) as f:
            return f.read()
    except FileNotFoundError:
        return "You are a Rastro catalog transform agent. " "Use catalog/service/execution tools, show diffs, and route final apply through dashboard review."


MASTER_PROMPT = _load_master_prompt()


async def handle_jsonrpc_message(client: RastroClient, message: dict) -> dict:
    """Handle a single JSON-RPC message."""
    method = message.get("method", "")
    msg_id = message.get("id")
    params = message.get("params", {})

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {"listChanged": False},
                    "prompts": {"listChanged": False},
                },
                "serverInfo": {
                    "name": "rastro-mcp",
                    "version": "0.1.0",
                },
            },
        }

    elif method == "notifications/initialized":
        return None  # No response for notifications

    elif method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"tools": TOOL_DEFINITIONS},
        }

    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        try:
            result = await dispatch_tool(client, tool_name, arguments)
            content_text = json.dumps(result, default=str, indent=2) if isinstance(result, (dict, list)) else str(result)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": content_text}],
                    "isError": False,
                },
            }
        except Exception as e:
            print(f"[rastro-mcp] Tool call failed ({tool_name}): {type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": "Tool execution failed. Check MCP server logs for details."}],
                    "isError": True,
                },
            }

    elif method == "prompts/list":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "prompts": [
                    {
                        "name": "master_prompt",
                        "description": "Single master prompt for the Rastro MCP workflow.",
                    }
                ]
            },
        }

    elif method == "prompts/get":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "messages": [
                    {
                        "role": "user",
                        "content": {"type": "text", "text": MASTER_PROMPT},
                    }
                ]
            },
        }

    else:
        if msg_id is not None:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }
        return None


async def run_stdio_server():
    """Run the MCP server over stdin/stdout using JSON-RPC."""
    auth = load_auth_from_env()
    client = RastroClient(auth)

    try:
        async for line in _iter_stdio_lines(sys.stdin):
            if not line:
                break

            line_str = line.decode("utf-8").strip()
            if not line_str:
                continue

            try:
                message = json.loads(line_str)
            except json.JSONDecodeError:
                print(f"[rastro-mcp] Ignoring invalid JSON-RPC line: {line_str[:200]}", file=sys.stderr)
                continue

            response = await handle_jsonrpc_message(client, message)
            if response is not None:
                response_str = json.dumps(response, default=str) + "\n"
                sys.stdout.write(response_str)
                sys.stdout.flush()

    finally:
        await client.close()


async def _iter_stdio_lines(stdin: Any) -> AsyncIterator[bytes]:
    """Yield stdio lines using a thread-based reader for cross-runtime stability."""
    buffer = getattr(stdin, "buffer", stdin)
    while True:
        line = await asyncio.to_thread(buffer.readline)
        if isinstance(line, str):
            line = line.encode("utf-8")
        if not line:
            break
        yield line


def main():
    """Entry point for the MCP server."""
    asyncio.run(run_stdio_server())


if __name__ == "__main__":
    main()
