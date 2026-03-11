# Rastro MCP

MCP server for Rastro catalog operations. Exposes catalog, service, execution, and local visualization tools for use with Claude, Codex, or any MCP-compatible client.

## Install

**From PyPI** (once published):
```bash
pip install rastro.ai
# or
uv add rastro.ai
```

**From source:**
```bash
git clone https://github.com/Rastro-AI/rastro-mcp
cd rastro-mcp
uv sync
```

## Required Environment
```bash
export RASTRO_API_KEY=rastro_pk_...        # Required
export RASTRO_BASE_URL=https://catalogapi.rastro.ai/api  # Production (default)
# export RASTRO_BASE_URL=http://127.0.0.1:8000/api       # Local dev
# export RASTRO_ORGANIZATION_ID=<uuid>                    # Optional, derived from key
```

## Run

```bash
# stdio transport (for MCP clients)
uv run python -m rastro_mcp.server

# or via CLI entry point (if installed from package)
rastro-mcp
```

### Claude Code / Claude Desktop config

Add to `.mcp.json` or `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "rastro": {
      "command": "uv",
      "args": ["run", "python", "-m", "rastro_mcp.server"],
      "env": {
        "RASTRO_API_KEY": "rastro_pk_...",
        "RASTRO_BASE_URL": "https://catalogapi.rastro.ai/api"
      }
    }
  }
}
```

## Tool Reference

### Catalog Tools

#### `catalog_list`
List all catalogs for the authenticated organization.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 50 | Max results to return |
| `offset` | integer | 0 | Pagination offset |

#### `catalog_get`
Get a single catalog by ID, including metadata and item counts.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |

#### `catalog_delete`
Delete a catalog (irreversible). Requires explicit confirmation phrase.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `confirm` | boolean | no | Must be `true` to execute |
| `confirmation` | string | no | Must exactly match `DELETE <catalog_id>` |
| `expected_name` | string | no | Safety check: must match catalog name |

#### `catalog_duplicate`
Duplicate a catalog schema and optionally copy source items.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Source catalog UUID |
| `name` | string | no | Name for the new catalog |
| `description` | string | no | Description for the new catalog |
| `include_items` | boolean | no | Copy items (default: false) |

#### `catalog_schema_get`
Get catalog schema definition with field types, descriptions, scopes, and workflow metadata.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `version` | string | no | Schema version (default: current) |

#### `catalog_taxonomy_get`
Get catalog taxonomy with hierarchy, attributes, and inheritance.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |

#### `catalog_update_quality_prompt`
Set the catalog's quality prompt used by the judge tool and readiness checks. Replaces the current prompt entirely.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `prompt` | string | yes | Quality prompt text (criteria for judging rows) |

#### `catalog_items_query`
Query catalog items with pagination, text search, and field sorting.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `limit` | integer | no | Max results (default: 50) |
| `offset` | integer | no | Pagination offset (default: 0) |
| `search` | string | no | Full-text search query |
| `sort_field` | string | no | Field to sort by |
| `sort_order` | string | no | `asc` or `desc` (default: asc) |

#### `catalog_item_get`
Get a single catalog item by ID with full data, entity type, and taxonomy attributes.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `item_id` | string | yes | Item UUID (database ID) |

#### `catalog_item_update`
Update a single catalog item's data directly. **Disabled by default** -- use `catalog_activity_create_transform` instead.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `item_id` | string | yes | Item UUID |
| `data` | object | yes | Key-value pairs to update |

Enable with `RASTRO_MCP_ENABLE_DIRECT_ITEM_UPDATE=true` (break-glass only).

#### `catalog_activity_list`
List activities for a catalog with optional status/type filters.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `status` | string | no | Filter: `pending_review`, `completed`, etc. |
| `activity_type` | string | no | Filter: `custom_transform`, etc. |
| `limit` | integer | no | Max results (default: 20) |
| `offset` | integer | no | Pagination offset (default: 0) |

#### `catalog_activity_get`
Get a single activity by ID with full metadata.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `activity_id` | string | yes | Activity UUID |

#### `catalog_activity_get_staged_changes`
Get staged changes for a pending activity. Returns paginated before/after data pairs.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `activity_id` | string | yes | Activity UUID |
| `limit` | integer | no | Max results per page (default: 50) |
| `offset` | integer | no | Pagination offset (default: 0) |

#### `catalog_visualize_local`
Build a self-contained local HTML viewer for either a catalog or an activity's staged changes, then optionally open it in the default browser.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | conditional | Catalog UUID to visualize |
| `activity_id` | string | conditional | Activity UUID to visualize |
| `mode` | string | no | `auto`, `catalog`, or `activity` (default: auto) |
| `title` | string | no | Custom title shown in the viewer |
| `limit` | integer | no | Max matching records to load (default: 500) |
| `offset` | integer | no | Offset into the matching records (default: 0) |
| `search` | string | no | Optional search filter passed to the backend |
| `output_dir` | string | no | Artifact output directory (default: `./work/visualizations`) |
| `open_browser` | boolean | no | Best-effort browser open after artifact generation (default: true) |

Outputs a `bundle.json`, a static `viewer.html`, and a localhost `viewer_url`. Prefer the `viewer_url` over opening `viewer.html` directly; the localhost path enables the local media proxy for remote images/documents. The artifact files are still usable when browser launch fails (for example in headless shells).

#### `catalog_activity_create_transform`
Create a custom transform activity with staged changes, script provenance, and audit metadata. Validates the bundle, stages all changes into a single pending-review activity (chunked internally if needed), and opens the dashboard review URL.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Target catalog UUID |
| `activity_message` | string | yes | Human-readable description |
| `script` | object | no | `{filename, content, sha256}` for audit |
| `diff_summary` | object | no | From `execution_local_diff_compute` |
| `validation_report` | object | no | From `execution_bundle_validate` |
| `staged_changes_inline` | array | no | Inline staged changes |
| `staged_changes_file_path` | string | no | Path to `.jsonl`/`.json`/`.parquet` file |
| `schema_changes` | object | no | Schema changes to apply |
| `taxonomy_changes` | object | no | Taxonomy changes to apply |
| `attachments` | array | no | Stored in `activity_context.attachments` |
| `activity_context` | object | no | Additional audit context |
| `base_snapshot_id` | string | no | Snapshot ID for audit trail |
| `auto_open_review` | boolean | no | Open dashboard URL (default: true) |

#### `catalog_activity_save_workflow`
Save an activity as a reusable workflow template.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `activity_id` | string | yes | Activity UUID |
| `workflow_name` | string | yes | Name for the saved workflow |
| `workflow_description` | string | no | Description |
| `python_code` | string | no | Transform code |
| `attachments` | array | no | Attachment metadata |
| `timeout_seconds` | integer | no | Execution timeout (default: 120) |

#### `catalog_snapshot_list`
List catalog snapshots for rollback/history.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `snapshot_type` | string | no | Filter by type |
| `limit` | integer | no | Max results (default: 20) |
| `offset` | integer | no | Pagination offset (default: 0) |

#### `catalog_snapshot_create`
Create a manual snapshot for rollback safety.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `reason` | string | no | Reason (default: "Manual snapshot from MCP") |

#### `catalog_snapshot_restore`
Restore a catalog to a specific snapshot.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `snapshot_id` | string | yes | Snapshot UUID to restore |

### Service Tools

#### `service_map_to_catalog_schema`
Map source items to a target catalog schema using AI enrichment. Always runs with `web_search=false`.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Target catalog UUID |
| `items` | array | yes | Source items to map |
| `prompt` | string | no | Mapping instructions (default: "Map source fields to target catalog schema") |
| `async_mode` | boolean | no | Run asynchronously (default: false) |
| `speed` | string | no | `fast`, `medium`, or `slow` (default: medium) |

#### `service_judge_catalog_rows`
Judge catalog rows for data quality. When `catalog_id` is provided, automatically loads the catalog's schema and quality_prompt (from readiness_config).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `rows` | array | yes | Rows to evaluate (max 500) |
| `catalog_id` | string | no | Catalog UUID (loads schema + quality_prompt automatically) |
| `schema` | object | no | Inline schema (only needed if no catalog_id) |
| `prompt` | string | no | Extra instructions (appended to catalog's quality_prompt) |
| `model` | string | no | Model preset (default: fast) |
| `max_rows` | integer | no | Max rows per request (default: 200) |

#### `service_image_run`
Submit an image editing job. Supports: generate, edit, inpaint, bg_remove, relight, upscale.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tool` | string | yes | `generate`, `edit`, `inpaint`, `bg_remove`, `relight`, `upscale` |
| `image_url` | string | conditional | Input image (required for edit/inpaint/bg_remove/relight/upscale) |
| `mask_url` | string | conditional | Mask image (required for inpaint) |
| `prompt` | string | conditional | Text prompt (required for generate/edit/inpaint) |
| `provider` | string | no | Model provider override |
| `quality` | string | no | `high` or `low` |
| `size` | string | no | Output size |
| `num_options` | integer | no | Number of results (default: 1) |
| `catalog_id` | string | no | Catalog ID for tracking |
| `item_id` | string | no | Item ID for tracking |

#### `service_image_status`
Get status/progress/result of an image editing run.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | string | yes | Run UUID |

#### `service_image_list`
List image editing runs with optional filters.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | no | Filter by catalog |
| `item_id` | string | no | Filter by item |
| `status` | string | no | Filter by status |
| `tool` | string | no | Filter by tool type |
| `limit` | integer | no | Max results (default: 20) |
| `offset` | integer | no | Pagination offset (default: 0) |

### Execution Tools (run locally)

#### `execution_catalog_snapshot_pull`
Export catalog rows + schema to local files (parquet/csv) for Python transforms. Uses fast parallel pagination.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID |
| `output_dir` | string | no | Output directory (default: `./snapshots`) |
| `format` | string | no | `parquet` or `csv` (default: parquet) |
| `sample_size` | integer | no | Limit rows for sampling (null = all) |
| `page_size` | integer | no | Rows per page (default: 400) |
| `max_concurrency` | integer | no | Parallel fetch threads (default: 8) |
| `prefer_raw` | boolean | no | Use `/raw-items` for full fidelity (default: true) |

#### `execution_local_diff_compute`
Compute row/field diff between before/after local datasets. Outputs `staged_changes.jsonl`.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `before_path` | string | yes | Path to before dataset (parquet/csv) |
| `after_path` | string | yes | Path to after dataset (parquet/csv) |
| `key_field` | string | no | Row matching column (default: `__catalog_item_id`). Null-key rows = new inserts. Use a business key (e.g. SKU) to match by that field. |

#### `execution_bundle_validate`
Validate a transform bundle before activity creation. Checks file existence, schema compatibility, row counts, product-variant integrity, and policy rules.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Catalog UUID for schema validation |
| `before_path` | string | no | Before dataset path |
| `after_path` | string | no | After dataset path |
| `script_path` | string | no | Transform script path |
| `staged_changes_path` | string | no | Staged changes file path |
| `diff_summary` | object | no | Diff summary from `execution_local_diff_compute` |
| `schema_changes` | object | no | Proposed schema changes |
| `taxonomy_changes` | object | no | Proposed taxonomy changes |
| `rules.allow_row_deletes` | boolean | no | Allow row deletions (default: false) |
| `rules.max_change_ratio_warning` | number | no | Warn above this change ratio (default: 0.2) |

#### `execution_catalog_stage_dataset`
One-command staging: compute diff from before/after datasets, validate bundle (including product-variant integrity), and create one pending-review activity.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `catalog_id` | string | yes | Target catalog UUID |
| `before_path` | string | yes | Before dataset path (parquet/csv) |
| `after_path` | string | yes | After dataset path (parquet/csv) |
| `activity_message` | string | yes | Human-readable review message |
| `key_field` | string | no | Row matching column (default: `__catalog_item_id`) |
| `script_path` | string | no | Python script path for audit provenance |
| `schema_changes` | object | no | Schema changes to apply |
| `taxonomy_changes` | object | no | Taxonomy changes to apply |
| `attachments` | array | no | Attachment metadata |
| `activity_context` | object | no | Additional audit context |
| `auto_open_review` | boolean | no | Open dashboard URL (default: true) |

### Prompts

#### `master_prompt`
Single unified prompt guiding the agent through catalog operations, product-variant handling, safety rules, and key field matching. Loaded from `rastro_mcp/prompts/master_prompt.md`.

## Typical Workflow

```
1. catalog_get / catalog_schema_get      -- understand the catalog
2. catalog_visualize_local               -- inspect catalog state or staged changes visually when useful
3. execution_catalog_snapshot_pull       -- pull data to local parquet
4. (your Python transform script)        -- modify the data
5. execution_catalog_stage_dataset       -- diff + validate + stage
6. Review in dashboard                   -- approve and apply
```

## Large Catalog Behavior
- `execution_catalog_snapshot_pull` prefers raw catalog rows (`product` + `variant`) via `/raw-items`, then falls back to legacy `/items`.
- `catalog_activity_create_transform` stages large change sets into **one activity** by chunk-appending staged changes, then finalizes to `pending_review`.
- `execution_catalog_stage_dataset` computes diff + stages everything into one pending-review activity in a single command.

## Safety Defaults

- **`catalog_item_update` disabled** -- backend PUT replaces full item data. Override: `RASTRO_MCP_ENABLE_DIRECT_ITEM_UPDATE=true`
- **Programmatic approve/apply disabled** -- review and apply from dashboard only.
- **Activity-first workflow** -- all writes go through staging for review.
- **Bundle validation** -- automatic schema, row count, and product-variant integrity checks.
- **Path safety** -- execution tools validate paths to prevent directory traversal.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RASTRO_API_KEY` | (required) | API key (`rastro_pk_*` prefix) |
| `RASTRO_BASE_URL` | `https://api.rastro.ai/api` | API base URL |
| `RASTRO_ORGANIZATION_ID` | (from key) | Organization UUID override |
| `RASTRO_MCP_ENABLE_DIRECT_ITEM_UPDATE` | `false` | Enable direct item PUT |
| `RASTRO_MCP_STAGE_BATCH_SIZE` | `2000` | Chunk size for staging large activities |
| `RASTRO_MCP_STAGE_RETRIES` | `3` | Retry count for staging chunks |
