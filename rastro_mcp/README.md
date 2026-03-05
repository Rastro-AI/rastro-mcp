# Rastro MCP

Minimal MCP server for Rastro catalog operations.

## Install
From repo root:
```bash
uv sync
```

## Required Environment
```bash
export RASTRO_API_KEY=<your_api_key>
# Optional override for local development:
# export RASTRO_BASE_URL=http://127.0.0.1:8000/api
# Production default:
# export RASTRO_BASE_URL=https://catalogapi.rastro.ai/api
```

## Run MCP Server (stdio)
```bash
uv run python -m rastro_mcp.server
```

## Tools
- `catalog_*`: catalog context + activity staging
- `service_*`: mapping/image services
- `execution_*`: snapshot, diff, bundle validation

### Large Catalog / Large Diff Behavior
- `execution_catalog_snapshot_pull` now prefers raw catalog rows (`product` + `variant`) via `/public/catalogs/{id}/raw-items`, then falls back to legacy transformed `/public/catalogs/{id}/items`.
- `catalog_activity_create_transform` now stages large change sets into **one activity** by chunk-appending staged changes behind the scenes, then finalizes to `pending_review`.
- The MCP command stays one-shot even when the backend needs multiple append calls internally.
- `execution_catalog_stage_dataset` computes diff + stages everything into one pending-review activity in a single command.

## Prompt
The server exposes one prompt only:
- `master_prompt` (`rastro_mcp/prompts/master_prompt.md`)

## Notes
- Activity-first workflow: stage changes, review in dashboard, then apply.
- For low-cost proof runs, keep tasks at <=5 records.

## Safety Defaults
- `catalog_item_update` is disabled by default because public PUT replaces full item data.
  - Override only for break-glass runs: `RASTRO_MCP_ENABLE_DIRECT_ITEM_UPDATE=true`
- Programmatic staged-change approve/apply is disabled in MCP.
  - Review, approve, and apply from dashboard only.
- Stage chunk size for very large activities can be tuned:
  - `RASTRO_MCP_STAGE_BATCH_SIZE` (default: `2000`)
  - `RASTRO_MCP_STAGE_RETRIES` (default: `3`)
- Pull concurrency can be tuned for large catalogs:
  - `RASTRO_MCP_PULL_MAX_CONCURRENCY` (default: `8`)
