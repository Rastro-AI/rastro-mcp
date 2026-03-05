# rastro-mcp

Standalone MCP server for Rastro catalog workflows.

## What this is

- API-key-only MCP server for Codex/Claude
- No direct database access needed
- Activity-first safety model (stage -> review in dashboard -> apply)

## Install

```bash
git clone https://github.com/Rastro-AI/rastro-mcp.git
cd rastro-mcp
uv sync
```

## Required environment

```bash
export RASTRO_API_KEY="rastro_pk_..."
export RASTRO_BASE_URL="https://catalogapi.rastro.ai/api"
# optional
# export RASTRO_ORGANIZATION_ID="<org_uuid>"
```

## Run (stdio)

```bash
uv run rastro-mcp
```

Equivalent:

```bash
uv run python -m rastro_mcp.server
```

## Codex / Claude MCP config

Use a workspace `.mcp.json` entry like:

```json
{
  "mcpServers": {
    "rastro": {
      "command": "bash",
      "args": [
        "-lc",
        "cd /ABSOLUTE/PATH/rastro-mcp && export RASTRO_API_KEY='rastro_pk_...' && export RASTRO_BASE_URL='https://catalogapi.rastro.ai/api' && uv run rastro-mcp"
      ]
    }
  }
}
```

## Safety defaults

- Programmatic approve/apply is disabled.
- Use dashboard review URL to approve/apply.
- Large staged updates are chunked internally into one activity.

## Docs

- Quickstart: https://docs.rastro.ai/mcp/quickstart
- Reference: https://docs.rastro.ai/mcp/reference

## License

MIT
