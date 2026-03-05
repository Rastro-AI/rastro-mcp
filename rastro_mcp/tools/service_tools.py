"""
service_* MCP tools - Wrappers for Rastro service APIs.

Tools:
- service_map_to_catalog_schema
- service_image_run
- service_image_status
- service_image_list
"""

from typing import Any, Dict, List, Optional

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.models.contracts import (
    ServiceImageListInput,
    ServiceImageRunInput,
    ServiceImageStatusInput,
    ServiceMapToCatalogSchemaInput,
)


async def service_map_to_catalog_schema(client: RastroClient, params: ServiceMapToCatalogSchemaInput) -> dict:
    """Map source items to a target catalog schema using the enrich API.

    Forces web_search=false and predict_taxonomy=false per spec.
    Builds output_schema from the target catalog schema fields.
    """
    # Fetch catalog schema to build output_schema
    schema = await client.get_catalog_schema(params.catalog_id)
    properties = schema.get("schema_definition", {}).get("properties", {})

    output_schema = []
    for field_name, field_def in properties.items():
        output_schema.append({
            "name": field_name,
            "type": field_def.get("type", "string"),
            "description": field_def.get("description", ""),
        })

    # Build enrich request
    enrich_payload: Dict[str, Any] = {
        "items": params.items,
        "output_schema": output_schema,
        "prompt": params.prompt,
        "speed": params.speed,
        "web_search": False,
        "predict_taxonomy": False,
        "staged_status": "draft",
        "include_service_usage": True,
        "catalog_id": params.catalog_id,
    }

    if params.async_mode:
        enrich_payload["async_mode"] = True

    result = await client.enrich(enrich_payload)
    return result


async def service_image_run(client: RastroClient, params: ServiceImageRunInput) -> dict:
    """Submit an image editing job.

    Supports: generate, edit, inpaint, bg_remove, relight, upscale.
    """
    payload: Dict[str, Any] = {
        "tool": params.tool,
        "num_options": params.num_options,
    }

    if params.image_url:
        payload["image_url"] = params.image_url
    if params.mask_url:
        payload["mask_url"] = params.mask_url
    if params.prompt:
        payload["prompt"] = params.prompt
    if params.provider:
        payload["provider"] = params.provider
    if params.quality:
        payload["quality"] = params.quality
    if params.size:
        payload["size"] = params.size
    if params.catalog_id:
        payload["catalog_id"] = params.catalog_id
    if params.item_id:
        payload["item_id"] = params.item_id

    return await client.image_run(payload)


async def service_image_status(client: RastroClient, params: ServiceImageStatusInput) -> dict:
    """Get status of an image editing run."""
    return await client.image_status(params.run_id)


async def service_image_list(client: RastroClient, params: ServiceImageListInput) -> dict:
    """List image editing runs with filters."""
    return await client.image_list(
        catalog_id=params.catalog_id,
        item_id=params.item_id,
        status=params.status,
        tool=params.tool,
        limit=params.limit,
        offset=params.offset,
    )
