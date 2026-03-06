"""
service_* MCP tools - Wrappers for Rastro service APIs.

Tools:
- service_map_to_catalog_schema
- service_judge_catalog_rows
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
    ServiceJudgeCatalogRowsInput,
    ServiceMapToCatalogSchemaInput,
)


SCHEMA_SAFE_MAPPING_GUARDRAILS = """
CRITICAL SCHEMA MAPPING RULES:
- Only map a value when it semantically matches the target field definition.
- If there is no exact/defensible mapping, leave the field null.
- Never repurpose fields as generic text buckets just to maximize fill rate.
- Respect field-specific constraints (enum, units, array element type, required intent).
- For fields described as JSON-encoded arrays, return valid JSON array strings only when the source truly supports it; otherwise return null.
- Do not copy source column names into unrelated target fields.
- Prefer precision over coverage. Missing is better than wrong.
""".strip()


def _normalize_schema_type(type_value: Any) -> str:
    if isinstance(type_value, str) and type_value.strip():
        return type_value.strip()
    if isinstance(type_value, list):
        for candidate in type_value:
            if isinstance(candidate, str) and candidate != "null":
                return candidate
    return "string"


def _extract_array_element_type(field_def: Dict[str, Any]) -> Optional[str]:
    items = field_def.get("items")
    if not isinstance(items, dict):
        return None
    item_type = _normalize_schema_type(items.get("type"))
    return item_type if item_type and item_type != "object" else None


def _compose_field_description(field_name: str, field_def: Dict[str, Any]) -> str:
    base_description = str(field_def.get("description", "") or "").strip()
    if not base_description:
        base_description = f"Populate `{field_name}` from source data when confidently available."

    metadata: List[str] = []
    scope = field_def.get("x-field-scope")
    if scope:
        metadata.append(f"scope={scope}")
    category = field_def.get("x-field-category")
    if category:
        metadata.append(f"category={category}")
    source = field_def.get("x-field-source")
    if source:
        metadata.append(f"source={source}")

    if metadata:
        base_description += f" Metadata: {', '.join(metadata)}."

    return base_description


def _build_output_schema(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    schema_def = schema.get("schema_definition", {}) or {}
    properties = schema_def.get("properties", {}) or {}
    required_fields = set(schema_def.get("required", []) or [])

    output_schema: List[Dict[str, Any]] = []
    for field_name, field_def_raw in properties.items():
        field_def = field_def_raw if isinstance(field_def_raw, dict) else {}
        normalized_type = _normalize_schema_type(field_def.get("type"))

        field_payload: Dict[str, Any] = {
            "name": field_name,
            "type": normalized_type,
            "description": _compose_field_description(field_name, field_def),
            "required": field_name in required_fields,
        }

        enum_values = field_def.get("enum")
        if isinstance(enum_values, list) and enum_values:
            field_payload["enum"] = [str(v) for v in enum_values]

        if normalized_type == "array":
            array_element_type = _extract_array_element_type(field_def)
            if array_element_type:
                field_payload["array_element_type"] = array_element_type
            items = field_def.get("items")
            if isinstance(items, dict):
                items_enum = items.get("enum")
                if isinstance(items_enum, list) and items_enum:
                    field_payload["items_enum"] = [str(v) for v in items_enum]

        unit_value = field_def.get("unit") or field_def.get("units") or field_def.get("x-unit")
        if isinstance(unit_value, str) and unit_value.strip():
            field_payload["unit"] = unit_value.strip()

        sample_values = field_def.get("examples")
        if isinstance(sample_values, list) and sample_values:
            field_payload["sample_values"] = [str(v) for v in sample_values[:8]]

        output_schema.append(field_payload)

    return output_schema


def _compose_mapping_prompt(user_prompt: str) -> str:
    prompt = (user_prompt or "").strip() or "Map source fields to target catalog schema"
    return f"{prompt}\n\n{SCHEMA_SAFE_MAPPING_GUARDRAILS}"


async def service_map_to_catalog_schema(client: RastroClient, params: ServiceMapToCatalogSchemaInput) -> dict:
    """Map source items to a target catalog schema using the enrich API.

    Forces web_search=false and predict_taxonomy=false per spec.
    Builds output_schema from the target catalog schema fields.
    """
    # Fetch catalog schema to build output_schema
    schema = await client.get_catalog_schema(params.catalog_id)
    output_schema = _build_output_schema(schema)
    composed_prompt = _compose_mapping_prompt(params.prompt)

    # Build enrich request
    enrich_payload: Dict[str, Any] = {
        "items": params.items,
        "output_schema": output_schema,
        "prompt": composed_prompt,
        "speed": params.speed,
        "web_search": False,
        "predict_taxonomy": False,
        "validate_semantics": True,
        "staged_status": "draft",
        "include_service_usage": True,
        "catalog_id": params.catalog_id,
    }

    if params.async_mode:
        enrich_payload["async_mode"] = True

    result = await client.enrich(enrich_payload)
    return result


def _fallback_judgment(rows: List[Dict[str, Any]], reason: str) -> Dict[str, Any]:
    judgments = []
    for i, row in enumerate(rows):
        judgments.append(
            {
                "row_index": i,
                "item_id": str(row.get("_id") or row.get("id") or row.get("specs.sku") or f"row_{i}"),
                "decision": "review_required",
                "confidence": 0.0,
                "reasons": [reason],
                "field_issues": [],
                "suggested_updates": {},
            }
        )
    return {
        "summary": {
            "pass": 0,
            "review_required": len(judgments),
            "fail": 0,
            "notes": [reason],
        },
        "judgments": judgments,
    }


async def service_judge_catalog_rows(client: RastroClient, params: ServiceJudgeCatalogRowsInput) -> dict:
    """Evaluate candidate catalog rows via remote /public/judge API."""
    if not params.rows:
        return {
            "summary": {"pass": 0, "review_required": 0, "fail": 0, "notes": ["no_rows"]},
            "judgments": [],
            "meta": {"rows_input": 0, "rows_judged": 0, "model": params.model},
        }

    rows = params.rows[: max(1, params.max_rows)]
    truncated = len(params.rows) > len(rows)

    if params.schema_input is None and not params.catalog_id:
        raise ValueError("Provide either schema or catalog_id for judging.")

    strictness = str(params.strictness or "medium").strip().lower()
    if strictness not in {"low", "medium", "high"}:
        strictness = "medium"

    payload: Dict[str, Any] = {
        "rows": rows,
        "rubric": params.rubric,
        "model": params.model,
        "strictness": strictness,
        "max_rows": params.max_rows,
    }
    if params.schema_input is not None:
        payload["schema"] = params.schema_input
    if params.catalog_id:
        payload["catalog_id"] = params.catalog_id
    if params.context:
        payload["context"] = params.context

    try:
        result = await client.judge_catalog_rows(payload)
    except Exception as exc:
        result = _fallback_judgment(rows, f"judge_error:{type(exc).__name__}")

    if not isinstance(result, dict):
        result = _fallback_judgment(rows, "judge_error:invalid_response_shape")

    result_meta = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    result_meta.update(
        {
            "rows_input": len(params.rows),
            "rows_judged": len(rows),
            "model": params.model,
            "strictness": strictness,
            "truncated": truncated,
        }
    )
    result["meta"] = result_meta
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
