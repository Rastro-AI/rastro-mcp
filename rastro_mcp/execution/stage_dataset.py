"""
execution_catalog_stage_dataset

One-command staging pipeline:
1. Compute local diff between before/after datasets
2. Create a single pending-review custom-transform activity
"""

import hashlib
import os
from typing import Optional

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.execution.diff_compute import diff_compute
from rastro_mcp.execution.path_safety import resolve_workspace_path
from rastro_mcp.models.contracts import (
    CatalogActivityCreateTransformInput,
    DiffComputeInput,
    ScriptInfo,
    StageDatasetInput,
    StageDatasetOutput,
)
from rastro_mcp.tools.catalog_tools import catalog_activity_create_transform


def _load_script_info(script_path: str) -> ScriptInfo:
    normalized = resolve_workspace_path(script_path, must_exist=True, expect_file=True, label="script_path")
    with open(normalized, "r") as f:
        content = f.read()
    sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return ScriptInfo(
        filename=os.path.basename(normalized),
        content=content,
        sha256=sha256,
    )


async def stage_dataset(client: RastroClient, params: StageDatasetInput) -> StageDatasetOutput:
    """Compute diff and stage all changes into a single pending-review activity."""
    diff_result = await diff_compute(
        DiffComputeInput(
            before_path=params.before_path,
            after_path=params.after_path,
            key_field=params.key_field,
            deterministic_key_fields=params.deterministic_key_fields,
            allow_row_index_fallback=params.allow_row_index_fallback,
        )
    )

    script_info: Optional[ScriptInfo] = None
    if params.script_path:
        script_info = _load_script_info(params.script_path)

    staged = await catalog_activity_create_transform(
        client,
        CatalogActivityCreateTransformInput(
            catalog_id=params.catalog_id,
            activity_message=params.activity_message,
            script=script_info,
            diff_summary=diff_result.diff_summary.model_dump(),
            staged_changes_file_path=diff_result.staged_changes_path,
            schema_changes=params.schema_changes,
            taxonomy_changes=params.taxonomy_changes,
            attachments=params.attachments,
            activity_context=params.activity_context,
            auto_open_review=params.auto_open_review,
        ),
    )

    return StageDatasetOutput(
        activity_id=staged.activity_id,
        status=staged.status,
        staged_count=staged.staged_count,
        review_url=staged.review_url,
        staged_changes_path=diff_result.staged_changes_path,
        diff_details_path=diff_result.diff_details_path,
        diff_summary=diff_result.diff_summary,
        sample_changes=diff_result.sample_changes,
        field_change_counts=diff_result.field_change_counts,
        entity_type_change_counts=diff_result.entity_type_change_counts,
        key_diagnostics=diff_result.key_diagnostics,
    )
