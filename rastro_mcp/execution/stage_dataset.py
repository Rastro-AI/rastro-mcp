"""
execution_catalog_stage_dataset

One-command staging pipeline:
1. Compute local diff between before/after datasets
2. Run bundle validation (including product-variant integrity)
3. Create a single pending-review custom-transform activity
"""

import hashlib
import os
from typing import Optional

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.execution.bundle_validate import bundle_validate
from rastro_mcp.execution.diff_compute import diff_compute
from rastro_mcp.execution.path_safety import resolve_workspace_path
from rastro_mcp.models.contracts import (
    BundleValidateInput,
    CatalogActivityCreateTransformInput,
    DiffComputeInput,
    ScriptInfo,
    StageDatasetInput,
    StageDatasetOutput,
    ValidationRules,
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
    """Compute diff, validate bundle, and stage all changes into a single pending-review activity."""
    diff_result = await diff_compute(
        DiffComputeInput(
            before_path=params.before_path,
            after_path=params.after_path,
            key_field=params.key_field,
        )
    )

    # Run bundle validation before staging (catches schema mismatches, orphan product_ids, etc.)
    validation = await bundle_validate(
        client,
        BundleValidateInput(
            catalog_id=params.catalog_id,
            before_path=params.before_path,
            after_path=params.after_path,
            script_path=params.script_path,
            staged_changes_path=diff_result.staged_changes_path,
            diff_summary=diff_result.diff_summary.model_dump(),
            schema_changes=params.schema_changes,
            taxonomy_changes=params.taxonomy_changes,
            rules=ValidationRules(),
        ),
    )
    if not validation.valid:
        error_msgs = "; ".join(e.message for e in validation.errors)
        raise ValueError(f"Bundle validation failed: {error_msgs}")

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
            validation_report=validation.model_dump(),
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
        diff_summary=diff_result.diff_summary,
        sample_changes=diff_result.sample_changes,
    )
