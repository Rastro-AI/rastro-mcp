"""
execution_* MCP tools - Local snapshot, diff, and validation.

Tools:
- execution_catalog_snapshot_pull
- execution_local_diff_compute
- execution_bundle_validate
- execution_catalog_stage_dataset
"""

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.execution.bundle_validate import bundle_validate as _bundle_validate
from rastro_mcp.execution.diff_compute import diff_compute as _diff_compute
from rastro_mcp.execution.snapshot_pull import snapshot_pull as _snapshot_pull
from rastro_mcp.execution.stage_dataset import stage_dataset as _stage_dataset
from rastro_mcp.models.contracts import (
    BundleValidateInput,
    BundleValidateOutput,
    DiffComputeInput,
    DiffComputeOutput,
    StageDatasetInput,
    StageDatasetOutput,
    SnapshotPullInput,
    SnapshotPullOutput,
)


async def execution_catalog_snapshot_pull(client: RastroClient, params: SnapshotPullInput) -> SnapshotPullOutput:
    """Pull catalog data + schema to local files for Python transforms."""
    return await _snapshot_pull(client, params)


async def execution_local_diff_compute(params: DiffComputeInput) -> DiffComputeOutput:
    """Compute row/field diff between local before/after datasets."""
    return await _diff_compute(params)


async def execution_bundle_validate(client: RastroClient, params: BundleValidateInput) -> BundleValidateOutput:
    """Validate a transform bundle before creating an activity."""
    return await _bundle_validate(client, params)


async def execution_catalog_stage_dataset(client: RastroClient, params: StageDatasetInput) -> StageDatasetOutput:
    """One-command: compute diff and stage all changes into one pending-review activity."""
    return await _stage_dataset(client, params)
