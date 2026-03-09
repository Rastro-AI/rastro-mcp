"""
Pydantic models for MCP tool inputs and outputs.

These contracts define the exact shape of data flowing through each MCP tool.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════════════════
# Execution tool contracts
# ═══════════════════════════════════════════════════════════════════════════════


class SnapshotFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class SnapshotPullInput(BaseModel):
    catalog_id: str
    output_dir: str = "./snapshots"
    format: SnapshotFormat = SnapshotFormat.PARQUET
    sample_size: Optional[int] = None
    page_size: int = 400
    max_concurrency: int = 8
    prefer_raw: bool = True


class SnapshotPullOutput(BaseModel):
    catalog_id: str
    snapshot_path: str
    schema_path: str
    rows: int
    columns: int
    base_snapshot_id: Optional[str] = None


class StageDatasetInput(BaseModel):
    catalog_id: str
    before_path: str
    after_path: str
    activity_message: str
    key_field: str = "__catalog_item_id"
    script_path: Optional[str] = None
    schema_changes: Optional[Dict[str, Any]] = None
    taxonomy_changes: Optional[Dict[str, Any]] = None
    attachments: Optional[List[Dict[str, Any]]] = None
    activity_context: Optional[Dict[str, Any]] = None
    auto_open_review: bool = True


class DiffComputeInput(BaseModel):
    before_path: str
    after_path: str
    key_field: str = "__catalog_item_id"


class DiffSummary(BaseModel):
    rows_before: int = 0
    rows_after: int = 0
    rows_added: int = 0
    rows_removed: int = 0
    rows_modified: int = 0
    columns_added: List[str] = Field(default_factory=list)
    columns_removed: List[str] = Field(default_factory=list)


class SampleChange(BaseModel):
    key: str
    field: str
    before: Any = None
    after: Any = None


class DiffComputeOutput(BaseModel):
    diff_summary: DiffSummary
    staged_changes_path: str
    sample_changes: List[SampleChange] = Field(default_factory=list)


class StageDatasetOutput(BaseModel):
    activity_id: str
    status: str
    staged_count: int
    review_url: str
    staged_changes_path: str
    diff_summary: DiffSummary
    sample_changes: List[SampleChange] = Field(default_factory=list)


class StagedChangeRow(BaseModel):
    catalog_item_id: Optional[str] = None
    catalog_item_entity_type: Optional[str] = None
    before_data: Optional[Dict[str, Any]] = None
    after_data: Dict[str, Any]
    source_data: Optional[Dict[str, Any]] = None
    is_new_item: bool = False
    row_index: int = 0


class ValidationRules(BaseModel):
    allow_row_deletes: bool = False
    max_change_ratio_warning: float = 0.2


class BundleValidateInput(BaseModel):
    catalog_id: str
    before_path: Optional[str] = None
    after_path: Optional[str] = None
    script_path: Optional[str] = None
    staged_changes_path: Optional[str] = None
    diff_summary: Optional[Dict[str, Any]] = None
    schema_changes: Optional[Dict[str, Any]] = None
    taxonomy_changes: Optional[Dict[str, Any]] = None
    rules: ValidationRules = Field(default_factory=ValidationRules)


class ValidationIssue(BaseModel):
    code: str
    message: str
    fix_hint: Optional[str] = None


class BundleValidateOutput(BaseModel):
    valid: bool
    errors: List[ValidationIssue] = Field(default_factory=list)
    warnings: List[ValidationIssue] = Field(default_factory=list)
    computed: Dict[str, Any] = Field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════════
# Catalog tool contracts
# ═══════════════════════════════════════════════════════════════════════════════


class CatalogListInput(BaseModel):
    limit: int = 50
    offset: int = 0


class CatalogGetInput(BaseModel):
    catalog_id: str


class CatalogSchemaGetInput(BaseModel):
    catalog_id: str
    version: Optional[str] = None


class CatalogTaxonomyGetInput(BaseModel):
    catalog_id: str


class CatalogItemsQueryInput(BaseModel):
    catalog_id: str
    limit: int = 50
    offset: int = 0
    search: Optional[str] = None
    sort_field: Optional[str] = None
    sort_order: str = "asc"


class CatalogActivityListInput(BaseModel):
    catalog_id: str
    status: Optional[str] = None
    activity_type: Optional[str] = None
    limit: int = 20
    offset: int = 0


class CatalogActivityGetInput(BaseModel):
    activity_id: str


class CatalogItemGetInput(BaseModel):
    catalog_id: str
    item_id: str


class CatalogItemUpdateInput(BaseModel):
    catalog_id: str
    item_id: str
    data: Dict[str, Any]


class CatalogActivityGetStagedChangesInput(BaseModel):
    activity_id: str
    limit: int = 50
    offset: int = 0


class ScriptInfo(BaseModel):
    filename: str
    content: str
    sha256: str


class CatalogActivityCreateTransformInput(BaseModel):
    catalog_id: str
    activity_message: str
    script: Optional[ScriptInfo] = None
    diff_summary: Optional[Dict[str, Any]] = None
    validation_report: Optional[Dict[str, Any]] = None
    staged_changes_inline: Optional[List[Dict[str, Any]]] = None
    staged_changes_file_path: Optional[str] = None
    schema_changes: Optional[Dict[str, Any]] = None
    taxonomy_changes: Optional[Dict[str, Any]] = None
    attachments: Optional[List[Dict[str, Any]]] = None
    activity_context: Optional[Dict[str, Any]] = None
    session_context: Optional[Dict[str, Any]] = None
    base_snapshot_id: Optional[str] = None
    auto_open_review: bool = True


class CreateTransformOutput(BaseModel):
    activity_id: str
    status: str
    staged_count: int
    review_url: str


class CatalogSnapshotListInput(BaseModel):
    catalog_id: str
    snapshot_type: Optional[str] = None
    limit: int = 20
    offset: int = 0


class CatalogSnapshotCreateInput(BaseModel):
    catalog_id: str
    reason: str = "Manual snapshot from MCP"


class CatalogSnapshotRestoreInput(BaseModel):
    catalog_id: str
    snapshot_id: str


class CatalogDuplicateInput(BaseModel):
    catalog_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    include_items: bool = False


class CatalogDeleteInput(BaseModel):
    catalog_id: str
    confirm: bool = False
    confirmation: Optional[str] = None
    expected_name: Optional[str] = None


class CatalogActivitySaveWorkflowInput(BaseModel):
    catalog_id: str
    activity_id: str
    workflow_name: str
    workflow_description: Optional[str] = None
    python_code: Optional[str] = None
    attachments: Optional[List[Dict[str, Any]]] = None
    timeout_seconds: int = 120


# ═══════════════════════════════════════════════════════════════════════════════
# Service tool contracts
# ═══════════════════════════════════════════════════════════════════════════════


class CatalogUpdateQualityPromptInput(BaseModel):
    catalog_id: str
    prompt: str


class ServiceMapToCatalogSchemaInput(BaseModel):
    catalog_id: str
    items: List[Dict[str, Any]]
    prompt: str = "Map source fields to target catalog schema"
    async_mode: bool = False
    speed: str = "medium"


class ServiceJudgeCatalogRowsInput(BaseModel):
    model_config = {"populate_by_name": True}

    rows: List[Dict[str, Any]]
    catalog_id: Optional[str] = None
    schema_input: Optional[Dict[str, Any]] = Field(default=None, alias="schema")
    prompt: Optional[str] = None
    model: str = "fast"
    max_rows: int = 200


class ServiceImageRunInput(BaseModel):
    tool: str  # generate|edit|inpaint|bg_remove|relight|upscale
    image_url: Optional[str] = None
    mask_url: Optional[str] = None
    prompt: Optional[str] = None
    provider: Optional[str] = None
    quality: Optional[str] = None
    size: Optional[str] = None
    num_options: int = 1
    catalog_id: Optional[str] = None
    item_id: Optional[str] = None


class ServiceImageStatusInput(BaseModel):
    run_id: str


class ServiceImageListInput(BaseModel):
    catalog_id: Optional[str] = None
    item_id: Optional[str] = None
    status: Optional[str] = None
    tool: Optional[str] = None
    limit: int = 20
    offset: int = 0
