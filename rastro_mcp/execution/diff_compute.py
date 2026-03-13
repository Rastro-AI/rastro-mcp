"""
execution_local_diff_compute

Computes row/field diff from local before/after datasets.
Outputs a staged_changes JSONL file compatible with the Rastro activity staged change model.
"""

import json
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import pandas as pd

from rastro_mcp.execution.path_safety import resolve_workspace_path
from rastro_mcp.models.contracts import DiffComputeInput, DiffComputeOutput, DiffSummary, SampleChange


def _normalize_value(v: Any) -> Any:
    """Normalize a value for comparison (handle NaN, None, type coercion)."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


def _values_differ(a: Any, b: Any) -> bool:
    """Check if two values are meaningfully different."""
    a = _normalize_value(a)
    b = _normalize_value(b)
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    # Numeric comparisons should treat equivalent values as unchanged (1 == 1.0).
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return float(a) != float(b)
    # Compare as strings if types differ
    if type(a) != type(b):
        return str(a) != str(b)
    return a != b


def _load_dataframe(path: str) -> pd.DataFrame:
    """Load a dataframe from parquet or CSV."""
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    elif path.endswith(".csv"):
        return pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file format: {path}. Use .parquet or .csv")


def _normalize_key(v: Any) -> Optional[str]:
    """Normalize key field values to stable string IDs."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def _row_to_dict(row: pd.Series) -> Dict[str, Any]:
    """Convert a pandas row to a clean dict, dropping NaN/None."""
    d = {}
    for k, v in row.items():
        v = _normalize_value(v)
        if v is not None:
            d[str(k)] = v
    return d


def _write_staged_changes(staged_changes: List[Dict[str, Any]], after_path: str) -> str:
    """Write staged changes beside the after dataset, with a workspace-local fallback."""
    preferred_dir = os.path.dirname(after_path) or "."
    preferred_path = os.path.join(preferred_dir, "staged_changes.jsonl")

    def _write(path: str) -> None:
        with open(path, "w") as f:
            for change in staged_changes:
                f.write(json.dumps(change, default=str) + "\n")

    try:
        _write(preferred_path)
        return preferred_path
    except OSError:
        fallback_dir = resolve_workspace_path("./work/staged_changes", label="staged_changes_output_dir")
        os.makedirs(fallback_dir, exist_ok=True)
        fallback_path = os.path.join(fallback_dir, f"staged_changes_{uuid4().hex}.jsonl")
        _write(fallback_path)
        return fallback_path


SYSTEM_COLUMNS = {"__catalog_item_id", "__entity_type", "__parent_id", "__current_version"}


async def diff_compute(params: DiffComputeInput) -> DiffComputeOutput:
    """Compute diff between before and after datasets."""
    before_path = resolve_workspace_path(params.before_path, must_exist=True, expect_file=True, label="before_path")
    after_path = resolve_workspace_path(params.after_path, must_exist=True, expect_file=True, label="after_path")

    before_df = _load_dataframe(before_path)
    after_df = _load_dataframe(after_path)
    key_field = params.key_field

    if key_field not in before_df.columns:
        raise ValueError(f"Key field '{key_field}' not found in before dataset. Columns: {list(before_df.columns)}")
    if key_field not in after_df.columns:
        raise ValueError(f"Key field '{key_field}' not found in after dataset. Columns: {list(after_df.columns)}")

    # Build lookup dicts and track null-key rows (treated as additions in after dataset)
    before_lookup: Dict[str, pd.Series] = {}
    after_lookup: Dict[str, pd.Series] = {}
    after_null_key_rows: List[pd.Series] = []

    for _, row in before_df.iterrows():
        k = _normalize_key(row.get(key_field))
        if k is not None:
            before_lookup[k] = row

    for _, row in after_df.iterrows():
        k = _normalize_key(row.get(key_field))
        if k is None:
            after_null_key_rows.append(row)
        else:
            after_lookup[k] = row

    before_keys = set(before_lookup.keys())
    after_keys = set(after_lookup.keys())
    added_keys = after_keys - before_keys
    removed_keys = before_keys - after_keys
    common_keys = before_keys & after_keys

    # Detect column changes (exclude system columns)
    before_data_cols = set(before_df.columns) - SYSTEM_COLUMNS
    after_data_cols = set(after_df.columns) - SYSTEM_COLUMNS
    columns_added = sorted(after_data_cols - before_data_cols)
    columns_removed = sorted(before_data_cols - after_data_cols)

    # Compute modifications
    staged_changes: List[Dict[str, Any]] = []
    sample_changes: List[SampleChange] = []
    modified_count = 0
    row_index = 0

    # Added rows
    for key in sorted(added_keys):
        after_row = after_lookup[key]
        after_data = _row_to_dict(after_row)
        # Strip system columns from data payloads
        item_data = {k: v for k, v in after_data.items() if k not in SYSTEM_COLUMNS}
        entity_type = after_data.get("__entity_type")

        staged_changes.append(
            {
                "catalog_item_id": None,
                "catalog_item_entity_type": entity_type,
                "before_data": None,
                "after_data": item_data,
                "source_data": item_data,
                "is_new_item": True,
                "row_index": row_index,
            }
        )
        row_index += 1

    # Added rows with null key (new inserts that don't yet have catalog IDs)
    for after_row in after_null_key_rows:
        after_data = _row_to_dict(after_row)
        item_data = {k: v for k, v in after_data.items() if k not in SYSTEM_COLUMNS}
        entity_type = after_data.get("__entity_type")

        staged_changes.append(
            {
                "catalog_item_id": None,
                "catalog_item_entity_type": entity_type,
                "before_data": None,
                "after_data": item_data,
                "source_data": item_data,
                "is_new_item": True,
                "row_index": row_index,
            }
        )
        row_index += 1

    # Removed rows — staged as deletion entries (before_data set, after_data is empty)
    for key in sorted(removed_keys):
        before_row = before_lookup[key]
        before_data = _row_to_dict(before_row)
        item_id = before_data.get("__catalog_item_id")
        entity_type = before_data.get("__entity_type")
        before_item = {k: v for k, v in before_data.items() if k not in SYSTEM_COLUMNS}

        staged_changes.append(
            {
                "catalog_item_id": item_id,
                "catalog_item_entity_type": entity_type,
                "before_data": before_item,
                "after_data": {},
                "source_data": None,
                "is_new_item": False,
                "is_delete": True,
                "row_index": row_index,
            }
        )
        row_index += 1

    # Modified rows
    for key in sorted(common_keys):
        before_row = before_lookup[key]
        after_row = after_lookup[key]

        before_data = _row_to_dict(before_row)
        after_data = _row_to_dict(after_row)

        # Compare non-system fields
        all_fields = set(list(before_data.keys()) + list(after_data.keys())) - SYSTEM_COLUMNS
        changed_fields = {}
        for field in all_fields:
            bv = before_data.get(field)
            av = after_data.get(field)
            if _values_differ(bv, av):
                changed_fields[field] = (bv, av)

        if changed_fields:
            modified_count += 1
            item_id = before_data.get("__catalog_item_id")
            entity_type = before_data.get("__entity_type")
            before_item = {}
            after_item = {}
            for field, (before_value, after_value) in changed_fields.items():
                if before_value is not None:
                    before_item[field] = before_value
                if after_value is not None:
                    after_item[field] = after_value

            staged_changes.append(
                {
                    "catalog_item_id": item_id,
                    "catalog_item_entity_type": entity_type,
                    "before_data": before_item,
                    "after_data": after_item,
                    "source_data": None,
                    "is_new_item": False,
                    "row_index": row_index,
                }
            )
            row_index += 1

            # Collect sample changes (up to 10)
            if len(sample_changes) < 10:
                for field, (bv, av) in list(changed_fields.items())[:2]:
                    sample_changes.append(SampleChange(key=key, field=field, before=bv, after=av))

    staged_path = _write_staged_changes(staged_changes, after_path)

    diff_summary = DiffSummary(
        rows_before=len(before_df),
        rows_after=len(after_df),
        rows_added=len(added_keys) + len(after_null_key_rows),
        rows_removed=len(removed_keys),
        rows_modified=modified_count,
        columns_added=columns_added,
        columns_removed=columns_removed,
    )

    return DiffComputeOutput(
        diff_summary=diff_summary,
        staged_changes_path=staged_path,
        sample_changes=sample_changes,
    )
