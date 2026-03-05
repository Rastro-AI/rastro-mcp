"""
execution_local_diff_compute

Computes row/field diff from local before/after datasets.
Outputs a staged_changes JSONL file compatible with the Rastro activity staged change model.
"""

import hashlib
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

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


SYSTEM_COLUMNS = {"__catalog_item_id", "__entity_type", "__parent_id", "__current_version"}


def _normalize_for_hash(v: Any) -> Any:
    """Normalize a value for deterministic key hashing."""
    v = _normalize_value(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, default=str)
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)


def _derive_match_key(
    row: pd.Series,
    row_position: int,
    key_field: str,
    key_field_present: bool,
    deterministic_key_fields: List[str],
    allow_row_index_fallback: bool,
) -> Tuple[Optional[str], str]:
    """Derive deterministic key and strategy used for row matching."""
    if key_field_present:
        key_val = _normalize_key(row.get(key_field))
        if key_val is not None:
            return f"k:{key_val}", "key_field"

    if deterministic_key_fields:
        identity: Dict[str, Any] = {}
        for field in deterministic_key_fields:
            value = _normalize_value(row.get(field))
            if value is not None:
                identity[field] = _normalize_for_hash(value)

        if identity:
            digest = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            return f"f:{digest}", "deterministic_fields"

    if allow_row_index_fallback:
        return f"r:{row_position}", "row_index"

    return None, "unmatched"


def _count_duplicate_keys(df: pd.DataFrame, key_field: str) -> int:
    """Count duplicate non-null values in the key field."""
    if key_field not in df.columns:
        return 0
    keys = df[key_field].apply(_normalize_key)
    return int(keys.dropna().duplicated().sum())


def _ensure_entity_bucket(entity_type_change_counts: Dict[str, Dict[str, int]], entity_type: str) -> Dict[str, int]:
    if entity_type not in entity_type_change_counts:
        entity_type_change_counts[entity_type] = {"added": 0, "removed": 0, "modified": 0}
    return entity_type_change_counts[entity_type]


async def diff_compute(params: DiffComputeInput) -> DiffComputeOutput:
    """Compute diff between before and after datasets."""
    before_path = resolve_workspace_path(params.before_path, must_exist=True, expect_file=True, label="before_path")
    after_path = resolve_workspace_path(params.after_path, must_exist=True, expect_file=True, label="after_path")

    before_df = _load_dataframe(before_path)
    after_df = _load_dataframe(after_path)
    key_field = params.key_field
    deterministic_requested = params.deterministic_key_fields or []

    key_field_present_before = key_field in before_df.columns
    key_field_present_after = key_field in after_df.columns
    if not key_field_present_before and not key_field_present_after and not deterministic_requested and not params.allow_row_index_fallback:
        raise ValueError(
            f"Key field '{key_field}' is missing in both datasets and no fallback strategy is enabled. "
            "Provide deterministic_key_fields or allow_row_index_fallback=true."
        )

    deterministic_key_fields = [field for field in deterministic_requested if field in before_df.columns and field in after_df.columns and field not in SYSTEM_COLUMNS]
    ignored_deterministic_fields = [field for field in deterministic_requested if field not in deterministic_key_fields]

    # Detect column changes (exclude system columns)
    before_data_cols = set(before_df.columns) - SYSTEM_COLUMNS
    after_data_cols = set(after_df.columns) - SYSTEM_COLUMNS
    columns_added = sorted(after_data_cols - before_data_cols)
    columns_removed = sorted(before_data_cols - after_data_cols)

    # Build deterministic row groups (preserve duplicates instead of overwriting by key).
    before_groups: Dict[str, List[Tuple[int, pd.Series, str]]] = defaultdict(list)
    after_groups: Dict[str, List[Tuple[int, pd.Series, str]]] = defaultdict(list)
    before_unmatched = 0
    after_unmatched = 0
    before_strategy_counts: Dict[str, int] = defaultdict(int)
    after_strategy_counts: Dict[str, int] = defaultdict(int)

    for row_position, (_, row) in enumerate(before_df.iterrows()):
        match_key, strategy = _derive_match_key(
            row=row,
            row_position=row_position,
            key_field=key_field,
            key_field_present=key_field_present_before,
            deterministic_key_fields=deterministic_key_fields,
            allow_row_index_fallback=params.allow_row_index_fallback,
        )
        before_strategy_counts[strategy] += 1
        if match_key is None:
            before_unmatched += 1
            continue
        before_groups[match_key].append((row_position, row, strategy))

    for row_position, (_, row) in enumerate(after_df.iterrows()):
        match_key, strategy = _derive_match_key(
            row=row,
            row_position=row_position,
            key_field=key_field,
            key_field_present=key_field_present_after,
            deterministic_key_fields=deterministic_key_fields,
            allow_row_index_fallback=params.allow_row_index_fallback,
        )
        after_strategy_counts[strategy] += 1
        if match_key is None:
            after_unmatched += 1
            continue
        after_groups[match_key].append((row_position, row, strategy))

    if before_unmatched > 0 or after_unmatched > 0:
        raise ValueError(
            "Unable to derive deterministic match keys for all rows. "
            f"before_unmatched={before_unmatched}, after_unmatched={after_unmatched}. "
            "Provide deterministic_key_fields or enable allow_row_index_fallback."
        )

    # Compute modifications
    staged_changes: List[Dict[str, Any]] = []
    row_diffs: List[Dict[str, Any]] = []
    sample_changes: List[SampleChange] = []
    field_change_counts: Dict[str, int] = defaultdict(int)
    entity_type_change_counts: Dict[str, Dict[str, int]] = {}
    match_strategy_counts: Dict[str, int] = defaultdict(int)
    modified_count = 0
    added_count = 0
    removed_count = 0
    row_index = 0

    all_match_keys = sorted(set(before_groups.keys()) | set(after_groups.keys()))

    def _append_added_row(after_row: pd.Series, match_key: str, strategy: str):
        nonlocal row_index, added_count, modified_count
        after_data = _row_to_dict(after_row)
        item_data = {k: v for k, v in after_data.items() if k not in SYSTEM_COLUMNS}
        catalog_item_id = _normalize_key(after_data.get("__catalog_item_id"))
        entity_type = after_data.get("__entity_type")
        entity_type_label = str(entity_type) if entity_type is not None else "unknown"
        change_type = "added" if catalog_item_id is None else "modified_unmatched"

        staged_changes.append({
            "catalog_item_id": catalog_item_id,
            "catalog_item_entity_type": entity_type,
            "before_data": None,
            "after_data": item_data,
            "source_data": item_data,
            "is_new_item": catalog_item_id is None,
            "row_index": row_index,
        })
        row_diffs.append(
            {
                "match_key": match_key,
                "match_strategy": strategy,
                "change_type": change_type,
                "catalog_item_id": catalog_item_id,
                "catalog_item_entity_type": entity_type,
                "changed_fields": sorted(item_data.keys()),
                "before_data": None,
                "after_data": item_data,
            }
        )
        if catalog_item_id is None:
            _ensure_entity_bucket(entity_type_change_counts, entity_type_label)["added"] += 1
            added_count += 1
        else:
            _ensure_entity_bucket(entity_type_change_counts, entity_type_label)["modified"] += 1
            modified_count += 1
        row_index += 1

    def _append_removed_row(before_row: pd.Series, match_key: str, strategy: str):
        nonlocal row_index, removed_count
        before_data = _row_to_dict(before_row)
        item_id = _normalize_key(before_data.get("__catalog_item_id"))
        entity_type = before_data.get("__entity_type")
        entity_type_label = str(entity_type) if entity_type is not None else "unknown"
        before_item = {k: v for k, v in before_data.items() if k not in SYSTEM_COLUMNS}

        if item_id is None:
            row_diffs.append(
                {
                    "match_key": match_key,
                    "match_strategy": strategy,
                    "change_type": "removed_untracked",
                    "catalog_item_id": None,
                    "catalog_item_entity_type": entity_type,
                    "changed_fields": sorted(before_item.keys()),
                    "before_data": before_item,
                    "after_data": None,
                }
            )
            return

        staged_changes.append({
            "catalog_item_id": item_id,
            "catalog_item_entity_type": entity_type,
            "before_data": before_item,
            "after_data": {},
            "source_data": None,
            "is_new_item": False,
            "is_delete": True,
            "row_index": row_index,
        })
        row_diffs.append(
            {
                "match_key": match_key,
                "match_strategy": strategy,
                "change_type": "removed",
                "catalog_item_id": item_id,
                "catalog_item_entity_type": entity_type,
                "changed_fields": sorted(before_item.keys()),
                "before_data": before_item,
                "after_data": None,
            }
        )
        _ensure_entity_bucket(entity_type_change_counts, entity_type_label)["removed"] += 1
        removed_count += 1
        row_index += 1

    for match_key in all_match_keys:
        before_rows = before_groups.get(match_key, [])
        after_rows = after_groups.get(match_key, [])
        matched_count = min(len(before_rows), len(after_rows))

        for idx in range(matched_count):
            _, before_row, before_strategy = before_rows[idx]
            _, after_row, after_strategy = after_rows[idx]
            strategy_label = before_strategy if before_strategy == after_strategy else f"{before_strategy}->{after_strategy}"
            match_strategy_counts[strategy_label] += 1

            before_data = _row_to_dict(before_row)
            after_data = _row_to_dict(after_row)

            # Compare non-system fields
            all_fields = sorted((set(before_data.keys()) | set(after_data.keys())) - SYSTEM_COLUMNS)
            changed_fields: Dict[str, Tuple[Any, Any]] = {}
            for field in all_fields:
                bv = before_data.get(field)
                av = after_data.get(field)
                if _values_differ(bv, av):
                    changed_fields[field] = (bv, av)
                    field_change_counts[field] += 1

            if not changed_fields:
                continue

            item_id = _normalize_key(before_data.get("__catalog_item_id")) or _normalize_key(after_data.get("__catalog_item_id"))
            entity_type = after_data.get("__entity_type") if after_data.get("__entity_type") is not None else before_data.get("__entity_type")
            entity_type_label = str(entity_type) if entity_type is not None else "unknown"
            before_item = {k: v for k, v in before_data.items() if k not in SYSTEM_COLUMNS}
            after_item = {k: v for k, v in after_data.items() if k not in SYSTEM_COLUMNS}
            is_new_item = item_id is None
            change_type = "added_from_match" if is_new_item else "modified"

            staged_changes.append({
                "catalog_item_id": item_id,
                "catalog_item_entity_type": entity_type,
                "before_data": before_item if not is_new_item else None,
                "after_data": after_item,
                "source_data": None,
                "is_new_item": is_new_item,
                "row_index": row_index,
            })
            row_diffs.append(
                {
                    "match_key": match_key,
                    "match_strategy": strategy_label,
                    "change_type": change_type,
                    "catalog_item_id": item_id,
                    "catalog_item_entity_type": entity_type,
                    "changed_fields": sorted(changed_fields.keys()),
                    "before_data": before_item,
                    "after_data": after_item,
                }
            )
            if is_new_item:
                _ensure_entity_bucket(entity_type_change_counts, entity_type_label)["added"] += 1
                added_count += 1
            else:
                _ensure_entity_bucket(entity_type_change_counts, entity_type_label)["modified"] += 1
                modified_count += 1
            row_index += 1

            # Collect sample changes (up to 10).
            if len(sample_changes) < 10:
                for field in sorted(changed_fields.keys())[:2]:
                    bv, av = changed_fields[field]
                    sample_changes.append(SampleChange(key=match_key, field=field, before=bv, after=av))
                    if len(sample_changes) >= 10:
                        break

        for _, after_row, strategy in after_rows[matched_count:]:
            match_strategy_counts[f"added:{strategy}"] += 1
            _append_added_row(after_row, match_key, strategy)

        for _, before_row, strategy in before_rows[matched_count:]:
            match_strategy_counts[f"removed:{strategy}"] += 1
            _append_removed_row(before_row, match_key, strategy)

    # Write staged changes to JSONL
    work_dir = os.path.dirname(after_path) or "."
    staged_path = os.path.join(work_dir, "staged_changes.jsonl")
    with open(staged_path, "w") as f:
        for change in staged_changes:
            f.write(json.dumps(change, default=str) + "\n")

    key_diagnostics = {
        "key_field": key_field,
        "key_field_present_before": key_field_present_before,
        "key_field_present_after": key_field_present_after,
        "before_missing_key_values": int(before_df[key_field].apply(_normalize_key).isna().sum()) if key_field_present_before else len(before_df),
        "after_missing_key_values": int(after_df[key_field].apply(_normalize_key).isna().sum()) if key_field_present_after else len(after_df),
        "before_duplicate_key_values": _count_duplicate_keys(before_df, key_field),
        "after_duplicate_key_values": _count_duplicate_keys(after_df, key_field),
        "deterministic_key_fields_used": deterministic_key_fields,
        "ignored_deterministic_key_fields": ignored_deterministic_fields,
        "allow_row_index_fallback": params.allow_row_index_fallback,
        "before_match_strategy_counts": dict(before_strategy_counts),
        "after_match_strategy_counts": dict(after_strategy_counts),
        "pair_match_strategy_counts": dict(match_strategy_counts),
    }

    diff_summary = DiffSummary(
        rows_before=len(before_df),
        rows_after=len(after_df),
        rows_added=added_count,
        rows_removed=removed_count,
        rows_modified=modified_count,
        columns_added=columns_added,
        columns_removed=columns_removed,
    )

    diff_details = {
        "summary": diff_summary.model_dump(),
        "key_diagnostics": key_diagnostics,
        "field_change_counts": dict(sorted(field_change_counts.items(), key=lambda item: item[0])),
        "entity_type_change_counts": entity_type_change_counts,
        "row_diffs": row_diffs,
    }
    diff_details_path = os.path.join(work_dir, "diff_details.json")
    with open(diff_details_path, "w") as f:
        json.dump(diff_details, f, default=str)

    return DiffComputeOutput(
        diff_summary=diff_summary,
        staged_changes_path=staged_path,
        diff_details_path=diff_details_path,
        sample_changes=sample_changes,
        field_change_counts=dict(sorted(field_change_counts.items(), key=lambda item: item[0])),
        entity_type_change_counts=entity_type_change_counts,
        key_diagnostics=key_diagnostics,
    )
