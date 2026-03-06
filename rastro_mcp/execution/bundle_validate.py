"""
execution_bundle_validate

Strict preflight gate before activity creation.
Validates files, data integrity, schema compatibility, and policy rules.
"""

import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.execution.path_safety import UnsafePathError, resolve_workspace_path
from rastro_mcp.models.contracts import BundleValidateInput, BundleValidateOutput, ValidationIssue

SUPPORTED_DATASET_EXTS = {".parquet", ".csv"}
SUPPORTED_STAGED_EXTS = {".jsonl", ".json", ".parquet"}
SYSTEM_COLUMNS = {"__catalog_item_id", "__entity_type", "__parent_id", "__current_version"}


def _compute_file_sha256(path: str) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_staged_changes(path: str) -> List[Dict[str, Any]]:
    """Load staged changes from JSONL, JSON, or parquet."""
    if path.endswith(".jsonl"):
        changes = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    changes.append(json.loads(line))
        return changes
    elif path.endswith(".json"):
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    elif path.endswith(".parquet"):
        df = pd.read_parquet(path)
        return df.to_dict("records")
    else:
        raise ValueError(f"Unsupported staged changes format: {path}")


def _load_dataset(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".parquet":
        return pd.read_parquet(path)
    if ext == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported dataset format: {path}")


def _file_ext(path: str) -> str:
    return os.path.splitext(path)[1].lower()


def _is_delete_staged_change(change: Dict[str, Any]) -> bool:
    if change.get("is_new_item"):
        return False
    if not change.get("catalog_item_id"):
        return False
    after_data = change.get("after_data")
    return after_data is None or (isinstance(after_data, dict) and len(after_data) == 0)


def _extract_declared_schema_additions(schema_changes: Optional[Dict[str, Any]]) -> Set[str]:
    if not schema_changes:
        return set()

    # New contract: {mode: "batch_fields_v1", payload: {fields_to_add: [...]}}
    if schema_changes.get("mode") == "batch_fields_v1":
        payload = schema_changes.get("payload") or {}
        fields_to_add = payload.get("fields_to_add") or []
        names = set()
        for field in fields_to_add:
            if isinstance(field, dict):
                name = field.get("field_name")
                if name:
                    names.add(name)
        return names

    # Backward compatibility with older MCP prototype
    add_fields = schema_changes.get("add_fields")
    if isinstance(add_fields, dict):
        return set(add_fields.keys())

    return set()


def _is_json_encoded_array_field(field_def: Dict[str, Any]) -> bool:
    description = str(field_def.get("description", "")).lower()
    return "json-encoded array" in description


def _is_valid_json_array_string(value: str) -> bool:
    try:
        parsed = json.loads(value)
    except Exception:
        return False
    return isinstance(parsed, list)


def _validate_file_path(
    path: str,
    label: str,
    allowed_exts: Set[str],
    errors: List[ValidationIssue],
) -> Optional[str]:
    try:
        normalized = resolve_workspace_path(path, must_exist=True, expect_file=True, label=label)
    except UnsafePathError as exc:
        errors.append(ValidationIssue(code="INVALID_PATH", message=str(exc)))
        return None

    if not os.access(normalized, os.R_OK):
        errors.append(ValidationIssue(code="FILE_NOT_READABLE", message=f"{label} file is not readable: {normalized}"))
        return None

    ext = _file_ext(normalized)
    if ext not in allowed_exts:
        errors.append(
            ValidationIssue(
                code="INVALID_FORMAT",
                message=f"{label} file format '{ext or 'none'}' is not supported",
                fix_hint=f"Use one of: {sorted(allowed_exts)}",
            )
        )
        return None

    return normalized


def _normalize_key_series(df: pd.DataFrame, key_field: str) -> pd.Series:
    return df[key_field].dropna().astype(str).str.strip()


async def bundle_validate(client: RastroClient, params: BundleValidateInput) -> BundleValidateOutput:
    """Validate a transform bundle before creating an activity."""
    errors: List[ValidationIssue] = []
    warnings: List[ValidationIssue] = []
    computed: Dict[str, Any] = {}

    before_path = None
    after_path = None
    staged_changes_path = None
    script_path = None

    # ── File existence/readability/format checks ─────────────────────
    if params.before_path:
        before_path = _validate_file_path(params.before_path, "Before dataset", SUPPORTED_DATASET_EXTS, errors)
    if params.after_path:
        after_path = _validate_file_path(params.after_path, "After dataset", SUPPORTED_DATASET_EXTS, errors)
    if params.staged_changes_path:
        staged_changes_path = _validate_file_path(params.staged_changes_path, "Staged changes", SUPPORTED_STAGED_EXTS, errors)

    if params.script_path:
        script_path = _validate_file_path(params.script_path, "Script", {".py"}, errors)
        if script_path and not errors:
            computed["script_sha256"] = _compute_file_sha256(script_path)

    # If critical files missing, return early
    if errors:
        return BundleValidateOutput(valid=False, errors=errors, warnings=warnings, computed=computed)

    # ── Load data for validation ─────────────────────────────────────
    before_df = None
    after_df = None
    staged_changes = None

    if before_path:
        try:
            before_df = _load_dataset(before_path)
        except Exception as e:
            errors.append(ValidationIssue(code="INVALID_FORMAT", message=f"Cannot read before file: {e}"))

    if after_path:
        try:
            after_df = _load_dataset(after_path)
        except Exception as e:
            errors.append(ValidationIssue(code="INVALID_FORMAT", message=f"Cannot read after file: {e}"))

    if staged_changes_path:
        try:
            staged_changes = _load_staged_changes(staged_changes_path)
        except Exception as e:
            errors.append(ValidationIssue(code="INVALID_FORMAT", message=f"Cannot read staged changes: {e}"))

    if errors:
        return BundleValidateOutput(valid=False, errors=errors, warnings=warnings, computed=computed)

    # ── Required system column checks ────────────────────────────────
    key_field = "__catalog_item_id"
    if before_df is not None and key_field not in before_df.columns:
        errors.append(ValidationIssue(code="MISSING_SYSTEM_COLUMN", message=f"Before dataset missing required system column '{key_field}'"))
    if after_df is not None and key_field not in after_df.columns:
        errors.append(ValidationIssue(code="MISSING_SYSTEM_COLUMN", message=f"After dataset missing required system column '{key_field}'"))

    if errors:
        return BundleValidateOutput(valid=False, errors=errors, warnings=warnings, computed=computed)

    # ── Key diagnostics (duplicate/null) ─────────────────────────────
    if before_df is not None:
        before_keys = _normalize_key_series(before_df, key_field)
        before_dupes = int(before_keys.duplicated().sum())
        before_nulls = int(before_df[key_field].isna().sum())
        computed["before_key_diagnostics"] = {
            "duplicate_keys": before_dupes,
            "null_keys": before_nulls,
            "rows": len(before_df),
        }
        if before_dupes > 0:
            errors.append(ValidationIssue(code="DUPLICATE_KEY", message=f"Before dataset contains {before_dupes} duplicate '{key_field}' values"))
        if before_nulls > 0:
            warnings.append(ValidationIssue(code="NULL_KEYS", message=f"Before dataset contains {before_nulls} null '{key_field}' values"))

    if after_df is not None:
        after_keys = _normalize_key_series(after_df, key_field)
        after_dupes = int(after_keys.duplicated().sum())
        after_nulls = int(after_df[key_field].isna().sum())
        computed["after_key_diagnostics"] = {
            "duplicate_keys": after_dupes,
            "null_keys": after_nulls,
            "rows": len(after_df),
        }
        if after_dupes > 0:
            errors.append(ValidationIssue(code="DUPLICATE_KEY", message=f"After dataset contains {after_dupes} duplicate '{key_field}' values"))
        if after_nulls > 0:
            warnings.append(ValidationIssue(code="NULL_KEYS", message=f"After dataset contains {after_nulls} null '{key_field}' values"))

    # ── Staged change diagnostics and delete-policy checks ───────────
    staged_added = 0
    staged_removed = 0
    staged_modified = 0
    if staged_changes is not None:
        for idx, change in enumerate(staged_changes):
            if not isinstance(change, dict):
                errors.append(ValidationIssue(code="INVALID_STAGED_CHANGE", message=f"Staged change at index {idx} is not an object"))
                continue
            if change.get("is_new_item") or not change.get("catalog_item_id"):
                staged_added += 1
            elif _is_delete_staged_change(change):
                staged_removed += 1
            else:
                staged_modified += 1

        if staged_removed > 0 and not params.rules.allow_row_deletes:
            errors.append(
                ValidationIssue(
                    code="ROW_DELETES_BLOCKED",
                    message=f"{staged_removed} staged deletion(s) found, but allow_row_deletes=false",
                    fix_hint="Set rules.allow_row_deletes=true or remove delete entries from staged changes",
                )
            )

        computed["staged_summary"] = {
            "total": len(staged_changes),
            "rows_added": staged_added,
            "rows_removed": staged_removed,
            "rows_modified": staged_modified,
        }

    # ── Diff/staged consistency checks ───────────────────────────────
    if params.diff_summary:
        ds = params.diff_summary
        ds_added = int(ds.get("rows_added", 0))
        ds_removed = int(ds.get("rows_removed", 0))
        ds_modified = int(ds.get("rows_modified", 0))
        computed["diff_summary"] = ds

        if staged_changes is not None:
            if ds_added != staged_added or ds_removed != staged_removed or ds_modified != staged_modified:
                errors.append(
                    ValidationIssue(
                        code="COUNT_MISMATCH",
                        message=(
                            "Diff summary and staged changes disagree: "
                            f"diff(added={ds_added}, removed={ds_removed}, modified={ds_modified}) vs "
                            f"staged(added={staged_added}, removed={staged_removed}, modified={staged_modified})"
                        ),
                        fix_hint="Re-run execution_local_diff_compute and regenerate staged_changes before staging activity",
                    )
                )

        if before_df is not None and "rows_before" in ds and int(ds["rows_before"]) != len(before_df):
            warnings.append(ValidationIssue(code="ROWS_BEFORE_MISMATCH", message=f"diff_summary.rows_before={ds['rows_before']} but before dataset has {len(before_df)} rows"))
        if after_df is not None and "rows_after" in ds and int(ds["rows_after"]) != len(after_df):
            warnings.append(ValidationIssue(code="ROWS_AFTER_MISMATCH", message=f"diff_summary.rows_after={ds['rows_after']} but after dataset has {len(after_df)} rows"))

    # ── Schema alignment checks ──────────────────────────────────────
    required_fields: List[str] = []
    schema_properties: Dict[str, Dict[str, Any]] = {}
    if params.catalog_id:
        try:
            schema = await client.get_catalog_schema(params.catalog_id)
            schema_properties = schema.get("schema_definition", {}).get("properties", {}) or {}
            schema_fields = set(schema_properties.keys())
            required_fields = list(schema.get("schema_definition", {}).get("required", []))
            declared_new = _extract_declared_schema_additions(params.schema_changes)

            if after_df is not None:
                after_cols = set(after_df.columns) - SYSTEM_COLUMNS
                unknown_cols = after_cols - schema_fields - declared_new
                for col in sorted(unknown_cols):
                    errors.append(
                        ValidationIssue(
                            code="UNKNOWN_COLUMN",
                            message=f"Field '{col}' is not in the catalog schema and not declared in schema_changes",
                            fix_hint="Add it via schema_changes.mode=batch_fields_v1.payload.fields_to_add or remove it from the transformed output",
                        )
                    )
        except Exception as e:
            warnings.append(ValidationIssue(
                code="SCHEMA_CHECK_SKIPPED",
                message=f"Could not fetch catalog schema for validation: {e}",
            ))

    # ── Required field checks on new rows ────────────────────────────
    if staged_changes and required_fields:
        for idx, change in enumerate(staged_changes):
            is_new = bool(change.get("is_new_item")) or not change.get("catalog_item_id")
            if not is_new:
                continue
            after_data = change.get("after_data") or {}
            if not isinstance(after_data, dict):
                errors.append(ValidationIssue(code="INVALID_STAGED_CHANGE", message=f"New item at index {idx} has invalid after_data type"))
                continue

            missing = []
            for field in required_fields:
                val = after_data.get(field)
                if val is None:
                    missing.append(field)
                elif isinstance(val, str) and val.strip() == "":
                    missing.append(field)
            if missing:
                errors.append(
                    ValidationIssue(
                        code="MISSING_REQUIRED_FIELD",
                        message=f"New item at row_index={change.get('row_index', idx)} missing required fields: {missing}",
                    )
                )

    # ── Semantic field-format checks for schema-described fields ─────
    if staged_changes and schema_properties:
        for idx, change in enumerate(staged_changes):
            after_data = change.get("after_data") or {}
            if not isinstance(after_data, dict):
                continue

            row_index = change.get("row_index", idx)
            for field, value in after_data.items():
                if field not in schema_properties:
                    continue
                if value is None or not isinstance(value, str):
                    continue
                value_str = value.strip()
                if not value_str:
                    continue

                field_def = schema_properties[field]
                if _is_json_encoded_array_field(field_def) and not _is_valid_json_array_string(value_str):
                    errors.append(
                        ValidationIssue(
                            code="FIELD_FORMAT_MISMATCH",
                            message=(
                                f"Field '{field}' at row_index={row_index} expects a JSON-encoded array string, "
                                f"but received non-array content"
                            ),
                            fix_hint="Provide a JSON array string (e.g. '[\"SECTION 1\", \"SECTION 2\"]') or leave the field empty",
                        )
                    )

    # ── Large changeset warning ──────────────────────────────────────
    touched = 0
    if params.diff_summary:
        touched = int(params.diff_summary.get("rows_added", 0)) + int(params.diff_summary.get("rows_modified", 0)) + int(params.diff_summary.get("rows_removed", 0))
    elif staged_changes is not None:
        touched = len(staged_changes)

    base_rows = len(before_df) if before_df is not None else int(params.diff_summary.get("rows_before", 0) if params.diff_summary else 0)
    if base_rows > 0:
        ratio = touched / base_rows
        if ratio > params.rules.max_change_ratio_warning:
            warnings.append(ValidationIssue(
                code="LARGE_CHANGESET",
                message=f"{ratio:.0%} of base rows affected ({touched}/{base_rows})",
            ))

    # ── Diff vs staged delete policy check (secondary gate) ──────────
    if params.diff_summary and not params.rules.allow_row_deletes and int(params.diff_summary.get("rows_removed", 0)) > 0:
        errors.append(
            ValidationIssue(
                code="ROW_DELETES_BLOCKED",
                message=f"diff_summary.rows_removed={params.diff_summary.get('rows_removed', 0)} but allow_row_deletes=false",
                fix_hint="Set rules.allow_row_deletes=true to permit deletions",
            )
        )

    valid = len(errors) == 0
    return BundleValidateOutput(valid=valid, errors=errors, warnings=warnings, computed=computed)
