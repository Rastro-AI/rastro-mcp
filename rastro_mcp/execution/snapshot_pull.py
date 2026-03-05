"""
execution_catalog_snapshot_pull

Exports catalog rows + schema locally for Python transforms.
Pulls data via the Rastro public API, flattens item.data into row fields,
and writes parquet/csv + schema JSON to disk.
"""

import json
import os
from typing import Optional

import pandas as pd

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.execution.path_safety import resolve_workspace_path
from rastro_mcp.models.contracts import SnapshotFormat, SnapshotPullInput, SnapshotPullOutput


def _coerce_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce mixed-type object columns to strings so parquet writes reliably.
    Keeps homogeneous numeric/bool columns unchanged.
    """
    safe_df = df.copy()
    for column in safe_df.columns:
        series = safe_df[column]
        if series.dtype != "object":
            continue

        non_null_types = set()
        for value in series:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            non_null_types.add(type(value))
            if len(non_null_types) > 1:
                break

        if len(non_null_types) > 1:
            safe_df[column] = series.map(
                lambda v: None if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
            )
    return safe_df


async def snapshot_pull(client: RastroClient, params: SnapshotPullInput) -> SnapshotPullOutput:
    """Pull catalog snapshot to local files."""
    catalog_id = params.catalog_id
    output_dir = resolve_workspace_path(params.output_dir, label="output_dir")
    os.makedirs(output_dir, exist_ok=True)

    # 1. Pull schema
    schema = await client.get_catalog_schema(catalog_id)
    schema_path = os.path.join(output_dir, f"catalog_{catalog_id}_schema.json")
    with open(schema_path, "w") as f:
        json.dump(schema, f, indent=2, default=str)

    # 2. Pull items (fast full pull by default; sample uses single-page fetch).
    page_size = max(1, params.page_size)
    max_concurrency = max(1, params.max_concurrency)
    prefer_raw = bool(params.prefer_raw)

    async def _pull_all(use_raw_endpoint: bool):
        candidate_sizes = [page_size]
        if page_size > 400:
            candidate_sizes.append(400)
        if page_size > 200:
            candidate_sizes.append(200)

        last_error: Optional[Exception] = None
        for size in dict.fromkeys(candidate_sizes):
            try:
                if use_raw_endpoint:
                    return await client.get_catalog_raw_items_all(
                        catalog_id=catalog_id,
                        page_size=size,
                        max_concurrency=max_concurrency,
                    )
                return await client.get_catalog_items_all(
                    catalog_id=catalog_id,
                    page_size=size,
                    max_concurrency=max_concurrency,
                )
            except Exception as exc:
                last_error = exc
                continue

        if last_error:
            raise last_error
        return []

    if params.sample_size:
        sample_limit = max(1, params.sample_size)
        if prefer_raw:
            try:
                resp = await client.get_catalog_raw_items(catalog_id, limit=sample_limit, offset=0)
            except Exception:
                resp = await client.get_catalog_items(catalog_id, limit=sample_limit, offset=0)
        else:
            resp = await client.get_catalog_items(catalog_id, limit=sample_limit, offset=0)
        all_items = (resp.get("items", []) or [])[:sample_limit]
    else:
        if prefer_raw:
            try:
                all_items = await _pull_all(use_raw_endpoint=True)
            except Exception:
                # Backward compatibility: fall back to public transformed items.
                all_items = await _pull_all(use_raw_endpoint=False)
        else:
            all_items = await _pull_all(use_raw_endpoint=False)

    # 3. Flatten item data into rows
    rows = []
    for item in all_items:
        row = {}
        # System columns
        row["__catalog_item_id"] = item.get("id", "")
        row["__entity_type"] = item.get("entity_type", "")
        row["__parent_id"] = item.get("parent_id", "")
        row["__current_version"] = item.get("current_version", "")

        # Flatten data dict
        data = item.get("data", {})
        if isinstance(data, dict):
            for k, v in data.items():
                # Convert nested structures to JSON strings for flat storage
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v, default=str)
                else:
                    row[k] = v
        rows.append(row)

    # 4. Write to file
    df = pd.DataFrame(rows)
    if params.format == SnapshotFormat.PARQUET:
        snapshot_path = os.path.join(output_dir, f"catalog_{catalog_id}.parquet")
        try:
            df.to_parquet(snapshot_path, index=False)
        except Exception:
            parquet_safe_df = _coerce_dataframe_for_parquet(df)
            parquet_safe_df.to_parquet(snapshot_path, index=False)
    else:
        snapshot_path = os.path.join(output_dir, f"catalog_{catalog_id}.csv")
        df.to_csv(snapshot_path, index=False)

    return SnapshotPullOutput(
        catalog_id=catalog_id,
        snapshot_path=snapshot_path,
        schema_path=schema_path,
        rows=len(df),
        columns=len(df.columns),
        base_snapshot_id=None,
    )
