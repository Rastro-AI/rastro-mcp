"""
catalog_visualize_local MCP tool.

Builds a self-contained local HTML artifact for visually inspecting either:
- a catalog's product/variant records
- an activity's staged changes
"""

from __future__ import annotations

import html
import hashlib
import ipaddress
import json
import mimetypes
import re
import shutil
import statistics
import threading
import webbrowser
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlparse
from urllib.request import Request, urlopen

from rastro_mcp.client.api_client import RastroClient
from rastro_mcp.execution.path_safety import UnsafePathError, resolve_workspace_path
from rastro_mcp.models.contracts import CatalogVisualizeLocalInput, CatalogVisualizeLocalOutput

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp", ".svg", ".tif", ".tiff"}
_DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".ppt", ".pptx", ".txt"}
_IMAGE_PATH_HINTS = ("/media/", "/images/", "/image/", "/img/", "/photos/", "/photo/", "/thumbnails/", "/thumbnail/", "/gallery/", "/cdn/shop/files/", "/cache/")
_IMAGE_QUERY_HINTS = ("format=jpg", "format=jpeg", "format=png", "format=webp", "format=avif", "fm=jpg", "fm=jpeg", "fm=png", "fm=webp", "fm=avif", "width=", "w=", "height=", "h=", "quality=", "q=", "crop=", "fit=")
_IMAGE_HOST_HINTS = ("cdn.shopify.com", "images.", "img.", "res.cloudinary.com", "cloudinary.com", "imgix.net", "scene7.com")
_DOCUMENT_PATH_HINTS = ("/manual", "/manuals", "/datasheet", "/spec", "/specs", "/brochure", "/download", "/downloads", "/document", "/documents")
_DOCUMENT_QUERY_HINTS = ("format=pdf", "download=", "filename=")
_TITLE_FIELDS = (
    "product_title",
    "title",
    "name",
    "product_name",
    "sku_short_description",
    "sku_marketing_description",
    "sku_long_description_1",
    "description",
    "label",
    "manufacturer_part_number",
    "part_number",
    "sku",
    "product_id",
    "id",
)
_IDENTIFIER_FIELDS = ("product_id", "sku", "sunco_sku", "manufacturer_part_number", "part_number", "upc", "mpn", "ean", "id")
_EXTERNAL_URL_FIELDS = ("product_url", "source_url", "url", "external_url", "manual_url", "spec_url", "website")
_ANALYTICS_SKIP_FIELDS = {
    "source_explanations",
    "source_metadata",
    "_source_urls",
    "sources",
    "web_sources",
    "variants",
}
_ACTIVITY_PAGE_SIZE = 100
_VIEWER_SERVER_LOCK = threading.Lock()
_VIEWER_SERVER: Optional["_LocalViewerServer"] = None


def _title_case(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[_\.]+", " ", str(value or ""))).strip().title()


def _slug_fragment(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-") or "record"


def _is_populated(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def _dedupe(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _is_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _path_extension_from_url(url: str) -> str:
    try:
        return Path(urlparse(url).path).suffix.lower()
    except Exception:
        return ""


def _is_image_url(url: str, key_path: str) -> bool:
    lower_key = key_path.lower()
    if _path_extension_from_url(url) in _IMAGE_EXTENSIONS:
        return True
    parsed = urlparse(url)
    lower_path = parsed.path.lower()
    lower_query = parsed.query.lower()
    hostname = (parsed.hostname or "").lower()
    key_has_image_hint = any(token in lower_key for token in ("image", "photo", "thumbnail", "gallery", "swatch", "media"))
    host_has_image_hint = any(token in hostname for token in _IMAGE_HOST_HINTS)
    path_has_image_hint = any(token in lower_path for token in _IMAGE_PATH_HINTS)
    query_has_image_hint = any(token in lower_query for token in _IMAGE_QUERY_HINTS)
    return key_has_image_hint and (host_has_image_hint or path_has_image_hint or query_has_image_hint)


def _is_document_url(url: str, key_path: str) -> bool:
    lower_key = key_path.lower()
    if _path_extension_from_url(url) in _DOCUMENT_EXTENSIONS:
        return True
    parsed = urlparse(url)
    lower_path = parsed.path.lower()
    lower_query = parsed.query.lower()
    key_has_document_hint = any(token in lower_key for token in ("pdf", "document", "manual", "spec", "datasheet", "sheet", "brochure", "download"))
    path_has_document_hint = any(token in lower_path for token in _DOCUMENT_PATH_HINTS)
    query_has_document_hint = any(token in lower_query for token in _DOCUMENT_QUERY_HINTS)
    return key_has_document_hint and (path_has_document_hint or query_has_document_hint)


def _collect_urls(value: Any, key_path: str = "") -> Tuple[List[str], List[str], List[str]]:
    images: List[str] = []
    documents: List[str] = []
    links: List[str] = []

    def _walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, nested in node.items():
                next_path = f"{path}.{key}" if path else str(key)
                _walk(nested, next_path)
            return
        if isinstance(node, list):
            for nested in node:
                _walk(nested, path)
            return
        if not _is_url(node):
            return

        url = str(node)
        if _is_image_url(url, path):
            images.append(url)
        elif _is_document_url(url, path):
            documents.append(url)
        else:
            links.append(url)

    _walk(value, key_path)
    return _dedupe(images), _dedupe(documents), _dedupe(links)


def _pick_first_present(data: Dict[str, Any], fields: Sequence[str]) -> Optional[str]:
    for field in fields:
        value = data.get(field)
        if _is_populated(value):
            return str(value)
    return None


def _pick_title(data: Dict[str, Any], fallback: str) -> str:
    return _pick_first_present(data, _TITLE_FIELDS) or fallback


def _pick_identifier(data: Dict[str, Any], fallback: str) -> str:
    return _pick_first_present(data, _IDENTIFIER_FIELDS) or fallback


def _pick_external_url(data: Dict[str, Any], links: Sequence[str]) -> Optional[str]:
    preferred = _pick_first_present(data, _EXTERNAL_URL_FIELDS)
    if preferred and _is_url(preferred):
        return preferred
    return links[0] if links else None


def _normalize_schema(schema_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(schema_payload, dict):
        return {"field_order": [], "fields": {}}

    schema_definition = schema_payload.get("schema_definition") or schema_payload.get("schema") or {}
    properties = schema_definition.get("properties") if isinstance(schema_definition, dict) else {}
    if not isinstance(properties, dict):
        properties = {}

    required_fields = set(schema_definition.get("required") or []) if isinstance(schema_definition, dict) else set()

    workflow_info_map: Dict[str, Dict[str, Any]] = {}
    for entry in schema_payload.get("fields_workflow_info") or []:
        if isinstance(entry, dict) and entry.get("field_name"):
            workflow_info_map[str(entry["field_name"])] = entry

    field_order = list(properties.keys())
    for field_name in workflow_info_map:
        if field_name not in field_order:
            field_order.append(field_name)

    fields: Dict[str, Dict[str, Any]] = {}
    for field_name in field_order:
        prop = properties.get(field_name) if isinstance(properties.get(field_name), dict) else {}
        info = workflow_info_map.get(field_name, {})
        prop_type = prop.get("type")
        if isinstance(prop_type, list):
            type_label = ", ".join(str(entry) for entry in prop_type)
        else:
            type_label = str(info.get("field_type") or prop_type or prop.get("format") or "")

        fields[field_name] = {
            "label": prop.get("title") or _title_case(field_name),
            "type": type_label or None,
            "description": info.get("description") or prop.get("description"),
            "scope": info.get("scope"),
            "required": bool(info.get("required")) or field_name in required_fields,
            "field_category": info.get("field_category"),
            "unit": info.get("unit"),
            "sample_values": info.get("sample_values") or [],
        }

    return {"field_order": field_order, "fields": fields}

def _ordered_field_names(names: Iterable[str], field_order: Sequence[str]) -> List[str]:
    known = [field for field in field_order if field in names]
    extras = sorted(field for field in names if field not in set(known))
    return known + extras


def _is_effectively_empty(value: Any) -> bool:
    return value in (None, "", [], {})


def _parse_numeric(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None

    cleaned = value.strip()
    if not cleaned:
        return None

    cleaned = cleaned.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _is_mostly_numeric(values: Sequence[Any]) -> bool:
    if not values:
        return False
    numeric_count = sum(1 for value in values if _parse_numeric(value) is not None)
    return numeric_count / len(values) >= 0.7


def _compute_overall_completion_rate(field_stats: Dict[str, Dict[str, Any]]) -> float:
    if not field_stats:
        return 0.0
    return round(sum(float(stats.get("completion_rate") or 0.0) for stats in field_stats.values()) / len(field_stats), 3)


def _compute_field_analytics(data_rows: Sequence[Dict[str, Any]], field_order: Sequence[str]) -> Dict[str, Any]:
    rows = [row for row in data_rows if isinstance(row, dict)]
    if not rows:
        return {"field_stats": {}, "overall_completion_rate": 0.0}

    all_field_names: set[str] = set()
    for row in rows:
        all_field_names.update(name for name in row.keys() if name not in _ANALYTICS_SKIP_FIELDS)

    ordered_field_names = _ordered_field_names(all_field_names, field_order)
    field_stats: Dict[str, Dict[str, Any]] = {}

    for field_name in ordered_field_names:
        values: List[Any] = []
        completed = 0

        for row in rows:
            value = row.get(field_name)
            if _is_effectively_empty(value):
                continue
            completed += 1
            values.append(value)

        stats: Dict[str, Any] = {
            "completed": completed,
            "total": len(rows),
            "completion_rate": round(completed / len(rows), 3) if rows else 0.0,
        }

        if values:
            if _is_mostly_numeric(values):
                numeric_values = [_parse_numeric(value) for value in values]
                numeric_values = [value for value in numeric_values if value is not None]
                if numeric_values:
                    stats["avg"] = round(statistics.mean(numeric_values), 2)
                    stats["min"] = round(min(numeric_values), 2)
                    stats["max"] = round(max(numeric_values), 2)
            else:
                lengths = [len(json.dumps(value, default=str)) if isinstance(value, (dict, list)) else len(str(value)) for value in values]
                stats["avg_length"] = round(statistics.mean(lengths), 0)
                stats["min_length"] = min(lengths)
                stats["max_length"] = max(lengths)

                histogram_candidates = [json.dumps(value, default=str, sort_keys=True) if isinstance(value, (dict, list)) else str(value) for value in values]
                unique_ratio = len(set(histogram_candidates)) / len(histogram_candidates) if histogram_candidates else 1.0
                if len(set(histogram_candidates)) <= 15 and unique_ratio <= 0.5:
                    counts = Counter(histogram_candidates)
                    stats["histogram"] = [
                        {"value": value, "count": count, "pct": round((count / len(histogram_candidates)) * 100, 1)}
                        for value, count in counts.most_common(10)
                    ]

        field_stats[field_name] = stats

    taxonomy_stats = None
    if any(field_name in all_field_names for field_name in ("category_id", "category_path", "taxonomy_attributes")):
        categorized_rows = [row for row in rows if not _is_effectively_empty(row.get("category_id")) or not _is_effectively_empty(row.get("category_path"))]
        categorized = len(categorized_rows)
        category_values = [str(row.get("category_path")) for row in categorized_rows if _is_populated(row.get("category_path"))]
        category_histogram = [
            {"value": value, "count": count, "pct": round((count / categorized) * 100, 1)}
            for value, count in Counter(category_values).most_common(10)
        ] if categorized else []

        attribute_names: set[str] = set()
        for row in categorized_rows:
            attrs = row.get("taxonomy_attributes")
            if isinstance(attrs, dict):
                attribute_names.update(attrs.keys())

        attribute_stats: Dict[str, Dict[str, Any]] = {}
        for attr_name in sorted(attribute_names):
            completed = sum(
                1
                for row in categorized_rows
                if isinstance(row.get("taxonomy_attributes"), dict)
                and not _is_effectively_empty((row.get("taxonomy_attributes") or {}).get(attr_name))
            )
            attribute_stats[attr_name] = {
                "completed": completed,
                "total": categorized,
                "completion_rate": round(completed / categorized, 3) if categorized else 0.0,
            }

        taxonomy_stats = {
            "categorized": categorized,
            "eligible": len(rows),
            "completion_rate": round(categorized / len(rows), 3) if rows else 0.0,
            "category_histogram": category_histogram,
            "attribute_stats": attribute_stats,
        }

    return {
        "field_stats": field_stats,
        "taxonomy_stats": taxonomy_stats,
        "overall_completion_rate": _compute_overall_completion_rate(field_stats),
    }


def _merge_variant_data(product_data: Dict[str, Any], variant: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(product_data)
    merged.pop("variants", None)

    attributes = variant.get("attributes")
    if isinstance(attributes, dict):
        merged["attributes"] = attributes
        for name, value in attributes.items():
            merged[f"attribute.{_slug_fragment(name)}"] = value

    meta_fields = variant.get("meta_fields")
    if isinstance(meta_fields, dict):
        merged["meta_fields"] = meta_fields
        for name, value in meta_fields.items():
            merged[name] = value

    for name, value in variant.items():
        if name in {"attributes", "meta_fields"}:
            continue
        merged[name] = value

    return merged


def _variant_descriptor(variant: Dict[str, Any], index: int) -> str:
    attributes = variant.get("attributes")
    if isinstance(attributes, dict):
        values = [str(value) for value in attributes.values() if _is_populated(value)]
        if values:
            return " / ".join(values)
    for field_name in ("sku", "sunco_sku", "upc"):
        value = variant.get(field_name)
        if _is_populated(value):
            return str(value)
    return f"Variant {index + 1}"


def _normalize_variant_record(
    product_record_id: str,
    product_title: str,
    product_data: Dict[str, Any],
    variant: Dict[str, Any],
    index: int,
    fallback_images: Sequence[str],
    fallback_documents: Sequence[str],
) -> Dict[str, Any]:
    variant_data = _merge_variant_data(product_data, variant)
    variant_images, variant_documents, variant_links = _collect_urls(variant_data)
    if not variant_images:
        variant_images = list(fallback_images)
    if not variant_documents:
        variant_documents = list(fallback_documents)

    descriptor = _variant_descriptor(variant, index)
    variant_identifier = _pick_identifier(variant_data, fallback=f"{product_record_id}-variant-{index + 1}")
    return {
        "id": f"{product_record_id}::variant::{index}",
        "entity_type": "variant",
        "title": f"{product_title} / {descriptor}" if descriptor else product_title,
        "identifier": variant_identifier,
        "images": variant_images,
        "documents": variant_documents,
        "external_url": _pick_external_url(variant_data, variant_links),
        "data": variant_data,
        "variant_count": 0,
        "parent_record_id": product_record_id,
    }


def _normalize_catalog_records(
    items: Sequence[Dict[str, Any]],
    field_order: Sequence[str],
    product_enabled: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    records: List[Dict[str, Any]] = []
    variant_records: List[Dict[str, Any]] = []

    for item in items:
        item_id = str(item.get("id") or f"record-{len(records) + 1}")
        data = item.get("data") or {}
        if not isinstance(data, dict):
            data = {}

        variants = data.get("variants") if isinstance(data.get("variants"), list) else []
        base_data = {key: value for key, value in data.items() if key != "variants"}
        product_images, product_documents, product_links = _collect_urls(data)
        product_title = _pick_title(base_data, fallback=item_id)

        normalized_variants = [
            _normalize_variant_record(
                product_record_id=item_id,
                product_title=product_title,
                product_data=base_data,
                variant=variant if isinstance(variant, dict) else {},
                index=index,
                fallback_images=product_images,
                fallback_documents=product_documents,
            )
            for index, variant in enumerate(variants)
        ]
        variant_records.extend(normalized_variants)

        record = {
            "id": item_id,
            "entity_type": "product" if product_enabled else "record",
            "title": product_title,
            "identifier": _pick_identifier(base_data, fallback=item_id),
            "images": product_images,
            "documents": product_documents,
            "external_url": _pick_external_url(base_data, product_links),
            "data": base_data,
            "variant_count": len(normalized_variants),
        }
        records.append(record)

    return records, variant_records


def _normalize_activity_records(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []

    for index, item in enumerate(items):
        item_id = str(item.get("id") or f"change-{index + 1}")
        before_data = item.get("before_data") if isinstance(item.get("before_data"), dict) else {}
        after_data = item.get("after_data") if isinstance(item.get("after_data"), dict) else {}
        effective_data = after_data or before_data
        images, documents, links = _collect_urls({"before_data": before_data, "after_data": after_data})
        review_info = item.get("review_info") if isinstance(item.get("review_info"), dict) else {}

        flags = [str(flag) for flag in (review_info.get("flags") or []) if _is_populated(flag)]
        for issue in review_info.get("field_issues") or []:
            if isinstance(issue, dict):
                field_name = issue.get("field") or "field"
                message = issue.get("message") or "Issue"
                flags.append(f"{field_name}: {message}")
            elif _is_populated(issue):
                flags.append(str(issue))

        change_type = "new" if item.get("is_new_item") or not before_data else "update"

        records.append(
            {
                "id": item_id,
                "change_type": change_type,
                "status": item.get("status"),
                "title": _pick_title(effective_data, fallback=item.get("catalog_item_id") or item_id),
                "identifier": _pick_identifier(effective_data, fallback=item.get("catalog_item_id") or item_id),
                "images": images,
                "documents": documents,
                "external_url": _pick_external_url(effective_data, links),
                "before_data": before_data,
                "after_data": after_data,
                "review_reasoning": review_info.get("reasoning"),
                "flags": flags,
                "row_index": item.get("row_index"),
                "catalog_item_id": item.get("catalog_item_id"),
                "entity_type": item.get("catalog_item_entity_type"),
            }
        )

    return records


def _viewer_source_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "viewer"


def _load_viewer_template() -> str:
    template_path = _viewer_source_dir() / "review_template.html"
    return template_path.read_text(encoding="utf-8")


def _copy_viewer_runtime_files(artifact_dir: Path) -> None:
    for asset_name in ("viewer_app.js",):
        shutil.copy2(_viewer_source_dir() / asset_name, artifact_dir / asset_name)


def _render_viewer_html(title: str) -> str:
    template = _load_viewer_template()
    return template.replace("__RASTRO_VIEWER_TITLE__", html.escape(title))


def _build_artifact_dir(output_dir: str, prefix: str, identifier: str) -> Path:
    try:
        safe_root = resolve_workspace_path(output_dir, label="output_dir")
    except UnsafePathError as exc:
        raise ValueError(str(exc)) from exc

    root_path = Path(safe_root)
    root_path.mkdir(parents=True, exist_ok=True)
    artifact_dir = root_path / f"{prefix}-{_slug_fragment(identifier)[:32]}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _is_proxyable_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False

    hostname = parsed.hostname.strip().lower()
    if hostname in {"localhost", "127.0.0.1", "::1"} or hostname.endswith(".local"):
        return False

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return True

    return not (address.is_private or address.is_loopback or address.is_link_local or address.is_reserved or address.is_multicast)


def _serve_file_response(handler: BaseHTTPRequestHandler, file_path: Path) -> None:
    try:
        payload = file_path.read_bytes()
    except FileNotFoundError:
        handler.send_error(404, "File not found")
        return

    content_type, encoding = mimetypes.guess_type(str(file_path))
    content_type = content_type or "application/octet-stream"
    if content_type.startswith("text/") and "charset" not in content_type:
        content_type = f"{content_type}; charset=utf-8"

    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(payload)))
    handler.send_header("Cache-Control", "no-store")
    if encoding:
        handler.send_header("Content-Encoding", encoding)
    handler.end_headers()
    handler.wfile.write(payload)


def _serve_proxy_response(handler: BaseHTTPRequestHandler, target_url: str) -> None:
    if not _is_proxyable_url(target_url):
        handler.send_error(400, "URL is not allowed for proxying")
        return

    request = Request(
        target_url,
        headers={
            "User-Agent": "rastro-mcp-viewer/1.0",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        },
    )

    try:
        with urlopen(request, timeout=20) as response:
            payload = response.read()
            content_type = response.headers.get("Content-Type") or "application/octet-stream"
            handler.send_response(response.getcode() or 200)
            handler.send_header("Content-Type", content_type)
            handler.send_header("Content-Length", str(len(payload)))
            handler.send_header("Cache-Control", "public, max-age=3600")
            handler.end_headers()
            handler.wfile.write(payload)
    except HTTPError as exc:
        handler.send_error(exc.code, exc.reason)
    except URLError as exc:
        handler.send_error(502, f"Upstream fetch failed: {exc.reason}")
    except Exception as exc:
        handler.send_error(502, f"Upstream fetch failed: {exc}")


def _make_viewer_request_handler(local_viewer_server: "_LocalViewerServer"):
    class ViewerRequestHandler(BaseHTTPRequestHandler):
        server_version = "RastroLocalViewer/1.0"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)

            if parsed.path == "/healthz":
                body = b"ok"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed.path == "/artifact":
                requested_path = (query.get("path") or [None])[0]
                if not requested_path:
                    self.send_error(400, "Missing path")
                    return
                try:
                    resolved = Path(resolve_workspace_path(requested_path, must_exist=True, expect_file=True, label="artifact path"))
                except UnsafePathError as exc:
                    self.send_error(403, str(exc))
                    return
                _serve_file_response(self, resolved)
                return

            if parsed.path.startswith("/artifacts/"):
                path_parts = [unquote(part) for part in parsed.path.split("/") if part]
                if len(path_parts) < 2:
                    self.send_error(400, "Missing artifact path")
                    return
                slug = path_parts[1]
                requested_parts = path_parts[2:]
                resolved = local_viewer_server.resolve_registered_artifact(slug, requested_parts)
                if resolved is None:
                    self.send_error(404, "Artifact file not found")
                    return
                _serve_file_response(self, resolved)
                return

            if parsed.path == "/proxy":
                target_url = (query.get("url") or [None])[0]
                if not target_url:
                    self.send_error(400, "Missing url")
                    return
                _serve_proxy_response(self, target_url)
                return

            self.send_error(404, "Not found")

        def log_message(self, format: str, *args: Any) -> None:
            return

    return ViewerRequestHandler


class _LocalViewerServer:
    def __init__(self) -> None:
        self._artifact_roots: Dict[str, Path] = {}
        self._artifact_roots_lock = threading.Lock()
        handler = _make_viewer_request_handler(self)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.host = "127.0.0.1"
        self.port = int(self._server.server_address[1])
        self._thread = threading.Thread(target=self._server.serve_forever, name="rastro-local-viewer", daemon=True)
        self._thread.start()

    def register_artifact_root(self, artifact_root: Path) -> str:
        resolved_root = artifact_root.resolve()
        digest = hashlib.sha1(str(resolved_root).encode("utf-8")).hexdigest()[:10]
        slug = f"{_slug_fragment(resolved_root.name)}-{digest}"
        with self._artifact_roots_lock:
            self._artifact_roots[slug] = resolved_root
        return slug

    def resolve_registered_artifact(self, slug: str, requested_parts: Sequence[str]) -> Optional[Path]:
        with self._artifact_roots_lock:
            artifact_root = self._artifact_roots.get(slug)

        if artifact_root is None:
            return None

        relative_path = Path(*requested_parts) if requested_parts else Path("viewer.html")
        candidate = (artifact_root / relative_path).resolve()
        if candidate != artifact_root and artifact_root not in candidate.parents:
            return None
        if not candidate.exists() or not candidate.is_file():
            return None
        return candidate

    def artifact_url_for(self, file_path: Path) -> str:
        resolved = file_path.resolve()
        artifact_root = resolved.parent
        slug = self.register_artifact_root(artifact_root)
        relative_path = resolved.relative_to(artifact_root)
        encoded_relative = "/".join(quote(part) for part in relative_path.parts)
        return f"http://{self.host}:{self.port}/artifacts/{slug}/{encoded_relative}"


def _get_viewer_server() -> _LocalViewerServer:
    global _VIEWER_SERVER
    with _VIEWER_SERVER_LOCK:
        if _VIEWER_SERVER is None:
            _VIEWER_SERVER = _LocalViewerServer()
        return _VIEWER_SERVER


async def _fetch_catalog_bundle(client: RastroClient, params: CatalogVisualizeLocalInput) -> Dict[str, Any]:
    catalog = await client.get_catalog(params.catalog_id)
    schema_warning: Optional[str] = None
    try:
        schema_payload = await client.get_catalog_schema(params.catalog_id)
    except Exception as exc:
        schema_payload = {}
        schema_warning = f"Schema could not be loaded: {exc}"

    items_response = await client.get_catalog_items(
        catalog_id=params.catalog_id,
        limit=params.limit,
        offset=params.offset,
        search=params.search,
    )
    items = items_response.get("items") or []
    total_available = int(items_response.get("total") or len(items))

    normalized_schema = _normalize_schema(schema_payload)
    product_enabled = bool(catalog.get("product_enabled")) or str(catalog.get("variant_mode") or "") == "product_grouped"
    records, variant_records = _normalize_catalog_records(items, normalized_schema["field_order"], product_enabled)
    field_analytics = _compute_field_analytics(
        [record.get("data") or {} for record in records] + [record.get("data") or {} for record in variant_records],
        normalized_schema["field_order"],
    )

    catalog_name = str(catalog.get("name") or params.catalog_id)
    title = params.title or f"{catalog_name} catalog"

    return {
        "mode": "catalog",
        "title": title,
        "catalog": {
            "id": catalog.get("id") or params.catalog_id,
            "name": catalog.get("name"),
            "description": catalog.get("description"),
            "variant_mode": catalog.get("variant_mode"),
            "product_enabled": product_enabled,
        },
        "activity": None,
        "schema": normalized_schema,
        "field_analytics": field_analytics,
        "records": records,
        "variant_records": variant_records,
        "_meta": {
            "catalog_id": params.catalog_id,
            "loaded_records": len(records),
            "total_available": total_available,
            "warnings": [schema_warning] if schema_warning else [],
        },
    }


async def _fetch_activity_page(
    client: RastroClient,
    activity_id: str,
    page: int,
    page_size: int,
    search: Optional[str],
) -> Dict[str, Any]:
    if hasattr(client, "_request"):
        request_params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if search:
            request_params["search_query"] = search
        return await client._request("GET", f"/activities/{activity_id}/staged-changes", params=request_params)

    response = await client.get_staged_changes(activity_id, limit=page_size, offset=(page - 1) * page_size)
    if search:
        lowered = search.lower()
        response_items = response.get("items") or []
        response["items"] = [
            item
            for item in response_items
            if lowered in json.dumps(item.get("after_data") or item.get("before_data") or {}, default=str).lower()
        ]
    return response


async def _fetch_activity_staged_changes(
    client: RastroClient,
    activity_id: str,
    limit: int,
    offset: int,
    search: Optional[str],
) -> Tuple[List[Dict[str, Any]], int]:
    current_page = (offset // _ACTIVITY_PAGE_SIZE) + 1
    skip_from_first_page = offset % _ACTIVITY_PAGE_SIZE
    collected: List[Dict[str, Any]] = []
    total_available = 0

    while len(collected) < limit:
        page = await _fetch_activity_page(client, activity_id, current_page, _ACTIVITY_PAGE_SIZE, search)
        page_items = list(page.get("items") or [])
        if current_page == (offset // _ACTIVITY_PAGE_SIZE) + 1 and skip_from_first_page:
            page_items = page_items[skip_from_first_page:]

        if total_available == 0:
            total_available = int(page.get("total") or 0)

        if not page_items:
            break

        remaining = limit - len(collected)
        collected.extend(page_items[:remaining])

        total_pages = int(page.get("total_pages") or 0)
        if len(collected) >= limit or (total_pages and current_page >= total_pages) or len(page_items) < _ACTIVITY_PAGE_SIZE:
            break
        current_page += 1

    return collected, total_available


async def _fetch_activity_bundle(client: RastroClient, params: CatalogVisualizeLocalInput) -> Dict[str, Any]:
    activity = await client.get_activity(params.activity_id)
    catalog_id = params.catalog_id or activity.get("catalog_id")

    catalog: Dict[str, Any] = {}
    schema_payload: Dict[str, Any] = {}
    warnings: List[str] = []
    if catalog_id:
        try:
            catalog = await client.get_catalog(catalog_id)
        except Exception as exc:
            warnings.append(f"Catalog metadata could not be loaded: {exc}")
        try:
            schema_payload = await client.get_catalog_schema(catalog_id)
        except Exception as exc:
            warnings.append(f"Schema could not be loaded: {exc}")

    review_summary: Dict[str, Any] = {}
    try:
        review_summary = await client.get_staged_changes_summary(params.activity_id)
    except Exception as exc:
        warnings.append(f"Activity summary could not be loaded: {exc}")

    staged_changes, total_available = await _fetch_activity_staged_changes(
        client=client,
        activity_id=params.activity_id,
        limit=params.limit,
        offset=params.offset,
        search=params.search,
    )

    normalized_schema = _normalize_schema(schema_payload)
    records = _normalize_activity_records(staged_changes)
    local_field_analytics = _compute_field_analytics([record.get("after_data") or {} for record in records], normalized_schema["field_order"])
    summary_field_stats = review_summary.get("field_stats") if isinstance(review_summary.get("field_stats"), dict) else None
    summary_taxonomy_stats = review_summary.get("taxonomy_stats") if isinstance(review_summary.get("taxonomy_stats"), dict) else None
    field_analytics = {
        "field_stats": summary_field_stats or local_field_analytics.get("field_stats") or {},
        "taxonomy_stats": summary_taxonomy_stats or local_field_analytics.get("taxonomy_stats"),
    }
    field_analytics["overall_completion_rate"] = _compute_overall_completion_rate(field_analytics["field_stats"])
    total_changes_from_summary = int(review_summary.get("total_changes") or 0)
    if total_changes_from_summary > 0:
        total_available = total_changes_from_summary
    review_url = f"https://dashboard.rastro.ai/catalog/{catalog_id}?activity={params.activity_id}" if catalog_id else None

    catalog_name = str(catalog.get("name") or catalog_id or "Standalone activity")
    title = params.title or f"{catalog_name} activity"

    return {
        "mode": "activity",
        "title": title,
        "catalog": {
            "id": catalog.get("id") or catalog_id,
            "name": catalog.get("name"),
            "description": catalog.get("description"),
            "variant_mode": catalog.get("variant_mode"),
            "product_enabled": bool(catalog.get("product_enabled")) or str(catalog.get("variant_mode") or "") == "product_grouped",
        }
        if catalog_id
        else None,
        "activity": {
            "id": activity.get("id") or params.activity_id,
            "catalog_id": catalog_id,
            "type": activity.get("type"),
            "status": activity.get("status"),
            "description": (
                ((activity.get("input") or {}).get("activity_message") if isinstance(activity.get("input"), dict) else None)
                or activity.get("last_message")
            ),
            "review_url": review_url,
        },
        "schema": normalized_schema,
        "field_analytics": field_analytics,
        "records": records,
        "variant_records": [],
        "_meta": {
            "catalog_id": catalog_id,
            "activity_id": params.activity_id,
            "loaded_records": len(records),
            "total_available": total_available,
            "warnings": warnings,
        },
    }


async def catalog_visualize_local(client: RastroClient, params: CatalogVisualizeLocalInput) -> CatalogVisualizeLocalOutput:
    """Generate a local HTML viewer for a catalog or activity and optionally open it."""
    mode = (params.mode or "auto").lower()
    resolved_mode = "activity" if mode == "activity" or (mode == "auto" and params.activity_id and not params.catalog_id) else "catalog"

    if resolved_mode == "catalog":
        bundle = await _fetch_catalog_bundle(client, params)
        artifact_identifier = params.catalog_id or "catalog"
    else:
        bundle = await _fetch_activity_bundle(client, params)
        artifact_identifier = params.activity_id or "activity"

    artifact_dir = _build_artifact_dir(params.output_dir, resolved_mode, artifact_identifier)
    bundle_path = artifact_dir / "bundle.json"
    viewer_path = artifact_dir / "viewer.html"

    output_warnings = list(bundle.get("_meta", {}).get("warnings") or [])
    title = str(bundle.get("title") or "Catalog visualization")

    bundle_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _copy_viewer_runtime_files(artifact_dir)
    viewer_path.write_text(_render_viewer_html(title), encoding="utf-8")

    viewer_server = _get_viewer_server()
    viewer_url = viewer_server.artifact_url_for(viewer_path)
    browser_open_attempted = bool(params.open_browser)
    opened_in_browser = False
    if params.open_browser:
        try:
            opened_in_browser = bool(webbrowser.open(viewer_url))
            if not opened_in_browser:
                output_warnings.append("Viewer artifact was created, but the local browser did not acknowledge the open request.")
        except Exception as exc:
            output_warnings.append(f"Viewer artifact was created, but opening the browser failed: {exc}")

    return CatalogVisualizeLocalOutput(
        mode=resolved_mode,
        title=title,
        catalog_id=bundle.get("_meta", {}).get("catalog_id"),
        activity_id=bundle.get("_meta", {}).get("activity_id"),
        artifact_dir=str(artifact_dir),
        bundle_path=str(bundle_path),
        viewer_path=str(viewer_path),
        viewer_url=viewer_url,
        loaded_records=int(bundle.get("_meta", {}).get("loaded_records") or 0),
        total_available=int(bundle.get("_meta", {}).get("total_available") or 0),
        browser_open_attempted=browser_open_attempted,
        opened_in_browser=opened_in_browser,
        warnings=output_warnings,
    )
