"""Schema scope semantics shared by bundle preflight and catalog validation."""

from typing import Any, Dict, Optional


def field_applies_to_entity(field_definition: Dict[str, Any], entity_type: Optional[str]) -> bool:
    """Unscoped fields belong to variants; unknown row types retain all checks.

    Untyped import rows can contain both product and variant data before the
    catalog writer splits them, so their required checks must not be relaxed.
    """
    scope = field_definition.get("x-field-scope") or "variant"
    return entity_type not in {"product", "variant"} or scope not in {"product", "variant"} or scope == entity_type
