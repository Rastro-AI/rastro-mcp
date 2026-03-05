"""
Path safety helpers for MCP local execution tools.

By default, all local file operations are constrained to a workspace root:
- `RASTRO_MCP_WORKSPACE_ROOT` if set
- otherwise the current working directory at runtime
"""

from pathlib import Path
import os


class UnsafePathError(ValueError):
    """Raised when a path is outside the allowed workspace boundary."""


def _workspace_root() -> Path:
    configured = os.environ.get("RASTRO_MCP_WORKSPACE_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path.cwd().resolve()


def resolve_workspace_path(
    path: str,
    *,
    must_exist: bool = False,
    expect_file: bool = False,
    expect_dir: bool = False,
    label: str = "Path",
) -> str:
    """
    Resolve and validate a path against the workspace root boundary.
    """
    if not isinstance(path, str) or not path.strip():
        raise UnsafePathError(f"{label} must be a non-empty string")

    root = _workspace_root()
    raw = Path(path).expanduser()
    resolved = (raw if raw.is_absolute() else (root / raw)).resolve(strict=False)

    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise UnsafePathError(f"{label} must stay within workspace root: {root}") from exc

    if must_exist and not resolved.exists():
        raise UnsafePathError(f"{label} does not exist: {resolved}")
    if expect_file and resolved.exists() and not resolved.is_file():
        raise UnsafePathError(f"{label} is not a file: {resolved}")
    if expect_dir and resolved.exists() and not resolved.is_dir():
        raise UnsafePathError(f"{label} is not a directory: {resolved}")

    return str(resolved)
