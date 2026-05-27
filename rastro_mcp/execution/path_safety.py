"""
Path safety helpers for MCP local execution tools.

By default, all local file operations are constrained to workspace roots:
- `RASTRO_MCP_WORKSPACE_ROOT` if set
- otherwise the current working directory at runtime
- `RASTRO_MCP_EXTRA_WORKSPACE_ROOTS` entries, when set
- the catalog-agent artifact root, `$TMPDIR/catalog_agent`, where production
  writes generated staged-change files
"""

from pathlib import Path
import os


class UnsafePathError(ValueError):
    """Raised when a path is outside the allowed workspace boundary."""


def _workspace_roots() -> list[Path]:
    configured = os.environ.get("RASTRO_MCP_WORKSPACE_ROOT")
    if configured:
        roots = [Path(configured).expanduser().resolve()]
    else:
        roots = [Path.cwd().resolve()]

    extra_roots = os.environ.get("RASTRO_MCP_EXTRA_WORKSPACE_ROOTS", "")
    for raw_root in extra_roots.split(os.pathsep):
        if raw_root.strip():
            roots.append(Path(raw_root).expanduser().resolve())

    catalog_agent_root = os.environ.get("CATALOG_AGENT_WORKSPACE_ROOT") or str(Path(os.getenv("TMPDIR", "/tmp")) / "catalog_agent")
    roots.append(Path(catalog_agent_root).expanduser().resolve())

    deduped: list[Path] = []
    for root in roots:
        if root not in deduped:
            deduped.append(root)
    return deduped


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

    roots = _workspace_roots()
    root = roots[0]
    raw = Path(path).expanduser()
    resolved = (raw if raw.is_absolute() else (root / raw)).resolve(strict=False)

    for allowed_root in roots:
        try:
            resolved.relative_to(allowed_root)
            break
        except ValueError:
            continue
    else:
        root_label = ", ".join(str(allowed_root) for allowed_root in roots)
        raise UnsafePathError(f"{label} must stay within workspace roots: {root_label}")

    if must_exist and not resolved.exists():
        raise UnsafePathError(f"{label} does not exist: {resolved}")
    if expect_file and resolved.exists() and not resolved.is_file():
        raise UnsafePathError(f"{label} is not a file: {resolved}")
    if expect_dir and resolved.exists() and not resolved.is_dir():
        raise UnsafePathError(f"{label} is not a directory: {resolved}")

    return str(resolved)
