"""Command-line entry point for rastro-mcp.

Default command starts the MCP stdio server.
`rastro-mcp login` runs browser-based token acquisition.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shlex
import threading
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

from rastro_mcp.server import main as run_server_main


DEFAULT_DASHBOARD_URL = "https://dashboard.rastro.ai"
DEFAULT_CALLBACK_HOST = "127.0.0.1"
DEFAULT_CALLBACK_PORT = 0
DEFAULT_TIMEOUT_SECONDS = 180


@dataclass
class LoginResult:
    token: str
    callback_url: str
    auth_url: str
    received_at: float


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rastro-mcp",
        description="Rastro MCP server and login helper",
    )
    subparsers = parser.add_subparsers(dest="command")

    login = subparsers.add_parser(
        "login",
        help="Open dashboard auth in browser and capture token via localhost callback.",
    )
    login.add_argument(
        "--dashboard-url",
        default=os.environ.get("RASTRO_DASHBOARD_URL", DEFAULT_DASHBOARD_URL),
        help="Dashboard base URL (default: https://dashboard.rastro.ai)",
    )
    login.add_argument(
        "--callback-host",
        default=DEFAULT_CALLBACK_HOST,
        help="Local callback host (default: 127.0.0.1)",
    )
    login.add_argument(
        "--callback-port",
        type=int,
        default=DEFAULT_CALLBACK_PORT,
        help="Local callback port (default: random available port)",
    )
    login.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Time to wait for browser callback before failing (default: 180)",
    )
    login.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not auto-open browser; print URL only.",
    )
    login.add_argument(
        "--write-env",
        help="Optional path to .env file where RASTRO_ACCESS_TOKEN will be upserted.",
    )
    login.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON output for automation instead of human-readable output.",
    )

    return parser


def _upsert_env_token(path: str, token: str) -> None:
    env_path = Path(path)
    existing_lines: list[str] = []
    if env_path.exists():
        existing_lines = env_path.read_text(encoding="utf-8").splitlines()

    replaced = False
    new_lines: list[str] = []
    for line in existing_lines:
        if line.startswith("RASTRO_ACCESS_TOKEN="):
            new_lines.append(f"RASTRO_ACCESS_TOKEN={token}")
            replaced = True
        else:
            new_lines.append(line)

    if not replaced:
        new_lines.append(f"RASTRO_ACCESS_TOKEN={token}")

    env_path.write_text("\n".join(new_lines).strip() + "\n", encoding="utf-8")


def _build_auth_url(dashboard_url: str, callback_url: str, state: str) -> str:
    base = dashboard_url.rstrip("/")
    query = urllib.parse.urlencode(
        {
            "callback_url": callback_url,
            "state": state,
        }
    )
    return f"{base}/mcp/auth/cli?{query}"


def _run_login_flow(
    *,
    dashboard_url: str,
    callback_host: str,
    callback_port: int,
    timeout_seconds: int,
    open_browser: bool,
) -> LoginResult:
    state = secrets.token_urlsafe(24)
    callback_event = threading.Event()
    result: dict[str, str] = {}

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (required by BaseHTTPRequestHandler)
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != "/callback":
                self.send_response(404)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Not found")
                return

            params = urllib.parse.parse_qs(parsed.query)
            token = params.get("token", [None])[0]
            received_state = params.get("state", [None])[0]

            if not token:
                self.send_response(400)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Missing token")
                return

            if received_state != state:
                self.send_response(400)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Invalid state")
                return

            result["token"] = token
            callback_event.set()

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                (
                    "<!doctype html><html><head><title>Rastro MCP Auth</title></head>"
                    "<body><h2>Authentication complete.</h2>"
                    "<p>You can close this tab and return to your terminal.</p>"
                    "</body></html>"
                ).encode("utf-8")
            )

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            # Keep login flow quiet.
            return

    server = ThreadingHTTPServer((callback_host, callback_port), CallbackHandler)
    callback_url = f"http://{callback_host}:{server.server_port}/callback"
    auth_url = _build_auth_url(dashboard_url, callback_url, state)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        if open_browser:
            webbrowser.open(auth_url)

        print(f"Authenticate at: {auth_url}")
        print(f"Waiting up to {timeout_seconds}s for callback on {callback_url} ...")

        if not callback_event.wait(timeout=timeout_seconds):
            raise TimeoutError("Timed out waiting for login callback.")

        token = result.get("token")
        if not token:
            raise RuntimeError("Login callback received but token missing.")

        return LoginResult(
            token=token,
            callback_url=callback_url,
            auth_url=auth_url,
            received_at=time.time(),
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _run_login_command(args: argparse.Namespace) -> int:
    try:
        result = _run_login_flow(
            dashboard_url=args.dashboard_url,
            callback_host=args.callback_host,
            callback_port=args.callback_port,
            timeout_seconds=args.timeout_seconds,
            open_browser=not args.no_browser,
        )
    except Exception as exc:
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"Authentication failed: {exc}")
        return 1

    if args.write_env:
        _upsert_env_token(args.write_env, result.token)

    if args.json:
        print(
            json.dumps(
                {
                    "ok": True,
                    "access_token": result.token,
                    "callback_url": result.callback_url,
                    "auth_url": result.auth_url,
                    "received_at": result.received_at,
                    "shell_export": f"export RASTRO_ACCESS_TOKEN={shlex.quote(result.token)}",
                    "wrote_env_path": args.write_env or None,
                }
            )
        )
    else:
        print("Authentication successful.")
        print(f"export RASTRO_ACCESS_TOKEN={shlex.quote(result.token)}")
        if args.write_env:
            print(f"Wrote RASTRO_ACCESS_TOKEN to {args.write_env}")

    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "login":
        raise SystemExit(_run_login_command(args))

    run_server_main()


if __name__ == "__main__":
    main()
