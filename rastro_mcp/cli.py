"""
CLI for Rastro MCP server.

Subcommands:
    login   Browser-based authentication (saves token to ~/.rastro/credentials)
    (none)  Start the MCP stdio server
"""

import http.server
import json
import secrets
import sys
import threading
import urllib.parse
import webbrowser
from pathlib import Path
from typing import Optional

CREDENTIALS_PATH = Path.home() / ".rastro" / "credentials"
DEFAULT_DASHBOARD_URL = "https://dashboard.rastro.ai"


def _save_token(token: str) -> Path:
    """Persist token to ~/.rastro/credentials."""
    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_PATH.write_text(json.dumps({"token": token}))
    CREDENTIALS_PATH.chmod(0o600)
    return CREDENTIALS_PATH


def load_token_from_file() -> Optional[str]:
    """Load saved token from ~/.rastro/credentials if it exists."""
    if not CREDENTIALS_PATH.is_file():
        return None
    try:
        data = json.loads(CREDENTIALS_PATH.read_text())
        return data.get("token")
    except (json.JSONDecodeError, OSError):
        return None


def _build_auth_url(dashboard_url: str, callback_url: str, state: str) -> str:
    query = urllib.parse.urlencode({"callback_url": callback_url, "state": state})
    return f"{dashboard_url.rstrip('/')}/mcp/auth/cli?{query}"


def login(
    dashboard_url: str = DEFAULT_DASHBOARD_URL,
    callback_host: str = "127.0.0.1",
    callback_port: int = 0,
    timeout_seconds: int = 180,
    no_browser: bool = False,
    output_json: bool = False,
) -> None:
    """Run browser-based login flow and save the token."""

    state = secrets.token_urlsafe(24)
    result: dict = {}
    error: Optional[str] = None

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal result, error
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)

            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                return

            received_state = params.get("state", [None])[0]
            if received_state != state:
                error = "State mismatch - possible CSRF attack"
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"State mismatch. Please try again.")
                return

            token = params.get("token", [None])[0]
            if not token:
                error = "No token received"
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"No token received. Please try again.")
                return

            result["token"] = token
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h2>Logged in to Rastro</h2>"
                b"<p>You can close this tab.</p></body></html>"
            )

        def log_message(self, format, *args):
            pass

    server = http.server.HTTPServer((callback_host, callback_port), CallbackHandler)
    port = server.server_address[1]
    callback_url = f"http://{callback_host}:{port}/callback"
    auth_url = _build_auth_url(dashboard_url, callback_url, state)

    if no_browser:
        print(f"Open this URL in your browser:\n{auth_url}", file=sys.stderr)
    else:
        print(f"Opening browser to log in at {dashboard_url} ...", file=sys.stderr)
        webbrowser.open(auth_url)

    server.timeout = timeout_seconds
    stop_event = threading.Event()

    def serve():
        while not stop_event.is_set() and not result and not error:
            server.handle_request()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)
    stop_event.set()
    server.server_close()

    if error:
        print(f"Login failed: {error}", file=sys.stderr)
        sys.exit(1)

    if not result.get("token"):
        print("Login timed out. Please try again.", file=sys.stderr)
        sys.exit(1)

    token = result["token"]
    cred_path = _save_token(token)

    if output_json:
        print(json.dumps({"token": token, "credentials_path": str(cred_path)}))
    else:
        print(f"Login successful! Token saved to {cred_path}", file=sys.stderr)
        print(f"\nexport RASTRO_AUTH_TOKEN={token}", file=sys.stderr)


def main():
    """CLI entry point: route to login or start the MCP server."""
    if len(sys.argv) > 1 and sys.argv[1] == "login":
        kwargs = {}
        args = sys.argv[2:]
        i = 0
        while i < len(args):
            if args[i] == "--dashboard-url" and i + 1 < len(args):
                kwargs["dashboard_url"] = args[i + 1]
                i += 2
            elif args[i] == "--callback-host" and i + 1 < len(args):
                kwargs["callback_host"] = args[i + 1]
                i += 2
            elif args[i] == "--callback-port" and i + 1 < len(args):
                kwargs["callback_port"] = int(args[i + 1])
                i += 2
            elif args[i] == "--timeout" and i + 1 < len(args):
                kwargs["timeout_seconds"] = int(args[i + 1])
                i += 2
            elif args[i] == "--no-browser":
                kwargs["no_browser"] = True
                i += 1
            elif args[i] == "--json":
                kwargs["output_json"] = True
                i += 1
            else:
                print(f"Unknown option: {args[i]}", file=sys.stderr)
                sys.exit(1)
        login(**kwargs)
    else:
        from rastro_mcp.server import main as server_main
        server_main()
