#!/usr/bin/env python3
"""End-to-end smoke for the standalone `arag` binary.

The script starts a local OpenAI-compatible fixture server, points a throwaway
Aragent config at it, and runs the real CLI in print mode. The fixture streams a
`read` tool call, receives the CLI's tool result, and streams the final answer.
No external network and no real credentials are involved: the fixture accepts
exactly one explicit local key.

Usage:
    python3 scripts/standalone-smoke.py --binary target/debug/arag

Exit 0 means the CLI completed the streamed tool roundtrip and printed the
payload that only the tool result contained.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PROVIDER = "fixture"
MODEL = "smoke-model"
FIXTURE_API_KEY = "fixture-static-key"
PAYLOAD = "STANDALONE-SMOKE-TOOL-PAYLOAD-9f3c2a"
TOOL_CALL_ID = "call_smoke_read_1"
READ_PATH = "smoke-payload.txt"
EXPECTED_FINAL = f"TOOL-ROUNDTRIP: {PAYLOAD}"


class FixtureState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.requests: list[dict] = []
        self.failures: list[str] = []

    def record(self, entry: dict) -> int:
        with self.lock:
            self.requests.append(entry)
            return len(self.requests)

    def fail(self, message: str) -> None:
        with self.lock:
            self.failures.append(message)


def correlated_tool_results(messages: list) -> list[dict]:
    """Tool results that belong to the fixture's streamed read call."""
    return [
        message for message in messages
        if isinstance(message, dict)
        and message.get("role") == "tool"
        and message.get("tool_call_id") == TOOL_CALL_ID
    ]


def replay_problems(messages: list) -> list[str]:
    """Validate the assistant call replayed on the follow-up request."""
    calls = [
        call
        for message in messages
        if isinstance(message, dict) and message.get("role") == "assistant"
        for call in (message.get("tool_calls") or [])
        if isinstance(call, dict) and call.get("id") == TOOL_CALL_ID
    ]
    if not calls:
        return [f"second request did not replay tool call {TOOL_CALL_ID}"]
    function = calls[0].get("function") or {}
    problems = []
    if function.get("name") != "read":
        problems.append(f"replayed tool call name is {function.get('name')!r}, expected 'read'")
    raw_arguments = function.get("arguments")
    try:
        arguments = json.loads(raw_arguments) if isinstance(raw_arguments, str) else None
    except ValueError:
        arguments = None
    if not isinstance(arguments, dict) or arguments.get("path") != READ_PATH:
        problems.append(
            f"replayed tool call arguments are {raw_arguments!r}, "
            f"expected a JSON object with path {READ_PATH!r}"
        )
    return problems


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *_args) -> None:  # keep CI output deterministic
        pass

    def _send_json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_sse(self, payloads: list) -> None:
        chunks = []
        for payload in payloads:
            text = payload if isinstance(payload, str) else json.dumps(payload, separators=(",", ":"))
            chunks.append(f"data: {text}\n\n")
        data = "".join(chunks).encode()
        self.close_connection = True
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        state: FixtureState = self.server.state
        if self.path != "/v1/chat/completions":
            state.fail(f"unexpected request path {self.path!r}")
            self._send_json(404, {"error": {"type": "not_found", "message": "unknown path"}})
            return
        if self.headers.get("Authorization") != f"Bearer {FIXTURE_API_KEY}":
            state.fail("fixture endpoint did not receive the configured bearer key")
            self._send_json(401, {"error": {"type": "invalid_key", "message": "bad fixture key"}})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, OSError) as error:
            state.fail(f"unparseable request body: {error}")
            self._send_json(400, {"error": {"type": "invalid_request", "message": "bad body"}})
            return

        messages = body.get("messages") if isinstance(body, dict) else None
        if not isinstance(messages, list):
            state.fail("request has no messages array")
            self._send_json(400, {"error": {"type": "invalid_request", "message": "bad messages"}})
            return
        correlated = correlated_tool_results(messages)
        gate_problems = replay_problems(messages)
        if not any(PAYLOAD in json.dumps(message) for message in correlated):
            gate_problems.append(
                f"no tool result for {TOOL_CALL_ID} carried the fixture payload"
            )
        index = state.record({
            "body": body,
            "auth_ok": True,
            "path": self.path,
        })

        if index == 1:
            self._send_sse([
                {
                    "choices": [{
                        "delta": {
                            "tool_calls": [{
                                "index": 0,
                                "id": TOOL_CALL_ID,
                                "type": "function",
                                "function": {
                                    "name": "read",
                                    "arguments": json.dumps({"path": "smoke-payload.txt"}),
                                },
                            }]
                        },
                        "finish_reason": None,
                    }]
                },
                {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]},
                "[DONE]",
            ])
            return

        if not gate_problems:
            self._send_sse([
                {"choices": [{"delta": {"content": "TOOL-ROUNDTRIP: "}, "finish_reason": None}]},
                {"choices": [{"delta": {"content": PAYLOAD}, "finish_reason": None}]},
                {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                {
                    "choices": [],
                    "usage": {"prompt_tokens": 24, "completion_tokens": 8, "total_tokens": 32},
                },
                "[DONE]",
            ])
            return

        state.fail("follow-up request failed fixture gate: " + "; ".join(gate_problems))
        self._send_sse([
            {"error": {"type": "fixture_error", "message": "; ".join(gate_problems)}},
            "[DONE]",
        ])


class FixtureServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address) -> None:
        super().__init__(address, FixtureHandler)
        self.state = FixtureState()


def write_fixture(root: Path, port: int) -> tuple[Path, Path, Path]:
    work = root / "work"
    home = root / "home"
    agent_dir = home / ".omp" / "agent"
    work.mkdir(parents=True)
    agent_dir.mkdir(parents=True)
    (work / "smoke-payload.txt").write_text(PAYLOAD + "\n", encoding="utf-8")
    (agent_dir / "config.yml").write_text(
        "\n".join([
            f"defaultModel: {PROVIDER}/{MODEL}",
            "providers:",
            f"  {PROVIDER}:",
            f"    baseUrl: http://127.0.0.1:{port}/v1",
            f"    apiKey: {FIXTURE_API_KEY}",
            "    auth: apiKey",
            "    api: openai-completions",
            "",
        ]),
        encoding="utf-8",
    )
    (agent_dir / "models.yml").write_text(
        "\n".join([
            "providers:",
            f"  {PROVIDER}:",
            f"    baseUrl: http://127.0.0.1:{port}/v1",
            "    api: openai-completions",
            f"    apiKey: {FIXTURE_API_KEY}",
            "    models:",
            f"      - id: {MODEL}",
            "        name: Smoke Model",
            "        contextWindow: 8192",
            "        maxTokens: 512",
            "        input: [text]",
            "",
        ]),
        encoding="utf-8",
    )
    return work, home, agent_dir


def summarize(requests: list[dict]) -> str:
    lines = []
    for number, request in enumerate(requests, 1):
        body = request.get("body") or {}
        messages = body.get("messages") or []
        roles = [message.get("role") for message in messages if isinstance(message, dict)]
        tools = [
            tool.get("function", {}).get("name")
            for tool in (body.get("tools") or [])
            if isinstance(tool, dict)
        ]
        lines.append(
            f"  request {number}: path={request.get('path')} auth_ok={request.get('auth_ok')} "
            f"model={body.get('model')!r} stream={body.get('stream')!r} "
            f"roles={roles} tools={tools}"
        )
    return "\n".join(lines) if lines else "  (no requests reached the fixture)"


def check_requests(state: FixtureState) -> list[str]:
    problems: list[str] = list(state.failures)
    requests = state.requests
    if len(requests) != 2:
        problems.append(f"expected exactly 2 model requests, saw {len(requests)}")
        return problems

    first = requests[0]["body"]
    if first.get("model") != MODEL:
        problems.append(f"first request model is {first.get('model')!r}, expected {MODEL!r}")
    if first.get("stream") is not True:
        problems.append("first request was not streamed")
    tool_names = [
        tool.get("function", {}).get("name")
        for tool in (first.get("tools") or [])
        if isinstance(tool, dict)
    ]
    if "read" not in tool_names:
        problems.append(f"first request did not offer the read tool (offered {tool_names})")

    second = requests[1]["body"]
    messages = second.get("messages") or []
    tool_messages = [
        message for message in messages
        if isinstance(message, dict) and message.get("role") == "tool"
    ]
    if not tool_messages:
        problems.append("second request carried no tool result message")
    else:
        correlated = correlated_tool_results(messages)
        if not correlated:
            problems.append(
                f"second request carried no tool result for tool_call_id {TOOL_CALL_ID}"
            )
        elif not any(PAYLOAD in json.dumps(message) for message in correlated):
            problems.append(
                f"second request tool result for {TOOL_CALL_ID} did not contain the fixture payload"
            )
    problems.extend(replay_problems(messages))
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        default="target/debug/arag",
        help="path to the built arag binary (default: target/debug/arag)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="seconds allowed for the CLI run (default: 120)",
    )
    args = parser.parse_args()

    binary = Path(args.binary).resolve()
    if not binary.is_file():
        print(f"standalone-smoke: FAIL binary not found: {binary}", file=sys.stderr)
        return 1
    if not os.access(binary, os.X_OK):
        print(
            f"standalone-smoke: FAIL binary is not executable: {binary} "
            "(artifact downloads may need chmod +x)",
            file=sys.stderr,
        )
        return 1

    server = FixtureServer(("127.0.0.1", 0))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    with tempfile.TemporaryDirectory(prefix="arag-standalone-smoke-") as raw_root:
        root = Path(raw_root)
        work, home, agent_dir = write_fixture(root, port)
        env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(home),
            "PI_CONFIG_DIR": str(home / ".omp"),
            "PI_CODING_AGENT_DIR": str(agent_dir),
            "TMPDIR": str(root),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "TERM": "dumb",
            "NO_COLOR": "1",
        }
        command = [
            str(binary),
            "-p",
            "--mode",
            "text",
            "--cwd",
            str(work),
            "--model",
            f"{PROVIDER}/{MODEL}",
            "--tools",
            "read",
            "--no-session",
            "--no-title",
            "--no-skills",
            "--no-rules",
            "--no-lsp",
            "--no-pty",
            "--no-prewalk",
            "--max-time",
            "60s",
            "Read smoke-payload.txt with the read tool, then report the payload it contains.",
        ]
        os_error = None
        try:
            completed = subprocess.run(
                command,
                cwd=str(work),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=args.timeout,
            )
            timed_out = False
        except subprocess.TimeoutExpired as error:
            completed = error
            timed_out = True
        except OSError as error:
            completed = None
            timed_out = False
            os_error = error
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        problems = []
        if os_error is not None:
            problems.append(f"could not start CLI: {os_error}")
        elif timed_out:
            problems.append("CLI timed out")
        stdout = "" if completed is None else completed.stdout
        stderr = "" if completed is None else completed.stderr
        if completed is not None and not timed_out and completed.returncode != 0:
            problems.append(f"CLI exited {completed.returncode}")
        problems.extend(check_requests(server.state))
        if EXPECTED_FINAL not in stdout:
            problems.append("final stdout did not contain the fixture-only answer")

        if problems:
            print("standalone-smoke: FAIL", file=sys.stderr)
            for problem in problems:
                print(f"  - {problem}", file=sys.stderr)
            print(f"command: {' '.join(command)}", file=sys.stderr)
            print("fixture requests:", file=sys.stderr)
            print(summarize(server.state.requests), file=sys.stderr)
            if timed_out:
                print(f"timed out after {args.timeout}s", file=sys.stderr)
            elif completed is not None:
                print(f"exit: {completed.returncode}", file=sys.stderr)
                print("--- stdout ---", file=sys.stderr)
                print(stdout, file=sys.stderr)
                print("--- stderr ---", file=sys.stderr)
                print(stderr, file=sys.stderr)
            return 1

    print("standalone-smoke: PASS")
    print(f"  binary: {binary}")
    print("  2 model requests: streamed read tool call -> tool result -> final text")
    print(f"  final: {EXPECTED_FINAL}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
