#!/usr/bin/env python3
"""Deterministic ACP end-to-end smoke harness (stdlib only).

Not a Cargo test: run against a fresh binary after the CLI hook lands:

    python3 crates/omp-cli/tests/acp_accept.py ./target/debug/omp

It builds an isolated `PI_CONFIG_DIR`/`HOME`/XDG tree, starts a local
OpenAI-compatible SSE provider on 127.0.0.1, and drives `omp acp` over stdio.
The provider is fully deterministic: prompts containing `PERMISSION` ask for a
`bash` tool call, `CANCEL` streams one chunk then stalls briefly, everything
else returns fixed text. Assertions target wire order and protocol shape, never
model prose.

Covered: initialize + advertised capabilities, new/list/load/resume/fork/close,
set_mode plan/default, set_config_option thinking, prompt block fallbacks,
permission reverse request + `allow_always` persistence, cancel notification,
detached plan continuation cancel (cooperative release only, not the 5s forced
close), settings-derived prewalk switch after the first mutation (target route
persists for the session scope), malformed-line recovery, unknown method, EOF
cleanup.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REQUEST_TIMEOUT = 20.0
PROMPT_TIMEOUT = 30.0


class Provider:
    """Local OpenAI-compatible streaming endpoint."""

    def __init__(self, model: str = "mock-model") -> None:
        self.model = model
        self.requests = 0
        # Detached plan continuations: when armed, the reminder turn (sent as a
        # `developer` message, not `user`) streams one chunk, stalls, then emits
        # a late chunk that a cancelled session must never publish. The stall is
        # 2s, well under the 5s `CANCEL_CLEANUP_TIMEOUT`: this exercises the
        # cooperative release path only, never the forced-close watchdog.
        self.hang_continuation = False
        self.hang_seconds = 2.0
        # Text returned when the request already contains a tool result.
        self.tool_result_text = "tool done"
        self.plan_yolo_root = False
        self.plan_yolo_target = False
        self.mounted_plan_target = False
        self.bodies: list[dict] = []
        # Plan-yolo abort fixture: after the proposal tool result, hold the
        # next request until the client cancels and releases this event.
        self.hang_after_proposal = False
        self.proposal_waiting = threading.Event()
        self.release_proposal = threading.Event()
        self.continuation_started = threading.Event()
        self.lock = threading.Lock()
        provider = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):  # silence
                pass

            def do_POST(self):  # noqa: N802
                if not self.path.endswith("/chat/completions"):
                    self.send_error(404)
                    return
                length = int(self.headers.get("content-length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                with provider.lock:
                    provider.requests += 1
                    provider.bodies.append(body)
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.send_header("cache-control", "no-cache")
                self.send_header("connection", "close")
                self.end_headers()
                for frame in provider.frames(body):
                    self.wfile.write(frame.encode("utf-8") + b"\n\n")
                    self.wfile.flush()
                self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/v1"

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def frames(self, body):
        messages = body.get("messages") or []
        last_user = ""
        tool_after_user = False
        for message in messages:
            role = message.get("role")
            if role == "tool":
                tool_after_user = True
            elif role == "user":
                last_user = text_of(message.get("content"))
                tool_after_user = False
        created = int(time.time())
        base = {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": created, "model": self.model}

        def chunk(delta, finish=None):
            payload = dict(base)
            payload["choices"] = [{"index": 0, "delta": delta, "finish_reason": finish}]
            return "data: " + json.dumps(payload)

        if "ORDINARY_PROPOSE" in last_user and not tool_after_user:
            yield chunk(
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_ordinary_propose",
                            "type": "function",
                            "function": {
                                "name": "write",
                                "arguments": json.dumps(
                                    {
                                        "path": "xd://propose",
                                        "content": json.dumps(
                                            {
                                                "title": "Manual plan",
                                                "slug": "manual-plan",
                                                "content": "# Manual\n",
                                            }
                                        ),
                                    }
                                ),
                            },
                        }
                    ],
                }
            )
            yield chunk({}, "tool_calls")
            yield "data: [DONE]"
            return

        if "MANUAL_PLAN" in last_user and not tool_after_user:
            yield chunk(
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_manual_blocked",
                            "type": "function",
                            "function": {
                                "name": "write",
                                "arguments": json.dumps(
                                    {"path": "manual-blocked.txt", "content": "must not land"}
                                ),
                            },
                        }
                    ],
                }
            )
            yield chunk({}, "tool_calls")
            yield "data: [DONE]"
            return

        if self.plan_yolo_root and "PLAN_YOLO" in last_user:
            tool_results = 0
            for message in messages:
                if message.get("role") == "user":
                    tool_results = 0
                elif message.get("role") == "tool":
                    tool_results += 1

            def tool_call(call_id, name, args):
                yield chunk(
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": call_id,
                                "type": "function",
                                "function": {"name": name, "arguments": json.dumps(args)},
                            }
                        ],
                    }
                )
                yield chunk({}, "tool_calls")
                yield "data: [DONE]"

            if tool_results == 0:
                yield from tool_call(
                    "call_plan_blocked",
                    "write",
                    {"path": "blocked.txt", "content": "must not land"},
                )
                return
            if tool_results == 1:
                yield from tool_call(
                    "call_plan_write",
                    "write",
                    {
                        "path": "local://plan-plan.md",
                        "content": "# Plan\n\nImplement probe.\n",
                    },
                )
                return
            if tool_results == 2 and self.hang_after_proposal:
                # The one-shot cutover commits at the proposal turn's end, so
                # there is no fourth root request to hold. Pause this planning
                # turn before the proposal is dispatched; cancellation must
                # abandon the arm without switching models.
                self.proposal_waiting.set()
                self.release_proposal.wait(PROMPT_TIMEOUT)
                yield chunk({"role": "assistant", "content": "planning aborted"})
                yield chunk({}, "stop")
                yield "data: [DONE]"
                return
            if tool_results == 2:
                yield from tool_call(
                    "call_plan_propose",
                    "write",
                    {"path": "xd://propose", "content": "Plan"},
                )
                return

        if self.mounted_plan_target:
            def emit_tool(call_id, name, arguments):
                yield chunk(
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": json.dumps(arguments),
                                },
                            }
                        ],
                    }
                )
                yield chunk({}, "tool_calls")
                yield "data: [DONE]"

            if self.requests == 1:
                # The plan-phase `write` must be hidden on the target schema
                # and at execution; this malicious call proves both.
                yield from emit_tool(
                    "call_hidden_write",
                    "write",
                    {"path": "mounted-blocked.txt", "content": "must not land"},
                )
                return
            if self.requests == 2:
                yield from emit_tool(
                    "call_probe_echo", "mcp__probe_echo", {"text": "hello"}
                )
                return
            yield chunk({"role": "assistant", "content": "mounted done"})
            yield chunk({}, "stop")
            yield "data: [DONE]"
            return

        if self.plan_yolo_target and self.requests == 1:
            yield chunk(
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_plan_probe",
                            "type": "function",
                            "function": {
                                "name": "write",
                                "arguments": json.dumps(
                                    {"path": "probe.txt", "content": "done"}
                                ),
                            },
                        }
                    ],
                }
            )
            yield chunk({}, "tool_calls")
            yield "data: [DONE]"
            return

        if tool_after_user:
            yield chunk({"role": "assistant", "content": self.tool_result_text})
            yield chunk({}, "stop")
            yield "data: [DONE]"
            return

        reminder_marker = "Plan mode turn ended without a required tool call."
        if self.hang_continuation and any(
            message.get("role") == "developer"
            and reminder_marker in text_of(message.get("content"))
            for message in messages
        ):
            self.continuation_started.set()
            yield chunk({"role": "assistant", "content": "continuation delta"})
            time.sleep(self.hang_seconds)
            # Only reaches a live reader; a cancelled session closed the socket.
            yield chunk({"role": "assistant", "content": "orphan delta"})
            yield chunk({}, "stop")
            yield "data: [DONE]"
            return

        if "PREWALK" in last_user:
            yield chunk(
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_prewalk_write",
                            "type": "function",
                            "function": {
                                "name": "write",
                                "arguments": json.dumps(
                                    {"path": "probe.txt", "content": "switched"}
                                ),
                            },
                        }
                    ],
                }
            )
            yield chunk({}, "tool_calls")
            yield "data: [DONE]"
            return

        if "PERMISSION" in last_user:
            yield chunk(
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_permission",
                            "type": "function",
                            "function": {"name": "bash", "arguments": "{\"command\":\"echo permission\"}"},
                        }
                    ],
                }
            )
            yield chunk({}, "tool_calls")
            yield "data: [DONE]"
            return

        if "CANCEL" in last_user:
            yield chunk({"role": "assistant", "content": "cancel pending"})
            time.sleep(1.0)
            yield chunk({}, "stop")
            yield "data: [DONE]"
            return

        yield chunk({"role": "assistant", "content": "hello from mock"})
        yield chunk({}, "stop")
        yield "data: [DONE]"


def text_of(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(part.get("text", "") for part in content if isinstance(part, dict))
    return ""


class Client:
    def __init__(
        self,
        binary: str,
        root: Path,
        provider: Provider,
        prewalk_target: Provider | None = None,
        plan_yolo_target: Provider | None = None,
        plan_yolo_tools: str = "write, read",
    ) -> None:
        self.config = root / "config"
        self.data = root / "data"
        self.state = root / "state"
        self.tmp = root / "tmp"
        self.agent_dir = self.config / "agent"
        for path in (self.config, self.agent_dir, self.data, self.state, self.tmp):
            path.mkdir(parents=True, exist_ok=True)
        config_text = (
            "defaultModel: mock/mock-model\n"
            "providers:\n"
            "  mock:\n"
            f"    baseUrl: \"{provider.base_url}\"\n"
            "    api: openai-completions\n"
            "    apiKey: test-key\n"
        )
        models_text = (
            "providers:\n"
            "  mock:\n"
            f"    baseUrl: \"{provider.base_url}\"\n"
            "    api: openai-completions\n"
            "    apiKey: test-key\n"
            "    models:\n"
            "      - id: mock-model\n"
            "        contextWindow: 128000\n"
        )
        target = prewalk_target or plan_yolo_target
        if target is not None:
            # A provider-level baseUrl would override the per-model endpoints;
            # leave routing to models.yml so root and target stay separate.
            config_text = config_text.replace(
                f"    baseUrl: \"{provider.base_url}\"\n", ""
            )
            if prewalk_target is not None:
                # Fresh ACP sessions carry the settings-derived prewalk arm; the
                # restricted tool selection opens the gate before a todo call.
                config_text += (
                    "prewalk:\n"
                    "  enabled: true\n"
                    "modelRoles:\n"
                    "  smol: mock/model-b\n"
                )
            if plan_yolo_target is not None:
                # The explicit plan-yolo flag is on the launch argv; yolo only
                # avoids a permission round-trip for the implementation write.
                config_text += "approvalMode: yolo\n"
            config_text += f"tools: [{plan_yolo_tools}]\n"
            models_text += (
                "      - id: model-b\n"
                "        contextWindow: 128000\n"
                f"        baseUrl: \"{target.base_url}\"\n"
            )
        (self.agent_dir / "config.yml").write_text(config_text, encoding="utf-8")
        (self.agent_dir / "models.json").write_text(
            '{"mock":{"mock-model":{"id":"mock-model","provider":"mock","contextWindow":128000}}}\n',
            encoding="utf-8",
        )
        (self.agent_dir / "models.yml").write_text(models_text, encoding="utf-8")
        env = dict(os.environ)
        env.update(
            PI_CONFIG_DIR=str(self.config),
            PI_CODING_AGENT_DIR=str(self.agent_dir),
            HOME=str(root),
            XDG_DATA_HOME=str(self.data),
            XDG_STATE_HOME=str(self.state),
            TMPDIR=str(self.tmp),
            NO_COLOR="1",
            TERM="dumb",
        )
        self.cwd = root / "workspace"
        self.cwd.mkdir()
        argv = [binary]
        if plan_yolo_target is not None:
            argv += ["--plan-yolo", "--plan-yolo-into", "mock/model-b"]
        argv += ["acp"]
        self.proc = subprocess.Popen(
            argv,
            cwd=str(self.cwd),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.frames: "queue.Queue[dict]" = queue.Queue()
        self.raw: list[dict] = []
        self.stderr: list[str] = []
        self.next_id = 1
        self.pending: dict[int, dict] = {}
        self.lock = threading.Lock()
        threading.Thread(target=self._read_stdout, daemon=True).start()
        threading.Thread(target=self._read_stderr, daemon=True).start()

    def _read_stderr(self) -> None:
        assert self.proc.stderr is not None
        for line in self.proc.stderr:
            self.stderr.append(line.rstrip())

    def _read_stdout(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                frame = json.loads(line)
            except json.JSONDecodeError:
                self.frames.put({"__bad_line__": line})
                continue
            with self.lock:
                self.raw.append(frame)
            self.frames.put(frame)

    # ---- wire helpers -------------------------------------------------

    def send(self, frame: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(frame) + "\n")
        self.proc.stdin.flush()

    def send_raw(self, line: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(line + "\n")
        self.proc.stdin.flush()

    def wait_frame(self, predicate, timeout=REQUEST_TIMEOUT):
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(
                    f"timeout waiting for frame; raw={self.raw[-8:]!r} stderr={self.stderr[-8:]!r}"
                )
            try:
                frame = self.frames.get(timeout=min(0.25, remaining))
            except queue.Empty:
                if self.proc.poll() is not None:
                    raise AssertionError(
                        f"ACP process exited {self.proc.returncode}; stderr={self.stderr[-20:]!r}"
                    )
                continue
            if predicate(frame):
                return frame

    def request(self, method, params=None, timeout=REQUEST_TIMEOUT):
        request_id = self.next_id
        self.next_id += 1
        frame = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            frame["params"] = params
        self.send(frame)
        response = self.wait_frame(
            lambda f: f.get("id") == request_id and ("result" in f or "error" in f), timeout
        )
        if "error" in response:
            raise AssertionError(f"{method} failed: {response['error']}")
        return response.get("result", {})

    def request_error(self, method, params=None):
        request_id = self.next_id
        self.next_id += 1
        frame = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            frame["params"] = params
        self.send(frame)
        response = self.wait_frame(
            lambda f: f.get("id") == request_id and ("result" in f or "error" in f)
        )
        return response.get("error")

    def notify(self, method, params=None):
        frame = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            frame["params"] = params
        self.send(frame)

    def prompt_answering_permission(self, session_id, text, option_id="allow_once", timeout=PROMPT_TIMEOUT):
        """Send a prompt and answer any `session/request_permission` frames
        that arrive before its response. Returns (response, requests)."""
        request_id = self.next_id
        self.next_id += 1
        frame = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session/prompt",
            "params": {"sessionId": session_id, "prompt": [{"type": "text", "text": text}]},
        }
        self.send(frame)
        requests = []
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"prompt {text!r} timed out; raw={self.raw[-8:]!r}")
            current = self.wait_frame(lambda _f: True, remaining)
            if current.get("method") == "session/request_permission":
                requests.append(current)
                self.send(
                    {
                        "jsonrpc": "2.0",
                        "id": current["id"],
                        "result": {"outcome": {"outcome": "selected", "optionId": option_id}},
                    }
                )
                continue
            if current.get("method") == "elicitation/create":
                requests.append(current)
                self.send(
                    {
                        "jsonrpc": "2.0",
                        "id": current["id"],
                        "result": {
                            "action": "accept",
                            "content": {"value": "Approve and execute"},
                        },
                    }
                )
                continue
            if current.get("id") == request_id and ("result" in current or "error" in current):
                if "error" in current:
                    raise AssertionError(f"prompt failed: {current['error']}")
                return current.get("result", {}), requests

    def updates_since(self, mark, session_id):
        with self.lock:
            frames = list(self.raw[mark:])
        return [
            frame["params"]["update"]
            for frame in frames
            if frame.get("method") == "session/update"
            and frame.get("params", {}).get("sessionId") == session_id
        ]

    def mark(self):
        with self.lock:
            return len(self.raw)

    def wait_exit(self, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return self.proc.returncode
            time.sleep(0.05)
        return None

    def close_stdin(self):
        assert self.proc.stdin is not None
        try:
            self.proc.stdin.close()
        except OSError:
            pass

    def kill(self):
        if self.proc.poll() is None:
            try:
                self.proc.kill()
            except OSError:
                pass
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass


def assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def run(binary: str) -> None:
    root = Path(tempfile.mkdtemp(prefix="acp-accept."))
    provider = Provider()
    provider.start()
    client = Client(binary, root, provider)
    try:
        # 1. initialize + exact capability matrix
        caps = client.request("initialize", {"protocolVersion": 1, "clientCapabilities": {}})
        assert_eq(caps["protocolVersion"], 1, "protocolVersion")
        assert_eq(caps["agentCapabilities"]["loadSession"], True, "loadSession")
        assert_eq(caps["agentCapabilities"]["mcpCapabilities"], {"http": True, "sse": True}, "mcp caps")
        assert_eq(
            caps["agentCapabilities"]["promptCapabilities"],
            {"embeddedContext": True, "image": True},
            "prompt caps",
        )
        assert_eq(
            set(caps["agentCapabilities"]["sessionCapabilities"].keys()),
            {"list", "fork", "resume", "close"},
            "session caps",
        )
        method_ids = [method["id"] for method in caps["authMethods"]]
        assert_eq(method_ids, ["agent"], "auth methods")
        assert client.request_error("authenticate", {"methodId": "nope"})["message"].startswith(
            "Unknown ACP auth method"
        ), "unknown auth method"

        initialized_terminal = client.request(
            "initialize",
            {"protocolVersion": 1, "clientCapabilities": {"auth": {"terminal": True}}},
        )
        terminal_ids = [method["id"] for method in initialized_terminal["authMethods"]]
        assert_eq(terminal_ids, ["agent", "terminal"], "terminal auth advertised")

        cwd = str(client.cwd)
        relative = client.request_error("session/new", {"cwd": "relative"})
        assert relative is not None, "relative cwd must fail"

        # 2. session/new + bootstrap
        mark = client.mark()
        created = client.request("session/new", {"cwd": cwd, "mcpServers": []})
        session_id = created["sessionId"]
        assert session_id, "sessionId"
        assert created["configOptions"], "configOptions"
        assert [mode["id"] for mode in created["modes"]["availableModes"]] == ["default", "plan"], "modes"
        bootstrap = client.wait_frame(
            lambda f: f.get("method") == "session/update"
            and f.get("params", {}).get("sessionId") == session_id
            and f["params"]["update"].get("sessionUpdate") == "session_info_update"
        )
        assert bootstrap, "bootstrap session info"
        updates = client.updates_since(mark, session_id)
        assert any(u.get("sessionUpdate") == "available_commands_update" for u in updates), "commands bootstrap"

        # 3. plain prompt: ordered chunks before the response
        mark = client.mark()
        response = client.request("session/prompt", {"sessionId": session_id, "prompt": [{"type": "text", "text": "hello"}]}, PROMPT_TIMEOUT)
        assert_eq(response["stopReason"], "end_turn", "plain stop reason")
        updates = client.updates_since(mark, session_id)
        chunks = [u for u in updates if u.get("sessionUpdate") == "agent_message_chunk"]
        assert chunk_text(chunks) == "hello from mock", f"chunks {chunks!r}"
        assert any(u.get("sessionUpdate") == "session_info_update" for u in updates), "turn session info"

        # 4. permission: ask once, persist allow_always, no second request
        first, permission_requests = client.prompt_answering_permission(
            session_id, "PERMISSION one", "allow_always"
        )
        assert_eq(first["stopReason"], "end_turn", "permission stop")
        assert_eq(len(permission_requests), 1, "permission request count")
        request = permission_requests[0]
        options = [option["optionId"] for option in request["params"]["options"]]
        assert_eq(options, ["allow_once", "allow_always", "reject_once", "reject_always"], "permission options")
        assert_eq(request["params"]["sessionId"], session_id, "permission session")
        second, repeated = client.prompt_answering_permission(session_id, "PERMISSION two")
        assert_eq(second["stopReason"], "end_turn", "second permission stop")
        assert_eq(len(repeated), 0, "allow_always persistence")

        # 5. cancel notification: cancelled response, session still usable
        mark = client.mark()
        request_id = client.next_id
        client.next_id += 1
        client.send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "session/prompt",
                "params": {"sessionId": session_id, "prompt": [{"type": "text", "text": "CANCEL please"}]},
            }
        )
        client.wait_frame(
            lambda f: f.get("method") == "session/update"
            and f.get("params", {}).get("sessionId") == session_id
            and f["params"]["update"].get("sessionUpdate") == "agent_message_chunk",
            PROMPT_TIMEOUT,
        )
        client.notify("session/cancel", {"sessionId": session_id})
        cancelled = client.wait_frame(
            lambda f: f.get("id") == request_id and ("result" in f or "error" in f), 10.0
        )
        assert_eq(cancelled.get("result", {}).get("stopReason"), "cancelled", "cancel stop reason")
        time.sleep(0.3)
        after = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "after cancel"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(after["stopReason"], "end_turn", "prompt after cancel")

        # 6. list paging
        listing = client.request("session/list", {"cwd": cwd})
        ids = [session["sessionId"] for session in listing["sessions"]]
        assert session_id in ids, f"session list {ids!r}"
        assert "nextCursor" not in listing or int(listing["nextCursor"]) > 0, "cursor"

        # 7. mode + config option
        client.request("session/set_mode", {"sessionId": session_id, "modeId": "plan"})
        plan_updates = [
            u
            for u in client.updates_since(mark, session_id)
            if u.get("sessionUpdate") == "current_mode_update"
        ]
        assert plan_updates, "current_mode_update"
        client.request("session/set_mode", {"sessionId": session_id, "modeId": "default"})
        boolean_error = client.request_error(
            "session/set_config_option",
            {"sessionId": session_id, "configId": "mode", "value": True},
        )
        assert "Unsupported boolean ACP config option" in boolean_error["message"], "boolean config"
        configured = client.request(
            "session/set_config_option",
            {"sessionId": session_id, "configId": "thinking", "value": "off"},
        )
        assert configured["configOptions"], "config options response"

        # 8. resume after close
        client.request("session/close", {"sessionId": session_id})
        assert_eq(client.request("session/close", {"sessionId": session_id}), {}, "close idempotent")
        resumed = client.request("session/resume", {"sessionId": session_id, "cwd": cwd})
        assert resumed["configOptions"], "resume config"
        resumed_prompt = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "resumed"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(resumed_prompt["stopReason"], "end_turn", "resume prompt")

        # 9. cancel a detached plan continuation: the prompt response leaves
        # first, the reminder turn stalls, and `session/cancel` must release
        # ownership cooperatively (the 2s stall unwinds well inside the 5s
        # forced-close watchdog, which this case deliberately does not reach)
        # while the orphaned continuation is never published. Only the
        # cooperative path is asserted here; the forced-close boundary is the
        # gate unit tests' claim, not this fixture.
        plan_session = client.request("session/new", {"cwd": cwd, "mcpServers": []})["sessionId"]
        client.request("session/set_mode", {"sessionId": plan_session, "modeId": "plan"})
        provider.hang_continuation = True
        mark = client.mark()
        plan_prompt = client.request(
            "session/prompt",
            {"sessionId": plan_session, "prompt": [{"type": "text", "text": "draft the plan"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(plan_prompt["stopReason"], "end_turn", "plan prompt response")
        assert provider.continuation_started.wait(10.0), (
            f"detached continuation never reached the provider; stderr={client.stderr[-8:]!r}"
        )
        # Disarm first: a retry of the interrupted request must not stall again.
        provider.hang_continuation = False
        client.notify("session/cancel", {"sessionId": plan_session})
        deadline = time.monotonic() + PROMPT_TIMEOUT
        resolved = None
        while time.monotonic() < deadline:
            error = client.request_error(
                "session/prompt",
                {"sessionId": plan_session, "prompt": [{"type": "text", "text": "after continuation cancel"}]},
            )
            if error is None:
                resolved = "accepted"
                break
            if error.get("code") == -32003:  # session_busy is bounded, never permanent
                time.sleep(0.2)
                continue
            resolved = f"closed: {error.get('message')}"
            break
        assert resolved == "accepted", (
            "cooperative cancel must release the continuation, not force-close the session: "
            f"{resolved}"
        )
        # A cancelled continuation may not publish its late chunk.
        time.sleep(provider.hang_seconds + 1.0)
        late = [update for update in client.updates_since(mark, plan_session) if "orphan delta" in chunk_text([update])]
        assert not late, f"orphaned continuation published after cancel: {late!r}"

        # 10. fork
        forked = client.request("session/fork", {"sessionId": session_id, "cwd": cwd})
        fork_id = forked["sessionId"]
        assert fork_id and fork_id != session_id, "fork identity"
        listing = client.request("session/list", {"cwd": cwd})
        assert {session_id, fork_id} <= {session["sessionId"] for session in listing["sessions"]}, "fork listed"

        # 11. load replays history before the response
        client.request("session/close", {"sessionId": session_id})
        client.request("session/close", {"sessionId": fork_id})
        mark = client.mark()
        loaded = client.request("session/load", {"sessionId": session_id, "cwd": cwd})
        assert loaded["configOptions"], "load config"
        replayed = client.updates_since(mark, session_id)
        assert any(u.get("sessionUpdate") == "user_message_chunk" for u in replayed), "load replay user"
        assert any(u.get("sessionUpdate") == "agent_message_chunk" for u in replayed), "load replay assistant"

        # 12. malformed line + unknown method keep the connection alive
        client.send_raw("{not json")
        parse_error = client.wait_frame(lambda f: f.get("error", {}).get("code") == -32700)
        assert parse_error, "parse error"
        unknown = client.request_error("totally/unknown", {})
        assert_eq(unknown["code"], -32601, "unknown method")
        again = client.request("initialize", {"protocolVersion": 1, "clientCapabilities": {}})
        assert_eq(again["protocolVersion"], 1, "connection alive")

        # 13. EOF cleanup
        client.close_stdin()
        code = client.wait_exit(10.0)
        assert code is not None, f"process did not exit on EOF; stderr={client.stderr[-20:]!r}"
    finally:
        client.kill()
        provider.stop()
        print(f"acp_accept artifacts retained at: {root}")


def run_prewalk(binary: str) -> None:
    """ACP + settings-derived prewalk: switch after the first mutation and keep
    the new route for the session scope while the root provider is untouched."""
    root = Path(tempfile.mkdtemp(prefix="acp-prewalk."))
    root_provider = Provider()
    target_provider = Provider()
    # The switch request carries the write result as context; the target still
    # answers normally (no tool call is requested).
    target_provider.tool_result_text = "hello from mock"
    root_provider.start()
    target_provider.start()
    client = Client(binary, root, root_provider, prewalk_target=target_provider)
    try:
        client.request("initialize", {"protocolVersion": 1, "clientCapabilities": {}})
        cwd = str(client.cwd)
        created = client.request("session/new", {"cwd": cwd, "mcpServers": []})
        session_id = created["sessionId"]
        mark = client.mark()
        response = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "PREWALK mutate"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(response["stopReason"], "end_turn", "prewalk stop reason")
        assert_eq(root_provider.requests, 1, "root provider request count")
        assert_eq(target_provider.requests, 1, "target provider request count")
        updates = client.updates_since(mark, session_id)
        chunks = [u for u in updates if u.get("sessionUpdate") == "agent_message_chunk"]
        assert_eq(chunk_text(chunks), "hello from mock", "target provider response")
        assert any("Prewalk: switched to mock/model-b" in line for line in client.stderr), (
            f"missing switch notice; stderr={client.stderr[-10:]!r}"
        )

        # The switch lives in the session scope: the next prompt keeps using the
        # target endpoint, and the root provider is never contacted again.
        follow = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "follow up"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(follow["stopReason"], "end_turn", "post-switch stop reason")
        assert_eq(target_provider.requests, 2, "target keeps serving the session")
        assert_eq(root_provider.requests, 1, "root provider stays at one request")

        client.close_stdin()
        code = client.wait_exit(10.0)
        assert code is not None, f"ACP did not exit; stderr={client.stderr[-20:]!r}"
    finally:
        client.kill()
        root_provider.stop()
        target_provider.stop()
        print(f"acp_prewalk artifacts retained at: {root}")


def run_plan_yolo(binary: str) -> None:
    """ACP explicit plan-yolo: guard, auto-approved proposal, target switch,
    live handoff, then the target keeps serving the session."""
    root = Path(tempfile.mkdtemp(prefix="acp-plan-yolo."))
    root_provider = Provider()
    root_provider.plan_yolo_root = True
    target_provider = Provider()
    target_provider.model = "model-b"
    target_provider.plan_yolo_target = True
    target_provider.tool_result_text = "hello from target"
    root_provider.start()
    target_provider.start()
    client = Client(binary, root, root_provider, plan_yolo_target=target_provider)
    try:
        client.request(
            "initialize",
            {
                "protocolVersion": 1,
                "clientCapabilities": {"elicitation": {"form": {}}},
            },
        )
        cwd = str(client.cwd)
        created = client.request("session/new", {"cwd": cwd, "mcpServers": []})
        session_id = created["sessionId"]
        mark = client.mark()
        response = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "PLAN_YOLO start"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(response["stopReason"], "end_turn", "plan-yolo stop reason")
        assert_eq(root_provider.requests, 3, "guard, plan write, proposal")
        assert_eq(target_provider.requests, 2, "target write plus final reply")
        assert not (client.cwd / "blocked.txt").exists(), (
            "the plan guard must deny a working-tree write"
        )
        assert (client.cwd / "probe.txt").read_text(encoding="utf-8") == "done"
        assert any("Plan-yolo: plan approved, switched to mock/model-b" in line
                   for line in client.stderr), (
            f"missing plan-yolo notice; stderr={client.stderr[-10:]!r}"
        )
        assert not any("plan-yolo switch failed" in line for line in client.stderr), (
            f"a completed switch must not warn; stderr={client.stderr[-10:]!r}"
        )
        updates = client.updates_since(mark, session_id)
        mode_updates = [
            update
            for update in updates
            if update.get("sessionUpdate") == "current_mode_update"
        ]
        assert any(update.get("currentModeId") == "plan" for update in mode_updates), (
            "the armed plan-yolo phase must be announced"
        )
        assert any(update.get("currentModeId") == "default" for update in mode_updates), (
            "the cutover must publish the default mode"
        )
        chunks = [u for u in updates if u.get("sessionUpdate") == "agent_message_chunk"]
        assert "hello from target" in chunk_text(chunks), chunk_text(chunks)

        follow = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "follow up"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(follow["stopReason"], "end_turn", "post-plan stop reason")
        assert_eq(target_provider.requests, 3, "target keeps serving the session")
        assert_eq(root_provider.requests, 3, "root provider stays at three requests")

        # After the one-shot cutover, a client-selected manual plan must install
        # the ordinary guard again: the working-tree write is denied.
        client.request("session/set_mode", {"sessionId": session_id, "modeId": "plan"})
        manual = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "MANUAL_PLAN"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(manual["stopReason"], "end_turn", "manual plan stop reason")
        assert not (client.cwd / "manual-blocked.txt").exists(), (
            "a post-cutover manual plan must be write-guarded"
        )
        assert_eq(target_provider.requests, 5, "manual plan ran on the target route")
        assert_eq(root_provider.requests, 3, "root provider stays at three requests")

        # Abort an armed planning turn before its proposal is dispatched: the
        # incomplete arm must not survive into the next prompt or auto-switch.
        abort_session = client.request("session/new", {"cwd": cwd, "mcpServers": []})[
            "sessionId"
        ]
        root_provider.hang_after_proposal = True
        root_provider.proposal_waiting.clear()
        root_provider.release_proposal.clear()
        request_id = client.next_id
        client.next_id += 1
        client.send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "session/prompt",
                "params": {
                    "sessionId": abort_session,
                    "prompt": [{"type": "text", "text": "PLAN_YOLO abort"}],
                },
            }
        )
        assert root_provider.proposal_waiting.wait(PROMPT_TIMEOUT), (
            f"proposal never reached the provider; stderr={client.stderr[-8:]!r}"
        )
        client.notify("session/cancel", {"sessionId": abort_session})
        root_provider.release_proposal.set()
        cancelled = client.wait_frame(
            lambda f: f.get("id") == request_id and ("result" in f or "error" in f),
            PROMPT_TIMEOUT,
        )
        assert_eq(
            cancelled.get("result", {}).get("stopReason"),
            "cancelled",
            "aborted plan-yolo proposal stop reason",
        )
        root_provider.hang_after_proposal = False
        target_before = target_provider.requests
        # Leaving the incomplete arm abandons it; re-entering plan must install
        # the ordinary client-approved proposal path, never plan-yolo.
        client.request("session/set_mode", {"sessionId": abort_session, "modeId": "default"})
        client.request("session/set_mode", {"sessionId": abort_session, "modeId": "plan"})
        ordinary_mark = client.mark()
        ordinary, permissions = client.prompt_answering_permission(
            abort_session, "ORDINARY_PROPOSE", "allow_once"
        )
        assert_eq(ordinary["stopReason"], "end_turn", "ordinary plan proposal")
        assert permissions and permissions[0].get("method") == "elicitation/create", (
            "manual plan must propose through the client approval dialog"
        )
        ordinary_updates = client.updates_since(ordinary_mark, abort_session)
        assert any(
            update.get("sessionUpdate") == "current_mode_update"
            and update.get("currentModeId") == "default"
            for update in ordinary_updates
        ), "approved ordinary plan must publish the default mode"
        assert_eq(
            target_provider.requests,
            target_before,
            "ordinary proposal must never auto-switch models",
        )
        after_abort = client.request(
            "session/prompt",
            {"sessionId": abort_session, "prompt": [{"type": "text", "text": "NEXT"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(after_abort["stopReason"], "end_turn", "post-abort prompt")
        assert_eq(
            target_provider.requests,
            target_before,
            "an aborted approval must not auto-switch on the next prompt",
        )

        client.close_stdin()
        code = client.wait_exit(10.0)
        assert code is not None, f"ACP did not exit; stderr={client.stderr[-20:]!r}"
    finally:
        client.kill()
        root_provider.stop()
        target_provider.stop()
        print(f"acp_plan_yolo artifacts retained at: {root}")


def run_plan_yolo_mounted_tool(binary: str) -> None:
    """Plan-yolo hides only the plan-added builtin while a mounted MCP tool
    survives the cutover schema and executes."""
    root = Path(tempfile.mkdtemp(prefix="acp-plan-yolo-mcp."))
    root_provider = Provider()
    root_provider.plan_yolo_root = True
    target_provider = Provider()
    target_provider.model = "model-b"
    target_provider.mounted_plan_target = True
    script = root / "probe_mcp.py"
    script.write_text(
        r"""import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    mid = msg.get("id")
    method = msg.get("method")
    if method == "initialize":
        result = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "probe", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {
            "tools": [{
                "name": "echo",
                "description": "Echo result",
                "inputSchema": {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                },
            }],
        }
    elif method == "tools/call":
        result = {
            "content": [{"type": "text", "text": "echo result"}],
            "isError": False,
        }
    else:
        result = {}
    if mid is not None:
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": mid, "result": result}) + "\n")
        sys.stdout.flush()
""",
        encoding="utf-8",
    )
    mcp_servers = [
        {"name": "probe", "command": "python3", "args": [str(script)]}
    ]
    root_provider.start()
    target_provider.start()
    client = Client(
        binary,
        root,
        root_provider,
        plan_yolo_target=target_provider,
        plan_yolo_tools="read, mcp__probe_echo",
    )
    try:
        client.request(
            "initialize",
            {
                "protocolVersion": 1,
                "clientCapabilities": {"elicitation": {"form": {}}},
            },
        )
        cwd = str(client.cwd)
        session_id = client.request(
            "session/new", {"cwd": cwd, "mcpServers": mcp_servers}
        )["sessionId"]
        response = client.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "PLAN_YOLO mounted"}]},
            PROMPT_TIMEOUT,
        )
        assert_eq(response["stopReason"], "end_turn", "mounted plan stop reason")
        assert_eq(root_provider.requests, 3, "mounted root request count")
        assert_eq(target_provider.requests, 3, "hidden write, echo, final")

        def tool_names(body):
            names = set()
            for tool in body.get("tools") or []:
                function = tool.get("function") or {}
                name = function.get("name") or tool.get("name")
                if name:
                    names.add(name)
            return names

        assert "write" in tool_names(root_provider.bodies[0]), (
            "plan phase must receive the added write tool"
        )
        target_tools = tool_names(target_provider.bodies[0])
        assert "write" not in target_tools, f"hidden write leaked to schema: {target_tools}"
        assert "mcp__probe_echo" in target_tools, (
            f"mounted tool disappeared from schema: {target_tools}"
        )
        assert not (client.cwd / "blocked.txt").exists(), "plan guard must deny writes"
        assert not (client.cwd / "mounted-blocked.txt").exists(), (
            "hidden write must not execute after cutover"
        )
        target_wire = [json.dumps(body) for body in target_provider.bodies]
        assert any("not available in the active tool selection" in body for body in target_wire[1:]), (
            f"hidden write denial missing: {target_wire[-2:]!r}"
        )
        assert any("echo result" in body for body in target_wire[1:]), (
            f"mounted echo result missing: {target_wire[-2:]!r}"
        )
        assert any(
            "Plan mode: the working tree is read-only" in json.dumps(body)
            for body in root_provider.bodies
        ), "plan-phase write guard must stay active"
    finally:
        client.kill()
        root_provider.stop()
        target_provider.stop()
        print(f"acp_plan_yolo_mounted artifacts retained at: {root}")


def chunk_text(chunks):
    parts = []
    for update in chunks:
        content = update.get("content") or {}
        if content.get("type") == "text":
            parts.append(content.get("text", ""))
    return "".join(parts)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", help="path to the built omp binary")
    args = parser.parse_args(argv)
    binary = str(Path(args.binary).resolve())
    if not Path(binary).is_file():
        raise SystemExit(f"binary not found: {binary}")
    run(binary)
    run_prewalk(binary)
    run_plan_yolo(binary)
    run_plan_yolo_mounted_tool(binary)
    print("acp_accept: OK")


if __name__ == "__main__":
    main()
