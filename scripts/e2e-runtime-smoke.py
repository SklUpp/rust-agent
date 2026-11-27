#!/usr/bin/env python3
"""Offline E2E smoke suite for arag (R9 migration, R11 doctor read-only, R10 /retry & Alt+R).

Uses an in-process OpenAI-compatible SSE server and isolated HOME/PTYs; zero
external network or provider credentials required.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pty
import select
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable


def default_binary() -> str:
    env_bin = os.environ.get("ARAG_BIN")
    if env_bin:
        return env_bin
    cache_root = Path.home() / ".cache" / "rust-agent-artifacts"
    if cache_root.is_dir():
        candidates = sorted(
            (p / "arag" for p in cache_root.iterdir() if (p / "arag").is_file()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return str(candidates[0])
    debug_bin = Path("target/debug/arag")
    if debug_bin.is_file():
        return str(debug_bin)
    return "arag"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
def section_codes(report: dict, name: str) -> list[str]:
    for section in report.get("sections", []):
        if isinstance(section, dict) and section.get("name") == name:
            return [f["code"] for f in section.get("findings", []) if isinstance(f, dict)]
    raise AssertionError(f"section {name!r} missing from doctor report: {report}")


def write_fixture_configs(agent_dir: Path, port: int) -> None:
    agent_dir.mkdir(parents=True, exist_ok=True)
    (agent_dir / "models.yml").write_text(
        f"""providers:
  fixture:
    api: openai-completions
    baseUrl: http://127.0.0.1:{port}/v1
    apiKey: fixture-key
    models:
      - id: smoke-model
        name: Smoke Model
        contextWindow: 8192
        maxTokens: 512
        input: [text]
""",
        encoding="utf-8",
    )
    (agent_dir / "config.yml").write_text(
        "defaultModel: fixture/smoke-model\nmemory:\n  backend: off\n",
        encoding="utf-8",
    )


def make_isolated_env(home: Path, *, explicit_agent_dir: Path | None = None) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CACHE_HOME": str(home / ".cache"),
        "TMPDIR": str(home / "tmp"),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TERM": "xterm-256color",
        "NO_COLOR": "1",
    }
    if explicit_agent_dir is not None:
        env["PI_CONFIG_DIR"] = str(explicit_agent_dir)
        env["PI_CODING_AGENT_DIR"] = str(explicit_agent_dir)
    (home / "tmp").mkdir(parents=True, exist_ok=True)
    return env


class MockOpenAIServer:
    def __init__(self, handler_fn: Callable[[int, dict, BaseHTTPRequestHandler], None]) -> None:
        self.requests: list[dict] = []
        self._handler_fn = handler_fn
        self._lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:
                pass

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0") or "0")
                body = json.loads(self.rfile.read(length) or b"{}")
                with outer._lock:
                    outer.requests.append(body)
                    idx = len(outer.requests)
                outer._handler_fn(idx, body, self)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self._thread.join(timeout=5)


def send_sse(handler: BaseHTTPRequestHandler, events: list[dict | str]) -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream")
    handler.send_header("Cache-Control", "no-cache")
    handler.end_headers()
    for ev in events:
        payload = ev if isinstance(ev, str) else json.dumps(ev)
        handler.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
        handler.wfile.flush()


def read_pty_until(
    master_fd: int,
    buf: bytearray,
    predicate: Callable[[str], bool] | None = None,
    timeout: float = 5.0,
) -> str:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        text = buf.decode("utf-8", errors="replace")
        if predicate is not None and predicate(text):
            return text
        remaining = max(0.05, end - time.monotonic())
        ready, _, _ = select.select([master_fd], [], [], min(0.25, remaining))
        if ready:
            try:
                chunk = os.read(master_fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            buf.extend(chunk)
    return buf.decode("utf-8", errors="replace")


def test_r9_migration_and_r11_doctor(binary: Path, timeout: float) -> None:
    def on_req(idx: int, body: dict, handler: BaseHTTPRequestHandler) -> None:
        send_sse(
            handler,
            [
                {"choices": [{"delta": {"role": "assistant", "content": "MIGRATION_TURN_OK"}, "finish_reason": None}]},
                {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                "[DONE]",
            ],
        )

    srv = MockOpenAIServer(on_req)
    tmp = Path(tempfile.mkdtemp(prefix="arag-e2e-r9-"))
    try:
        home = tmp / "home"
        work = tmp / "work"
        legacy_root = home / ".omp"
        legacy_agent = legacy_root / "agent"
        work.mkdir(parents=True)
        write_fixture_configs(legacy_agent, srv.port)

        hook = legacy_agent / "hook.sh"
        hook.write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
        os.chmod(hook, 0o700)
        os.chmod(legacy_agent / "models.yml", 0o600)
        os.chmod(legacy_agent / "config.yml", 0o600)

        rel_link = legacy_agent / "config-link.yml"
        rel_link.symlink_to("config.yml")

        legacy_db = legacy_agent / "agent.db"
        con = sqlite3.connect(legacy_db)
        con.execute("CREATE TABLE probe (k TEXT PRIMARY KEY, v TEXT)")
        con.execute("INSERT INTO probe VALUES ('origin', 'omp')")
        con.commit()
        con.close()
        os.chmod(legacy_db, 0o600)

        pre_hashes = {
            "models.yml": sha256_file(legacy_agent / "models.yml"),
            "config.yml": sha256_file(legacy_agent / "config.yml"),
            "hook.sh": sha256_file(hook),
        }

        env = make_isolated_env(home)

        # Read-only startup checks must NOT create ~/.aragent
        for flag in ("--help", "-h", "--version", "-V"):
            p = subprocess.run([str(binary), flag], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
            check(p.returncode == 0, f"{flag} failed rc={p.returncode}: {p.stderr}")
            check(not (home / ".aragent").exists(), f"{flag} must not create ~/.aragent")

        doc_before = subprocess.run(
            [str(binary), "--cwd", str(work), "doctor", "--json"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        check("panicked" not in doc_before.stderr, f"doctor panicked: {doc_before.stderr}")
        check(doc_before.returncode in (0, 1), f"doctor failed rc={doc_before.returncode}: {doc_before.stderr}")
        check(not (home / ".aragent").exists(), "read-only doctor must not create ~/.aragent")
        rep_before = json.loads(doc_before.stdout)
        check(rep_before["summary"]["errors"] == 0, f"unexpected doctor errors: {rep_before}")
        cfg_codes_before = section_codes(rep_before, "config")
        check("legacy-pending" in cfg_codes_before, f"expected legacy-pending in config section: {cfg_codes_before}")

        # Normal run triggers copy-only R9 migration
        run_p = subprocess.run(
            [
                str(binary),
                "--cwd", str(work),
                "--no-session", "--no-skills", "--no-rules", "--no-title", "--no-lsp", "--no-pty", "--no-prewalk",
                "--mode", "text",
                "-p", "hello",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        check(run_p.returncode == 0, f"normal run failed rc={run_p.returncode}: {run_p.stderr}")
        check("MIGRATION_TURN_OK" in run_p.stdout, f"missing MIGRATION_TURN_OK: {run_p.stdout}")

        dst_root = home / ".aragent"
        dst_agent = dst_root / "agent"
        check((dst_root / ".migrated-from-omp-v1").is_file(), "missing .migrated-from-omp-v1")
        check((dst_agent / "hook.sh").is_file() and os.access(dst_agent / "hook.sh", os.X_OK), "executable bit lost on hook.sh")
        check((dst_agent / "config-link.yml").is_symlink(), "relative symlink not preserved")

        for name, expected_sha in pre_hashes.items():
            check(sha256_file(legacy_agent / name) == expected_sha, f"legacy {name} modified")

        con_dst = sqlite3.connect(dst_agent / "agent.db")
        integrity = con_dst.execute("PRAGMA integrity_check").fetchone()[0]
        row = con_dst.execute("SELECT v FROM probe WHERE k = 'origin'").fetchone()
        con_dst.close()
        check(integrity == "ok" and row == ("omp",), f"migrated sqlite invalid: integrity={integrity} row={row}")

        doc_after = subprocess.run(
            [str(binary), "--cwd", str(work), "doctor", "--json"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        check(doc_after.returncode in (0, 1), f"post-migration doctor failed: {doc_after.stderr}")
        rep_after = json.loads(doc_after.stdout)
        check(rep_after["summary"]["errors"] == 0, f"post-migration doctor errors: {rep_after}")
        cfg_codes_after = section_codes(rep_after, "config")
        check("legacy-root" in cfg_codes_after and "legacy-pending" not in cfg_codes_after, f"unexpected post-migration codes: {cfg_codes_after}")
        print("[PASS] 1. R9 copy-only migration + R11 doctor --json read-only")
    finally:
        srv.close()
        shutil.rmtree(tmp, ignore_errors=True)


def test_r10_repl_retry_sigint(binary: Path, timeout: float) -> None:
    first_chunk = threading.Event()
    release_first = threading.Event()

    def on_req(idx: int, body: dict, handler: BaseHTTPRequestHandler) -> None:
        if idx == 1:
            handler.send_response(200)
            handler.send_header("Content-Type", "text/event-stream")
            handler.send_header("Cache-Control", "no-cache")
            handler.end_headers()
            c = {"id": "c1", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {"role": "assistant", "content": "PARTIAL_STREAM "}, "finish_reason": None}]}
            handler.wfile.write(f"data: {json.dumps(c)}\n\n".encode())
            handler.wfile.flush()
            first_chunk.set()
            release_first.wait(timeout=15)
        else:
            send_sse(
                handler,
                [
                    {"choices": [{"delta": {"role": "assistant", "content": "RETRY_DONE_OK"}, "finish_reason": None}]},
                    {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                    "[DONE]",
                ],
            )

    srv = MockOpenAIServer(on_req)
    tmp = Path(tempfile.mkdtemp(prefix="arag-e2e-r10-repl-"))
    proc = None
    try:
        home = tmp / "home"
        work = tmp / "work"
        agent_dir = tmp / "agent"
        work.mkdir(parents=True)
        write_fixture_configs(agent_dir, srv.port)
        env = make_isolated_env(home, explicit_agent_dir=agent_dir)

        master, slave = pty.openpty()
        proc = subprocess.Popen(
            [str(binary), "--cwd", str(work), "--model", "fixture/smoke-model", "--no-skills", "--no-rules", "--no-title", "repl"],
            stdin=slave, stdout=slave, stderr=slave, env=env, close_fds=True, text=False,
        )
        os.close(slave)
        buf = bytearray()
        read_pty_until(master, buf, lambda s: "> " in s, timeout=5.0)
        os.write(master, b"please count\n")
        check(first_chunk.wait(timeout=min(10.0, timeout)), "first SSE chunk never arrived")
        read_pty_until(master, buf, lambda s: "PARTIAL_STREAM" in s, timeout=2.0)
        proc.send_signal(signal.SIGINT)
        read_pty_until(master, buf, lambda s: "did not retry them automatically" in s, timeout=4.0)
        release_first.set()
        time.sleep(0.3)
        check(len(srv.requests) == 1, f"expected NO auto-retry, saw {len(srv.requests)} requests")

        os.write(master, b"/retry\n")
        out = read_pty_until(master, buf, lambda s: "RETRY_DONE_OK" in s, timeout=8.0)
        os.write(master, b"/exit\n")
        proc.wait(timeout=10)
        os.close(master)

        check(len(srv.requests) == 2, f"expected 2 requests after /retry, saw {len(srv.requests)}: {out}")
        check("RETRY_DONE_OK" in out, f"missing RETRY_DONE_OK: {out}")
        req2_blob = json.dumps(srv.requests[1]).lower()
        check("interrupt" in req2_blob or "partial" in req2_blob, f"missing interruption warning in retry request: {req2_blob[:500]}")
        print("[PASS] 2. R10 REPL /retry after SIGINT (no auto-retry + warning context)")
    finally:
        release_first.set()
        if proc and proc.poll() is None:
            proc.kill()
        srv.close()
        shutil.rmtree(tmp, ignore_errors=True)


def test_r10_repl_retry_unknown_outcome(binary: Path, timeout: float) -> None:
    def on_req(idx: int, body: dict, handler: BaseHTTPRequestHandler) -> None:
        if idx == 1:
            send_sse(
                handler,
                [
                    {
                        "choices": [{
                            "delta": {
                                "tool_calls": [{
                                    "index": 0,
                                    "id": "call_unknown_1",
                                    "type": "function",
                                    "function": {
                                        "name": "bash",
                                        "arguments": json.dumps({"command": "echo TOOL_RUNNING_MARKER; sleep 30"}),
                                    },
                                }]
                            },
                            "finish_reason": None,
                        }]
                    },
                    {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]},
                    "[DONE]",
                ],
            )
        else:
            send_sse(
                handler,
                [
                    {"choices": [{"delta": {"role": "assistant", "content": "UNKNOWN_OUTCOME_RETRY_OK"}, "finish_reason": None}]},
                    {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                    "[DONE]",
                ],
            )

    srv = MockOpenAIServer(on_req)
    tmp = Path(tempfile.mkdtemp(prefix="arag-e2e-r10-unk-"))
    proc = None
    try:
        home = tmp / "home"
        work = tmp / "work"
        agent_dir = tmp / "agent"
        work.mkdir(parents=True)
        write_fixture_configs(agent_dir, srv.port)
        env = make_isolated_env(home, explicit_agent_dir=agent_dir)

        master, slave = pty.openpty()
        proc = subprocess.Popen(
            [str(binary), "--cwd", str(work), "--model", "fixture/smoke-model", "--yolo", "--no-skills", "--no-rules", "--no-title", "repl"],
            stdin=slave, stdout=slave, stderr=slave, env=env, close_fds=True, text=False,
        )
        os.close(slave)
        buf = bytearray()
        read_pty_until(master, buf, lambda s: "> " in s, timeout=5.0)
        os.write(master, b"run bash command\n")
        read_pty_until(master, buf, lambda s: "TOOL_RUNNING_MARKER" in s or "bash" in s.lower(), timeout=8.0)
        time.sleep(0.5)
        proc.kill()
        proc.wait(timeout=5)
        os.close(master)
        proc = None
        check(len(srv.requests) == 1, f"expected NO auto-retry before reopen, saw {len(srv.requests)}")

        # Reopen crashed session with --continue: unresolved tool intent must trigger [y/N] gate
        master, slave = pty.openpty()
        proc = subprocess.Popen(
            [str(binary), "--continue", "--cwd", str(work), "--model", "fixture/smoke-model", "--yolo", "--no-skills", "--no-rules", "--no-title", "repl"],
            stdin=slave, stdout=slave, stderr=slave, env=env, close_fds=True, text=False,
        )
        os.close(slave)
        buf = bytearray()
        read_pty_until(master, buf, lambda s: "> " in s, timeout=5.0)
        check(len(srv.requests) == 1, f"expected NO auto-retry on reopen, saw {len(srv.requests)}")

        # First /retry must demand explicit [y/N]; answering n cancels
        os.write(master, b"/retry\n")
        out_confirm = read_pty_until(master, buf, lambda s: "[y/N]" in s, timeout=5.0)
        check("[y/N]" in out_confirm, f"expected [y/N] confirmation gate, got:\n{out_confirm}")
        os.write(master, b"n\n")
        out_cancel = read_pty_until(master, buf, lambda s: "retry cancelled" in s, timeout=5.0)
        check("retry cancelled" in out_cancel, f"expected retry cancelled, got:\n{out_cancel}")
        check(len(srv.requests) == 1, f"declined retry must not send request: {len(srv.requests)}")

        # Second /retry + y proceeds
        os.write(master, b"/retry\n")
        read_pty_until(master, buf, lambda s: out_cancel.count("[y/N]") < s.count("[y/N]"), timeout=5.0)
        os.write(master, b"y\n")
        out_done = read_pty_until(master, buf, lambda s: "UNKNOWN_OUTCOME_RETRY_OK" in s, timeout=8.0)
        os.write(master, b"/exit\n")
        proc.wait(timeout=10)
        os.close(master)

        check(len(srv.requests) == 2, f"expected 2 requests after confirmed retry, saw {len(srv.requests)}")
        check("UNKNOWN_OUTCOME_RETRY_OK" in out_done, f"missing UNKNOWN_OUTCOME_RETRY_OK:\n{out_done}")
        print("[PASS] 3. R10 UnknownOutcome [y/N] confirmation gate (SIGKILL + --continue: decline cancels, confirm retries)")
    finally:
        if proc and proc.poll() is None:
            proc.kill()
        srv.close()
        shutil.rmtree(tmp, ignore_errors=True)


def test_r10_tui_alt_r(binary: Path, timeout: float) -> None:
    first_chunk = threading.Event()
    release_first = threading.Event()

    def on_req(idx: int, body: dict, handler: BaseHTTPRequestHandler) -> None:
        if idx == 1:
            handler.send_response(200)
            handler.send_header("Content-Type", "text/event-stream")
            handler.send_header("Cache-Control", "no-cache")
            handler.end_headers()
            c = {"id": "c1", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {"role": "assistant", "content": "TUI_PARTIAL "}, "finish_reason": None}]}
            handler.wfile.write(f"data: {json.dumps(c)}\n\n".encode())
            handler.wfile.flush()
            first_chunk.set()
            release_first.wait(timeout=15)
        else:
            send_sse(
                handler,
                [
                    {"choices": [{"delta": {"role": "assistant", "content": "TUI_ALTR_OK"}, "finish_reason": None}]},
                    {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                    "[DONE]",
                ],
            )

    srv = MockOpenAIServer(on_req)
    tmp = Path(tempfile.mkdtemp(prefix="arag-e2e-r10-tui-"))
    proc = None
    try:
        home = tmp / "home"
        work = tmp / "work"
        agent_dir = tmp / "agent"
        work.mkdir(parents=True)
        write_fixture_configs(agent_dir, srv.port)
        env = make_isolated_env(home, explicit_agent_dir=agent_dir)

        master, slave = pty.openpty()
        proc = subprocess.Popen(
            [str(binary), "--cwd", str(work), "--model", "fixture/smoke-model", "--no-skills", "--no-rules", "--no-title"],
            stdin=slave, stdout=slave, stderr=slave, env=env, close_fds=True, text=False,
        )
        os.close(slave)
        buf = bytearray()
        read_pty_until(master, buf, timeout=1.0)
        os.write(master, b"hello tui\r")
        check(first_chunk.wait(timeout=min(10.0, timeout)), "first TUI SSE chunk never arrived")
        read_pty_until(master, buf, lambda s: "TUI_PARTIAL" in s, timeout=2.0)
        os.write(master, b"\x03")
        time.sleep(0.8)
        release_first.set()
        time.sleep(0.4)
        check(len(srv.requests) == 1, f"expected NO auto-retry in TUI, saw {len(srv.requests)}")

        os.write(master, b"\x1br")
        out = read_pty_until(master, buf, lambda s: "TUI_ALTR_OK" in s, timeout=8.0)
        os.write(master, b"/exit\r")
        proc.wait(timeout=10)
        os.close(master)

        check(len(srv.requests) == 2, f"expected 2 requests after Alt+R, saw {len(srv.requests)}")
        check("TUI_ALTR_OK" in out, f"missing TUI_ALTR_OK in TUI output:\n{out[-800:]}")
        print("[PASS] 4. R10 TUI Alt+R manual retry")
    finally:
        release_first.set()
        if proc and proc.poll() is None:
            proc.kill()
        srv.close()
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=default_binary(), help="Path to built arag binary")
    parser.add_argument("--timeout", type=float, default=60.0, help="Per-command timeout in seconds")
    parser.add_argument(
        "--skip-doctor-readonly",
        action="store_true",
        help="Skip R9/R11 read-only doctor check (for older pre-fix binaries only)",
    )
    args = parser.parse_args()

    binary = Path(args.binary).resolve()
    check(binary.is_file() and os.access(binary, os.X_OK), f"executable arag binary not found: {binary}")
    print(f"Binary: {binary}")

    if not args.skip_doctor_readonly:
        test_r9_migration_and_r11_doctor(binary, args.timeout)
    test_r10_repl_retry_sigint(binary, args.timeout)
    test_r10_repl_retry_unknown_outcome(binary, args.timeout)
    test_r10_tui_alt_r(binary, args.timeout)
    print("ALL OFFLINE E2E RUNTIME SMOKE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
