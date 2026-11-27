#!/usr/bin/env python3
"""Live validation of the `arag` binary artifact against a real 9Router instance."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def default_binary() -> str:
    if env_bin := os.environ.get("ARAG_BIN"):
        return env_bin
    cache_dir = Path.home() / ".cache" / "rust-agent-artifacts"
    if cache_dir.is_dir():
        candidates = sorted(
            cache_dir.glob("*/arag"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return str(candidates[0])
    if on_path := shutil.which("arag"):
        return on_path
    return str(cache_dir / "45906993418" / "arag")


def run_cmd(
    argv: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def parse_ndjson(stdout: str) -> list[dict]:
    events: list[dict] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        events.append(json.loads(line))
    return events


def extract_session_id(events: list[dict]) -> str:
    for ev in events:
        if ev.get("type") == "session" and ev.get("id"):
            return str(ev["id"])
        if ev.get("type") == "session_header" and ev.get("session_id"):
            return str(ev["session_id"])
        if ev.get("session_id"):
            return str(ev["session_id"])
    raise ValidationError(f"session_id missing in NDJSON events: {events[:3]}")


class ValidationError(RuntimeError):
    """Raised when a live artifact check fails."""


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise ValidationError(msg)


def final_assistant_text(events: list[dict]) -> str:
    last_text = ""
    for ev in events:
        if ev.get("type") == "message_end":
            msg = ev.get("message") or {}
            if msg.get("role") == "assistant":
                parts = [
                    block.get("text", "")
                    for block in (msg.get("content") or [])
                    if isinstance(block, dict) and block.get("type") == "text"
                ]
                if parts:
                    last_text = "".join(parts)
    return last_text


def find_sqlite_dbs(session_dir: Path) -> list[Path]:
    return sorted(p for p in session_dir.rglob("*.db") if p.is_file())
def find_mcp_filesystem() -> list[str] | None:
    if env_cmd := os.environ.get("MCP_FILESYSTEM_CMD"):
        trimmed = env_cmd.strip()
        if trimmed:
            return [trimmed]
    prefix = os.environ.get("PREFIX", "/data/data/com.termux/files/usr")
    candidates = [
        shutil.which("mcp-server-filesystem"),
        f"{prefix}/lib/node_modules/omniroute/node_modules/.bin/mcp-server-filesystem",
        f"{prefix}/bin/mcp-server-filesystem",
        "/data/data/com.termux/files/usr/lib/node_modules/omniroute/node_modules/.bin/mcp-server-filesystem",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return [candidate]
    js_candidates = [
        f"{prefix}/lib/node_modules/omniroute/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js",
        f"{prefix}/lib/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js",
    ]
    node_bin = shutil.which("node")
    if node_bin:
        for js in js_candidates:
            if Path(js).is_file():
                return [node_bin, js]
    return None


def prepare_isolated_live_home(live_home: Path) -> tuple[dict[str, str], Path | None]:
    source_agent = next(
        (p for p in (Path.home() / ".aragent" / "agent", Path.home() / ".omp" / "agent") if p.is_dir()),
        None,
    )
    legacy_agent = live_home / ".omp" / "agent"
    legacy_agent.mkdir(parents=True, exist_ok=True)
    if source_agent is not None:
        for fname in ("models.yml", "config.yml"):
            src = source_agent / fname
            if src.is_file():
                dst = legacy_agent / fname
                shutil.copy2(src, dst)
                os.chmod(dst, 0o600)
        src_db = source_agent / "agent.db"
        if src_db.is_file():
            dst_db = legacy_agent / "agent.db"
            s_con = sqlite3.connect(str(src_db))
            d_con = sqlite3.connect(str(dst_db))
            try:
                s_con.backup(d_con)
            finally:
                d_con.close()
                s_con.close()
            os.chmod(dst_db, 0o600)
    env = os.environ.copy()
    env["HOME"] = str(live_home)
    env["XDG_CONFIG_HOME"] = str(live_home / ".config")
    env["XDG_DATA_HOME"] = str(live_home / ".local" / "share")
    env["XDG_CACHE_HOME"] = str(live_home / ".cache")
    env.pop("PI_CONFIG_DIR", None)
    env.pop("PI_CODING_AGENT_DIR", None)
    env.pop("OMP_PROFILE", None)
    env.pop("PI_PROFILE", None)
    return env, legacy_agent if source_agent is not None else None

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        default=default_binary(),
        help="Path to arag binary (defaults to ARAG_BIN, newest cached CI artifact, or PATH)",
    )
    parser.add_argument(
        "--model",
        default="9router/cx/gpt-5.6-luna",
        help="Primary 9Router model id",
    )
    parser.add_argument(
        "--secondary-model",
        default="9router/ocg/deepseek-v4.1-flash",
        help="Secondary 9Router model id",
    )
    parser.add_argument("--keep", action="store_true", help="Keep temporary directory")
    parser.add_argument("--no-isolate-home", action="store_true", help="Use real HOME instead of isolated copy of ~/.omp/agent")
    parser.add_argument("--require-mcp-fs", action="store_true", help="Fail instead of skipping if mcp-server-filesystem is unavailable")
    args = parser.parse_args()

    binary = Path(args.binary).resolve()
    if not binary.exists():
        print(f"FAIL: binary not found: {binary}", file=sys.stderr)
        return 2

    tmp_root = Path(tempfile.mkdtemp(prefix="arag-9router-val-"))
    print(f"Workspace root: {tmp_root}")
    try:
        work = tmp_root / "work"
        sess_dir = tmp_root / "sessions"
        import_sess_dir = tmp_root / "import_sessions"
        out_dir = tmp_root / "exports"
        iso_home = tmp_root / "iso_home"
        for d in (work, sess_dir, import_sess_dir, out_dir, iso_home):
            d.mkdir(parents=True, exist_ok=True)

        # Synthetic test crate
        (work / "src").mkdir()
        (work / "src" / "lib.rs").write_text(
            "pub fn add_bonus(x: i32) -> i32 {\n"
            "    x + 10\n"
            "}\n\n"
            "#[cfg(test)]\n"
            "mod tests {\n"
            "    use super::*;\n\n"
            "    #[test]\n"
            "    fn checks_bonus() {\n"
            "        assert_eq!(add_bonus(8), 50);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        (work / "README.md").write_text(
            "# Sample Crate\n\nMarker: TEST_MARKER_9ROUTER_771\n",
            encoding="utf-8",
        )
        git_init = run_cmd(["git", "init", "-q"], cwd=work)
        check(git_init.returncode == 0, git_init.stderr)
        run_cmd(["git", "config", "user.email", "smoke@example.com"], cwd=work)
        run_cmd(["git", "config", "user.name", "Smoke Test"], cwd=work)
        run_cmd(["git", "add", "."], cwd=work)
        git_commit = run_cmd(["git", "commit", "-q", "-m", "init"], cwd=work)
        check(git_commit.returncode == 0, git_commit.stderr)

        # Keep user ~/.omp memory untouched during 9Router runs while retaining auth/models.yml
        no_mem_cfg = tmp_root / "no_mem_config.yml"
        no_mem_cfg.write_text("memory:\n  backend: off\n", encoding="utf-8")
        if args.no_isolate_home:
            live_env = None
            seeded_legacy_agent = None
        else:
            live_home = tmp_root / "live_home"
            live_env, seeded_legacy_agent = prepare_isolated_live_home(live_home)

        # 1. Coding roundtrip via real 9Router: read -> edit -> bash (rustc --test)
        t0 = time.monotonic()
        prompt1 = (
            "In src/lib.rs, fix `add_bonus` so `checks_bonus` passes (change `+ 10` to `+ 42`). "
            "Use `read` and `edit`, then run "
            "`rustc --edition 2024 --test src/lib.rs -o target_test && ./target_test` "
            "using `bash`. Reply concisely with `FIXED_OK` and the test result."
        )
        p1 = run_cmd(
            [
                str(binary),
                "--cwd", str(work),
                "--session-dir", str(sess_dir),
                "--config", str(no_mem_cfg),
                "--model", args.model,
                "--no-skills",
                "--no-rules",
                "--no-title",
                "--yolo",
                "--mode", "json",
                "-p", prompt1,
            ],
            env=live_env,
            timeout=150,
        )
        check(p1.returncode == 0, f"Test 1 failed ({p1.returncode}):\nSTDOUT:\n{p1.stdout}\nSTDERR:\n{p1.stderr}")
        events1 = parse_ndjson(p1.stdout)
        session_id = extract_session_id(events1)
        lib_text = (work / "src" / "lib.rs").read_text(encoding="utf-8")
        check("x + 42" in lib_text, f"edit did not update src/lib.rs:\n{lib_text}")
        test_bin = work / "target_test"
        check(test_bin.exists(), "bash step did not compile target_test")
        check_bin = run_cmd([str(test_bin)], cwd=work)
        check(check_bin.returncode == 0, check_bin.stderr)
        print(f"[PASS] 1. Coding roundtrip via {args.model} ({time.monotonic() - t0:.1f}s), session={session_id}")

        # 2. Durable SQLite checks
        dbs = find_sqlite_dbs(sess_dir)
        check(bool(dbs), f"No SQLite db found in {sess_dir}")
        db_path = dbs[0]
        conn = sqlite3.connect(str(db_path))
        counts = {}
        for tbl in ("sessions", "branches", "messages", "executions", "stream_drafts"):
            counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        conn.close()
        check(counts["sessions"] >= 1, f"Expected >=1 session, got {counts}")
        check(counts["messages"] >= 2, f"Expected >=2 messages, got {counts}")
        check(counts["executions"] >= 2, f"Expected >=2 executions (read/edit/bash), got {counts}")
        check(counts["stream_drafts"] == 0, f"Expected clean stream_drafts, got {counts}")
        print(f"[PASS] 2. SQLite durability check: {counts}")
        # 3. --resume follow-up via real 9Router
        p2 = run_cmd(
            [
                str(binary),
                "--cwd", str(work),
                "--session-dir", str(sess_dir),
                "--config", str(no_mem_cfg),
                "--model", args.model,
                "--no-tools",
                "--no-skills",
                "--no-rules",
                "--no-title",
                "--resume", session_id,
                "-p", "What integer literal did you change `+ 10` to in src/lib.rs? Reply with the number only.",
            ],
            env=live_env,
            timeout=90,
        )
        check(p2.returncode == 0, f"Resume failed: {p2.stderr}\n{p2.stdout}")
        check("42" in p2.stdout, f"Resume lost context:\n{p2.stdout}")
        print("[PASS] 3. Session --resume verified (recalled 42 without tools)")

        # 4. R6 export (json/md/html), inspect, import round-trip
        for fmt, ext in (("json", "json"), ("md", "md"), ("html", "html")):
            out_file = out_dir / f"session.{ext}"
            ex = run_cmd(
                [
                    str(binary),
                    "--session-dir", str(sess_dir),
                    "export", session_id,
                    "--output", str(out_file),
                    "--format", fmt,
                ],
                env=live_env,
            )
            check(ex.returncode == 0, f"export {fmt} failed: {ex.stderr}")
            check(out_file.exists() and out_file.stat().st_size > 100, f"export {fmt} empty output")
        bundle = json.loads((out_dir / "session.json").read_text(encoding="utf-8"))
        check(bundle.get("schema_version") == 7, f"Unexpected schema_version: {bundle.keys()}")
        check(bundle.get("session", {}).get("id") == session_id, f"Bundle session id mismatch: {bundle.get('session')}")

        insp = run_cmd(
            [
                str(binary),
                "--session-dir", str(sess_dir),
                "inspect", session_id, "--context", "--json",
            ],
            env=live_env,
        )
        check(insp.returncode == 0, f"inspect failed: {insp.stderr}")
        insp_data = json.loads(insp.stdout)
        check(insp_data.get("id") == session_id, f"inspect top-level id mismatch: {insp_data}")

        imp = run_cmd(
            [
                str(binary),
                "import", str(out_dir / "session.json"),
                "--from", "bundle",
                "--session-dir", str(import_sess_dir),
                "--json",
            ],
            env=live_env,
        )
        check(imp.returncode == 0, f"import failed: {imp.stderr}")
        imp_dbs = find_sqlite_dbs(import_sess_dir)
        check(bool(imp_dbs), "Import did not create SQLite db")
        imp_conn = sqlite3.connect(str(imp_dbs[0]))
        imp_msgs = imp_conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        imp_execs = imp_conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        imp_conn.close()
        check(imp_msgs >= counts["messages"], f"Imported messages lost: {imp_msgs} < {counts['messages']}")
        check(imp_execs >= counts["executions"], f"Imported executions lost: {imp_execs} < {counts['executions']}")
        print(f"[PASS] 4. R6 export(json/md/html) + inspect + import roundtrip (msgs={imp_msgs}, execs={imp_execs})")

        # 5a. Subagent `task` (--no-session, per S4 durable boundary gate)
        t1 = time.monotonic()
        prompt_sub = (
            "You MUST call the `task` tool (do not read README.md directly) to inspect README.md "
            "and return the exact Marker token found there. Reply with that exact Marker token."
        )
        p_sub = run_cmd(
            [
                str(binary),
                "--cwd", str(work),
                "--no-session",
                "--config", str(no_mem_cfg),
                "--model", args.model,
                "--no-skills",
                "--no-rules",
                "--no-title",
                "--yolo",
                "--mode", "json",
                "-p", prompt_sub,
            ],
            env=live_env,
            timeout=180,
        )
        check(p_sub.returncode == 0, f"Subagent test failed: {p_sub.stderr}\n{p_sub.stdout}")
        sub_events = parse_ndjson(p_sub.stdout)
        check(
            any(
                ev.get("type") == "tool_execution_end" and ev.get("toolName") == "task" and not ev.get("isError")
                for ev in sub_events
            ),
            "task tool did not succeed",
        )
        check(
            "TEST_MARKER_9ROUTER_771" in final_assistant_text(sub_events),
            f"Marker missing in final assistant answer: {final_assistant_text(sub_events)!r}",
        )
        print(f"[PASS] 5a. Subagent `task` via {args.model} ({time.monotonic() - t1:.1f}s)")

        # 5b. Secondary model + ast_grep under durable session
        t2 = time.monotonic()
        p_ast = run_cmd(
            [
                str(binary),
                "--cwd", str(work),
                "--session-dir", str(sess_dir),
                "--config", str(no_mem_cfg),
                "--model", args.secondary_model,
                "--tools", "ast_grep",
                "--no-skills",
                "--no-rules",
                "--no-title",
                "--yolo",
                "--mode", "json",
                "-p", "Use `ast_grep` on `src/lib.rs` with pattern `pub fn $NAME($$$ARGS) -> $RET { $$$BODY }` (lang rust) and reply with the matched function name.",
            ],
            env=live_env,
            timeout=120,
        )
        check(p_ast.returncode == 0, f"ast_grep failed: {p_ast.stderr}\n{p_ast.stdout}")
        ast_events = parse_ndjson(p_ast.stdout)
        check(
            any(
                ev.get("type") == "tool_execution_end"
                and ev.get("toolName") == "ast_grep"
                and not ev.get("isError")
                for ev in ast_events
            ),
            f"Expected successful ast_grep tool call in {ast_events}",
        )
        check("add_bonus" in final_assistant_text(ast_events), f"add_bonus missing from final answer: {ast_events}")
        print(f"[PASS] 5b. ast_grep via {args.secondary_model} ({time.monotonic() - t2:.1f}s)")

        # 6. Isolated HOME checks: R8 trust store + R5 memory + CLI read/grep
        iso_agent_dir = iso_home / ".omp" / "agent"
        iso_env = os.environ.copy()
        iso_env["HOME"] = str(iso_home)
        iso_env["XDG_CONFIG_HOME"] = str(iso_home / ".config")
        iso_env["XDG_DATA_HOME"] = str(iso_home / ".local" / "share")
        iso_env["PI_CONFIG_DIR"] = str(iso_agent_dir)
        iso_env["PI_CODING_AGENT_DIR"] = str(iso_agent_dir)
        iso_env.pop("OMP_PROFILE", None)
        iso_env.pop("PI_PROFILE", None)

        trust_repo = tmp_root / "trust_repo"
        (trust_repo / ".claude").mkdir(parents=True)
        hook_file = trust_repo / ".claude" / "settings.json"
        hook_file.write_text('{"hooks": {}}', encoding="utf-8")

        st1 = run_cmd([str(binary), "--cwd", str(trust_repo), "trust", "status", "--json"], env=iso_env)
        check(st1.returncode == 0 and json.loads(st1.stdout).get("status") == "untrusted", st1.stdout + st1.stderr)
        gr = run_cmd([str(binary), "--cwd", str(trust_repo), "trust", "grant", "--json"], env=iso_env)
        check(gr.returncode == 0, gr.stdout + gr.stderr)
        st2 = run_cmd([str(binary), "--cwd", str(trust_repo), "trust", "status", "--json"], env=iso_env)
        check(st2.returncode == 0 and json.loads(st2.stdout).get("status") == "trusted", st2.stdout + st2.stderr)
        hook_file.write_text('{"hooks": {"PreToolUse": []}}', encoding="utf-8")
        st3 = run_cmd([str(binary), "--cwd", str(trust_repo), "trust", "status", "--json"], env=iso_env)
        check(st3.returncode == 0 and json.loads(st3.stdout).get("status") == "invalidated", st3.stdout + st3.stderr)
        rv = run_cmd([str(binary), "--cwd", str(trust_repo), "trust", "revoke", "--json"], env=iso_env)
        check(rv.returncode == 0 and json.loads(rv.stdout).get("revoked") is True, rv.stdout + rv.stderr)
        st4 = run_cmd([str(binary), "--cwd", str(trust_repo), "trust", "status", "--json"], env=iso_env)
        check(st4.returncode == 0 and json.loads(st4.stdout).get("status") == "untrusted", st4.stdout + st4.stderr)
        print("[PASS] 6. R8 D20 trust store lifecycle (untrusted -> trusted -> invalidated -> revoked)")

        rd = run_cmd([str(binary), "--cwd", str(work), "read", "src/lib.rs"], env=iso_env)
        check(rd.returncode == 0 and "x + 42" in rd.stdout, rd.stderr)
        gp = run_cmd([str(binary), "--cwd", str(work), "grep", "add_bonus", str(work)], env=iso_env)
        check(gp.returncode == 0 and "add_bonus" in gp.stdout, gp.stderr)

        iso_agent_dir.mkdir(parents=True, exist_ok=True)
        (iso_agent_dir / "config.yml").write_text("memory:\n  backend: local\n", encoding="utf-8")

        mem_add = run_cmd(
            [str(binary), "--cwd", str(work), "memory", "create", "--project", "add_bonus adds 42 in sample crate"],
            env=iso_env,
        )
        check(mem_add.returncode == 0, f"memory create failed: {mem_add.stderr}")
        mem_search = run_cmd([str(binary), "--cwd", str(work), "memory", "search", "add_bonus"], env=iso_env)
        check(mem_search.returncode == 0 and "add_bonus" in mem_search.stdout, mem_search.stdout + mem_search.stderr)
        mem_list = run_cmd([str(binary), "--cwd", str(work), "memory", "list", "--project"], env=iso_env)
        check(mem_list.returncode == 0 and "add_bonus" in mem_list.stdout, mem_list.stdout + mem_list.stderr)
        print("[PASS] 7. CLI read/grep + R5 memory FTS5 create/search/list")

        # 8. R3 BashPolicy deny blocks even under --yolo
        deny_cfg = tmp_root / "deny_cfg.yml"
        deny_cfg.write_text(
            "memory:\n  backend: off\nbash:\n  patterns:\n    - match: \"*FORBIDDEN_SMOKE_CMD*\"\n      approval: \"deny\"\n",
            encoding="utf-8",
        )
        blocked_marker = work / "blocked.txt"
        p_deny = run_cmd(
            [
                str(binary),
                "--cwd", str(work),
                "--session-dir", str(sess_dir),
                "--config", str(deny_cfg),
                "--model", args.model,
                "--no-skills",
                "--no-rules",
                "--no-title",
                "--yolo",
                "--mode", "json",
                "-p", "Try running `echo FORBIDDEN_SMOKE_CMD > blocked.txt` with bash. If blocked by policy, report BLOCKED_OK.",
            ],
            env=live_env,
            timeout=90,
        )
        check(not blocked_marker.exists(), "BashPolicy deny failed to block command under --yolo!")
        deny_events = parse_ndjson(p_deny.stdout)
        forbidden_call_ids = {
            ev.get("toolCallId")
            for ev in deny_events
            if ev.get("type") == "tool_execution_start"
            and ev.get("toolName") == "bash"
            and "FORBIDDEN_SMOKE_CMD" in str((ev.get("args") or {}).get("command", ""))
        }
        check(bool(forbidden_call_ids), "Expected bash tool_execution_start for FORBIDDEN_SMOKE_CMD")
        matched_denials = [
            ev
            for ev in deny_events
            if ev.get("type") == "tool_execution_end"
            and ev.get("toolCallId") in forbidden_call_ids
            and ev.get("isError") is True
        ]
        check(bool(matched_denials), f"Expected correlated bash error end for {forbidden_call_ids}")
        denial_text = " ".join(
            block.get("text", "")
            for ev in matched_denials
            for block in ((ev.get("result") or {}).get("content") or [])
            if isinstance(block, dict)
        ).lower()
        check(
            "bash pattern `*forbidden_smoke_cmd*` matched" in denial_text
            or "blocked by bash policy" in denial_text,
            f"Expected bash policy rejection message, got: {denial_text!r}",
        )
        print(f"[PASS] 8. R3 BashPolicy deny enforced under --yolo (rc={p_deny.returncode})")
        # 9. R9 copy-only migration check on isolated legacy home copy
        if seeded_legacy_agent is not None and live_env is not None:
            aragent_root = Path(live_env["HOME"]) / ".aragent"
            check((aragent_root / ".migrated-from-omp-v1").is_file(), "R9 migration marker missing in live_home/.aragent")
            migrated_db = aragent_root / "agent" / "agent.db"
            if (seeded_legacy_agent / "agent.db").is_file():
                check(migrated_db.is_file(), "R9 migrated agent.db missing")
                m_con = sqlite3.connect(str(migrated_db))
                check(m_con.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "R9 migrated agent.db failed integrity_check")
                m_con.close()
            print("[PASS] 9. R9 copy-only migration on isolated ~/.omp/agent copy")

        # 10. R0.4 live external stdio mcp-server-filesystem roundtrip via 9Router
        mcp_target = find_mcp_filesystem()
        if mcp_target is None:
            if args.require_mcp_fs:
                raise ValidationError("mcp-server-filesystem not found and --require-mcp-fs is set")
            print("[SKIP] 10. R0.4 live stdio mcp-server-filesystem (server binary not found)")
        else:
            mcp_work = tmp_root / "mcp_work"
            (mcp_work / ".omp").mkdir(parents=True)
            probe_token = f"MCP_FS_PAYLOAD_{os.getpid()}"
            probe_file = mcp_work / "probe.txt"
            probe_file.write_text(probe_token + "\n", encoding="utf-8")
            (mcp_work / ".omp" / "mcp.json").write_text(
                json.dumps(
                    {
                        "mcpServers": {
                            "filesystem": {
                                "command": mcp_target[0],
                                "args": [*mcp_target[1:], str(mcp_work)],
                            }
                        }
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            tr = run_cmd([str(binary), "--cwd", str(mcp_work), "trust", "grant", "--json"], env=live_env)
            check(tr.returncode == 0, f"MCP trust grant failed: {tr.stderr}")
            p_mcp = run_cmd(
                [
                    str(binary),
                    "--cwd", str(mcp_work),
                    "--no-session",
                    "--config", str(no_mem_cfg),
                    "--model", args.secondary_model,
                    "--no-skills",
                    "--no-rules",
                    "--no-title",
                    "--yolo",
                    "--mode", "json",
                    "-p", f"Use the MCP filesystem tool (not built-in read) to read {probe_file} and reply with the exact token inside.",
                ],
                env=live_env,
                timeout=120,
            )
            check(p_mcp.returncode == 0, f"Live MCP filesystem failed: {p_mcp.stderr}\n{p_mcp.stdout}")
            mcp_events = parse_ndjson(p_mcp.stdout)
            check(
                any(
                    ev.get("type") == "tool_execution_end"
                    and str(ev.get("toolName", "")).startswith("mcp__filesystem_")
                    and not ev.get("isError")
                    for ev in mcp_events
                ),
                f"Expected successful mcp__filesystem_* call in {mcp_events}",
            )
            check(probe_token in final_assistant_text(mcp_events), f"Missing {probe_token} in final text: {final_assistant_text(mcp_events)!r}")
            print("[PASS] 10. R0.4 live stdio mcp-server-filesystem roundtrip via 9Router")

        print("ALL LIVE 9ROUTER + ARTIFACT VALIDATIONS PASSED")
        return 0
    finally:
        if not args.keep:
            shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
