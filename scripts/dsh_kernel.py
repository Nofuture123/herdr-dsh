#!/usr/bin/env python3
"""DshKernel — one `dsh --profile headless <task>` process per task.

Streams the harness reasoning (stderr) into a per-task log for the pane
tailer, captures the final assistant message (stdout), runs verify commands
when the spec asks for them, and exposes the same snapshot contract the
executor loop knows from NAR-style kernels."""
import json, os, shlex, signal, subprocess, threading, time

try:
    import secrets
    _hex = secrets.token_hex
except Exception:
    _hex = lambda n: os.urandom(n // 2).hex()

BASE = os.path.expanduser(os.environ.get("DSH_BRIDGE_DIR", "~/.local/share/dsh-bridge"))
LOGS = os.path.join(BASE, "logs")
_TERMINAL = {"succeeded", "failed", "cancelled", "killed"}

def dsh_command():
    env_bin = os.environ.get("DSH_BIN")
    if env_bin:
        return shlex.split(env_bin)
    import shutil
    dsh = shutil.which("dsh")
    if dsh:
        return [dsh]
    npx = shutil.which("npx") or "/opt/homebrew/bin/npx"
    return [npx, "--yes", "@deepseek-ai/dsh"]

class DshKernel:
    """Same surface the executor loop expects: submit/wait/snapshot/cancel/kill/inspect."""

    def __init__(self):
        self.states = {}
        self.procs = {}
        self.lock = threading.Lock()

    # -- lifecycle --
    def submit(self, agent, goal, ws, scope_files=None, forbid=None, verify=None,
               mode="yolo", permission_policy="allow", timeout_sec=600,
               idempotency_key=None, session_ref=None, **_ignored):
        tid = "t-" + _hex(6)
        logdir = os.path.join(LOGS, tid)
        os.makedirs(logdir, exist_ok=True)
        try: os.chmod(logdir, 0o700)
        except OSError: pass
        cmd = dsh_command() + ["--profile", "headless", goal]
        state = {"task_id": tid, "status": "running", "goal": goal,
                 "workspace": ws, "error": None, "native_session_id": None,
                 "result": {"worker_summary": "", "verify": [], "duration_sec": None,
                            "stream_log": os.path.join(logdir, "stream.log"),
                            "diff": {"changed_files": []}},
                 "_rc": None}
        with self.lock:
            self.states[tid] = state
        t0 = time.time()

        def run():
            try:
                with open(os.path.join(logdir, "stream.log"), "a") as streamlog:
                    proc = subprocess.Popen(cmd, cwd=ws, stdout=subprocess.PIPE,
                                            stderr=streamlog, start_new_session=True,
                                            text=True)
                    with self.lock:
                        self.procs[tid] = proc
                    try:
                        out, _ = proc.communicate(timeout=float(timeout_sec))
                    except subprocess.TimeoutExpired:
                        self._kill_tree(proc)
                        out, _ = proc.communicate()
                        state["error"] = f"timeout after {int(timeout_sec)}s"
                    state["_rc"] = proc.returncode
                state["result"]["worker_summary"] = (out or "").strip()[-4000:]
                # verify commands are the permission to gate success
                for vcmd in (verify or []):
                    v = subprocess.run(["sh", "-c", vcmd], cwd=ws,
                                       capture_output=True, text=True, timeout=300)
                    state["result"]["verify"].append(
                        {"cmd": vcmd, "exit_code": v.returncode,
                         "ok": v.returncode == 0,
                         "log": (v.stdout or "")[-2000:]})
                state["result"]["changed_files"] = self._git_changes(ws)
                state["result"]["duration_sec"] = round(time.time() - t0, 1)
                if state["error"]:
                    state["status"] = "failed"
                elif state["_rc"] != 0:
                    state["status"] = "failed"
                elif state["result"]["verify"] and not all(v["ok"] for v in state["result"]["verify"]):
                    state["status"] = "failed"
                else:
                    state["status"] = "succeeded"
                state["ok"] = state["status"] == "succeeded"
            except Exception as e:
                state["status"] = "failed"
                state["ok"] = False
                state["error"] = str(e)
            with self.lock:
                self.procs.pop(tid, None)

        threading.Thread(target=run, daemon=True).start()
        return {"task_id": tid, "status": "running",
                "result": {"stream_log": os.path.join(logdir, "stream.log")}}

    def _git_changes(self, ws):
        try:
            p = subprocess.run(["git", "status", "--porcelain"], cwd=ws,
                               capture_output=True, text=True, timeout=10)
            if p.returncode != 0:
                return []
            return [l[3:].strip() for l in (p.stdout or "").splitlines() if l.strip()]
        except Exception:
            return []

    def _kill_tree(self, proc):
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except Exception:
            try: proc.terminate()
            except Exception: pass

    # -- polling surface --
    def wait(self, tid, timeout_sec=None):
        if timeout_sec is None: timeout_sec = 0.5
        deadline = time.time() + float(timeout_sec)
        while time.time() < deadline:
            if self.states.get(tid, {}).get("status") in _TERMINAL:
                break
            time.sleep(0.1)
        return self.snapshot(tid)

    def snapshot(self, tid):
        st = self.states.get(tid)
        if st is None:
            return {"task_id": tid, "status": "unknown"}
        return dict(st)

    def cancel(self, tid):
        proc = self.procs.get(tid)
        if proc:
            self._kill_tree(proc)
            with self.lock:
                if tid in self.states:
                    self.states[tid]["status"] = "cancelled"
            return {"confirmed": True}
        return {"confirmed": False}

    def kill(self, tid):
        proc = self.procs.get(tid)
        if proc:
            try: os.killpg(proc.pid, signal.SIGKILL)
            except Exception: pass
        with self.lock:
            if tid in self.states:
                self.states[tid]["status"] = "killed"
        return {"killed": True}

    def inspect(self, tid, what="summary"):
        st = self.states.get(tid, {})
        if what == "status": return st.get("status")
        return json.dumps({k: v for k, v in st.items() if not k.startswith("_")}, ensure_ascii=False)

    def list_tasks(self):
        return [dict(task_id=t, status=s.get("status"), goal=s.get("goal",""),
                     created=s.get("result",{}).get("duration_sec"))
                for t, s in sorted(self.states.items())]
