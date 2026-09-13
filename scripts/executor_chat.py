#!/usr/bin/env python3
"""zcodecli chat — ONE ZCode session per Herdr agent pane. PURE transport CLI.

  <text>   -> a turn in this pane's session (owner-authorized full access)
  {json}   -> strict spec, fail-closed; edit/yolo requires verify
  /cancel  -> tri-state cancel of the running turn
  /status | /help | /quit
Facts reported (status/verify_ok/out_of_scope); acceptance = master's job.
"""
import json, os, queue, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from executor_common import (broker, build_kernel, report, remember_session,
                             emit, collect, persist, sanitize, stream_native_output,
                             receipt_ok, receipt_err, attach_summary_full, WARN, marker_line,
                             display_text, _C_DIM)

def _c(n): return f"\033[{n}m"
DIM, BOLD, GREEN, RED, YEL, CYA, RST = _c(2), _c(1), _c(32), _c(31), _c(33), _c(36), _c(0)
WS = os.path.realpath(os.environ.get("QAB_WORKSPACE") or os.getcwd())
MODE = os.environ.get("QAB_DEFAULT_MODE", "yolo")
POLICY = os.environ.get("QAB_DEFAULT_POLICY", "allow")
GUI_TOOLS = ["computer-use", "computer_use", "mcp__computer-use", "cua", "screenshot", "gui"]

class Chat:
    def __init__(self, kernel, out=print, ws=None):
        self.kernel = kernel; self.out = out
        self.ws = ws or WS
        self.busy = None; self.root = None; self.q = queue.Queue()
        self.current_nonce = None; self.current_rid = None

    def sig(self, kind, *fields):
        line = marker_line(kind, " ".join(str(f) for f in fields))
        if line:
            self.out(line)

    def submit(self, spec, rid):
        if spec.get("idempotency_key"):
            broker.idempotency_state(spec["idempotency_key"], broker.fingerprint(spec),
                                     "submitting", request_id=rid)
        kwargs = dict(scope_files=spec["scope"] or None, forbid=GUI_TOOLS,
                      verify=spec["verify"] or None, mode=spec["mode"],
                      permission_policy=spec["policy"], timeout_sec=float(spec["timeout"]),
                      idempotency_key=spec.get("idempotency_key"))
        if self.root: kwargs["session_ref"] = self.root   # pane == one session
        try:
            snap = self.kernel.submit("zcode", spec["goal"], spec["workspace"], **kwargs)
        except Exception as e:
            # never reached the queue: release the key so a retry is legal
            if spec.get("idempotency_key"):
                broker.idempotency_release(spec["idempotency_key"])
            self.busy = None; report("idle", force=True)
            receipt_err(spec.get("nonce"), f"submit failed: {e}")
            return self.sig("error", sanitize(f"submit failed: {e}", 200))
        tid = snap.get("task_id")
        broker.attach_task(rid, tid)
        broker.set_owner(tid, os.environ.get("HERDR_PANE_ID"))
        if spec.get("idempotency_key"):
            broker.idempotency_state(spec["idempotency_key"], broker.fingerprint(spec),
                                     "attached", task_id=tid, request_id=rid)
        self.busy = tid
        self.current_rid = rid
        self.current_nonce = spec.get("nonce")
        self.sig("accepted", rid, tid, snap.get("status"), spec.get("nonce"))
        receipt_ok(spec.get("nonce"), request_id=rid, task_id=tid, status=snap.get("status"))
        if not os.path.exists(os.path.join(spec["workspace"], ".git")):
            self.out(f"{WARN}⚠ workspace is not a git repo — changed_files evidence "
                     f"will be unavailable{RST}")
        if getattr(self, "_tail", None):
            self._tail.set()   # a pane runs one task at a time; replace the tailer
        self._tail = threading.Event()
        self._tail_thread = threading.Thread(
            target=stream_native_output, args=(tid, self.out, self._tail), daemon=True)
        self._tail_thread.start()
        return tid

    def stop_tail(self):
        """Stop the live tailer and let it flush remaining lines BEFORE the
        result block prints, so pane order stays chronological."""
        if getattr(self, "_tail", None):
            self._tail.set()
        if getattr(self, "_tail_thread", None):
            self._tail_thread.join(timeout=1.0)

    def _release_if_never_ran(self, spec, data):
        """workspace_busy means the task never executed; free the idempotency key."""
        if (data.get("status") == "failed" and spec and spec.get("idempotency_key")
                and "workspace_busy" in str(data.get("error") or "")):
            broker.idempotency_release(spec["idempotency_key"])

    def background_wait(self, tid, rid, nonce=None):
        """Task still running at wait-timeout: keep busy truthful until terminal."""
        spec = getattr(self, "active_spec", None)
        t0 = time.time()
        def waiter():
            while True:
                snap = self.kernel.wait(tid, timeout_sec=1.0)
                if snap.get("status") not in ("submitted", "running", "cancel_requested", None):
                    break
            data = collect(snap)
            data["request_id"] = rid
            attach_summary_full(data, tid)
            remember_session(data.get("native_session_id"))
            persist({**snap, "request_id": rid, "status": data["status"],
                     "ok": data["ok"], "verify_ok": data["verify_ok"],
                     "out_of_scope": data["out_of_scope"],
                     "changed_files": data["changed_files"],
                     "summary": data["summary"], "usage": data["usage"],
                     "summary_full": data.get("summary_full")})
            self._release_if_never_ran(spec, data)
            self.stop_tail()
            self.sig("result", rid, tid, nonce or self.current_nonce)
            mark = "✓" if data["ok"] else "✗"
            self.out(f"{_C_DIM}── {mark} {tid} {data['status']} · "
                     f"verify_ok={data['verify_ok']} ({round(time.time() - t0)}s) ──{RST}")
            line = marker_line("summary", display_text(data["summary"], 200))
            if line:
                self.out(line)
            if getattr(self, "_tail", None): self._tail.set()
            if self.busy == tid: self.busy = None
            report("idle")
        threading.Thread(target=waiter, daemon=True).start()

    def run_turn(self, line):
        rid = broker.new_request_id()
        try:
            if line.startswith("{"):
                spec = broker.normalize_spec(json.loads(line), self.ws, MODE, POLICY,
                                             require_verify=True)
            else:
                spec = broker.normalize_spec({"goal": line, "workspace": self.ws},
                                             self.ws, MODE, POLICY)
        except (broker.SpecError, json.JSONDecodeError) as e:
            raw_nonce = None
            try: raw_nonce = json.loads(line).get("nonce")
            except Exception: pass
            err = {"ok": False, "error": sanitize(f"rejected (fail-closed): {e}", 200)}
            if raw_nonce: err["nonce"] = raw_nonce
            receipt_err(raw_nonce, err["error"], request_id=rid)
            return self.sig("error", json.dumps(err, ensure_ascii=True))
        self.current_nonce = spec.get("nonce")
        if spec.get("idempotency_key"):
            st, rec = broker.idempotency_claim(spec["idempotency_key"], broker.fingerprint(spec))
            if st == "conflict":
                receipt_err(self.current_nonce, "idempotency_conflict: same key, different spec", request_id=rid)
                return self.sig("error", json.dumps({"ok": False, "error": "idempotency_conflict", "nonce": self.current_nonce}, ensure_ascii=True))
            if st == "indeterminate":
                receipt_err(self.current_nonce, "idempotency_indeterminate: previous attempt crashed mid-submit; do not blind-rerun", request_id=rid)
                return self.sig("error", json.dumps({"ok": False, "error": "idempotency_indeterminate: previous attempt crashed mid-submit; do not blind-rerun", "nonce": self.current_nonce}, ensure_ascii=True))
            if st == "duplicate":
                return self.sig("accepted", rec["request_id"], rec.get("task_id") or "-", "duplicate", self.current_nonce)
            rid = rec["request_id"]
        broker.save_request(rid, spec)
        self.active_spec = spec
        self.out(f"{DIM}── {time.strftime('%H:%M:%S')} ▶ {display_text(spec['goal'], 60)}{RST}")
        report("working", sanitize(spec["goal"], 80))
        tid = self.submit(spec, rid)
        cancelled_by_us = False
        deadline = time.time() + spec["timeout"]
        while time.time() < deadline:
            try:
                while True:
                    raw = self.q.get_nowait()
                    l = (raw or "").strip()
                    if l == "/cancel" or l.startswith("/cancel "):
                        ctid = l.split()[-1] if " " in l else tid
                        if ctid != tid:
                            self.sig("error", f"busy: {tid} running; cannot cancel {ctid} from here")
                        else:
                            self.out(f"{YEL}cancelling {tid} …{RST}")
                            self.kernel.cancel(tid)
                            cancelled_by_us = True
                    elif l:
                        incoming = None
                        if l.startswith("{"):
                            try: incoming = json.loads(l).get("nonce")
                            except Exception: pass
                        bn = f" {incoming}" if incoming else ""
                        receipt_err(incoming or self.current_nonce, f"busy: {tid} running")
                        self.sig("error", f"busy: {tid} running{bn}")
            except queue.Empty:
                pass
            snap = self.kernel.wait(tid, timeout_sec=0.5)
            if snap.get("status") not in ("submitted", "running", "cancel_requested", None):
                break
        if snap.get("status") in ("submitted", "running", "cancel_requested", None):
            if cancelled_by_us:
                self.out("cancel_requested — not yet confirmed stopped; busy held until terminal")
            else:
                self.out(f"[zcodecli:wait_timeout] {rid} {tid} (task continues; busy held)")
            self.background_wait(tid, rid)
            return
        if cancelled_by_us and snap.get("status") == "cancelled":
            self.out("cancelled (confirmed stopped)")
        elif cancelled_by_us:
            self.out(f"already_terminal ({snap.get('status')}) — task finished during cancel")
        data = collect(snap)
        data["request_id"] = rid
        attach_summary_full(data, tid)
        self._release_if_never_ran(spec, data)
        if not self.root and tid: self.root = tid
        remember_session(data.get("native_session_id"))
        persist({**snap, "request_id": rid, "status": data["status"],
                 "ok": data["ok"], "verify_ok": data["verify_ok"],
                 "out_of_scope": data["out_of_scope"],
                 "changed_files": data["changed_files"],
                 "summary": data["summary"], "usage": data["usage"],
                 "summary_full": data.get("summary_full")})
        self.stop_tail()
        self.sig("result", rid, tid, self.current_nonce)
        mark = "✓" if data["ok"] else "✗"
        self.out(f"{_C_DIM}── {mark} {tid} {data['status']} · "
                 f"verify_ok={data['verify_ok']} ──{RST}")
        if data.get("summary"):
            self.out(f"{_C_ACC}┃ answer{_RST} {display_text(data['summary'], 600)}")
        if getattr(self, "_tail", None): self._tail.set()
        busy_none(self)
        report("idle")

def busy_none(chat):
    chat.busy = None

class _ChatChat(Chat):
    pass

def _plugin_version():
    try:
        mf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "herdr-plugin.toml")
        for line in open(mf):
            if line.startswith("version"):
                return line.split("=", 1)[1].strip().strip('"')
    except Exception:
        pass
    return "?"

def main(kernel=None):
    kernel = kernel or build_kernel()
    c = Chat(kernel)
    W = 78
    c.out(f"{CYA}╭{'─'*(W-2)}╮{RST}")
    c.out(f"{CYA}│{RST} {BOLD}zcode chat{RST} · one ZCode session, this pane {DIM}(GLM-5.3-Flash · plugin {_plugin_version()}){RST}")
    c.out(f"{CYA}│{RST} workspace {DIM}{c.ws}{RST}")
    c.out(f"{CYA}│{RST} mode {YEL}{MODE}{RST} · policy {YEL}{POLICY}{RST} · {DIM}/quit to end{RST}")
    c.out(f"{CYA}╰{'─'*(W-2)}╯{RST}")
    report("idle", force=True)
    c.sig("ready")
    def reader():
        for raw in sys.stdin: c.q.put(raw)
        c.q.put(None)
    threading.Thread(target=reader, daemon=True).start()
    while True:
        raw = c.q.get()
        if raw is None: break
        line = raw.strip()
        if not line: continue
        try:
            if line == "/quit": report("idle"); sys.exit(0)
            if line == "/status":
                c.out(f"session task: {c.root or '(none yet)'} · busy: {c.busy}"); continue
            if line == "/cancel" or line.startswith("/cancel "):
                tid = c.busy or (line.split()[-1] if " " in line else None)
                if not tid: c.out("nothing running"); continue
                if tid != c.busy and c.busy:
                    c.sig("error", f"busy: {c.busy} running; cannot cancel {tid} from here")
                    continue
                snap0 = c.kernel.snapshot(tid)
                if not snap0 or not snap0.get("task_id"):
                    c.out(f"task {tid} not known to this executor (stale/foreign)"); continue
                pre = c.kernel.snapshot(tid).get("status")
                if pre in ("succeeded", "failed", "cancelled", "killed"):
                    c.out(f"already_terminal ({pre})")
                else:
                    c.kernel.cancel(tid)
                    deadline = time.time() + 10
                    while time.time() < deadline:
                        st = c.kernel.snapshot(tid).get("status")
                        if st not in ("running", "submitted", "cancelling", "cancel_requested", None): break
                        time.sleep(0.3)
                    else:
                        c.kernel.kill(tid)
                    st = c.kernel.snapshot(tid).get("status")
                    if st == "cancelled":
                        c.out("cancelled (confirmed stopped)")
                    elif st in ("succeeded", "failed"):
                        c.out(f"already_terminal ({st})")
                    else:
                        c.out(f"cancel_requested (state {st}) — busy held until terminal")
                        if c.busy == tid:
                            c.background_wait(tid, getattr(c, "current_rid", None),
                                              nonce=getattr(c, "current_nonce", None))
                    if st in ("cancelled", "succeeded", "failed", "killed") and c.busy == tid:
                        c.busy = None
                    report("idle")
                continue
            if line == "/help": c.out(__doc__); continue
            if c.busy:
                incoming = None
                if line.startswith("{"):
                    try: incoming = json.loads(line).get("nonce")
                    except Exception: pass
                bn = f" {incoming}" if incoming else (f" {c.current_nonce}" if getattr(c, "current_nonce", None) else "")
                receipt_err(incoming or c.current_nonce, f"busy: {c.busy} running; /cancel to stop")
                c.sig("error", f"busy: {c.busy} running; /cancel to stop{bn}")
                continue
            c.run_turn(line)
        except SystemExit:
            raise
        except Exception as e:
            c.sig("error", sanitize(f"executor: {e}", 200))
            report("idle", force=True)
        c.sig("ready")

if __name__ == "__main__":
    main()
