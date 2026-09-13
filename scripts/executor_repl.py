#!/usr/bin/env python3
"""dsh-bridge executor — reception pane (serial task queue). PURE transport.

Protocol (one line per stdin):
  <text>                    -> freeform task (owner-authorized full access)
  {json}                    -> strict spec, fail-closed; edit/yolo requires verify
  /continue <req_id> <text> -> rework turn; inherits parent spec, sets session_ref
  /cancel                   -> cancel the running task (tri-state, honest)
  /status | /list | /inspect <id> | /help | /quit

The bridge is transport + bookkeeping ONLY: it reports facts (status / verify_ok /
out_of_scope) and never issues acceptance verdicts or rework limits — those belong
to the master agent (see skills/zcode-bridge/SKILL.md).
Markers are signals carrying identifiers only; evidence is results/<task_id>.json.
"""
import json, os, queue, subprocess, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from executor_common import (broker, build_kernel, report, remember_session,
                             emit, collect, persist, sanitize, stream_native_output,
                             receipt_ok, receipt_err, attach_summary_full, WARN, marker_line,
                             display_text, _C_DIM)

def _c(n): return f"\033[{n}m"
DIM, BOLD, GREEN, RED, YEL, CYA, RST = _c(2), _c(1), _c(32), _c(31), _c(33), _c(36), _c(0)
DEFAULT_WS = os.path.realpath(os.environ.get("QAB_DEFAULT_WORKSPACE",
                                             os.path.expanduser("~/projects/dsh-demo")))
DEFAULT_MODE = os.environ.get("QAB_DEFAULT_MODE", "yolo")
DEFAULT_POLICY = os.environ.get("QAB_DEFAULT_POLICY", "allow")
REQUIRE_VERIFY = os.environ.get("QAB_REQUIRE_VERIFY", "1") == "1"
GUI_TOOLS = ["computer-use", "computer_use", "mcp__computer-use", "cua", "screenshot", "gui"]

class Reception:
    def __init__(self, kernel, out=print, pane_id=None):
        self.kernel = kernel
        self.out = out
        self.pane_id = pane_id or os.environ.get("HERDR_PANE_ID")
        self.busy = None            # task_id
        self.active_request = None  # request_id
        self.current_nonce = None
        self.last = None            # last request record (spec + task_id)

    # -- markers: identifiers only, no business verdicts --
    def sig_accepted(self, rid, tid, state, nonce=None):
        line = marker_line("accepted", f"{rid} {tid} {state}" + (f" {nonce}" if nonce else ""))
        if line:
            self.out(line)
        receipt_ok(nonce, request_id=rid, task_id=tid, status=state)

    def sig_result(self, rid, tid):
        line = marker_line("result", f"{rid} {tid}")
        if line:
            self.out(line)

    def sig_error(self, msg, nonce=None):
        nonce = nonce or self.current_nonce
        receipt_err(nonce, msg)
        emit(self.out, "error", {"ok": False, "error": sanitize(msg, 200),
                                 **({"nonce": nonce} if nonce else {})})

    # -- lifecycle --
    def submit(self, spec, rid):
        if spec.get("idempotency_key"):
            broker.idempotency_state(spec["idempotency_key"], broker.fingerprint(spec),
                                     "submitting", request_id=rid)
        kwargs = dict(scope_files=spec["scope"] or None, forbid=GUI_TOOLS,
                      verify=spec["verify"] or None, mode=spec["mode"],
                      permission_policy=spec["policy"], timeout_sec=float(spec["timeout"]),
                      idempotency_key=spec.get("idempotency_key"))
        if spec.get("session_ref"):
            kwargs["session_ref"] = spec["session_ref"]
        try:
            snap = self.kernel.submit("zcode", spec["goal"], spec["workspace"], **kwargs)
        except Exception:
            # never reached the queue: release the key so a retry is legal
            if spec.get("idempotency_key"):
                broker.idempotency_release(spec["idempotency_key"])
            raise
        tid = snap.get("task_id")
        broker.attach_task(rid, tid)
        broker.set_owner(tid, self.pane_id)
        if spec.get("idempotency_key"):
            broker.idempotency_state(spec["idempotency_key"], broker.fingerprint(spec),
                                     "attached", task_id=tid, request_id=rid)
        self.busy, self.active_request = tid, rid
        self.current_nonce = spec.get("nonce")
        self.sig_accepted(rid, tid, snap.get("status"), spec.get("nonce"))
        if not os.path.exists(os.path.join(spec["workspace"], ".git")):
            self.out(f"{WARN}⚠ workspace is not a git repo — changed_files evidence "
                     f"will be unavailable{RST}")
        if getattr(self, "_tail", None):
            self._tail.set()
        self._tail = threading.Event()
        self._tail_thread = threading.Thread(
            target=stream_native_output, args=(tid, self.out, self._tail), daemon=True)
        self._tail_thread.start()
        return tid

    def poll(self, tid, deadline):
        """Wait to terminal; honors /cancel arriving on the queue mid-run."""
        t0 = time.time()
        while time.time() < deadline:
            try:
                while True:
                    raw = self.q.get_nowait()
                    l = (raw or "").strip()
                    if l == "/cancel" or l.startswith("/cancel "):
                        ctid = l.split()[-1] if " " in l else tid
                        if ctid == tid or not self.busy:
                            self.do_cancel(ctid)   # tri-state honest outcome; keep polling
                        else:
                            self.sig_error(f"busy: {tid} running; cannot cancel {ctid} from here")
                    elif l:
                        incoming = None
                        if l.startswith("{"):
                            try: incoming = json.loads(l).get("nonce")
                            except Exception: pass
                        self.sig_error(f"busy: {tid} running", nonce=incoming)
            except queue.Empty:
                pass
            snap = self.kernel.wait(tid, timeout_sec=0.5)
            if snap.get("status") not in ("submitted", "running", "cancel_requested", None):
                return snap, time.time() - t0
        return self.kernel.snapshot(tid), time.time() - t0

    def finish(self, snap, spec, rid, dur):
        data = collect(snap)
        data.update({"request_id": rid, "_dur": round(dur, 1),
                     "result_file": broker.result_path(data["task_id"])})
        attach_summary_full(data, data["task_id"])
        # a task that died on the workspace lock never executed: release the
        # idempotency key so the caller's retry with the same key is legal
        if (data.get("status") == "failed" and spec.get("idempotency_key")
                and "workspace_busy" in str(data.get("error") or "")):
            broker.idempotency_release(spec["idempotency_key"])
        # one facts file per task: raw snapshot + flattened facts (master reads this)
        persist({**snap, "request_id": rid, "status": data["status"],
                 "ok": data["ok"], "verify_ok": data["verify_ok"],
                 "out_of_scope": data["out_of_scope"],
                 "changed_files": data["changed_files"],
                 "summary": data["summary"], "usage": data["usage"]})
        if getattr(self, "_tail", None):
            self._tail.set()
        if getattr(self, "_tail_thread", None):
            self._tail_thread.join(timeout=1.0)
        self.sig_result(rid, data["task_id"])
        mark = "✓" if data["ok"] else "✗"
        self.out(f"{_C_DIM}── {mark} {data['task_id']} {data['status']} · "
                 f"verify_ok={data['verify_ok']} ({data.get('_dur', '?')}s) ──{RST}")
        if data.get("summary"):
            self.out(f"{_C_ACC}┃ answer{_RST} {display_text(data['summary'], 600)}")
        if getattr(self, "_tail", None):
            self._tail.set()
        remember_session(data.get("native_session_id"))
        self.busy = self.active_request = None
        report("idle")
        return data

    def background_wait(self, tid, spec, rid):
        def waiter():
            snap, _ = self.poll(tid, time.time() + 24 * 3600)
            if snap.get("status") in ("submitted", "running", "cancel_requested", None):
                self.sig("wait_cap", rid, tid, snap.get("status"))   # honest: still running, busy held
                return   # do NOT fabricate a verdict; busy stays, /cancel still works
            self.finish(snap, spec, rid, 0.0)
        threading.Thread(target=waiter, daemon=True).start()

    def run_task(self, spec, rid):
        self.active_spec, self.active_request = spec, rid
        self.out(f"{DIM}── {time.strftime('%H:%M:%S')} ▶ {display_text(spec['goal'], 60)}{RST}")
        report("working", sanitize(spec["goal"], 80))
        tid = self.submit(spec, rid)
        snap, dur = self.poll(tid, time.time() + spec["timeout"])
        if snap.get("status") in ("submitted", "running", None):
            self.out(f"[zcodecli:wait_timeout] {rid} {tid} (task continues; busy held)")
            return self.background_wait(tid, spec, rid)
        return self.finish(snap, spec, rid, dur)

    # -- entry points --
    def start(self, line):
        rid = broker.new_request_id()
        try:
            if line.startswith("{"):
                spec = broker.normalize_spec(json.loads(line), DEFAULT_WS, DEFAULT_MODE,
                                             DEFAULT_POLICY, require_verify=REQUIRE_VERIFY)
            else:
                spec = broker.normalize_spec({"goal": line, "workspace": DEFAULT_WS},
                                             DEFAULT_WS, DEFAULT_MODE, DEFAULT_POLICY)
        except (broker.SpecError, json.JSONDecodeError) as e:
            raw_nonce = None
            try: raw_nonce = json.loads(line).get("nonce")
            except Exception: pass
            return self.sig_error(f"rejected (fail-closed): {e}", nonce=raw_nonce)
        fp = broker.fingerprint(spec)
        if spec.get("idempotency_key"):
            st, rec = broker.idempotency_claim(spec["idempotency_key"], fp)
            if st == "conflict":
                raw_nonce = None
                try: raw_nonce = json.loads(line).get("nonce")
                except Exception: pass
                return self.sig_error("idempotency_conflict: same key, different spec", nonce=raw_nonce)
            if st == "indeterminate":
                raw_nonce = None
                try: raw_nonce = json.loads(line).get("nonce")
                except Exception: pass
                return self.sig_error("idempotency_indeterminate: previous attempt crashed "
                                      "mid-submit; inspect manually, do not blind-rerun", nonce=raw_nonce)
            if st == "duplicate":
                self.out(f"{YEL}↺ duplicate submit{RST}")
                raw_nonce = None
                try: raw_nonce = json.loads(line).get("nonce")
                except Exception: pass
                return self.sig_accepted(rec["request_id"], rec.get("task_id") or "-", "duplicate",
                                         nonce=raw_nonce)
            rid = rec["request_id"]
        broker.save_request(rid, spec)
        self.last = spec
        self.run_task(spec, rid)

    def do_continue(self, rest):
        rid_prefix, _, text = rest.partition(" ")
        if not rid_prefix.startswith("r-") or not text.strip():
            return self.sig_error("usage: /continue <parent_request_id> <text> "
                                  "(acceptance and rework limits are the master's job)")
        try:
            parent = broker.load_request(rid_prefix)
        except Exception:
            return self.sig_error(f"unknown parent request {rid_prefix}")
        spec = dict(parent["spec"]); spec["goal"] = text
        spec.pop("idempotency_key", None)
        if parent.get("task_id"):
            spec["session_ref"] = parent["task_id"]     # real native-session continuation
        rid = broker.new_request_id()
        broker.save_request(rid, spec, {"rework_of": rid_prefix})
        self.run_task(spec, rid)

    def do_cancel(self, tid=None):
        tid = tid or self.busy
        if not tid:
            self.out("nothing running"); return None
        snap = self.kernel.snapshot(tid)
        if not snap or not snap.get("task_id"):
            self.out(f"task {tid} is not known to this executor "
                     f"(stale store entry or foreign pane); use nar kill if force-needed")
            return "unknown_to_executor"
        owner = broker.owner_of(tid)
        if owner != os.environ.get("HERDR_PANE_ID"):
            self.out(f"task {tid} owner mismatch (owner={owner or 'none'}); refusing to cancel/kill here")
            return "foreign_owner"
        pre = self.kernel.snapshot(tid).get("status")
        if pre in ("succeeded", "failed", "cancelled", "killed"):
            self.out(f"already_terminal ({pre}) — not claiming a cancel")
            if tid != self.busy: self.busy = None if self.busy == tid else self.busy
            return "already_terminal"
        if tid != self.busy and self.busy:
            self.sig_error(f"task {tid} is not the running task ({self.busy}); cannot cancel from here")
            return "not_owner"
        busy_was_mine = (tid == self.busy)
        self.out(f"{YEL}cancelling {tid} …{RST}")
        self.kernel.cancel(tid)
        deadline = time.time() + 10
        while time.time() < deadline:
            st = self.kernel.snapshot(tid).get("status")
            if st not in ("running", "submitted", "cancelling", "cancel_requested", None):
                break
            time.sleep(0.3)
        else:
            self.kernel.kill(tid)
            st = self.kernel.snapshot(tid).get("status")
        if st == "cancelled":
            self.out("✓ cancelled (confirmed stopped)")
        elif st in ("succeeded", "failed"):
            self.out(f"already_terminal ({st}) — task finished during cancel")
        elif st in ("running", "cancel_requested"):
            self.out("cancel_requested — not yet confirmed stopped; busy held until terminal")
            return "cancel_requested"   # busy stays; background waiter finalizes
        else:
            self.out(f"final state {st}")
        if busy_was_mine:
            self.busy = self.active_request = None
            report("idle")
        return st

    def handle(self, line):
        line = line.strip()
        if not line: return
        if line.startswith("/"):
            cmd, _, rest = line.partition(" ")
            if cmd == "/help": self.out(__doc__)
            elif cmd == "/quit": report("idle"); sys.exit(0)
            elif cmd == "/cancel":
                if self.busy: self.do_cancel()
                else: self.out("nothing running")
            elif cmd == "/list":
                for t in self.kernel.list_tasks():
                    self.out(f"● {t['status']:<10} {t['task_id']}  {t['goal'][:60]}")
            elif cmd == "/status":
                import executor_common
                self.out(json.dumps({"busy": self.busy, "last_session": executor_common.LAST_SESSION[0]}))
            elif cmd == "/inspect":
                self.out(sanitize(str(self.kernel.inspect(rest.strip(), "summary")), 1500))
            elif cmd == "/continue": self.do_continue(rest)
            else: self.sig_error(f"unknown command {cmd}")
            return
        if self.busy:
            return self.sig_error(f"busy: {self.busy} running; serial queue — /cancel or wait")
        self.start(line)

    def process(self, line):
        """Main-loop body: honors /cancel mid-run, otherwise dispatches."""
        line = line.strip()
        if self.busy and not (line == "/cancel" or line.startswith("/cancel ")):
            if line:
                incoming = None
                if line.startswith("{"):
                    try: incoming = json.loads(line).get("nonce")
                    except Exception: pass
                self.sig_error(f"busy: {self.busy} running", nonce=incoming)
            return
        if line == "/cancel" or line.startswith("/cancel "):
            if self.busy:
                self.do_cancel()
            elif line.startswith("/cancel "):
                self.do_cancel(line.split()[-1])
            return
        try:
            self.handle(line)
        except SystemExit:
            raise
        except Exception as e:
            self.sig_error(f"executor: {e}")
            report("idle", force=True)


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
    r = Reception(kernel)
    W = 78
    r.out(f"{CYA}╭{'─'*(W-2)}╮{RST}")
    r.out(f"{CYA}│{RST} {BOLD}dsh-bridge executor{RST} · reception queue {DIM}(pure transport · plugin {_plugin_version()}){RST}")
    r.out(f"{CYA}│{RST} workspace {DIM}{DEFAULT_WS}{RST}  mode {YEL}{DEFAULT_MODE}{RST} · policy {YEL}{DEFAULT_POLICY}{RST}")
    r.out(f"{CYA}│{RST} {DIM}facts reported; acceptance & rework discipline = master's job{RST}")
    r.out(f"{CYA}╰{'─'*(W-2)}╯{RST}")
    report("idle", force=True)
    _ready = marker_line("ready")
    if _ready:
        r.out(_ready)
    r.q = queue.Queue()
    def reader():
        for raw in sys.stdin: r.q.put(raw)
        r.q.put(None)
    threading.Thread(target=reader, daemon=True).start()
    while True:
        raw = r.q.get()
        if raw is None: break
        r.process(raw)
        line = marker_line("ready")
        if line:
            r.out(line)

if __name__ == "__main__":
    main()
