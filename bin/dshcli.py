#!/usr/bin/env python3
"""dshcli — delegate tasks to the DeepSeek Harness (dsh) from herdr panes.

  dshcli open                    open the executor pane (tab placement)
  dshcli send <json|text>        send a task (JSON = strict spec; text = freeform)
  dshcli result [--machine]      wait for the latest task's terminal result
  dshcli read [--lines N]        read recent executor output
  dshcli list                    known tasks from the evidence store
  dshcli cancel <task_id>        cancel a running task
  dshcli chat                    start a chat session in this pane
"""
import argparse, json, os, re, secrets, subprocess, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import broker  # noqa: E402  (same disk protocol: receipts/ results/ owners/ requests/)

LABEL = "dsh-bridge"
HERDR = os.environ.get("HERDR_BIN_PATH", "herdr")

def herdr(args, timeout=60):
    p = subprocess.run([HERDR] + args, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr

def resolve_pane(explicit=None):
    if explicit:
        return explicit
    rc, out, err = herdr(["pane", "list", "--json"])
    m = re.findall(r'"pane_id"\s*:\s*"([^"]+)"[^{}]*?"label"\s*:\s*"%s"' % LABEL, out or "")
    if m:
        return m[-1]
    m = re.findall(r'"label"\s*:\s*"%s"[^{}]*?"pane_id"\s*:\s*"([^"]+)"' % LABEL, out or "")
    return m[-1] if m else None

def auto_open_executor(timeout_s=3.0):
    _, stdout, _ = herdr(["plugin", "pane", "open", "--plugin", "dsh",
                          "--entrypoint", "executor", "--placement", "tab"])
    try:
        pid = json.loads(stdout)["result"]["plugin_pane"]["pane"]["pane_id"]
        print(f"note: executor pane was missing — auto-opened {pid}", file=sys.stderr)
        return pid
    except Exception:
        pass
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        pid = resolve_pane()
        if pid:
            return pid
        time.sleep(0.25)
    return None

def need_pane(explicit=None):
    pid = resolve_pane(explicit)
    if not pid:
        pid = auto_open_executor()
    if not pid:
        print("executor pane not found and auto-open failed. run: dshcli open", file=sys.stderr)
        sys.exit(2)
    return pid

def latest_task_for_pane(pane):
    best = (0.0, None)
    try:
        for f in os.listdir(broker.OWNERS):
            if not f.endswith(".json"):
                continue
            try:
                rec = json.load(open(os.path.join(broker.OWNERS, f)))
            except Exception:
                continue
            if (pane is None or rec.get("pane_id") == pane) and rec.get("ts", 0) > best[0]:
                best = (rec.get("ts", 0), rec.get("task_id"))
    except OSError:
        pass
    return best[1]

def cmd_open(a):
    rc, out, err = herdr(["plugin", "pane", "open", "--plugin", "dsh",
                          "--entrypoint", "executor", "--placement", a.placement])
    print((out or err).strip()); return rc

def cmd_send(a):
    pid = need_pane(getattr(a, "pane", None))
    ws = os.path.realpath(a.workspace or os.getcwd())
    if not os.path.isdir(ws):
        print(f"workspace not a dir: {ws}", file=sys.stderr); return 2
    text = a.text.strip()
    nonce = secrets.token_hex(4)
    flags = any(getattr(a, k, None) for k in ("verify", "mode", "policy", "scope", "key", "timeout"))
    if text.startswith("{"):
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                obj.setdefault("workspace", ws)
                nonce = obj.get("nonce") or nonce
                obj["nonce"] = nonce
                text = json.dumps(obj, ensure_ascii=False)
        except json.JSONDecodeError:
            pass
    elif not a.raw and not text.startswith("/"):
        obj = {"goal": a.text, "workspace": ws, "nonce": nonce}
        if a.verify: obj["verify"] = a.verify
        if a.mode: obj["mode"] = a.mode
        if a.policy: obj["policy"] = a.policy
        if a.scope: obj["scope"] = [x.strip() for x in a.scope.split(",") if x.strip()]
        if a.key: obj["idempotency_key"] = a.key
        if a.timeout: obj["timeout"] = a.timeout
        text = json.dumps(obj, ensure_ascii=False)
    elif flags and not a.raw:
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                if a.verify: obj["verify"] = a.verify
                if a.mode: obj["mode"] = a.mode
                if a.policy: obj["policy"] = a.policy
                if a.scope: obj["scope"] = [x.strip() for x in a.scope.split(",") if x.strip()]
                if a.key: obj["idempotency_key"] = a.key
                if a.timeout: obj["timeout"] = a.timeout
                nonce = obj.get("nonce") or nonce
                obj["nonce"] = nonce
                text = json.dumps(obj, ensure_ascii=False)
        except json.JSONDecodeError:
            pass
    rc, stdout, stderr = herdr(["pane", "run", pid, text])
    if rc != 0 and "pane_not_found" in (stderr or ""):
        pid = need_pane(None)   # self-heal: targeted pane was closed
        rc, stdout, stderr = herdr(["pane", "run", pid, text])
    if rc != 0:
        print((stderr or stdout).strip(), file=sys.stderr); return rc
    if a.raw or text.startswith("/") or not text.startswith("{"):
        print(f"sent to pane {pid} (workspace: {ws})"); return 0
    deadline = time.time() + 10
    while time.time() < deadline:
        rec = broker.load_receipt(nonce)
        if rec:
            if rec.get("ok"):
                print("accepted:", rec.get("task_id", "-"), rec.get("status", "")); return 0
            print(rec.get("error", "rejected"), file=sys.stderr); return 5
        time.sleep(0.4)
    print("no receipt within 10s (task may still be queued)", file=sys.stderr)
    return 4

_TERMINAL = {"succeeded", "failed", "cancelled", "killed"}

def cmd_result(a):
    pid = resolve_pane(getattr(a, "pane", None))
    deadline = time.time() + a.timeout / 1000
    tid = latest_task_for_pane(pid) if pid or True else None
    while True:
        if tid and broker.TASK_RE.match(tid) and os.path.exists(broker.result_path(tid)):
            try:
                st = json.load(open(broker.result_path(tid))).get("status") or ""
            except Exception:
                st = ""
            if st in _TERMINAL:
                break
        if time.time() > deadline:
            print("no terminal result within timeout", file=sys.stderr); return 1
        time.sleep(0.5)
    rf = broker.result_path(tid)
    full = json.load(open(rf))
    res = full.get("result") or {}
    data = {"task_id": tid, "status": full.get("status"),
            "summary": (res.get("worker_summary") or "")[:400],
            "summary_full": full.get("summary_full"),
            "changed_files": full.get("changed_files") or [],
            "verify": res.get("verify") or [],
            "verify_ok": full.get("verify_ok"),
            "error": full.get("error"),
            "usage": res.get("usage")}
    if a.machine:
        print(json.dumps(data, ensure_ascii=False)); return 0
    print(f"● {data['status']}  {tid}  verify_ok={data['verify_ok']}")
    for v in data["verify"]:
        print(("  ✓ " if v.get("ok") else "  ✗ ") + str(v.get("cmd", "")))
    if data["summary"]:
        print(data["summary"][:600])
    if data.get("error"):
        print("error:", str(data["error"])[:300], file=sys.stderr)
    return 0

def cmd_read(a):
    pid = need_pane(getattr(a, "pane", None))
    rc, out, err = herdr(["pane", "read", pid, "--source", "visible", "--lines", str(a.lines)])
    print(out or err); return rc

def cmd_list(a):
    tasks = []
    for f in sorted(os.listdir(broker.RESULTS)):
        if not f.endswith(".json"):
            continue
        try:
            d = json.load(open(os.path.join(broker.RESULTS, f)))
            tasks.append((d.get("created_at", ""), d.get("task_id", f[:-5]),
                          d.get("status", "?"), (d.get("summary") or "")[:60]))
        except Exception:
            continue
    tasks.sort(reverse=True)
    for _, tid, st, sm in tasks[:20]:
        print(f"● {st:<10} {tid}  {sm}")
    print(f"{len(tasks)} task(s)", file=sys.stderr)
    return 0

def cmd_open_session(a):
    """Best-effort: open the dsh session recorded around a task's completion.

    DshKernel does not capture dsh session ids, so this resolves by workspace
    bucket + file mtime closest to the task's completion time."""
    tid = a.task_id
    if not broker.TASK_RE.match(tid or ""):
        print("usage: dshcli open-session <task_id>", file=sys.stderr); return 2
    rf = broker.result_path(tid)
    if not os.path.exists(rf):
        print(f"no evidence file for {tid}", file=sys.stderr); return 1
    full = json.load(open(rf))
    ws = full.get("workspace") or ""
    bucket = os.path.expanduser(
        "~/.dsh/sessions/" + re.sub(r"[^A-Za-z0-9]", "-", ws))
    want = 0.0
    try:
        want = os.path.getmtime(rf)
    except OSError:
        pass
    best, best_dt = None, 1e18
    if os.path.isdir(bucket):
        for f in os.listdir(bucket):
            fp = os.path.join(bucket, f)
            if not f.startswith("session-"):
                continue
            try:
                dt = abs(os.path.getmtime(fp) - want)
            except OSError:
                continue
            if dt < best_dt:
                best, best_dt = (f, fp), dt
    if not best:
        print(f"no dsh session found under {bucket}", file=sys.stderr); return 1
    sess = best[0].removeprefix("session-").removesuffix(".json")
    print(f"note: matched {best[0]} (Δ{int(best_dt)}s)", file=sys.stderr)
    cmd = (os.environ.get("DSH_BIN") or "dsh").split() + \
          ["--profile", "tui", "--resume", sess]
    if a.print:
        print(" ".join(cmd)); return 0
    print(f"opening {sess} …", file=sys.stderr)
    os.execvp(cmd[0], cmd)


def cmd_cancel(a):
    tid = a.task_id
    if not tid or not broker.TASK_RE.match(tid):
        print("usage: dshcli cancel <task_id>", file=sys.stderr); return 2
    pid = broker.owner_of(tid) or need_pane(getattr(a, "pane", None))
    rc, out, err = herdr(["pane", "run", pid, f"/cancel {tid}"])
    print((out or err).strip()[:200]); return rc

def cmd_chat(a):
    if a.workspace: os.environ["QAB_WORKSPACE"] = a.workspace
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "executor_chat.py")
    os.execv(sys.executable, [sys.executable, script])

p = argparse.ArgumentParser(prog="dshcli")
p.add_argument("--pane", default=None)
sub = p.add_subparsers(dest="cmd")
s = sub.add_parser("open"); s.add_argument("--placement", default="tab"); s.set_defaults(fn=cmd_open)
s = sub.add_parser("send"); s.add_argument("text"); s.add_argument("--workspace", default=None)
s.add_argument("--raw", action="store_true")
s.add_argument("--verify", default=None, help="verify command (required for edit/yolo)")
s.add_argument("--mode", default=None, choices=["plan", "build", "edit", "yolo"])
s.add_argument("--policy", default=None, choices=["allow", "deny"])
s.add_argument("--scope", default=None, help="comma-separated relative paths")
s.add_argument("--key", default=None, help="idempotency_key")
s.add_argument("--timeout", type=int, default=None)
s.set_defaults(fn=cmd_send)
s = sub.add_parser("result"); s.add_argument("--machine", action="store_true")
s.add_argument("--timeout", type=int, default=300000); s.set_defaults(fn=cmd_result)
s = sub.add_parser("read"); s.add_argument("--lines", type=int, default=40); s.set_defaults(fn=cmd_read)
s = sub.add_parser("list"); s.set_defaults(fn=cmd_list)
s = sub.add_parser("cancel"); s.add_argument("task_id"); s.set_defaults(fn=cmd_cancel)
s = sub.add_parser("open-session"); s.add_argument("task_id"); s.add_argument("--print", action="store_true"); s.set_defaults(fn=cmd_open_session)
s = sub.add_parser("chat"); s.add_argument("--workspace", default=None); s.set_defaults(fn=cmd_chat)
a = p.parse_args()
if not getattr(a, "cmd", None):
    a.cmd = "chat"; a.workspace = None; a.fn = cmd_chat
sys.exit(a.fn(a) or 0)
