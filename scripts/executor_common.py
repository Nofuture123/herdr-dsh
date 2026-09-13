#!/usr/bin/env python3
"""Shared plumbing for zcode-bridge executors: herdr agent-state reporting,
in-process NAR kernel access, and the trusted result pipeline."""
import importlib.util, json, os, socket, sys, threading, time

def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

_here = os.path.dirname(os.path.abspath(__file__))
broker = _load("broker", os.path.join(_here, "broker.py"))
broker.init_dirs()

BASE = broker.BASE
NAR_VENV = os.path.join(BASE, "venv")
RESULTS = broker.RESULTS

def build_kernel():
    """DshKernel: one `dsh --profile headless <task>` process per task."""
    from dsh_kernel import DshKernel
    return DshKernel()

# ---------- herdr agent-state (socket protocol, best-effort) ----------
H_ENV = os.environ.get("HERDR_ENV")
H_SOCK = os.environ.get("HERDR_SOCKET_PATH")
H_PANE = os.environ.get("HERDR_PANE_ID")
H_SRC = "herdr:dsh"
_seq = [int(time.time() * 1000)]
_last_state = [None]
LAST_SESSION = [None]

def _next():
    _seq[0] += 1
    return _seq[0]

def _send(req, t=0.5):
    if H_ENV != "1" or not H_SOCK or not H_PANE:
        return
    for timeout in (t, 1.5):
        try:
            sk = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sk.settimeout(timeout)
            sk.connect(H_SOCK)
            sk.sendall((json.dumps(req) + "\n").encode())
            sk.recv(512)
            sk.close()
            return
        except OSError:
            continue

def report(state, message=None, force=False):
    if not force and state == _last_state[0]:
        return
    _last_state[0] = state
    p = {"pane_id": H_PANE, "source": H_SRC, "agent": "dsh",
         "state": state, "message": message, "seq": _next()}
    if LAST_SESSION[0]:
        p["agent_session_id"] = LAST_SESSION[0]
    _send({"id": f"{H_SRC}:{time.time()}:{_next()}",
           "method": "pane.report_agent", "params": p})

def report_session(sid):
    if not sid:
        return
    _send({"id": f"{H_SRC}:session:{time.time()}:{_next()}",
           "method": "pane.report_agent_session",
           "params": {"pane_id": H_PANE, "source": H_SRC, "agent": "dsh",
                      "seq": _next(), "agent_session_id": sid}})

def remember_session(native_session_id):
    if native_session_id:
        LAST_SESSION[0] = native_session_id
        report_session(native_session_id)

# ---------- output hygiene ----------
import re as _re
_CTRL = _re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

def sanitize(text, limit=600):
    if text is None:
        return ""
    text = _CTRL.sub(" ", str(text)).replace("\n", " ")
    return text[:limit]
# protocol marker tints (literal text preserved — pane read strips ANSI,
# so client parsing by the plain [zcodecli:...] prefix keeps working)
_SIGC = {"done": "\033[1;38;2;159;245;200m", "accepted": "\033[38;2;159;245;200m",
         "error": "\033[38;2;255;123;147m", "result": "\033[36m",
         "ready": "\033[38;2;101;125;98m",
         "summary": "\033[2;38;2;101;125;98m",
         "wait_timeout": "\033[33m"}
if os.environ.get("NO_COLOR") or os.environ.get("QAB_EXEC_PLAIN"):
    _SIGC = {}

# Pane markers are for HUMANS; machines read receipts/ + results/. By default
# only state changes worth a human's eye are shown; QAB_EXEC_MARKERS=1 shows all.
_HIDDEN_MARKERS = {"ready", "accepted", "summary", "wait_timeout",
                   "result", "done"}

def marker_line(kind, body=""):
    if kind in _HIDDEN_MARKERS and os.environ.get("QAB_EXEC_MARKERS") != "1":
        return None
    c = _SIGC.get(kind, "")
    if kind == "summary":   # machine receipt for agents: fully dimmed, short
        return f"{c}[zcodecli:summary]{_RST}{c} {body}{_RST}" if c else \
               f"[zcodecli:summary] {body}"
    return f"{c}[zcodecli:{kind}]{_RST} {body}" if c else f"[zcodecli:{kind}] {body}"

def emit(out, kind, payload):
    c = _SIGC.get(kind, "")
    out(f"{c}[zcodecli:{kind}]{_RST} " + json.dumps(payload, ensure_ascii=True))

def receipt_ok(nonce, **fields):
    """Durable accepted-ack for the send client (survives pane wrapping)."""
    if nonce:
        try:
            broker.save_receipt(nonce, {"ok": True, **fields})
        except Exception:
            pass

def receipt_err(nonce, error, **fields):
    """Durable rejection-ack for the send client (survives pane wrapping)."""
    if nonce:
        try:
            broker.save_receipt(nonce, {"ok": False, "error": str(error)[:300], **fields})
        except Exception:
            pass

# NOTE: acceptance/verdict logic deliberately lives in the MASTER (shared skill),
# not here. The bridge reports facts only: status / verify_ok / out_of_scope.

def collect(d):
    """Flatten a NAR task snapshot into the trusted result dict."""
    res = d.get("result") or {}
    diff = res.get("diff") or {}
    verifies = res.get("verify") or []
    return {
        "task_id": d.get("task_id"), "status": d.get("status"), "ok": bool(d.get("ok")),
        "summary": (res.get("worker_summary") or "")[:600].replace("\n", " "),
        "changed_files": diff.get("changed_files") or [],
        "out_of_scope": diff.get("out_of_scope") or [],
        "verify_ok": all(v.get("ok") for v in verifies) if verifies else None,
        "verify": verifies, "native_session_id": d.get("native_session_id"),
        "usage": res.get("usage"), "error": d.get("error"),
        "duration_sec": res.get("duration_sec"),
    }

def attach_summary_full(data, task_id):
    """Recover the FULL last assistant message (NAR caps worker_summary, which
    can cut verdict lines); stored only when it beats the capped summary."""
    try:
        full = native_final_text(task_id)
    except Exception:
        full = None
    full = (full or "").strip()
    if full and full != (data.get("summary") or "").strip():
        data["summary_full"] = full
    return data

def persist(d):
    tid = d.get("task_id")
    if tid and broker.TASK_RE.match(tid):
        with open(broker.result_path(tid), "w") as f:
            json.dump(d, f, indent=1)
        os.chmod(broker.result_path(tid), 0o600)

# ---------- live native output streaming ----------
# Palette: pi's "enchanted-forest" theme (awesome-pi-themes), truecolor.
_RST = "\033[0m"
_C_MUTED = "\033[38;2;157;187;155m"    # thinkingText #9dbb9b
_C_TOOL = "\033[38;2;183;245;176m"     # toolTitle  #b7f5b0
_C_OK = "\033[38;2;159;245;200m"       # success    #9ff5c8
_C_ERR = "\033[38;2;255;123;147m"      # error      #ff7b93
_C_DIM = "\033[38;2;101;125;98m"       # dim        #657d62
_C_DEEP = "\033[38;2;47;158;68m"       # accentDeep #2f9e44 (code-block border)
_C_HEAD = "\033[38;2;183;245;176m"     # mdHeading  #b7f5b0
_C_ITAL = "\033[2;3;38;2;157;187;155m"  # dim+italic muted (thinking)
_C_ACC = "\033[38;2;126;231;135m"      # accent     #7ee787 (inline code)
_C_OKP = "\033[38;2;159;245;200m"      # ok-ish marker tint
_C_ERRP = "\033[38;2;255;123;147m"     # error marker tint
WARN = "\033[38;2;214;198;95m"         # warning    #d6c65f
_MD_BOLD = _re.compile(r"\*\*(.+?)\*\*")
_MD_CODE = _re.compile(r"`([^`]+)`")

def _inline_md(line):
    """Render inline markdown (bold, inline code, list bullets) with theme colors."""
    if "**" in line:
        line = _MD_BOLD.sub(lambda m: f"\033[1m{m.group(1)}{_RST}", line)
    if "`" in line:
        line = _MD_CODE.sub(lambda m: f"{_C_ACC}{m.group(1)}{_RST}", line)
    if line.lstrip().startswith(("- ", "* ")):
        line = line.replace("-", "•", 1) if line.lstrip().startswith("- ") else \
               line.replace("*", "•", 1)
    return line

def display_text(line, limit=400):
    """Sanitized single-line display text: code fences stripped, inline
    markdown (bold/code) styled. Used for thinking, summaries, goal echoes."""
    line = sanitize(line, limit)
    line = _re.sub(r"```[a-zA-Z0-9_-]*", "", line)
    return _inline_md(line)
if os.environ.get("NO_COLOR") or os.environ.get("QAB_EXEC_PLAIN"):
    _C_MUTED = _C_TOOL = _C_OK = _C_ERR = _C_DIM = WARN = ""

def native_log_path(task_id):
    base = os.path.expanduser(os.environ.get("DSH_LOGS_DIR", "~/.local/share/dsh-bridge/logs"))
    return os.path.join(base, task_id, "stream.log")

def stream_native_output(task_id, out, stop):
    """Tail the dsh headless reasoning stream (plain lines) into the pane.
    QAB_EXEC_QUIET=1 mutes everything."""
    if not task_id or os.environ.get("QAB_EXEC_QUIET", "") == "1" \
            or not broker.TASK_RE.match(task_id):
        return
    path = native_log_path(task_id)
    pos = 0
    t0 = time.time()
    last_emit = [t0]

    def heartbeat():
        if os.environ.get("QAB_EXEC_HEARTBEAT") != "1":
            return
        now = time.time()
        if now - last_emit[0] >= 20:
            last_emit[0] = now
            out(f"{_C_DIM}── {int(now - t0)}s ──{_RST}")

    def emit_line(sx):
        last_emit[0] = time.time()
        out(f"{_C_ITAL}· {sanitize(sx, 400)}{_RST}")

    try:
        idle_polls = 0
        while not stop.is_set():
            if not os.path.exists(path):
                stop.wait(0.4)
                heartbeat()
                continue
            with open(path, "r", errors="replace") as f:
                f.seek(pos)
                chunk = f.read()
                pos = f.tell()
            if not chunk:
                idle_polls += 1
                stop.wait(0.25 if idle_polls < 8 else 1.0)
                heartbeat()
                continue
            idle_polls = 0
            for line in chunk.splitlines():
                if line.strip():
                    emit_line(line)
    finally:
        pass
