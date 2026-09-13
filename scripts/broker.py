#!/usr/bin/env python3
"""Request broker for zcode-bridge executors.

Owns the trust boundary: request lifecycle, atomic disk records, cross-process
idempotency, and the ONLY sanctioned way to turn a task_id into a result path.
Markers on a terminal are signals; files here are evidence.
"""
import fcntl, hashlib, hmac, json, os, re, secrets, tempfile, time

BASE = os.path.expanduser("~/.local/share/dsh-bridge")
REQUESTS = os.path.join(BASE, "requests")
LEDGERS = os.path.join(BASE, "ledgers")
RESULTS = os.path.join(BASE, "results")
OWNERS = os.path.join(BASE, "owners")
RECEIPTS = os.path.join(BASE, "receipts")
TASK_RE = re.compile(r"^t-[0-9a-f]+$")
REQ_RE = re.compile(r"^r-[0-9a-f]{16}$")
NONCE_RE = re.compile(r"^[0-9a-zA-Z_-]{1,64}$")
ALLOWED_KEYS = {"goal", "workspace", "mode", "scope", "verify", "policy",
                "timeout", "idempotency_key", "session_ref", "forbid", "nonce"}
MODES = {"plan", "build", "edit", "yolo"}
POLICIES = {"allow", "deny"}


def _secure(path, dir=False):
    os.makedirs(path, 0o700 if dir else os.path.dirname(path), exist_ok=True) if dir else None
    if dir:
        os.chmod(path, 0o700)


def init_dirs():
    for d in (REQUESTS, LEDGERS, RESULTS, OWNERS, RECEIPTS):
        os.makedirs(d, exist_ok=True)
        os.chmod(d, 0o700)
        for f in os.listdir(d):
            fp = os.path.join(d, f)
            if os.path.isfile(fp):
                os.chmod(fp, 0o600)


class SpecError(ValueError):
    pass


def normalize_spec(obj, default_ws, default_mode="yolo", default_policy="allow",
                   require_verify=None):
    """Fail-closed spec normalization. Returns spec dict; raises SpecError."""
    if not isinstance(obj, dict):
        raise SpecError("task must be a JSON object")
    unknown = set(obj) - ALLOWED_KEYS
    if unknown:
        raise SpecError(f"unknown field(s): {sorted(unknown)}")
    goal = obj.get("goal")
    if not isinstance(goal, str) or not goal.strip():
        raise SpecError("goal is required and must be a non-empty string")
    ws = obj.get("workspace", default_ws)
    if not isinstance(ws, str) or not os.path.isdir(ws):
        raise SpecError(f"workspace is not a directory: {ws!r}")
    ws = os.path.realpath(ws)   # one canonical form everywhere: locks, sessions, JSON
    mode = obj.get("mode", default_mode)
    if mode not in MODES:
        raise SpecError(f"mode must be one of {sorted(MODES)}")
    policy = obj.get("policy", default_policy)
    if policy not in POLICIES:
        raise SpecError(f"policy must be one of {sorted(POLICIES)}")
    scope = obj.get("scope", [])
    if not isinstance(scope, list) or any(not isinstance(x, str) for x in scope):
        raise SpecError("scope must be a list of relative paths")
    verify = obj.get("verify", [])
    if isinstance(verify, str):
        verify = [verify]
    if not isinstance(verify, list) or any(not isinstance(v, str) or not v.strip() for v in verify):
        raise SpecError("verify must be a non-empty command string or list")
    if require_verify and mode in ("edit", "yolo") and not verify:
        raise SpecError(f"verify is required for {mode} tasks (fail closed)")
    timeout = obj.get("timeout", 600)
    if not isinstance(timeout, int) or not (10 <= timeout <= 7200):
        raise SpecError("timeout must be integer seconds in [10, 7200]")
    ikey = obj.get("idempotency_key")
    if ikey is not None and (not isinstance(ikey, str) or not ikey.strip()):
        raise SpecError("idempotency_key must be a non-empty string")
    sref = obj.get("session_ref")
    if sref is not None and not isinstance(sref, str):
        raise SpecError("session_ref must be a string task_id")
    nonce = obj.get("nonce")
    if nonce is not None and not isinstance(nonce, str):
        raise SpecError("nonce must be a string")
    return {"goal": goal, "workspace": ws, "mode": mode, "scope": scope,
            "verify": verify, "policy": policy, "timeout": timeout,
            "idempotency_key": ikey, "session_ref": sref, "nonce": nonce}


def fingerprint(spec):
    canon = json.dumps({k: spec.get(k) for k in sorted(spec)}, sort_keys=True,
                       ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


def new_request_id():
    return "r-" + secrets.token_hex(8)


def save_request(request_id, spec, extra=None):
    if not REQ_RE.match(request_id):
        raise ValueError("bad request_id")
    rec = {"request_id": request_id, "spec": spec, "ts": time.time(),
           "fingerprint": fingerprint(spec)}
    if extra:
        rec.update(extra)
    fd, tmp = tempfile.mkstemp(dir=REQUESTS, prefix="tmp-")
    with os.fdopen(fd, "w") as f:
        json.dump(rec, f, ensure_ascii=True)
    os.chmod(tmp, 0o600)
    os.replace(tmp, os.path.join(REQUESTS, request_id + ".json"))
    return rec


def attach_task(request_id, task_id):
    if not (REQ_RE.match(request_id or "") and TASK_RE.match(task_id or "")):
        raise ValueError("bad id")
    p = os.path.join(REQUESTS, request_id + ".json")
    rec = json.load(open(p))
    rec["task_id"] = task_id
    fd, tmp = tempfile.mkstemp(dir=REQUESTS, prefix="tmp-")
    with os.fdopen(fd, "w") as f:
        json.dump(rec, f, ensure_ascii=True)
    os.chmod(tmp, 0o600)
    os.replace(tmp, p)


def load_request(request_id):
    if not REQ_RE.match(request_id or ""):
        raise ValueError("bad request_id")
    return json.load(open(os.path.join(REQUESTS, request_id + ".json")))


def find_request_by_task(task_id):
    if not TASK_RE.match(task_id or ""):
        return None
    for f in os.listdir(REQUESTS):
        if not f.endswith(".json"):
            continue
        try:
            rec = json.load(open(os.path.join(REQUESTS, f)))
        except Exception:
            continue
        if rec.get("task_id") == task_id:
            return rec
    return None


def result_path(task_id):
    if not TASK_RE.match(task_id or ""):
        raise ValueError(f"invalid task_id: {task_id!r}")
    return os.path.join(RESULTS, task_id + ".json")


def idempotency_claim(key, fp):
    """Returns (status, record): status in {"new","duplicate","conflict"}.
    duplicate -> record has request_id of the ORIGINAL request."""
    if not key:
        return "new", None
    h = hashlib.sha256(key.encode()).hexdigest()[:24]
    lockp = os.path.join(LEDGERS, h + ".lock")
    recp = os.path.join(LEDGERS, h + ".json")
    with open(lockp, "w") as lockf:
        fcntl.flock(lockf, fcntl.LOCK_EX)
        try:
            if os.path.exists(recp):
                rec = json.load(open(recp))
                if not hmac.compare_digest(rec.get("fingerprint", ""), fp):
                    return "conflict", rec
                if rec.get("state") in ("claimed", "submitting") or (
                        rec.get("state") == "attached" and not rec.get("task_id")):
                    return "indeterminate", rec   # previous owner died mid-flight; NEVER auto-rerun
                return "duplicate", rec
            rid = new_request_id()
            rec = {"key": key, "fingerprint": fp, "request_id": rid,
                   "task_id": None, "state": "claimed"}
            fd, tmp = tempfile.mkstemp(dir=LEDGERS, prefix="tmp-")
            with os.fdopen(fd, "w") as f:
                json.dump(rec, f)
            os.chmod(tmp, 0o600)
            os.replace(tmp, recp)
            return "new", rec
        finally:
            fcntl.flock(lockf, fcntl.LOCK_UN)


def idempotency_release(key):
    """Drop the ledger record so the same key can be legally resubmitted.

    ONLY for tasks that never started executing (e.g. rejected with
    workspace_busy) — a released key loses its once-only guarantee.
    """
    if not key:
        return
    h = hashlib.sha256(key.encode()).hexdigest()[:24]
    for suffix in (".json", ".lock"):
        try:
            os.remove(os.path.join(LEDGERS, h + suffix))
        except FileNotFoundError:
            pass


def idempotency_state(key, fp, state, task_id=None, request_id=None):
    """Transition ledger state: claimed -> submitting -> attached -> terminal."""
    if not key:
        return
    h = hashlib.sha256(key.encode()).hexdigest()[:24]
    recp = os.path.join(LEDGERS, h + ".json")
    lockp = os.path.join(LEDGERS, h + ".lock")
    with open(lockp, "w") as lockf:
        fcntl.flock(lockf, fcntl.LOCK_EX)
        try:
            rec = json.load(open(recp)) if os.path.exists(recp) else {}
            if request_id and rec.get("request_id") != request_id:
                return
            rec["state"] = state
            if task_id is not None:
                rec["task_id"] = task_id
            fd, tmp = tempfile.mkstemp(dir=LEDGERS, prefix="tmp-")
            with os.fdopen(fd, "w") as f:
                json.dump(rec, f)
            os.chmod(tmp, 0o600)
            os.replace(tmp, recp)
        finally:
            fcntl.flock(lockf, fcntl.LOCK_UN)


def idempotency_attach(key, fp, request_id, task_id):
    idempotency_state(key, fp, "attached", task_id=task_id, request_id=request_id)


def idempotency_terminal(key, fp):
    idempotency_state(key, fp, "terminal")


def set_owner(task_id, pane_id):
    """task -> owner pane mapping so cancels can be routed to the right pane."""
    if not TASK_RE.match(task_id or "") or not pane_id:
        return
    fd, tmp = tempfile.mkstemp(dir=OWNERS, prefix="tmp-")
    with os.fdopen(fd, "w") as f:
        json.dump({"task_id": task_id, "pane_id": pane_id, "ts": time.time()}, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, os.path.join(OWNERS, task_id + ".json"))


def owner_of(task_id):
    if not TASK_RE.match(task_id or ""):
        return None
    p = os.path.join(OWNERS, task_id + ".json")
    if not os.path.exists(p):
        return None
    try:
        return json.load(open(p)).get("pane_id")
    except Exception:
        return None


# ---------- receipts: fold-proof submit acknowledgements ----------
# Pane markers can be soft-wrapped and lost to a narrow viewport; a receipt file
# is the durable ack a `send` client polls. Keyed by nonce (client-generated).

def receipt_path(nonce):
    if not NONCE_RE.match(nonce or ""):
        raise ValueError(f"invalid nonce: {nonce!r}")
    return os.path.join(RECEIPTS, "n-" + hashlib.sha256(nonce.encode()).hexdigest()[:24] + ".json")


def save_receipt(nonce, payload):
    try:
        p = receipt_path(nonce)
    except ValueError:
        return
    rec = dict(payload)
    rec["nonce"] = nonce
    rec["ts"] = time.time()
    fd, tmp = tempfile.mkstemp(dir=RECEIPTS, prefix="tmp-")
    with os.fdopen(fd, "w") as f:
        json.dump(rec, f, ensure_ascii=True)
    os.chmod(tmp, 0o600)
    os.replace(tmp, p)


def load_receipt(nonce):
    try:
        p = receipt_path(nonce)
    except ValueError:
        return None
    if not os.path.exists(p):
        return None
    try:
        return json.load(open(p))
    except Exception:
        return None
