#!/usr/bin/env python3
"""Unit tests for the request broker (no ZCode, no network, no herdr)."""
import importlib.util, json, os, sys, tempfile, unittest
from concurrent.futures import ThreadPoolExecutor

spec = importlib.util.spec_from_file_location("broker", os.path.join(
    os.path.dirname(__file__), "..", "scripts", "broker.py"))
broker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(broker)

# redirect broker storage to a temp dir
tmp = tempfile.mkdtemp()
broker.REQUESTS = os.path.join(tmp, "requests")
broker.LEDGERS = os.path.join(tmp, "ledgers")
broker.RESULTS = os.path.join(tmp, "results")
broker.init_dirs()

WS = tempfile.mkdtemp()
GOOD = {"goal": "do x", "workspace": WS, "mode": "yolo", "verify": "true"}


class TestNormalize(unittest.TestCase):
    def test_ok(self):
        s = broker.normalize_spec(GOOD, WS)
        self.assertEqual(s["goal"], "do x")
        self.assertEqual(s["mode"], "yolo")

    def test_fail_closed_bad_json_shape(self):
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec("just text", WS)          # plain text must NOT pass as task
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec({"nogoal": 1}, WS)
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec({"goal": ""}, WS)
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec({"goal": "x", "evil": 1}, WS)   # unknown field
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec({"goal": "x", "workspace": "/no/such/dir"}, WS)
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec({"goal": "x", "mode": "sudo"}, WS)
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec({"goal": "x", "timeout": 10 ** 6}, WS)

    def test_verify_required_for_write_modes(self):
        with self.assertRaises(broker.SpecError):
            broker.normalize_spec({"goal": "x", "mode": "edit"}, WS, require_verify=True)
        s = broker.normalize_spec({"goal": "x", "mode": "edit"}, WS, require_verify=True) \
            if False else broker.normalize_spec({"goal": "x", "mode": "plan"}, WS, require_verify=True)
        self.assertEqual(s["verify"], [])

    def test_verify_string_coerces_to_list(self):
        s = broker.normalize_spec({"goal": "x", "verify": "pytest -q"}, WS)
        self.assertEqual(s["verify"], ["pytest -q"])


class TestIds(unittest.TestCase):
    def test_task_id_validation(self):
        self.assertTrue(broker.TASK_RE.match("t-0abc123def"))
        self.assertFalse(broker.TASK_RE.match("../../etc"))
        self.assertFalse(broker.TASK_RE.match("t-../../x"))
        with self.assertRaises(ValueError):
            broker.result_path("../../secret")

    def test_request_roundtrip(self):
        rid = broker.new_request_id()
        broker.save_request(rid, GOOD)
        rec = broker.load_request(rid)
        self.assertEqual(rec["spec"]["goal"], "do x")
        broker.attach_task(rid, "t-abc123")
        self.assertEqual(broker.find_request_by_task("t-abc123")["request_id"], rid)


class TestIdempotency(unittest.TestCase):
    def test_same_key_same_fp_duplicate(self):
        st1, r1 = broker.idempotency_claim("k1", "fpA")
        self.assertEqual(st1, "new")
        broker.idempotency_state("k1", "fpA", "terminal", request_id=r1["request_id"])
        st2, r2 = broker.idempotency_claim("k1", "fpA")
        self.assertEqual(st2, "duplicate")
        self.assertEqual(r1["request_id"], r2["request_id"])

    def test_indeterminate_on_stuck_claim(self):
        broker.idempotency_claim("k1i", "fpA")
        st, _ = broker.idempotency_claim("k1i", "fpA")   # still 'claimed' = crashed mid-flight
        self.assertEqual(st, "indeterminate")

    def test_same_key_diff_fp_conflict(self):
        broker.idempotency_claim("k2", "fpX")
        st, _ = broker.idempotency_claim("k2", "fpY")
        self.assertEqual(st, "conflict")

    def test_no_key_always_new(self):
        st, _ = broker.idempotency_claim(None, "fpZ")
        self.assertEqual(st, "new")

    def test_release_allows_legal_resubmit(self):
        st, rec = broker.idempotency_claim("krel", "fpA")
        self.assertEqual(st, "new")
        broker.idempotency_state("krel", "fpA", "attached", task_id="t-abc123",
                                 request_id=rec["request_id"])
        st2, _ = broker.idempotency_claim("krel", "fpA")   # attached: blocked as duplicate
        self.assertEqual(st2, "duplicate")
        broker.idempotency_release("krel")                 # task never ran (workspace_busy)
        st3, _ = broker.idempotency_claim("krel", "fpA")
        self.assertEqual(st3, "new")


class TestWorkspaceCanonical(unittest.TestCase):
    def test_realpath_unifies_alias_and_real(self):
        real = tempfile.mkdtemp()
        alias = real + ".alias"
        os.symlink(real, alias)
        try:
            s1 = broker.normalize_spec({"goal": "x", "workspace": alias}, alias)
            s2 = broker.normalize_spec({"goal": "x", "workspace": real}, real)
            self.assertEqual(s1["workspace"], os.path.realpath(real))
            self.assertEqual(s1["workspace"], s2["workspace"])
        finally:
            os.remove(alias)

    def test_concurrent_same_key_never_reruns(self):
        def claim(i):
            return broker.idempotency_claim("race2", "fpR")[0]
        with ThreadPoolExecutor(8) as ex:
            results = list(ex.map(claim, range(16)))
        self.assertEqual(results.count("new"), 1)        # exactly one winner
        self.assertEqual(results.count("indeterminate"), 15)  # everyone else must NOT rerun

    def test_attach(self):
        st, rec = broker.idempotency_claim("k3", "fp3")
        broker.idempotency_attach("k3", "fp3", rec["request_id"], "t-abc456")
        st, rec = broker.idempotency_claim("k3", "fp3")
        self.assertEqual(st, "duplicate"); self.assertEqual(rec["task_id"], "t-abc456")


class TestPerms(unittest.TestCase):
    def test_dirs_and_files_secure(self):
        broker.init_dirs()
        rid = broker.new_request_id()
        broker.save_request(rid, GOOD)
        for d in (broker.REQUESTS, broker.LEDGERS, broker.RESULTS):
            self.assertEqual(oct(os.stat(d).st_mode & 0o777), "0o700")
        f = os.path.join(broker.REQUESTS, rid + ".json")
        self.assertEqual(oct(os.stat(f).st_mode & 0o777), "0o600")


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestReceipts(unittest.TestCase):
    def test_roundtrip_and_bad_nonce(self):
        broker.save_receipt("n-test1", {"ok": True, "task_id": "t-abc123"})
        r = broker.load_receipt("n-test1")
        self.assertTrue(r["ok"])
        self.assertEqual(r["task_id"], "t-abc123")
        self.assertIn("nonce", r)
        self.assertIsNone(broker.load_receipt("../evil"))
        self.assertIsNone(broker.load_receipt("never-sent"))
