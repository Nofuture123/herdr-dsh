#!/usr/bin/env python3
"""DshKernel tests with a fake dsh binary — no network, no real harness."""
import importlib.util, json, os, sys, tempfile, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m

tmp = tempfile.mkdtemp()
os.environ["DSH_BRIDGE_DIR"] = os.path.join(tmp, "bridge")
dk = load("dsh_kernel", os.path.join(ROOT, "scripts", "dsh_kernel.py"))

FAKE = f'''#!/usr/bin/env python3
import sys
args = sys.argv[1:]
if args and args[0] == "--profile":
    sys.stderr.write("reasoning: thinking about it\\nreasoning: done thinking\\n")
    print("final answer: 42")
    sys.exit(0)
sys.exit(3)
'''
WS = tempfile.mkdtemp()
BIN = os.path.join(tmp, "bin")
os.makedirs(BIN, exist_ok=True)
FAKE_PATH = os.path.join(BIN, "dsh")
open(FAKE_PATH, "w").write(FAKE)
os.chmod(FAKE_PATH, 0o755)


def kernel():
    os.environ["DSH_BIN"] = f"{FAKE_PATH} --profile"
    return dk.DshKernel()


class TestDshKernel(unittest.TestCase):
    def setUp(self):
        self._old_bin = os.environ.get("DSH_BIN")
        os.environ["DSH_BIN"] = f"{FAKE_PATH} --profile"

    def tearDown(self):
        if self._old_bin is None:
            os.environ.pop("DSH_BIN", None)
        else:
            os.environ["DSH_BIN"] = self._old_bin

    def test_submit_wait_success_and_summary(self):
        k = dk.DshKernel()
        snap = k.submit("dsh", "answer the ultimate question", WS,
                        mode="yolo", verify=["true"], timeout_sec=30)
        tid = snap["task_id"]
        self.assertTrue(tid.startswith("t-"))
        final = k.wait(tid, timeout_sec=15)
        self.assertEqual(final["status"], "succeeded")
        self.assertIn("final answer: 42", final["result"]["worker_summary"])
        self.assertTrue(final["result"]["verify"][0]["ok"])
        self.assertTrue(os.path.exists(os.path.join(
            os.environ["DSH_BRIDGE_DIR"], "logs", tid, "stream.log")))
        self.assertTrue(os.path.exists(final["result"]["stream_log"]))

    def test_verify_failure_marks_failed(self):
        k = dk.DshKernel()
        snap = k.submit("dsh", "x", WS, mode="yolo",
                        verify=["exit 1"], timeout_sec=30)
        final = k.wait(snap["task_id"], timeout_sec=15)
        self.assertEqual(final["status"], "failed")
        self.assertFalse(final["result"]["verify"][0]["ok"])

    def test_unknown_task_snapshot(self):
        self.assertEqual(dk.DshKernel().snapshot("t-000000000000")["status"],
                         "unknown")


if __name__ == "__main__":
    unittest.main(verbosity=2)
