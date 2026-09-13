#!/bin/sh
# One-command verification gate for zcode-bridge.
set -e
cd "$(dirname "$0")/.."
echo "== shell syntax =="; sh -n scripts/*.sh bin/*.sh
echo "== manifest =="; python3 -c "import tomllib; tomllib.loads(open('herdr-plugin.toml').read())"
python3 - <<'PY'
import tomllib
m = tomllib.loads(open("herdr-plugin.toml").read())
assert any(p["id"] == "executor" for p in m.get("panes", [])), "executor pane missing"
assert any(a["id"] == "open-here" for a in m.get("actions", [])), "open-here action missing"
print("manifest entries ok")
PY
echo "== python compile =="; python3 -m py_compile scripts/*.py
echo "== unit tests =="
if python3 -m unittest discover -s tests -v > /tmp/dshcli-ut.log 2>&1; then
  tail -3 /tmp/dshcli-ut.log
else
  rc=$?
  tail -25 /tmp/dshcli-ut.log
  echo "UNIT TESTS FAILED ($rc)"
  exit "$rc"
fi
echo "== broker live dirs =="; python3 -c "
import importlib.util
spec = importlib.util.spec_from_file_location('b','scripts/broker.py'); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
m.init_dirs(); print('storage ok:', m.BASE)"
echo "ALL GATES PASSED"
